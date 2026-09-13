// place_order.js — покупки лицензий через POST /orders (сага PlaceOrderWorkflow
// на Temporal, docs/adr/0004-*, docs/adr/0018-*).
//
// ЧТО ДОКАЗЫВАЕТ СЦЕНАРИЙ: два разных инварианта идемпотентности под
// нагрузкой, а не только "воркфлоу когда-нибудь завершается":
//
//   1. ПОСЛЕДОВАТЕЛЬНЫЙ повтор с уже ЗАВЕРШЁННЫМ ключом (REPEAT_RATE доля
//      итераций) обязан вернуть 200 с ТЕМ ЖЕ order.id и НЕ порождать вторую
//      сагу — см. app.OrderService.PlaceOrder: ports.IdempotencyDone отдаёт
//      сохранённый ответ, не трогая ни BuyerID/ListingID, ни Temporal.
//   2. ПАРАЛЛЕЛЬНЫЙ повтор с ключом, который ещё В ПРОЦЕССЕ (два запроса
//      с одним Idempotency-Key почти одновременно), обязан породить РОВНО
//      ОДИН заказ: либо один 200 и один 409 (ports.IdempotencyInProgress →
//      domain.ErrIdempotencyInProgress → codes.Aborted → HTTP 409), либо,
//      если оба успели прочитать "ещё не начато" до записи Begin (гонка на
//      границе, которую и проверяет нагрузка, а не единичный curl), — два
//      200, но с ОДИНАКОВЫМ order.id. Второй заказ с ДРУГИМ id для того же
//      ключа — это нарушение контракта идемпотентности, и именно это
//      находит на нагрузке, а не в единичном ручном запросе (make order-place).
//
// idempotency_violations ниже — метрика ИМЕННО этого нарушения; порог
// count==0 — жёсткий, а не "меньше процента": один дублирующийся платёж
// не бывает "почти нормой".
//
// Сага асинхронна (POST /orders возвращает status=pending сразу же, см.
// package doc internal/adapters/http в order-service) — поэтому для доли
// заказов сценарий ещё и опрашивает GET /orders/{id}, как это делает
// make demo-saga-ok, и мерит время до терминального статуса.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:58080';
const BUYER_POOL = parseInt(__ENV.BUYER_POOL || '20', 10);
// Доля итераций, которые вместо нового заказа ПОВТОРЯЮТ предыдущий ключ
// этого же VU — намеренно только предыдущий (не случайный чужой), потому
// что для повтора нужен ключ, который этот же VU уже когда-то использовал.
const REPEAT_RATE = parseFloat(__ENV.REPEAT_RATE || '0.2');
// Доля НОВЫХ (не повторных) итераций, которые дополнительно стреляют
// ПАРАЛЛЕЛЬНЫМ дублем того же запроса тем же ключом (http.batch — оба
// запроса уходят одновременно, а не один за другим) — см. пункт 2 выше.
const CONCURRENT_REPEAT_RATE = parseFloat(__ENV.CONCURRENT_REPEAT_RATE || '0.1');
const POLL_MAX = parseInt(__ENV.POLL_MAX || '5', 10);
const POLL_INTERVAL_S = parseFloat(__ENV.POLL_INTERVAL_S || '1');

// POST /orders отвечает 200 (не 201 — это не "создан ресурс", а "принято
// в обработку", см. handler.go), 404 — карточка не найдена, 400 —
// FailedPrecondition (карточка не published) или пустой ключ, 409 —
// параллельный повтор в процессе. Все они ЗНАЕМЫЕ исходы; настоящая
// проблема — 5xx/таймаут, и только они остаются в http_req_failed.
http.setResponseCallback(http.expectedStatuses(200, 400, 404, 409));

const idempotencyViolations = new Counter('idempotency_violations');
const idempotencyConflicts409 = new Counter('idempotency_conflicts_409');
const idempotentDuplicateReturned = new Counter('idempotent_duplicate_returned');
const ordersPlaced = new Counter('orders_placed');
const listingUnavailable = new Counter('listing_unavailable');
const sagaTimeToTerminal = new Trend('saga_time_to_terminal_ms', true);
const sagaFailedRate = new Rate('saga_failed_rate');
const unexpectedErrors = new Rate('unexpected_errors');

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    place_order: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '15s', target: 10 },
        { duration: '45s', target: 10 },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '10s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.02'],
    unexpected_errors: ['rate<0.02'],
    // Инвариант идемпотентности — см. заголовок файла. Это САМ смысл теста.
    idempotency_violations: ['count==0'],
    // POST /orders не ждёт сагу (см. package doc order/internal/adapters/http)
    // — он обязан отвечать быстро вне зависимости от того, как долго
    // разбирается Temporal.
    'http_req_duration{name:place_order}': ['p(95)<500', 'p(99)<1000'],
  },
};

// setup() — пул реально опубликованных карточек. Без него сценарий бы либо
// падал на 404 у каждого запроса, либо (что хуже) молча бил в одну и ту же
// карточку, не проверяя ничего, кроме частного случая одного listing_id.
export function setup() {
  const res = http.get(`${BASE_URL}/catalog/listings?limit=100`);
  if (res.status !== 200) {
    console.warn(`place_order: не удалось прочитать каталог (status=${res.status})`);
    return { listingIDs: [] };
  }
  const body = res.json();
  const listingIDs = (body.listings || [])
    .filter((l) => l.status === 'published')
    .map((l) => l.id);
  if (listingIDs.length === 0) {
    console.warn('place_order: нет опубликованных карточек — сначала make demo-media && make demo-catalog');
  }
  return { listingIDs };
}

// lastKeyPerVU — состояние per-VU: k6 запускает отдельный JS-рантайм на
// каждый VU, поэтому module-scope переменная безопасно живёт весь прогон
// ОДНОГО VU и не видна другим — ничего похожего на SharedArray здесь не
// нужно, "разделять" между итерациями нужно только с самим собой.
let lastKey = null;
let lastOrderID = null;

function newIdempotencyKey() {
  // Префикс k6 — чтобы в базе/логах сразу отличать нагрузочный трафик
  // от make order-place и ручных проверок.
  return `k6-place-order-${__VU}-${__ITER}-${Date.now()}`;
}

function placeOrder(buyerID, listingID, key) {
  return http.post(
    `${BASE_URL}/orders`,
    JSON.stringify({ buyer_id: buyerID, listing_id: listingID }),
    {
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
      tags: { name: 'place_order' },
    },
  );
}

function pollOrderStatus(orderID) {
  let status = 'pending';
  for (let i = 0; i < POLL_MAX; i++) {
    sleep(POLL_INTERVAL_S);
    const res = http.get(`${BASE_URL}/orders/${orderID}`, { tags: { name: 'get_order' } });
    if (res.status !== 200) break;
    status = res.json('status');
    if (status === 'completed' || status === 'failed') break;
  }
  return status;
}

export default function (data) {
  const buyerID = (__VU % BUYER_POOL) + 1;

  // ── Повтор с уже отработавшим ключом (последовательная идемпотентность) ──
  if (lastKey && Math.random() < REPEAT_RATE) {
    const res = placeOrder(buyerID, /* listingID неважен, ключ уже занят */ 'ignored', lastKey);
    const ok = check(res, {
      'повтор: 200 с тем же order.id': (r) => r.status === 200 && r_id(r) === lastOrderID,
    });
    if (res.status === 200 && r_id(res) !== lastOrderID) {
      idempotencyViolations.add(1);
    } else if (ok) {
      idempotentDuplicateReturned.add(1);
    } else if (!ok && res.status !== 200) {
      unexpectedErrors.add(1);
    }
    sleep(0.1);
    return;
  }

  if (!data.listingIDs || data.listingIDs.length === 0) {
    sleep(0.5);
    return;
  }

  const listingID = data.listingIDs[Math.floor(Math.random() * data.listingIDs.length)];
  const key = newIdempotencyKey();

  // ── Параллельный повтор ОДНИМ И ТЕМ ЖЕ ключом (гонка на Begin) ──────────
  if (Math.random() < CONCURRENT_REPEAT_RATE) {
    const responses = http.batch([
      ['POST', `${BASE_URL}/orders`, JSON.stringify({ buyer_id: buyerID, listing_id: listingID }),
        { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, tags: { name: 'place_order_concurrent' } }],
      ['POST', `${BASE_URL}/orders`, JSON.stringify({ buyer_id: buyerID, listing_id: listingID }),
        { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, tags: { name: 'place_order_concurrent' } }],
    ]);

    const ids = responses.filter((r) => r.status === 200).map((r) => r.json('id'));
    const conflicts = responses.filter((r) => r.status === 409).length;
    idempotencyConflicts409.add(conflicts);

    const uniqueIDs = new Set(ids);
    if (uniqueIDs.size > 1) {
      // Оба ответили 200, но с РАЗНЫМИ id для одного ключа — это и есть
      // нарушение, ради обнаружения которого написана параллельная ветка.
      idempotencyViolations.add(1);
    } else if (ids.length > 0) {
      ordersPlaced.add(1);
      lastKey = key;
      lastOrderID = [...uniqueIDs][0];
    } else if (conflicts < 2) {
      // Ни одного 200 и не оба 409 — например, 404/400 (карточка исчезла
      // между setup() и этой итерацией) — не нарушение идемпотентности,
      // но и не то, что тест должен молча проглотить.
      listingUnavailable.add(1);
    }
    sleep(0.1);
    return;
  }

  // ── Обычная покупка ──────────────────────────────────────────────────────
  const res = placeOrder(buyerID, listingID, key);
  if (res.status === 200) {
    const orderID = res.json('id');
    ordersPlaced.add(1);
    lastKey = key;
    lastOrderID = orderID;

    // Не на каждой итерации: опрос GET /orders/{id} — это ДОПОЛНИТЕЛЬНАЯ
    // нагрузка поверх основного потока покупок, и полный опрос каждой
        // saga задрал бы RPS теста без пользы для инварианта идемпотентности.
    if (Math.random() < 0.3) {
      const start = Date.now();
      const finalStatus = pollOrderStatus(orderID);
      sagaTimeToTerminal.add(Date.now() - start);
      sagaFailedRate.add(finalStatus === 'failed' ? 1 : 0);
    }
  } else if (res.status === 404) {
    listingUnavailable.add(1);
  } else if (res.status !== 400 && res.status !== 409) {
    unexpectedErrors.add(1);
  }

  sleep(0.2);
}

// r_id — крошечный помощник: res.json('id') на не-200 ответе (например,
// 409 без тела order) не должен уронить скрипт исключением.
function r_id(res) {
  try {
    return res.json('id');
  } catch (e) {
    return undefined;
  }
}

export function handleSummary(data) {
  const m = data.metrics;
  const get = (name, stat) => (m[name] && m[name].values && m[name].values[stat]) || 0;
  const summary = {
    scenario: 'place_order',
    generated_at: new Date().toISOString(),
    orders_placed: get('orders_placed', 'count'),
    idempotency_violations: get('idempotency_violations', 'count'),
    idempotency_conflicts_409: get('idempotency_conflicts_409', 'count'),
    idempotent_duplicate_returned: get('idempotent_duplicate_returned', 'count'),
    saga_failed_rate: get('saga_failed_rate', 'rate'),
    saga_time_to_terminal_p99_ms: get('saga_time_to_terminal_ms', 'p(99)'),
    place_order_p50_ms: get('http_req_duration', 'med'),
    place_order_p95_ms: get('http_req_duration', 'p(95)'),
    place_order_p99_ms: get('http_req_duration', 'p(99)'),
  };
  return {
    stdout: `\n${JSON.stringify(summary, null, 2)}\n`,
    'summary-place-order.json': JSON.stringify(summary, null, 2),
  };
}

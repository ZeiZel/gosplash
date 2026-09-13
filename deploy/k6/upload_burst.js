// upload_burst.js — всплеск загрузок в media (POST /media/upload).
//
// ЧТО ДОКАЗЫВАЕТ СЦЕНАРИЙ: rate limiter (pkg/redisx, token bucket, фаза 2.3)
// не даёт одному пользователю продавить media трафиком выше лимита из
// MEDIA_UPLOAD_RATE_CAPACITY/MEDIA_UPLOAD_RATE_REFILL_PER_SEC (.env.example:
// по умолчанию 10 токенов, пополнение 0.5/с). Пул пользователей НАМЕРЕННО
// маленький (USER_POOL, по умолчанию 5) и много меньше числа VU — иначе
// каждый виртуальный пользователь получил бы свой собственный бакет
// (лимит per-user, см. handler.go allowUpload: ключ "upload:<user_id>") и
// исчерпать лимит нагрузкой было бы нечем.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ (STYLE.md, §3): 429 с Retry-After — это ОЖИДАЕМЫЙ,
// ПРАВИЛЬНЫЙ ответ инфраструктуры под нагрузкой, а не отказ сервиса. Если
// считать 429 как http_req_failed, тест либо придётся зажимать порогом
// ошибок в 80%+ (бессмысленно — порог перестаёт быть сигналом регрессии),
// либо он будет красным всегда, хотя рейт-лимитер работает ИМЕННО так, как
// задумано. Поэтому 429 размечается через http.setResponseCallback как
// ОЖИДАЕМЫЙ статус (http_req_failed остаётся метрикой РЕАЛЬНЫХ сбоев —
// 5xx, обрыв соединения), а факт срабатывания лимитера считается отдельной
// бизнес-метрикой rate_limited. УСПЕХ этого теста — когда rate_limited > 0
// И http_req_failed остаётся почти нулевым: это значит, что сервис не упал
// под всплеском, а вежливо попросил подождать именно тех, кто превысил
// свою квоту.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import encoding from 'k6/encoding';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:58080';

// Пул пользователей меньше, чем ожидаемое число одновременных VU — так
// несколько VU неизбежно делят один и тот же токен-бакет в Redis и
// вычерпывают его быстрее, чем он пополняется (0.5 токена/с ~ 1 токен
// на пользователя каждые 2 секунды).
const USER_POOL = parseInt(__ENV.USER_POOL || '5', 10);

// 1×1 прозрачный PNG — тот же файл, что `make demo-file` кладёт в /tmp
// (см. корневой Makefile). Не нужен реальный файл на диске: k6 гоняет это
// как многократно переиспользуемый ArrayBuffer, upload_burst не измеряет
// скорость передачи байт, только поведение лимитера и латентность ответа.
const TINY_PNG = encoding.b64decode(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==',
  'std',
);

// Успешный ответ — 201; 429 — тоже ожидаемый (см. заголовок файла).
// Всё остальное (500, обрыв соединения, таймаут) попадает в http_req_failed
// и это уже настоящая проблема сервиса под нагрузкой.
http.setResponseCallback(http.expectedStatuses(201, 429));

const rateLimited = new Counter('rate_limited');
const rateLimitedNoRetryAfter = new Counter('rate_limited_missing_retry_after');
const uploaded = new Counter('uploads_succeeded');
const retryAfterSeconds = new Trend('retry_after_seconds');
const unexpectedStatus = new Rate('unexpected_status');

export const options = {
  // p(99) явно в summaryTrendStats: по умолчанию k6 считает только до p(95)
  // в текстовой сводке, а docs/perf/README.md требует именно p50/p95/p99.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    upload_burst: {
      executor: 'ramping-arrival-rate',
      // arrival-rate, а не ramping-vus: нас интересует ИМЕННО скорость
      // запросов в секунду (это то, от чего зависит наполнение бакета),
      // а не число параллельных "клиентов" само по себе.
      startRate: 5,
      timeUnit: '1s',
      preAllocatedVUs: 30,
      maxVUs: 150,
      stages: [
        { target: 10, duration: '10s' },  // разгон
        { target: 80, duration: '5s' },   // сам всплеск (burst)
        { target: 80, duration: '25s' },  // держим давление — лимитер должен устоять
        { target: 0, duration: '10s' },   // спад
      ],
    },
  },
  thresholds: {
    // Реальные сбои сервиса (не 429, не 201) — их не должно быть вовсе.
    http_req_failed: ['rate<0.02'],
    unexpected_status: ['rate<0.02'],
    // Сам факт срабатывания лимитера — то, что этот тест обязан доказать.
    // Если rate_limited == 0, тест НИЧЕГО не проверил (либо Redis лежит
    // и лимитер работает fail-open, либо VU/скорость занижены).
    rate_limited: ['count>20'],
    // У КАЖДОГО 429 обязан быть Retry-After — иначе клиенту нечем
    // руководствоваться при повторе (see handler.go: math.Ceil(retryAfter)).
    rate_limited_missing_retry_after: ['count==0'],
    // Успешные загрузки не должны деградировать по времени ответа —
    // 429 отвечает мгновенно (без похода в PostgreSQL/S3), поэтому если
    // p95 успешных загрузок растёт, деградирует что-то ДРУГОЕ, не лимитер.
    'http_req_duration{status:201}': ['p(95)<2000'],
  },
};

export default function () {
  const userID = (__VU % USER_POOL) + 1;

  const res = http.post(
    `${BASE_URL}/media/upload`,
    {
      file: http.file(TINY_PNG, `burst-${__VU}-${__ITER}.png`, 'image/png'),
      title: `k6 upload burst vu=${__VU} iter=${__ITER}`,
    },
    {
      headers: { 'X-User-Id': String(userID) },
      tags: { name: 'media_upload' },
    },
  );

  if (res.status === 201) {
    uploaded.add(1);
    check(res, { 'upload: id в ответе': (r) => !!r.json('id') });
  } else if (res.status === 429) {
    // ЭТО УСПЕХ, А НЕ ОШИБКА — см. заголовок файла.
    rateLimited.add(1);
    const retryAfter = res.headers['Retry-After'] || res.headers['retry-after'];
    if (!retryAfter) {
      rateLimitedNoRetryAfter.add(1);
    } else {
      retryAfterSeconds.add(Number(retryAfter));
    }
    check(res, { '429: Retry-After присутствует': () => !!retryAfter });
  } else {
    // http.expectedStatuses выше не включает этот код — сюда попадают
    // только настоящие сбои (5xx и т.п.), unexpected_status их подсвечивает
    // отдельно от 429, чтобы не перепутать "лимитер сработал" с "упало".
    unexpectedStatus.add(1);
  }

  sleep(0.05);
}

// handleSummary — короткая сводка в консоль плюс JSON для docs/perf
// (см. deploy/k6/README.md и docs/perf/README.md про формат хранения).
export function handleSummary(data) {
  const m = data.metrics;
  const get = (name, stat) => (m[name] && m[name].values && m[name].values[stat]) || 0;
  const summary = {
    scenario: 'upload_burst',
    generated_at: new Date().toISOString(),
    requests_total: get('http_reqs', 'count'),
    uploads_succeeded: get('uploads_succeeded', 'count'),
    rate_limited_429: get('rate_limited', 'count'),
    rate_limited_missing_retry_after: get('rate_limited_missing_retry_after', 'count'),
    real_failures: get('http_req_failed', 'passes'),
    p50_ms: get('http_req_duration', 'med'),
    p95_ms: get('http_req_duration', 'p(95)'),
    p99_ms: get('http_req_duration', 'p(99)'),
  };
  return {
    stdout: `\n${JSON.stringify(summary, null, 2)}\n`,
    'summary-upload-burst.json': JSON.stringify(summary, null, 2),
  };
}

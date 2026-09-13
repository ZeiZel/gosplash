// catalog_read.js — чтение ленты (GET /catalog/listings) и карточек
// (GET /catalog/listings/{id}) под нагрузкой.
//
// ЧТО ДОКАЗЫВАЕТ СЦЕНАРИЙ: разницу p99 между "прогретым" и "холодным"
// кэшем каталога (pkg/redisx, cache-aside, фаза 2.2) — это буквально
// критерий готовности фазы 2 (docs/PLAN.md: demo-redis показывает разницу
// p99 с кэшем и без). Здесь тот же вопрос задаётся не одиночными curl'ами
// (make demo-redis), а под конкурентной нагрузкой, где имеет значение ещё
// и singleflight на промахе — без него параллельный "холодный" трафик по
// одному и тому же id устроил бы PostgreSQL "thundering herd" на каждый TTL.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ — как смоделировать "тепло" и "холодно" без доступа
// k6 к Redis (у k6 нет докер-сокета и он не может дёрнуть `make redis-flush`
// изнутри сценария): вместо сброса кэша меняется РАЗМЕР рабочего множества
// ключей, а не сам факт наличия кэша.
//
//   CACHE_MODE=warm — все VU колотят в ОДИН и тот же маленький набор карточек
//                     (HOT_POOL_SIZE, по умолчанию 3). После пары первых
//                     запросов (промах + прогрев) почти 100% попаданий —
//                     именно так выглядит устоявшийся кэш популярных карточек
//                     в проде (Парето: 20% карточек дают 80% просмотров).
//   CACHE_MODE=cold — каждый запрос идёт за СЛУЧАЙНОЙ карточкой из всего
//                     пула, полученного в setup(). Рабочее множество кэша
//                     ограничено (CACHE_TTL=10m в .env.example, а не
//                     бесконечный Redis), поэтому при достаточно широком
//                     пуле процент попаданий держится низким постоянно —
//                     эквивалент того, что происходит сразу после
//                     `make redis-flush` под непрерывным трафиком, а не
//                     только в первую секунду после сброса.
//
// Перед прогоном CACHE_MODE=cold имеет смысл один раз выполнить
// `make redis-flush`, чтобы первые же запросы гарантированно были
// промахами, а не случайно тёплыми от предыдущего прогона — но даже без
// этого широкий случайный пул быстро вытесняет прогретые ключи под нагрузкой.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:58080';
const MODE = (__ENV.CACHE_MODE || 'warm').toLowerCase(); // warm | cold
const HOT_POOL_SIZE = parseInt(__ENV.HOT_POOL_SIZE || '3', 10);
const FEED_PAGE_SIZE = parseInt(__ENV.FEED_PAGE_SIZE || '20', 10);

if (MODE !== 'warm' && MODE !== 'cold') {
  throw new Error(`CACHE_MODE должен быть "warm" или "cold", получено: ${MODE}`);
}

const feedDuration = new Trend('catalog_feed_duration', true);
const cardDuration = new Trend('catalog_card_duration', true);
const unexpectedErrors = new Rate('unexpected_errors');

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    catalog_read: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '15s', target: 20 },
        { duration: '1m', target: 20 },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    unexpected_errors: ['rate<0.01'],
    // Пороги РАЗНЫЕ для тёплого и холодного режима — тег cache_mode
    // проставляется на каждый запрос (см. default ниже). Числа — грубая
    // отправная точка для локального docker compose, а не SLA; первый
    // реальный прогон (docs/perf/README.md) их уточнит.
    'http_req_duration{cache_mode:warm}': ['p(95)<150', 'p(99)<300'],
    'http_req_duration{cache_mode:cold}': ['p(95)<600', 'p(99)<1200'],
  },
};

// setup() бежит один раз до стадий нагрузки — какие бы VU потом ни читали
// карточки, им нужен РЕАЛЬНЫЙ пул id, а не выдуманные значения (иначе
// cold-режим будет измерять не промахи кэша, а 404 от несуществующих id).
export function setup() {
  const res = http.get(`${BASE_URL}/catalog/listings?limit=100`);
  if (res.status !== 200) {
    console.warn(`catalog_read: не удалось прочитать ленту (status=${res.status}) — карточки читаться не будут`);
    return { ids: [] };
  }
  const body = res.json();
  const ids = (body.listings || []).map((l) => l.id).filter(Boolean);
  if (ids.length === 0) {
    console.warn('catalog_read: лента пуста — сначала make demo-media && make demo-catalog, иначе тест меряет только 404');
  }
  return { ids };
}

export default function (data) {
  // 1. Лента — первая страница одна и та же во всех режимах: в реальном
  // трафике САМАЯ частая карточка чтения, и она тёплая почти всегда,
  // независимо от cache_mode этого прогона (это отдельный, не варьируемый
  // сигнал: если p99 ленты внезапно растёт даже в MODE=warm — подозревать
  // не кэш, а сам PostgreSQL/реплику).
  const feedRes = http.get(`${BASE_URL}/catalog/listings?limit=${FEED_PAGE_SIZE}`, {
    tags: { cache_mode: MODE, endpoint: 'feed' },
  });
  feedDuration.add(feedRes.timings.duration);
  const feedOK = check(feedRes, { 'feed: 200': (r) => r.status === 200 });
  if (!feedOK) unexpectedErrors.add(1);

  // 2. Карточка — вот где режим и решает hit/miss (см. заголовок файла).
  if (data.ids && data.ids.length > 0) {
    const id =
      MODE === 'warm'
        ? data.ids[__VU % Math.min(HOT_POOL_SIZE, data.ids.length)]
        : data.ids[Math.floor(Math.random() * data.ids.length)];

    const cardRes = http.get(`${BASE_URL}/catalog/listings/${id}`, {
      tags: { cache_mode: MODE, endpoint: 'card' },
    });
    cardDuration.add(cardRes.timings.duration);
    const cardOK = check(cardRes, { 'card: 200': (r) => r.status === 200 });
    if (!cardOK) unexpectedErrors.add(1);
  }

  sleep(0.2);
}

// handleSummary — JSON рядом с человекочитаемым выводом, чтобы p50/p95/p99
// можно было прямо вставить в docs/perf/<дата>-catalog-read.md, не пересчитывая
// их руками из сырого вывода k6 (см. deploy/k6/README.md).
export function handleSummary(data) {
  const m = data.metrics;
  const get = (name, stat) => (m[name] && m[name].values && m[name].values[stat]) || 0;
  const summary = {
    scenario: 'catalog_read',
    cache_mode: MODE,
    generated_at: new Date().toISOString(),
    requests_total: get('http_reqs', 'count'),
    rps: get('http_reqs', 'rate'),
    feed_p99_ms: get('catalog_feed_duration', 'p(99)'),
    card_p99_ms: get('catalog_card_duration', 'p(99)'),
    overall_p50_ms: get('http_req_duration', 'med'),
    overall_p95_ms: get('http_req_duration', 'p(95)'),
    overall_p99_ms: get('http_req_duration', 'p(99)'),
    unexpected_error_rate: get('unexpected_errors', 'rate'),
  };
  return {
    stdout: `\n${JSON.stringify(summary, null, 2)}\n`,
    [`summary-catalog-read-${MODE}.json`]: JSON.stringify(summary, null, 2),
  };
}

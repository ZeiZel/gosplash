# k6 — нагрузочные сценарии (фаза 6)

Три сценария, каждый доказывает конкретный инвариант, а не просто "сервис
не упал под нагрузкой":

| Файл | Что доказывает | Критерий готовности |
|---|---|---|
| `upload_burst.js` | rate limiter media отдаёт `429` + `Retry-After`, а не падает | фаза 2, п. 2.3 |
| `catalog_read.js` | разница p99 между тёплым и холодным кэшем каталога | фаза 2 (`docs/PLAN.md`: «готово, когда `demo-redis` показывает разницу p99») |
| `place_order.js` | идемпотентность `POST /orders` не ломается под конкурентной нагрузкой | фаза 3, ADR 0018 |

Подробное обоснование каждого сценария — в комментарии-шапке самого файла
(`docs/STYLE.md`: комментарии объясняют «почему», а не «что»).

## Предпосылки

Нужен поднятый стенд:

```bash
make up
make migrate
make run-media      # отдельный терминал
make run-catalog    # отдельный терминал
make run-wallet     # отдельный терминал — только для place_order.js
make run-order      # отдельный терминал — только для place_order.js
make run-worker     # отдельный терминал — только для place_order.js
```

Для `catalog_read.js` и `place_order.js` в каталоге должны быть опубликованные
карточки — `make demo-media && make demo-catalog` (или несколько раз
`make upload`), иначе сценарий честно предупредит `console.warn` и не будет
изображать нагрузку на пустом месте.

## Запуск

### Через `make` (docker run grafana/k6, ничего локально ставить не надо)

```bash
make k6-upload                      # upload_burst.js
make k6-catalog MODE=warm           # catalog_read.js, прогретый кэш
make k6-catalog MODE=cold           # catalog_read.js, холодный кэш
make k6-order                       # place_order.js
```

Цели в `mk/load.mk` дергают `docker run --rm -i grafana/k6:latest run` с
примонтированным `deploy/k6` и `BASE_URL=http://host.docker.internal:58080` —
тем же приёмом, каким `deploy/nginx/gateway.conf` достаёт до сервисов на
хосте (см. комментарий в `deploy/compose/docker-compose.gateway.yml`). `--add-host` в
команде нужен, чтобы это работало не только на Docker Desktop
(macOS/Windows), но и на Linux, где `host.docker.internal` без него не
резолвится.

### Локальный k6-бинарь (если уже стоит `brew install k6` / есть в PATH)

```bash
k6 run deploy/k6/upload_burst.js
k6 run -e CACHE_MODE=cold deploy/k6/catalog_read.js
k6 run -e REPEAT_RATE=0.3 -e CONCURRENT_REPEAT_RATE=0.15 deploy/k6/place_order.js
```

По умолчанию все три сценария бьют в `http://localhost:58080` (гейтвей
NGINX) — переопределяется через `-e BASE_URL=...`.

## Переменные окружения сценариев

| Сценарий | Переменная | По умолчанию | Смысл |
|---|---|---|---|
| upload_burst | `USER_POOL` | 5 | сколько разных `X-User-Id` в трафике — намеренно меньше числа VU, иначе лимитеру нечего лимитировать |
| catalog_read | `CACHE_MODE` | `warm` | `warm` — узкий горячий набор id, `cold` — случайный id из всего пула на каждый запрос |
| catalog_read | `HOT_POOL_SIZE` | 3 | сколько карточек в горячем наборе для `CACHE_MODE=warm` |
| place_order | `REPEAT_RATE` | 0.2 | доля итераций, повторяющих ПРЕДЫДУЩИЙ ключ этого же VU (последовательная идемпотентность) |
| place_order | `CONCURRENT_REPEAT_RATE` | 0.1 | доля НОВЫХ итераций, которые вместо одного запроса стреляют ДВУМЯ параллельными с одним ключом (`http.batch`) |
| place_order | `BUYER_POOL` | 20 | разброс `buyer_id`, чтобы не упереться в баланс одного покупателя |

## Как читать числа

Каждый сценарий печатает в конце JSON-сводку (через `handleSummary`) и
одновременно кладёт её в файл `summary-<сценарий>[-<режим>].json` в текущей
директории — оттуда эти числа переносятся в `docs/perf/` по шаблону
`docs/perf/README.md`.

- **upload_burst.js**: `rate_limited_429` должно быть заметно больше нуля —
  если оно 0, тест ничего не доказал (либо Redis лежит и лимитер работает
  fail-open, либо `USER_POOL`/скорость запросов занижены). `real_failures`
  (5xx, обрыв соединения) должен оставаться около нуля — это ЕДИНСТВЕННАЯ
  метрика, которая здесь означает "сервис сломался"; 429 к ней не относится.
- **catalog_read.js**: сравнивай `overall_p99_ms` (или `card_p99_ms` отдельно
  от ленты) между прогоном `CACHE_MODE=warm` и `CACHE_MODE=cold` — это и есть
  число из критерия готовности фазы 2. Разница должна быть в разы, а не в
  проценты; если её нет — либо catalog ещё не подключил `pkg/redisx`
  (`GetOrLoad*`), либо `HOT_POOL_SIZE` слишком большой относительно `CACHE_TTL`.
- **place_order.js**: `idempotency_violations` ОБЯЗАН быть 0 — это порог
  `count==0`, а не "меньше процента", и его провал — единственный по-
  настоящему красный флаг сценария. `idempotent_duplicate_returned` и
  `idempotency_conflicts_409` — это НЕ ошибки, это подтверждение, что
  идемпотентность вообще была протестирована (аналог `rate_limited` в
  upload_burst.js).

## Куда смотреть в Grafana во время прогона

`http://localhost:53000` → дашборд **gosplash overview**
(`deploy/observability/grafana/dashboards/overview.json`):

- **RED — HTTP**: `Rate` подскочит на время прогона, `Errors` (доля 5xx)
  должна остаться плоской у нуля во всех трёх сценариях — рост здесь важнее
  собственных чисел k6, потому что это тот же сигнал, но со стороны сервиса,
  а не клиента.
- **Duration — p50/p95/p99**: сравнивай глазами с тем, что печатает
  `handleSummary` — если Grafana видит p99 хуже, чем k6 (или наоборот),
  разница — это сетевой путь k6 → NGINX → сервис, который сам k6 не видит.
- **Kafka и outbox** (только для `place_order.js`, косвенно и для
  `upload_burst.js` — media тоже пишет в outbox при загрузке): `Очередь
  outbox` не должна расти без возврата к нулю — растущий `outbox_pending`
  во время нагрузочного теста означает то же самое, что и в проде
  (`docs/runbooks/outbox.md`), просто вызвано тестом, а не инцидентом.
- Для `place_order.js` дополнительно открой Temporal UI
  (`http://localhost:58233`) и посмотри `Workflows` — под нагрузкой должно
  быть видно много `PlaceOrderWorkflow`, подавляющее большинство `Completed`
  и лишь единицы `Failed` (карточка стала недоступна между `setup()` и
  итерацией — это не баг теста, а нормальная гонка на живых данных).

## Что не проверено на этой машине

Docker-демон здесь был поднят, поэтому сами сценарии реально исполнялись
через `docker run grafana/k6` — против собственного мини-мока HTTP (не
против реального стенда gosplash: `docker compose up` этого стенда не
поднимался в рамках этой задачи). Значит, подтверждено: скрипты валидны для
k6 (парсятся, все использованные функции API существуют, пороги и
кастомные метрики считаются, `handleSummary` пишет корректный JSON), но
числа из раздела «как читать числа» никогда не снимались с реального
media/catalog/order — сравнение p99 тёплого/холодного кэша и профиль
idempotency-нагрузки предстоит снять при первом реальном прогоне
(см. `docs/perf/README.md`).

# gosplash — план реализации

Этот файл — **источник правды** по объёму и порядку работ. `TODO.md` остаётся
учебным материалом («что потрогать руками») по уже сделанному, но планом больше
не является: расхождения между ними разрешаются в пользу PLAN.md.

Читать сверху вниз. Фаза считается закрытой, только когда выполнены **все**
критерии готовности — следующая фаза строится поверх предыдущей, и отлаживать
два незакрытых слоя разом дорого.

---

## 1. Аудит: что было на 2026-09-12

Снимок состояния **до фазы 0**. Пути и имена здесь относятся к старой
раскладке (`libs/pkg`, `Image`, `image.uploaded`) — что и почему переехало,
см. [ADR 0001](adr/0001-struktura-repozitoriya.md) и
[ADR 0003](adr/0003-imenovanie-sobytiy-i-envelope.md), результат — в
[docs/phases/00.md](phases/00.md).

Состояние проверено сборкой всех девяти модулей `go.work` — компилировалось всё.

### Работает и проверено глазами

| Слой | Что именно | Файлы |
|---|---|---|
| Монорепа | `go.work`, 9 модулей, сервисы импортируют только `gosplash/libs` | `go.work` |
| Инфраструктура | 6×PostgreSQL, Kafka (KRaft) + UI + `kafka-init`, Redis, MinIO, Temporal + UI, Jaeger, NGINX; всё собрано через `include` | `docker-compose.yml`, `docker/*.yml` |
| PostgreSQL | шардирование `fnv32(user_id)%N`, scatter-gather, логическая репликация primary→replica, роутинг чтений через `dbresolver` | `libs/pkg/db/db.go` |
| Kafka | продюсер (`AllISRAcks`, идемпотентный) и консьюмер (`DisableAutoCommit`, ручной commit) на franz-go | `libs/pkg/kafka/kafka.go` |
| S3 | потоковая загрузка, presigned URL | `libs/pkg/s3/s3.go` |
| media | вертикальный срез: HTTP upload → MinIO → шард PG → Kafka; gRPC `GetImage` | `services/media/**` |
| catalog | Kafka-консьюмер → gRPC в media → upsert в primary; публичное чтение с реплики; настройка репликации из миграций | `services/catalog/**` |
| Контракты | `events.v1.ImageUploaded`, `media.v1.MediaService`; buf настроен, `managed mode` включён | `proto/**`, `buf.*.yaml` |
| Эксплуатация | 60+ целей: `up`, `migrate`, `doctor`, `demo`, `smoke`, `topic-tail`, `lag`, `psql-*`, `s3-tree`, `wf-*` | `Makefile` |

Качество существующего кода высокое: комментарии объясняют «почему так», а не
«что делает строка». Это задаёт планку для всего, что пишется дальше.

### Чего нет

- **Заглушки `log.Fatal`:** `thumbnail-worker`, `wallet`, `order` (api + worker).
- **Пустые контракты:** `catalog.proto`, `wallet.proto`, `order.proto` — только
  `syntax` и `package`.
- **Ни одного теста.** Ни unit, ни интеграционных, ни `testcontainers`.
- **Нет `docs/`** — ни плана, ни ADR, ни runbook'ов (создаётся этим коммитом).
- Нет outbox, `processed_events`, retry/DLQ-обработки (топики созданы, но никто
  в них не пишет), Redis не используется ни одной строкой Go-кода, трассировки
  нет (Jaeger поднят вхолостую), метрик нет, `/readyz` нет, CI нет.
- Публикация в Kafka идёт **после** коммита и её ошибка только логируется —
  событие теряется молча (`services/media/internal/image/service.go`).
- Аутентификации нет: пользователь берётся из заголовка `X-User-Id`.

---

## 2. Четыре развилки и принятые решения

Спецификация в четырёх местах расходится с тем, что уже написано. Решения
приняты заказчиком явно и зафиксированы как ADR — здесь только сводка.

| # | Развилка | Решение | ADR |
|---|---|---|---|
| 1 | Структура репозитория: `libs/pkg` + `proto/<d>/v1` против `pkg/` + `proto/gosplash/<d>/v1` + `gen/go` | **Мигрировать на структуру спецификации** | [0001](adr/0001-struktura-repozitoriya.md) |
| 2 | Именование: `Image` / `image.uploaded` против `Photo` / `media.photo.uploaded` + `Envelope` | **Полный переход на конвенцию спецификации** | [0003](adr/0003-imenovanie-sobytiy-i-envelope.md) |
| 3 | Шардирование media против §13 «не шардировать» | **Сохранить шардирование** | [0002](adr/0002-sohranyaem-shardirovanie-media.md) |
| 4 | Сага: сразу Temporal против «сначала руками» | **Сразу Temporal** | [0004](adr/0004-saga-srazu-na-temporal.md) |

Развилка 3 имеет последствие, которое протянется через все фазы: **`outbox`,
`processed_events` и прочие служебные таблицы media живут на каждом шарде**, и
relay обходит все шарды. Это не осложнение ради осложнения — это честная цена
шардирования, и она проговаривается в комментариях к коду.

---

## 3. Целевая структура

Переезд выполняется в фазе 0 целиком, одним заходом: половина репозитория в
старой раскладке, половина в новой — худшее из возможных состояний.

```
gosplash/
├── go.work
├── go.mod                       # module gosplash — pkg/, gen/, tools/
├── proto/
│   ├── buf.yaml                 # переезжает из корня
│   ├── buf.gen.yaml
│   └── gosplash/                # пакет = путь: gosplash.media.v1
│       ├── events/v1/           # Envelope + payload'ы событий
│       ├── media/v1/
│       ├── catalog/v1/
│       ├── wallet/v1/
│       ├── order/v1/
│       ├── analytics/v1/        # фаза 4
│       └── search/v1/           # фаза 5
├── gen/go/gosplash/<domain>/v1/ # сгенерированный код, коммитится
├── pkg/                         # только инфраструктура, без бизнес-логики
│   ├── config/                  # ← из configs/
│   ├── dbx/                     # ← из libs/pkg/db (шарды, реплика)
│   ├── s3x/                     # ← из libs/pkg/s3
│   ├── httpx/                   # middleware, ошибки, healthz/readyz, shutdown
│   ├── otelx/                   # трейсы, метрики, slog с trace_id
│   ├── kafkax/                  # franz-go: producer, consumer-loop, headers, otel
│   ├── outbox/                  # таблица + relay
│   ├── redisx/                  # cache-aside, lock, ratelimit, idempotency
│   ├── grpcx/                   # interceptors, health, client factory
│   └── resilience/              # retry с backoff+jitter, circuit breaker
├── services/<svc>/              # свой go.mod у каждого
│   ├── cmd/<svc>/main.go
│   └── internal/{domain,app,adapters,ports}
├── deploy/
│   ├── compose/                 # ← из docker/ + docker-compose.yml
│   ├── nginx/, temporal/
│   ├── helm/<service>/          # фаза 6
│   ├── k6/, chaos/              # фаза 6
├── docs/{PLAN.md,adr/,phases/,runbooks/,perf/}
├── tools/{automigrate,mediactl,kafka-reprocess,search-reindex}
└── Makefile
```

Что важно знать про переезд:

- **Корневой модуль `gosplash`** владеет `pkg/`, `gen/` и `tools/`. Модуль
  `gosplash/libs` исчезает; `gosplash/configs` складывается в `pkg/config`.
- **Импорт сгенерированного кода получается длинным** —
  `gosplash/gen/go/gosplash/media/v1`. Удвоение `gosplash` неизбежно: слева
  путь Go-модуля, справа namespace protobuf-пакета (`gosplash.media.v1`).
  Везде используется алиас (`mediav1 "…"`), как и сейчас.
- **Hexagonal-раскладка `internal/{domain,app,adapters,ports}`** применяется к
  новым сервисам сразу; `media` и `catalog` переводятся на неё в фазе 0 вместе
  с переименованием `Image` → `Photo` — дешевле сделать это сейчас, пока кода
  мало, чем вклинивать рефакторинг в середину фазы 3.

---

## 4. Фазы

Правило для каждой фазы без исключений: компилируется, `go test ./...` зелёный,
`docker compose up` поднимает всё, есть `make demo-<phase>`, который прогоняет
сценарий и печатает результат, и есть `docs/phases/NN.md` с описанием.

### Фаза 0 — фундамент и переезд · **готово**

Самая скучная и самая важная: после неё структура больше не меняется.
Отчёт: [docs/phases/00.md](phases/00.md).

| Шаг | Содержание |
|---|---|
| 0.1 ✅ | `docs/`: PLAN.md, ADR 0001–0006 |
| 0.2 ✅ | Переезд структуры целиком (§3): корневой модуль, `pkg/`, `proto/gosplash/**`, `gen/go`, `deploy/compose`. Правка `go.work`, `buf.*.yaml`, `Makefile`, `.env.example`, `README.md` |
| 0.3 ✅ | Переименование `Image` → `Photo`, топики → `media.photo.uploaded` и т.д., `kafka-init` переписан. `Envelope` введён и применён сразу (см. ADR 0003). Hexagonal-раскладка в media и catalog |
| 0.4 ✅ | `pkg/otelx`: OTel SDK (traces + metrics), `slog` JSON с `trace_id`/`span_id`, `otelgorm`. `pkg/httpx`: middleware (recovery, logging, RED-метрики), единый формат ошибки, `/healthz`, `/readyz` (PG ping + Kafka metadata + Redis ping), graceful shutdown 15 с, `pprof` на отдельном порту |
| 0.5 ✅ | `deploy/compose/observability.yml`: OTel Collector, Tempo, Loki + promtail, Prometheus, Grafana с дашбордом «gosplash overview». Jaeger уходит ([ADR 0005](adr/)) |
| 0.6 ✅ | Первые тесты: unit на `dbx.Shards.Index` (стабильность хэша), на `httpx`, на конфиг. `make test` перестаёт быть пустым |

**Готово, когда:** `make up` поднимает всё включая observability; `make demo-0`
дёргает media и catalog и печатает ссылку на трейс в Grafana; в Prometheus
видны RED-метрики обоих сервисов; `/readyz` краснеет при остановленном
PostgreSQL; `make demo` (старый сценарий загрузки) работает как прежде.

Собрано, `go vet` и `buf lint` чисты, 42 теста зелёные, `docker compose config`
валиден. Живьём стек не поднимался — на машине не был запущен docker-демон;
первый запуск начинать с `make nuke`, потому что переименование задело имена
баз, бакетов и топиков.

### Фаза 1 — Kafka по-взрослому · **готово**

| Шаг | Содержание |
|---|---|
| 1.1 | `Envelope` в `proto/gosplash/events/v1`: `event_id` (uuid v7), `event_type`, `aggregate_id`, `occurred_at_unix_ms`, `schema_version`, `Any payload`. Заголовки Kafka: `event_id`, `event_type`, `traceparent`, `producer` |
| 1.2 | `pkg/kafkax`: продюсер (`AllISRAcks`, snappy, `RecordDeliveryTimeout`), consumer-loop с `BlockRebalanceOnPoll` и горутиной на партицию, логи `OnPartitionsAssigned/Revoked`, прокидывание `traceparent`, метрики (`kafka_consumer_lag` через `kadm`, `kafka_messages_processed_total{topic,result}`, гистограмма) |
| 1.3 | `pkg/outbox`: таблица, запись в одной транзакции с бизнес-изменением, relay на `pgx` (`FOR UPDATE SKIP LOCKED`, батч 100), метрика `outbox_pending`. В media relay обходит **все шарды** |
| 1.4 | Классификация ошибок `ErrRetryable`/`ErrPermanent`, retry на месте (3 попытки, backoff+jitter) → `<topic>.retry` с заголовком `retry_at` → `<topic>.dlq` с `error`/`original_*` |
| 1.5 | `processed_events(event_id PK)` — вставка в той же транзакции, `ON CONFLICT DO NOTHING`. В thumbnail-worker и catalog |
| 1.6 | `thumbnail-worker`: WebP 320/800/1600, пул `runtime.NumCPU()`, валидация что файл — изображение, статус `ready`, событие `media.photo.thumbnail-ready` |
| 1.7 | `tools/kafka-reprocess`: переливка из DLQ обратно в основной топик |
| 1.8 | CI-проверка «нет прямого `Produce` вне `pkg/outbox`» |
| 1.9 | Интеграционный тест на `testcontainers-go`: «outbox доставляет ровно один раз при падении relay между produce и update» |

**Готово, когда** проходят все четыре сценария `make demo-kafka`: ребаланс на
двух инстансах, битое сообщение в DLQ, Kafka лежит 30 с → ничего не потеряно,
переигранное из DLQ событие отсечено `processed_events`.

### Фаза 2 — Redis · **готово**

| Шаг | Содержание |
|---|---|
| 2.1 | `pkg/redisx`: клиент с `MaxRetries`/`DialTimeout`/`PoolSize`, `redisotel`, namespace `gosplash:<service>:`, метрики hit/miss. Запрет `KEYS`, только `UNLINK` |
| 2.2 | Cache-aside карточки listing в catalog: TTL 10 мин ± 10 % джиттера, `singleflight` на промах, инвалидация по событиям |
| 2.3 | Token bucket на Lua: rate limit загрузок в media, `429` + `Retry-After` |
| 2.4 | Distributed lock (`SET NX PX` + токен + Lua-release + watchdog) в thumbnail-worker |
| 2.5 | Idempotency-key (`SET NX EX 24h`) — инфраструктура готовится здесь, применяется в order в фазе 3 |
| 2.6 | Sorted Set: топ-100 фото за сутки (`ZINCRBY`/`ZREVRANGE`, TTL 48 ч) |
| 2.7 | `deploy/k6/catalog_read.js` |

**Готово, когда** `make demo-redis` показывает разницу p99 с кэшем и без, и
разницу поведения при истёкшем TTL с `singleflight` и без него.

### Фаза 3 — gRPC, деньги, сага · **готово**

| Шаг | Содержание |
|---|---|
| 3.1 | `pkg/grpcx`: interceptors (otel, logging, recovery, auth по JWT из metadata, deadline-propagation), `grpc.health.v1`, фабрика клиентов с `round_robin` и keepalive |
| 3.2 | `pkg/resilience`: retry с backoff+jitter только на `Unavailable`/`DeadlineExceeded` и только для идемпотентных вызовов; circuit breaker (`sony/gobreaker`) |
| 3.3 | `wallet`: `accounts`, append-only `ledger_entries` (двойная запись), gRPC `GetBalance`/`ReserveFunds`/`ReleaseFunds`/`CommitFunds`, все с `idempotency_key` |
| 3.4 | `catalog`: gRPC `GetListing`/`GrantLicense`/`RevokeLicense`/`WatchListing` (server-streaming), grpc-gateway — один контракт как REST и как gRPC |
| 3.5 | `order`: `POST /orders` с `Idempotency-Key` (Redis + fallback-таблица, параллельный повтор → `409`), Temporal `PlaceOrderWorkflow` с компенсациями |
| 3.6 | `buf breaking` в CI; демонстрация эволюции схемы: добавленное поле не ломает старого клиента, переиспользованный номер — ломает сборку |

**Готово, когда:** успешный заказ списывает деньги покупателю, начисляет автору
и выдаёт лицензию; **при падении catalog деньги возвращаются**, что видно в
Temporal UI и в `ledger_entries`; убитый wallet даёт `503` за 50 мс, а не за
5 с (circuit breaker).

### Фаза 4 — analytics (ClickHouse) · **готово**

Топик `analytics.photo.viewed` (catalog эмитит батчами при `GET /listings/{id}`)
и `order.order.paid` → Kafka engine → `MergeTree` + `SummingMergeTree`.
gRPC `TopPhotos(period)`. HyperLogLog в Redis для уникальных просмотров.
README сервиса объясняет `ORDER BY (photo_id, ts)`, разреженный индекс,
отсутствие UPDATE и когда нужен `ReplacingMergeTree`.

**Готово, когда** `make demo-clickhouse` на 5 млн просмотров показывает время
`SELECT top-100 за неделю` в ClickHouse против того же запроса в PostgreSQL.

### Фаза 5 — search (Elasticsearch) · **готово**

Индекс `listings` с явным mapping (русский + английский анализаторы),
консьюмер `catalog.listing.*` с идемпотентной индексацией через `external_gte`,
`multi_match` с `fuzziness: AUTO`, фасеты через `aggs`, keyset-пагинация
`search_after`, `tools/search-reindex` с переключением alias без даунтайма.
README начинает с `tsvector` + GIN и объясняет, чего не хватает.

### Фаза 6 — эксплуатация · **готово**

Helm-чарт на сервис (probes, HPA, ConfigMap/Secret, headless Service для gRPC),
`make kind-up`, k6-сценарии `upload_burst`/`catalog_read`/`place_order` с
результатами в `docs/perf/`, `deploy/chaos/{kill-kafka,kill-wallet,slow-pg}.sh`,
runbook'и, alert rules (p99 > 500 мс, lag > 1000, outbox pending > 100).

---

## 5. Как фазы исполняются параллельно

Фазы упорядочены по зависимостям, но внутри фазы и между соседними фазами
работа делится на куски, которые не пересекаются по файлам. Куски исполняются
параллельно; волна закрывается только после того, как всё собрано вместе и
проверено целиком.

Главный риск параллельной работы в монорепе — не логика, а **общие файлы**.
Поэтому они разведены заранее, до запуска:

| Общий файл | Как разведён |
|---|---|
| `Makefile` | цели фазы кладутся в `mk/<тема>.mk`, подключаются через `-include mk/*.mk` |
| `pkg/config` | каждый пакет в `pkg/` объявляет СВОЙ `Config`; сервис в `main` перекладывает значения. Править `pkg/config` может только интегратор |
| `proto/**`, `gen/**` | контракты всех фаз написаны и сгенерированы заранее, дальше заморожены |
| `.env`, `.env.example` | переменные всех фаз добавлены заранее; исполнитель их не правит, а сообщает о недостающих |
| `docs/adr/` | номера выданы заранее, см. [adr/README.md](adr/README.md) |
| `go.mod` | зависимости всех фаз установлены заранее одним заходом |
| `deploy/compose/` | по файлу на технологию, общий `docker-compose.yml` правит только интегратор |

### Волны

| Волна | Куски (параллельно) | Зависит от |
|---|---|---|
| 1 | `pkg/kafkax` + `pkg/outbox` + `kafka-reprocess` · `pkg/redisx` · `pkg/grpcx` + `pkg/resilience` | фаза 0 |
| 2 | `thumbnail-worker` целиком · `media`: outbox и rate limit · `catalog`: кэш, `processed_events`, gRPC, gateway | волна 1 |
| 3 | `wallet`: счета и ledger · `order`: идемпотентность и сага на Temporal | волны 1–2 |
| 4 | `analytics` на ClickHouse · `search` на Elasticsearch | волны 1–3 |
| 5 | Helm и kind · k6, chaos, runbook'и · CI и линтеры | всё выше |

Между волнами интегратор делает то, что нельзя делегировать: `go mod tidy`,
сборку всех модулей, прогон тестов, сведение переменных окружения и правку
общих файлов по отчётам исполнителей.

---

## 6. ADR

Обязательные по спецификации плюс возникшие из аудита. Пишутся **в фазе, где
принимается решение**, а не задним числом.

| № | Тема | Фаза |
|---|---|---|
| 0001 | Структура репозитория: миграция на раскладку спецификации | 0 ✅ |
| 0002 | Шардирование media сохраняется вопреки §13 | 0 ✅ |
| 0003 | Именование событий и обязательный Envelope | 0 ✅ |
| 0004 | Сага сразу на Temporal, без промежуточной ручной | 0 ✅ |
| 0005 | Jaeger → OTel Collector + Tempo/Loki/Grafana | 0 ✅ |
| 0006 | GORM остаётся; pgx добавляется точечно для outbox-relay | 0 ✅ |
| 0007 | franz-go против sarama и kafka-go | 1 |
| 0008 | Transactional outbox против прямой публикации | 1 |
| 0009 | Ключи партиционирования топиков | 1 |
| 0010 | Схема ретраев и DLQ | 1 |
| 0011 | Polling-relay против Debezium/CDC | 1 |
| 0012 | Redis-лок против PostgreSQL advisory lock | 2 |
| 0013 | Cache-aside против write-through и refresh-ahead | 2 |
| 0014 | `ledger_entries` append-only с двойной записью | 3 |
| 0015 | Headless Service для gRPC: почему обычный Service плохо балансирует HTTP/2 | 3 |
| 0016 | ClickHouse через Kafka engine против push из Go | 4 |

---

## 7. Тестирование

Встроено в каждую фазу, отдельной фазы «написать тесты» нет — она всегда
откладывается.

- **Unit:** table-driven + `testify`, моки портов через `mockery`.
- **Integration:** `testcontainers-go` (PostgreSQL, Kafka, Redis). Обязательный
  тест фазы 1 — outbox доставляет ровно один раз при падении relay между
  produce и update.
- **Contract:** `buf breaking` + golden-тесты сериализации событий.
- **E2E:** `make demo-*` как исполняемые сценарии, проверяющие инварианты
  (баланс `ledger_entries` сходится в ноль, дублей в `catalog.listings` нет).
- **Chaos:** скрипты в `deploy/chaos/` (фаза 6).

CI (GitHub Actions): `buf lint`, `buf breaking`, `golangci-lint`, `go test -race`,
интеграционные тесты, `go vet` + проверка «нет прямого `Produce`».

---

## 8. Чего сознательно не делаем

Из §13 спецификации, плюс уточнения по итогам аудита:

- Kafka Streams / ksqlDB, service mesh, Debezium — упоминаются в ADR как
  следующий шаг, не внедряются.
- Multi-region и новое шардирование PostgreSQL. Существующее шардирование
  media сохраняется (ADR 0002), но не расширяется; партиционирование
  `ledger_entries` по месяцу допустимо как демонстрация.
- Фронтенд.
- Аутентификация остаётся заглушкой (`X-User-Id`) до фазы 3, где появляется
  JWT-интерсептор; полноценный auth-сервис вне объёма.

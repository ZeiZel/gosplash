# gosplash

Учебный бэкенд-проект: фотосток с продажей лицензий на изображения.
Авторы загружают фото, сервис генерирует превью, покупатели ищут изображения
и покупают лицензии с внутреннего баланса.

Цель проекта — на живой предметной области отработать типовые практики
backend/system design: микросервисы на Go, событийную обработку через Kafka,
шардирование и логическую репликацию PostgreSQL, распределённые транзакции
через паттерн Saga (Temporal), OLAP-аналитику на ClickHouse, поиск на
Elasticsearch, кэширование в Redis, gRPC-контракты на protobuf.

**Все фазы [docs/PLAN.md](docs/PLAN.md) (0–6) реализованы.** Этот README —
карта проекта и гайд по его изучению: откуда начинать, куда смотреть,
как с ним работать.

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Стек](#стек)
- [Сервисы и потоки данных](#сервисы-и-потоки-данных)
- [Структура репозитория](#структура-репозитория)
- [Как изучать проект: маршрут чтения](#как-изучать-проект-маршрут-чтения)
- [Шардирование и репликация](#шардирование-и-репликация)
- [События и контракты](#события-и-контракты)
- [Наблюдаемость](#наблюдаемость)
- [Тесты, линтеры, CI](#тесты-линтеры-ci)
- [Инструменты](#инструменты)
- [Документация](#документация)
- [Рецепты частых задач](#рецепты-частых-задач)
- [Известные упрощения](#известные-упрощения)
- [Порты и точки входа](#порты-и-точки-входа)

## Быстрый старт

```bash
make env         # .env из .env.example
make up          # поднять всю инфраструктуру (PG ×6, Kafka, Redis, MinIO,
                 #   Temporal, ClickHouse, Elasticsearch, observability)
make doctor      # проверить, что каждый компонент отвечает
make migrate     # схемы всех баз + настройка логической репликации каталога
make run-all     # запустить сервисы (в отдельном терминале)
make demo        # прогнать фичу «загрузка фото» целиком и показать результат
```

`make` без аргументов печатает список всех команд с описанием — их ~150,
сгруппированы по темам (PostgreSQL, Kafka, Redis, демо по фазам, k8s...).
Каждая фаза плана имеет своё демо: `make demo-kafka-*`, `demo-redis`,
`demo-wallet`, `demo-saga-ok` / `demo-saga-fail`, `demo-clickhouse`,
`demo-search` — это исполняемые сценарии, проверяющие инварианты фазы.

> **Windows:** команды рассчитаны на make + Git Bash. Если make не установлен,
> команды можно читать как документацию: рецепты лежат в `Makefile` и `mk/*.mk`
> и легко повторяются руками.

`make proto` (buf generate → `gen/go`) нужен только после правки контрактов;
сгенерированный код закоммичен. `make tools` ставит buf и плагины protoc.

## Стек

| Область | Технология |
|---|---|
| Язык | Go 1.27, Go Workspaces (монорепозиторий, 13 модулей) |
| OLTP | PostgreSQL 16: 2 шарда media + primary/replica catalog + wallet + order |
| OLAP | ClickHouse (аналитика просмотров и покупок), `clickhouse-go` |
| Поиск | Elasticsearch 8 (индекс `listings`), официальный go-клиент |
| Очереди | Apache Kafka (KRaft), `franz-go` |
| Кэш | Redis 7 (`go-redis`): cache-aside, лок, rate limit, топ-100, HyperLogLog |
| Оркестрация саг | Temporal, `go.temporal.io/sdk` |
| RPC | gRPC + protobuf (`buf`, `buf breaking` в CI); REST — ручные фасады в HTTP-адаптерах сервисов |
| Файлы | MinIO (S3-совместимое хранилище) |
| Обработка изображений | чистый Go: `golang.org/x/image/draw` (CatmullRom), превью — JPEG q85 |
| Вход | NGINX (reverse proxy) |
| Наблюдаемость | OpenTelemetry → Collector, Tempo (трейсы), Prometheus (метрики), Loki (логи), Grafana |
| Локальная среда | Docker Compose (`deploy/compose/`, стек по файлу на технологию) |
| K8s | Helm-чарты на сервис, kind, k6, chaos-скрипты (`deploy/`) |

## Сервисы и потоки данных

```
                       ┌────────────────────────── Kafka ──────────────────────────┐
                       │  media.photo.uploaded / .thumbnail-ready                  │
NGINX ─► media ───────►│  analytics.photo.viewed (батчи)      order.order.paid     │
          │  outbox    │  catalog.listing.*                                        │
          ▼            └───────┬───────────────┬──────────────┬───────────┬──────┘
       MinIO ◄── thumbnail-worker               │              │           │
                       │                       ▼              ▼           ▼
                       │                   catalog ──►     analytics    search
                       │                   (Redis,        (ClickHouse) (Elastic-
                       │                    replica PG)                  search)
NGINX ─► order ═══ Temporal workflow ═══► wallet + catalog (gRPC, компенсации)
```

Читается так: **синхронный запрос-ответ — gRPC** (стрелки `═══`), **факты,
не требующие ответа, — Kafka** (стрелки через шину). Загрузка файлов —
исключение: чистый HTTP multipart в media.

| Сервис | Роль | Что смотреть внутри |
|---|---|---|
| `services/media` | приём загрузок, оригиналы в MinIO, метаданные в шардированный PG | HTTP-хендлер, transactional outbox, rate limit |
| `services/thumbnail-worker` | Kafka-консьюмер: превью JPEG 320/800/1600, retry-топик, DLQ, идемпотентность | пул воркеров, distributed lock, `processed_events` |
| `services/catalog` | публичный read-API: карточка, лента (keyset-пагинация), лицензии; денормализованные данные | cache-aside + singleflight, реплика чтений, server-streaming `WatchListing` |
| `services/wallet` | балансы, double-entry ledger (`ledger_entries` append-only), резервирование | блокировки в порядке id, идемпотентные gRPC-операции |
| `services/order` | покупка лицензии: сага `PlaceOrderWorkflow` + удаление фото | два бинарника: API и Temporal worker; идемпотентность HTTP |
| `services/analytics` | агрегаты просмотров/покупок в ClickHouse; воронка загрузки (FunnelStats/AuthorStats) | Kafka engine vs Go-консьюмер ([ADR 0016]), батч-райтер, MV |
| `services/search` | полнотекстовый поиск с фасетами | явный mapping, `search_after`, переиндексация через alias |

[ADR 0016]: docs/adr/0016-clickhouse-kafka-engine.md

Подробные README с разбором «почему» — у [analytics](services/analytics/README.md),
[order](services/order/README.md) и [search](services/search/README.md).

## Структура репозитория

```
gosplash/
├── go.work                     # 13 модулей: корень + 7 сервисов + 5 инструментов
├── go.mod                      # module gosplash: pkg/, gen/
├── Makefile + mk/*.mk          # ~150 команд; цели по темам лежат в mk/<тема>.mk
│
├── proto/                      # исходники контрактов (buf lint/breaking отсюда)
│   └── gosplash/<домен>/v1/    # пакет = путь: gosplash.media.v1
├── gen/go/                     # сгенерированный код, коммитится
│
├── pkg/                        # общие библиотеки: только инфраструктура
│   ├── config/                 # единственное место, где читается окружение
│   ├── dbx/                    # gorm: шарды (Shards) и реплика (OpenWithReplica)
│   ├── httpx/                  # middleware, ошибки, healthz/readyz, shutdown
│   ├── grpcx/                  # interceptors (recovery/log/auth/deadline), health
│   ├── kafkax/                 # franz-go: продюсер, консьюмер, конверт, retry/DLQ
│   ├── outbox/                 # таблица + relay + retention опубликованных строк
│   ├── redisx/                 # кэш, лок, rate limit, idempotency, топ-N, HLL
│   ├── resilience/             # retry (backoff+jitter), circuit breaker
│   ├── idempotency/            # processed_events в транзакции консьюмера
│   ├── otelx/                  # трейсы, метрики, slog с trace_id
│   └── s3x/                    # MinIO
│
├── services/<сервис>/          # свой go.mod у каждого
│   ├── cmd/<бинарник>/main.go  # композиция зависимостей — только здесь
│   ├── internal/bootstrap/     # сборка приложения из адаптеров (Deps → App)
│   ├── internal/domain/        # предметная область, без тегов инфраструктуры
│   ├── internal/ports/         # интерфейсы, которыми ПОЛЬЗУЕТСЯ app
│   ├── internal/app/           # сценарии использования (тестируются без БД)
│   ├── internal/adapters/      # pg, kafka, redis, grpc, http, s3, ...
│   └── migrations/             # не в internal: сюда ходит tools/automigrate
│
├── tools/
│   ├── automigrate/            # единственный, кому можно знать про все сервисы
│   ├── archcheck/              # свой статанализатор архитектуры, включён в CI
│   ├── mediactl/               # gRPC-клиент к media для ручных проверок
│   ├── kafka-reprocess/        # переливка DLQ обратно в топик (dry-run есть)
│   └── search-reindex/         # переиндексация catalog → ES через alias
│
├── deploy/
│   ├── compose/                # docker-compose.yml + стек по файлу на технологию
│   ├── observability/          # collector, tempo, loki, prometheus, grafana
│   ├── nginx/, temporal/       # вход и оркестратор
│   ├── helm/, kind/, k6/, chaos/
│
└── docs/
    ├── PLAN.md                 # ← источник правды по объёму и порядку работ
    ├── STYLE.md                # соглашения кода проекта
    ├── adr/                    # 18 архитектурных решений: контекст → решение
    ├── phases/                 # отчёты по фазам
    ├── runbooks/               # что делать при DLQ, лаге реплики, зависшей саге
    └── perf/                   # результаты k6
```

Раскладка внутри сервиса — **ports & adapters** (гексагональная):

- `domain` не знает ни о чём, кроме себя. Никаких тегов `gorm`, `json`,
  `protobuf`: строка таблицы живёт в `adapters/pg`, представление для HTTP —
  в `adapters/http`, контракт — в `gen/go`;
- `ports` объявляет интерфейсы рядом с тем, кто ими ПОЛЬЗУЕТСЯ, а не рядом с
  реализацией: `app` зависит от своего интерфейса, а `adapters/pg` — от `app`;
- `app` содержит сценарии и тестируется без базы, S3 и Kafka —
  пример в `services/media/internal/app/service_test.go`;
- эти правила проверяет не ревью, а машина: `tools/archcheck` (запрет gorm/kgo/
  redis/grpc в `app` и `domain`, запрет прямого `Produce` в обход outbox,
  изоляция сервисов друг от друга) гоняется в CI.

Сервисы не импортируют друг друга — только корневой модуль `gosplash`
(контракты и инфраструктурная обвязка). Импорт сгенерированного кода выглядит
как `mediav1 "gosplash/gen/go/gosplash/media/v1"`: удвоение неизбежно — слева
путь Go-модуля, справа namespace protobuf-пакета ([ADR 0001](docs/adr/0001-struktura-repozitoriya.md)).

## Как изучать проект: маршрут чтения

Проект большой; читать его линейно сверху вниз бесполезно. Рабочий способ —
идти по следам одного запроса через все слои. Ниже два маршрута; первый
обязателен, второй — когда первый уже понятен.

### Маршрут 1. Загрузка фото (весь стек одной фичи)

Порядок подобран так, чтобы каждый следующий файл отвечал на вопрос,
оставленный предыдущим:

1. **Точка входа и композиция.** `services/media/cmd/media/main.go` —
   заметь: только сборка зависимостей (config → соединения → bootstrap.NewApp),
   ноль бизнес-логики. Что такое Deps и зачем отдельный `bootstrap` —
   `services/media/internal/bootstrap/app.go`.
2. **HTTP-граница.** `services/media/internal/adapters/http/handler.go`,
   `upload`: MaxBytesReader, sniffing magic bytes, rate limit, откуда берётся
   `user_id` (спойлер: из заголовка, см. [известные упрощения](#известные-упрощения)).
3. **Сценарий.** `services/media/internal/app/service.go`, `Upload`:
   валидация → генерация id → S3 → транзакция PG **вместе с outbox.Write**.
   Здесь главный урок фазы 1: событие и метаданные коммитятся одной
   транзакцией, двойной записи не бывает ([ADR 0008](docs/adr/0008-transactional-outbox.md)).
4. **Хранилище.** `services/media/internal/adapters/pg/photo_repository.go`:
   как GORM-модель отделена от доменной, как выбирается шард.
5. **Relay.** `pkg/outbox/relay.go`: `FOR UPDATE SKIP LOCKED`, батчи, обход
   всех шардов, метрика `outbox_pending`, retention опубликованных строк.
6. **Kafka-обвязка.** `pkg/kafkax/kafka.go` (продюсер/консьюмер, горутина на
   партицию в `engine.go`, traceparent в заголовках), конверт
   `Envelope` — `pkg/kafkax/envelope.go` ([ADR 0003](docs/adr/0003-imenovanie-sobytiy-i-envelope.md)).
7. **Консьюмер.** `services/thumbnail-worker`: retry на месте → retry-топик →
   DLQ (`pkg/kafkax/retry.go`, `dlq.go`), идемпотентность через
   `pkg/idempotency`, сам ресайз — `internal/adapters/imagex/processor.go`.
8. **Денормализация.** `services/catalog/internal/app/indexer.go`: тонкое
   событие из Kafka → gRPC `media.GetPhoto` за подробностями → upsert в
   primary. Почему событие «тонкое», а данные тянутся по gRPC — комментарии
   в `indexer.go`.
9. **Чтение с реплики.** `services/catalog/internal/adapters/pg/listing_repository.go`
   (keyset-пагинация, limit+1 вместо COUNT) + кэш `pkg/redisx/cache.go`
   (cache-aside + singleflight + джиттер TTL, [ADR 0013](docs/adr/0013-cache-aside.md)).

После маршрута имеет смысл прогнать `make demo` и `make demo-kafka-*` и
сопоставить вывод с прочитанным.

### Маршрут 2. Деньги: заказ и сага

1. `services/order/internal/adapters/http/handler.go` — приём `POST /orders`
   с `Idempotency-Key` ([ADR 0018](docs/adr/0018-idempotency-http.md)).
2. `services/order/internal/adapters/idempotency/store.go` — Redis-фастпас +
   PG-таблица как источник правды, TTL зависших ключей.
3. `services/order/internal/adapters/temporal/workflow.go` —
   `PlaceOrderWorkflow`: Reserve → GrantLicense → Payout/Commit, компенсации
   в обратном порядке, точка невозврата. Воркфлоу детерминирован: ни сети,
   ни времени внутри — только activity.
4. `services/wallet/internal/app/service.go` — двойная запись ledger,
   блокировки двух счетов в порядке возрастания id (защита от deadlock),
   идемпотентность по ключу операции.
5. `pkg/grpcx/client.go` — round_robin, keepalive, retry только для
   идемпотентных вызовов, circuit breaker снаружи retry
   (`pkg/resilience/`), headless Service ([ADR 0015](docs/adr/0015-headless-service-dlya-grpc.md)).
6. Прогнать: `make demo-saga-ok`, потом `make demo-saga-fail` и посмотреть
   компенсации в Temporal UI (http://localhost:58233).

### Маршрут 3. Аналитика и поиск (когда захочется OLAP)

- `services/analytics/README.md` — start here: почему колоночное хранение
  бьёт PostgreSQL на агрегатах и почему Kafka engine, а не Go-консьюмер
  ([ADR 0016](docs/adr/0016-clickhouse-kafka-engine.md)).
- `services/analytics/internal/adapters/clickhouse/` — батч-райтер, репозиторий,
  миграции с `MergeTree`/`SummingMergeTree` и материализованным представлением.
- `services/search/README.md` — почему не `tsvector`, дальше маппинг,
  консьюмер с `external_gte`, `tools/search-reindex`.
- Прогнать: `make demo-clickhouse`, `make demo-search`.

## Шардирование и репликация

Обе вещи в проекте настоящие, а не декоративные — вот как они устроены.
Раздел отвечает на три вопроса: **как данные раскладываются**, **как читаются
без scatter-gather** и **как посмотреть на это живьём**.

### Шардирование media: `fnv32(user_id) % N`

Таблица `photos` (и её служебные соседи: `outbox_media`, `processed_events`)
живёт на двух инстансах PostgreSQL. Шард выбирается хэшем автора:

- `pkg/dbx/db.go`, `Shards` — пул соединений «один на шард» и `ShardIndex`:
  `fnv32(user_id) % len(shards)`. Ключ — `user_id`, а не `photo_id`:
  все фото автора лежат в одном шарде, «мои фото» — один запрос без сбора
  с двух баз (обоснование — [ADR 0002](docs/adr/0002-sohranyaem-shardirovanie-media.md)).
- Репозиторий media открывает транзакцию уже на нужном шарде
  (`services/media/internal/adapters/pg/photo_repository.go`).
- Цена шардирования проговорена в коде: outbox и `processed_events` — по копии
  **на каждый шард**, и relay обходит все шарды по очереди
  (`pkg/outbox/relay.go`, `pollAllShards`). Добавить шард = добавить DSN в
  конфиг и прогнать миграцию; существующие данные не переезжают
  (ребалансировка сознательно вне объёма — см. PLAN §8).

Посмотреть живьём:

```bash
make shards               # какие инстансы и что на них
make pg-shard-balance     # сколько строк photos на каждом шарде
make psql-shard0          # и make psql-shard1 — руками убедиться, что
                          #   user_id распределились по хэшу
make demo                 # в конце печатает, на какой шард легло каждое фото
```

### Репликация catalog: primary → replica (логическая)

Каталог денормализован из событий Kafka в ОДНУ базу — и это осознанное
решение проблемы scatter-gather: публичному чтению не нужно знать про шарды
media вообще. А нагрузку чтения несёт реплика:

- `pkg/dbx/db.go`, `OpenWithReplica` — плагин gorm `dbresolver`: сам разводит
  запросы по типу SQL (SELECT → реплика, остальное → primary), а явные
  `db.Clauses(dbresolver.Read / dbresolver.Write)` форсируют выбор
  (`services/catalog/internal/adapters/pg/listing_repository.go`).
- Сама подписка `CREATE SUBSCRIPTION ... PUBLICATION` настраивается
  МИГРАЦИЕЙ: `services/catalog/migrations/auto.go` — логическая репликация
  Postgres (публикация таблицы, отдельный слот). Падение реплики не трогает
  primary; лаг виден в `pg_stat_subscription`.
- `internal/adapters/pg/tx.go`, `WithClaim` — паттерн «claim then process»:
  конкурентные реплики консьюмера не делают двойную работу.

Посмотреть живьём:

```bash
make replication          # статус подписки и лаг реплики
make psql-catalog-replica # убедиться, что данные доехали
make demo                 # печатает отставание реплики в конце
make psql-catalog         # primary: записи идут сюда
```

### Как это связано с остальным

- `wallet` и `order` живут в одиночных базах — им пока хватает (план
  сознательно не расширяет шардирование, PLAN §8);
- кэш Redis перед каталогом снимает часть нагрузки с реплики, а
  HyperLogLog в Redis считает уникальных зрителей, не трогая ClickHouse;
- партиционирование `photo_views` по месяцам со стороны catalog осталось
  как демонстрация range-партиций ([ADR 0009](docs/adr/0009-klyuchi-particionirovaniya.md)).

## События и контракты

- **gRPC** — синхронные команды и запросы, где нужен ответ сейчас
  (`wallet.ReserveFunds`, `catalog.GetListing`).
- **Kafka** — факты, ответа не ждём, слушателей много. Имена топиков —
  `<домен>.<сущность>.<факт>`: `media.photo.uploaded`, `order.order.paid`.
- Любое сообщение едет в **конверте** `gosplash.events.v1.Envelope`:
  `event_id` (uuid v7) для дедупликации, `event_type` для маршрутизации,
  `schema_version` для эволюции, payload — в `Any`.
- Payload'ы Kafka-сообщений тоже protobuf (`proto/gosplash/events/v1`).
- REST-фасады (`GET /listings/...`, `POST /orders`) — ручные HTTP-адаптеры
  поверх тех же сценариев app-слоя; контракт один — protobuf.
- Эволюция контрактов ограждена `buf breaking` в CI: новое поле не ломает
  клиента, переиспользованный номер поля ломает сборку PR.

## Наблюдаемость

Один стек на все сервисы: OTel SDK (`pkg/otelx`) → Collector → **Tempo**
(трейсы) / **Prometheus** (метрики) / **Loki** (логи через promtail), всё
сводится в **Grafana** (http://localhost:53000, дашборд «gosplash overview»).

- Каждая запись лога несёт `trace_id`/`span_id` — slog-хендлер подмешивает
  их из контекста (`pkg/otelx/otelx.go`);
- Спан создаётся на каждой входной точке: HTTP-middleware, gRPC-интерсептор,
  Kafka produce/consume — трейс переживает поездку через топик
  (`traceparent` в заголовках, в том числе через outbox);
- Метрики RED: `http_server_*`, `grpc_server_*`, `kafka_messages_processed_total`,
  `kafka_consumer_lag`, `outbox_pending` + алерты в
  `deploy/observability/alerts.yaml` (p99, lag, outbox);
- `make trace` — взять последний trace_id из логов и открыть в Grafana;
  `make logs-loki` / `make metrics` — то же для логов и метрик;
- pprof поднят на служебном порту каждого сервиса.

## Тесты, линтеры, CI

- Юнит-тесты: table-driven + testify, фейки портов руками (мок-фреймворков
  нет намеренно); сценарии app-слоя тестируются без БД/Kafka.
- Интеграционные: `testcontainers-go` (Postgres, Kafka, Redis), тег
  `//go:build integration`, отдельный CI-job.
- goleak в горутинных пакетах (`pkg/kafkax`, `pkg/outbox`, `pkg/redisx`,
  `pkg/resilience`) — утечки горутин валят тесты.
- `golangci-lint` (конфиг с русскими комментариями о каждом включённом
  линтере) + `tools/archcheck` — оба в CI, оба держат ноль находок.
- CI (`.github/workflows/ci.yml`): матрица модулей из go.work, `go test -race
  -shuffle=on`, интеграции с `-race`, drift-проверка `gen/` (перегенерация
  должна давать пустой diff), `modules-tidy` (дрейф go.mod), advisory-job
  govulncheck, сборка всех бинарников в `build/`.
- Локально: `make test`, `make test-race` (нужен gcc), `make lint`,
  `make ci-local` — повторяет CI-набор.

## Инструменты

| Инструмент | Зачем |
|---|---|
| `tools/automigrate` | накатывает схемы всех баз одной командой (`make migrate`) |
| `tools/archcheck` | статический анализ архитектурных правил, включён в CI |
| `tools/mediactl` | gRPC-клиент к media для ручных проверок |
| `tools/kafka-reprocess` | переливка DLQ → основной топик, с dry-run |
| `tools/search-reindex` | полная переиндексация catalog → ES через переключение alias |

## Документация

| Файл | Роль |
|---|---|
| [docs/PLAN.md](docs/PLAN.md) | источник правды по объёму и порядку работ; все фазы 0–6 закрыты |
| [docs/STYLE.md](docs/STYLE.md) | соглашения кода: комментарии, ошибки, логи, порты, конфигурация |
| [docs/adr/](docs/adr/) | 18 решений «контекст → решение → последствия»: outbox, saga, ClickHouse, ES... |
| [docs/phases/](docs/phases/) | отчёты по фазам (пока только фаза 0) |
| [docs/runbooks/](docs/runbooks/) | действия при инцидентах: DLQ, лаг Kafka/реплики, зависшая сага |
| [TODO.md](TODO.md) | учебные заметки «что потрогать руками» по уже сделанному |

Читать ADR полезно парами с кодом: почти каждое нетривиальное решение в
репозитории имеет комментарий, ссылающийся на ADR, и наоборот.

## Рецепты частых задач

**Изменить контракт (proto)**
`proto/gosplash/<домен>/v1/*.proto` → `make proto` → код в `gen/go`
обновился → правишь адаптеры. `buf breaking` в CI не даст сломать
совместимость; CI также проверяет, что `gen/` не разъехался с контрактами.

**Добавить событие**
Payload в `proto/gosplash/events/v1` → `make proto` → продюсер пишет
`outbox.Write` в той же транзакции (не `Produce` напрямую — archcheck
забракует) → консьюмер: `kafkax.NewConsumer` + `processed_events.Claim`.

**Добавить сервис**
Скелет — `services/analytics` (самый свежий): cmd/main → bootstrap →
domain/ports/app/adapters → свой `go.mod` → строка в `go.work` → миграции в
`migrations/auto.go` + регистрация в `tools/automigrate` → цели в `mk/` →
образ в `deploy/docker/` → строка MODULES в Makefile.

**Прогнать демо фазы**
`make demo` (загрузка), `make demo-kafka-{rebalance,dlq,outage,reprocess}`,
`make demo-redis`, `make demo-wallet`, `make demo-saga-{ok,fail}`,
`make demo-clickhouse`, `make demo-search`, `make demo-0`.

**Посмотреть, что происходит в Kafka**
Kafka UI (http://localhost:58090), `make topic-tail TOPIC=...`,
`make consumer-lag`, `make dlq-tail`.

**Нагрузка и хаос**
`make k6-{upload,catalog,order}`, `make chaos-{kafka,wallet,catalog,slow-pg}`,
результаты — `docs/perf/`; k8s: `make kind-up`, `make k8s-deploy`.

## Известные упрощения

Учебный проект: эти вещи упрощены сознательно, чтобы не заглушить основную
тему. Полный список с обоснованиями — PLAN §8 и ADR.

- **Аутентификации пользователей нет**: `user_id` приходит из заголовка
  `X-User-Id` / параметра. Для gRPC есть JWT-интерсептор (`pkg/grpcx`),
  полноценный auth-сервис вне объёма.
- **TLS выключен** в локальном стенде (gRPC, OTLP, БД — insecure);
 Production-пути (mTLS) не настроены — стенд локальный.
- Продюсер и консьюмеры Kafka в одном процессе с API (relay — горутина,
  а не отдельный деплой).
- Шардирование не расширяется и не ребалансируется; multi-region нет.
- gRPC reflection по умолчанию выключен (`GRPC_REFLECTION_ENABLED=true` —
  для локальной отладки с grpcurl).

## Порты и точки входа

Порты вынесены в диапазон `5xxxx`: на машине разработки 5432/5433/5434 и
9000/9001/9094 уже заняты другими проектами. Внутри docker-сети порты обычные.

| Точка входа | Адрес | Логин |
|---|---|---|
| API (NGINX) | http://localhost:58080 | — |
| Kafka UI | http://localhost:58090 | — |
| RedisInsight | http://localhost:58091 | — |
| MinIO console | http://localhost:59001 | `gosplash` / `gosplash` |
| Temporal UI | http://localhost:58233 | — |
| Grafana | http://localhost:53000 | анонимный вход |
| Prometheus | http://localhost:59090 | — |
| Tempo (API) | http://localhost:53200 | — |
| Loki (API) | http://localhost:53100 | — |
| ClickHouse HTTP | localhost:59440 | `gosplash` / `gosplash` |
| Elasticsearch | localhost:59200 | — |
| PostgreSQL (шарды media) | 55432, 55433 | `gosplash` / `gosplash` |
| PostgreSQL (catalog primary / реплика) | 55434 / 55438 | `gosplash` / `gosplash` |
| PostgreSQL (wallet / order) | 55435 / 55436 | `gosplash` / `gosplash` |
| Kafka (с хоста) | localhost:59094 | — |
| Redis | localhost:56379 | — |
| Temporal gRPC | localhost:57233 | namespace `gosplash` |

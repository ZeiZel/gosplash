# gosplash

Учебный бэкенд-проект: фотосток с продажей лицензий на изображения.
Авторы загружают фото, сервис генерирует превью, покупатели ищут изображения
и покупают лицензии с внутреннего баланса.

Цель проекта — на живой предметной области отработать типовые практики
backend/system design: микросервисы на Go, событийную обработку через Kafka,
шардирование и партиционирование PostgreSQL, кэширование в Redis,
распределённые транзакции через паттерн Saga (Temporal), gRPC-контракты на protobuf.

## Стек

| Область | Технология |
|---|---|
| Язык | Go 1.27, Go Workspaces (монорепозиторий) |
| Хранилище | PostgreSQL 16 (2 инстанса — шарды), GORM |
| Очереди | Apache Kafka (KRaft), `franz-go` |
| Кэш | Redis 7, `go-redis` |
| Оркестрация саг | Temporal, `go.temporal.io/sdk` |
| RPC | gRPC + protobuf, `buf`, `grpc-gateway` для HTTP/JSON |
| Файлы | MinIO (S3-совместимое хранилище) |
| Обработка изображений | `libvips` через `bimg` |
| Вход | NGINX (reverse proxy, балансировка реплик) |
| Наблюдаемость | OpenTelemetry, Tempo, Prometheus, Loki, Grafana |
| Локальная среда | Docker Compose |

## Сервисы

```
NGINX ──► media ──► MinIO
             │ outbox
             ▼
           Kafka: media.photo.uploaded
             │
             ▼
       thumbnail-worker ──► MinIO
             │
             ▼
           Kafka: media.photo.thumbnail-ready ──► catalog (Redis)
                                                     ▲
NGINX ──► order ──► Temporal workflow ───────────────┤ gRPC
                                                     ▼
                                                  wallet
```

### media-service
Приём загрузок (multipart HTTP), сохранение оригинала в MinIO, запись метаданных
в PostgreSQL. Публикует событие `media.photo.uploaded`; в фазе 1 публикация
переедет на **transactional outbox** — событие будет писаться в ту же
транзакцию, что и метаданные, а отдельный relay выгружать его в Kafka.

Таблица `photos` **шардируется по `user_id`** (`fnv32(user_id) % N`).
Все фото автора лежат в одном шарде.

### thumbnail-worker
Kafka-консьюмер (consumer group, несколько реплик). Генерирует превью
small/medium/large в WebP, кладёт в MinIO, публикует
`media.photo.thumbnail-ready`. Реализует retry-топик, DLQ и идемпотентность
через таблицу `processed_events`.

### catalog-service
Публичный read-API: карточка фото, лента, поиск по тегам. Хранит
денормализованный каталог (решает проблему scatter-gather по шардам).

Redis:
- cache-aside для карточек фото и профилей авторов, инвалидация по событиям Kafka;
- refresh-ahead для топ-100 популярных изображений;
- `singleflight` против cache stampede;
- rate limiting загрузок.

Таблица `photo_views` **партиционирована по месяцам** (range), старые партиции
детачатся и удаляются.

### wallet-service
Балансы авторов и покупателей, резервирование и списание средств.
Отдельная БД — участник саги.

### order-service
Покупка лицензии. Реализует **оркестрационную сагу** на Temporal:

1. `wallet.Reserve` — резерв суммы у покупателя
2. `catalog.CreateLicense` — создание лицензии
3. `wallet.Payout` — зачисление доли автору, подтверждение списания
4. публикация `license.purchased`

При ошибке компенсации выполняются в обратном порядке. Второй воркфлоу —
удаление изображения автором (каталог → превью → оригинал → аннулирование
лицензий).

Состоит из двух бинарников: gRPC API (стартует workflow) и Temporal worker
(исполняет workflow и activity). Activity ходят в другие сервисы по gRPC.
Остальные сервисы о Temporal не знают.

## Взаимодействие

- **gRPC** — синхронные команды и запросы, где нужен ответ сейчас
  (`wallet.ReserveFunds`, `catalog.GetListing`).
- **Kafka** — факты, ответа не ждём, слушателей много. Имена топиков —
  `<домен>.<сущность>.<факт>`: `media.photo.uploaded`, `order.order.paid`.
- Любое сообщение едет в **конверте** `gosplash.events.v1.Envelope`:
  `event_id` (uuid v7) для дедупликации, `event_type` для маршрутизации,
  `schema_version` для эволюции, сам payload — в `Any`.
- Снаружи — HTTP/JSON через `grpc-gateway`, сгенерированный из тех же proto.
  Исключение — загрузка файлов, это чистый HTTP-хендлер в media.
- Payload'ы Kafka-сообщений тоже описаны в protobuf
  (`proto/gosplash/events/v1`).

## Структура репозитория

```
gosplash/
├── go.work                     # корневой модуль + services/* + tools/*
├── go.mod                      # module gosplash: pkg/, gen/
├── Makefile                    # `make` без аргументов — список всех команд
│
├── proto/                      # исходники контрактов (не Go-модуль)
│   ├── buf.yaml, buf.gen.yaml  # все buf-команды выполняются ОТСЮДА
│   └── gosplash/<домен>/v1/    # пакет = путь: gosplash.media.v1
│
├── gen/go/                     # сгенерированный код, коммитится
│
├── pkg/                        # общие библиотеки: только инфраструктура
│   ├── config/                 # единственное место, где читается окружение
│   ├── dbx/                    # gorm: шарды (Shards) и реплика (OpenWithReplica)
│   ├── s3x/                    # MinIO
│   ├── httpx/                  # middleware, ошибки, healthz/readyz, shutdown
│   ├── otelx/                  # трейсы, метрики, slog с trace_id
│   ├── kafkax/                 # franz-go: продюсер, консьюмер, конверт, трассировка
│   ├── outbox/                 # таблица + relay          (фаза 1)
│   ├── redisx/                 # кэш, лок, rate limit     (фаза 2)
│   ├── grpcx/                  # interceptors, health     (фаза 3)
│   └── resilience/             # retry, circuit breaker   (фаза 3)
│
├── services/<сервис>/          # свой go.mod у каждого
│   ├── cmd/<сервис>/main.go    # композиция зависимостей — только здесь
│   ├── internal/domain/        # предметная область, без тегов инфраструктуры
│   ├── internal/ports/         # интерфейсы, которыми пользуется app
│   ├── internal/app/           # сценарии использования
│   ├── internal/adapters/      # pg, kafka, redis, grpc, http
│   └── migrations/             # не в internal: сюда ходит tools/automigrate
│
├── deploy/
│   ├── compose/                # docker-compose.yml + стеки по темам
│   ├── observability/          # конфиги collector, tempo, loki, prometheus, grafana
│   ├── nginx/, temporal/
│   ├── helm/, k6/, chaos/      # фаза 6
│
├── docs/
│   ├── PLAN.md                 # ← источник правды по объёму и порядку работ
│   ├── adr/                    # архитектурные решения: контекст → решение → последствия
│   └── phases/, runbooks/, perf/
│
└── tools/
    ├── automigrate/            # единственный, кому можно знать про все сервисы
    └── mediactl/               # gRPC-клиент к media для ручных проверок
```

Раскладка внутри сервиса — **ports & adapters** (гексагональная):

- `domain` не знает ни о чём, кроме себя. Никаких тегов `gorm`, `json`,
  `protobuf`: строка таблицы живёт в `adapters/pg`, представление для
  HTTP — в `adapters/http`, контракт — в `gen/go`;
- `ports` объявляет интерфейсы рядом с тем, кто ими ПОЛЬЗУЕТСЯ, а не рядом
  с реализацией. Это разворачивает зависимость: `app` зависит от своего
  интерфейса, а `adapters/pg` — от `app`;
- `app` содержит сценарии и тестируется без базы, S3 и Kafka. Как это
  выглядит — в `services/media/internal/app/service_test.go`.

Сервисы не импортируют друг друга — только корневой модуль `gosplash`
(контракты и инфраструктурная обвязка). Импорт сгенерированного кода выглядит
как `mediav1 "gosplash/gen/go/gosplash/media/v1"`: удвоение неизбежно, слева
путь Go-модуля, справа namespace protobuf-пакета
([ADR 0001](docs/adr/0001-struktura-repozitoriya.md)).

## Запуск

```bash
make              # список всех команд с описанием
make env          # .env из .env.example
make up           # поднять инфраструктуру
make doctor       # проверить, что каждый компонент отвечает
make migrate      # схемы всех баз + настройка репликации каталога
make run-all      # запустить сервисы (в отдельном терминале)
make demo         # прогнать фичу целиком и посмотреть на результат
```

`make proto` (buf generate → `gen/go`) нужен только после правки контрактов;
сгенерированный код закоммичен. `make tools` ставит `buf` и плагины protoc,
если их ещё нет.

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
| PostgreSQL (шарды media) | 55432, 55433 | `gosplash` / `gosplash` |
| PostgreSQL (catalog primary / реплика) | 55434 / 55438 | `gosplash` / `gosplash` |
| PostgreSQL (wallet / order) | 55435 / 55436 | `gosplash` / `gosplash` |
| Kafka (с хоста) | localhost:59094 | — |
| Redis | localhost:56379 | — |
| Temporal gRPC | localhost:57233 | namespace `gosplash` |

## Что уже работает

Одна фича прошита через весь стек как образец — «загрузка фото»:

```
POST /media/upload  ──►  MinIO                        файл
                    ──►  PostgreSQL шард 0 или 1      метаданные, шард = fnv32(user_id) % 2
                    ──►  Kafka: media.photo.uploaded  тонкое событие в конверте
                                 │
                                 ▼
                          catalog-consumer
                                 ├──► gRPC media.GetPhoto   подробности
                                 └──► catalog primary       денормализованная карточка
                                            │ логическая репликация
                                            ▼
                                     catalog replica  ◄── SELECT'ы публичного API
```

`make demo` прогоняет это целиком и показывает распределение по шардам,
вызов по gRPC, ленту каталога и отставание реплики.
`make demo-0` проверяет фазу 0: готовность, метрики, трейс.

Реализовано: контракты protobuf с конвертом события, gRPC между сервисами,
продюсер и консьюмер Kafka со сквозной трассировкой через топик, GORM с
шардированием и роутингом чтений на реплику, миграции всех баз одной командой,
S3, наблюдаемость (трейсы в Tempo, метрики в Prometheus, логи в Loki, одно
окно в Grafana), liveness/readiness и graceful shutdown.

Чего ещё нет: outbox, retry/DLQ, `processed_events`, Redis, wallet, order,
сага, аналитика и поиск. Порядок, в котором это появляется, — в
[docs/PLAN.md](docs/PLAN.md).

## План реализации

**Источник правды — [docs/PLAN.md](docs/PLAN.md).** Там же аудит текущего
состояния и критерии готовности каждой фазы. Архитектурные решения с
обоснованием — в [docs/adr/](docs/adr/).

| Фаза | Содержание | Статус |
|---|---|---|
| 0 | Структура, контракты, наблюдаемость, health, graceful shutdown, первые тесты | готово |
| 1 | `pkg/kafkax` и `pkg/outbox`, thumbnail-worker, идемпотентный консьюмер, retry/DLQ | — |
| 2 | `pkg/redisx`: cache-aside + singleflight, rate limit, лок, idempotency key | — |
| 3 | gRPC-сервисы wallet и catalog, gateway, circuit breaker, сага на Temporal | — |
| 4 | `analytics` на ClickHouse | — |
| 5 | `search` на Elasticsearch | — |
| 6 | Helm, kind, k6, chaos, runbook'и | — |

[TODO.md](TODO.md) остаётся учебным материалом «что потрогать руками» по уже
сделанному, но планом больше не является.

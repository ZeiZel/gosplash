# Photostock

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
| Язык | Go 1.23+, Go Workspaces (монорепозиторий) |
| Хранилище | PostgreSQL 16 (2 инстанса — шарды), GORM |
| Очереди | Apache Kafka (KRaft), `franz-go` |
| Кэш | Redis 7, `go-redis` |
| Оркестрация саг | Temporal, `go.temporal.io/sdk` |
| RPC | gRPC + protobuf, `buf`, `grpc-gateway` для HTTP/JSON |
| Файлы | MinIO (S3-совместимое хранилище) |
| Обработка изображений | `libvips` через `bimg` |
| Вход | NGINX (reverse proxy, балансировка реплик) |
| Наблюдаемость | OpenTelemetry, Jaeger |
| Локальная среда | Docker Compose |

## Сервисы

```
NGINX ──► media ──► MinIO
             │ outbox
             ▼
           Kafka: image.uploaded
             │
             ▼
       thumbnail-worker ──► MinIO
             │
             ▼
           Kafka: image.processed ──► catalog (Redis)
                                          ▲
NGINX ──► order ──► Temporal workflow ────┤ gRPC
                                          ▼
                                       wallet
```

### media-service
Приём загрузок (multipart HTTP), сохранение оригинала в MinIO, запись метаданных
в PostgreSQL. Публикует событие `image.uploaded` через **outbox pattern**:
событие пишется в той же транзакции, что и метаданные, отдельный relay
выгружает его в Kafka.

Таблица `images` **шардируется по `user_id`** (hash → 4 логических шарда
на 2 инстансах PG). Все фото автора лежат в одном шарде.

### thumbnail-worker
Kafka-консьюмер (consumer group, несколько реплик). Генерирует превью
small/medium/large в WebP, кладёт в MinIO, публикует `image.processed`.
Реализует retry-топик, DLQ и идемпотентность через таблицу `processed_events`.

### catalog-service
Публичный read-API: карточка фото, лента, поиск по тегам. Хранит
денормализованный каталог (решает проблему scatter-gather по шардам).

Redis:
- cache-aside для карточек фото и профилей авторов, инвалидация по событиям Kafka;
- refresh-ahead для топ-100 популярных изображений;
- `singleflight` против cache stampede;
- rate limiting загрузок.

Таблица `image_views` **партиционирована по месяцам** (range), старые партиции
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
  (`wallet.Reserve`, `catalog.GetImage`).
- **Kafka** — факты, ответа не ждём, слушателей много
  (`image.uploaded`, `image.processed`, `license.purchased`).
- Снаружи — HTTP/JSON через `grpc-gateway`, сгенерированный из тех же proto.
  Исключение — загрузка файлов, это чистый HTTP-хендлер в media.
- Payload'ы Kafka-сообщений тоже описаны в protobuf (`proto/events/`).

## Структура репозитория

```
photostock/
├── go.work                    # все модули: pkg/* и services/*
├── docker-compose.yml         # PG×2, Kafka, Redis, MinIO, Temporal, NGINX, Jaeger
├── Makefile
├── buf.yaml / buf.gen.yaml
│
├── proto/                     # исходники контрактов (не Go-модуль)
│   ├── media/v1/
│   ├── catalog/v1/
│   ├── wallet/v1/
│   ├── order/v1/
│   └── events/v1/             # Kafka-события
│
├── pkg/
│   ├── api/                   # сгенерированный код из proto
│   └── common/                # kafka, pg (роутер шардов), redis, grpc interceptors, logger
│
├── services/
│   ├── media/
│   ├── thumbnail-worker/
│   ├── catalog/
│   ├── wallet/
│   └── order/
│       ├── cmd/order/         # gRPC API
│       ├── cmd/worker/        # Temporal worker
│       └── internal/{handler,workflow,activity,repo}
│
└── deploy/
    ├── nginx/
    └── temporal/
```

Каждый сервис — отдельный `go.mod` со структурой `cmd/` + `internal/`
(`handler`, `service`, `repo`) и своими миграциями. Сервисы не импортируют
друг друга — только `pkg/api` и `pkg/common`.

## Запуск

```bash
make proto        # buf generate → pkg/api
make up           # docker compose up -d
make migrate      # миграции всех сервисов
make run-all      # запуск сервисов
```

Точки входа:
- API: `http://localhost:8080`
- Temporal UI: `http://localhost:8233`
- MinIO console: `http://localhost:9001`
- Jaeger: `http://localhost:16686`

## План реализации

1. media-service + MinIO + PostgreSQL + GORM — обычная загрузка
2. Kafka + thumbnail-worker + outbox
3. catalog-service + Redis
4. Партиционирование `image_views`
5. Шардирование `images` (рефакторинг существующего кода)
6. wallet + order: сага руками (таблица состояний + компенсации)
7. Перенос саги на Temporal
8. grpc-gateway, NGINX, OpenTelemetry

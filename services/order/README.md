# order-service

Приём заказов на покупку лицензии с ключом идемпотентности и сага покупки
на Temporal. Фаза 3 в `docs/PLAN.md`, решение — `docs/adr/0004-saga-srazu-na-temporal.md`.

## Два процесса

- `cmd/order` — HTTP (`:8104`) + gRPC (`:9104`) API. Принимает
  `POST /orders`, отвечает клиенту за миллисекунды, запускает сагу
  и уходит — саму сагу не ждёт.
- `cmd/worker` — Temporal worker. Исполняет `PlaceOrderWorkflow` и её
  activities, работает минутами (ретраи с backoff), не отвечает ни одному
  HTTP-клиенту напрямую.

Причина разделения подробно объяснена комментарием в начале
`cmd/order/main.go`: у процессов разная единица масштабирования (число
входящих запросов против числа одновременно исполняющихся саг) и разный
профиль отказа (падение API не трогает уже запущенную сагу — она живёт
в Temporal; падение воркера посреди activity не теряет прогресс саги —
Temporal переигрывает шаг на следующем поднявшемся воркере).

## Идемпотентность HTTP

`POST /orders` требует заголовок `Idempotency-Key`. Реализация и разбор
компромиссов — `internal/adapters/idempotency/store.go` и
`docs/adr/0018-idempotency-http.md`. Коротко: Redis — быстрый путь,
таблица `idempotency_keys` в PostgreSQL — источник правды, потому что
Redis может потерять ключ, а «списать деньги дважды» не должно зависеть
от того, пережил ли кэш рестарт.

## Сага PlaceOrderWorkflow

```
ReserveFunds(wallet) → GrantLicense(catalog) → CommitFunds(wallet) → ConfirmOrder
```

При ошибке любого шага — компенсации в ОБРАТНОМ порядке:
`RevokeLicense(catalog)`, `ReleaseFunds(wallet)`. Реализация —
`internal/adapters/temporal/workflow.go` (оркестрация) и `activities.go`
(всё, что реально ходит в сеть/базу).

## Что Temporal делает за нас

`docs/adr/0004` требует явно перечислить это текстом, со ссылками на код —
вот этот список.

1. **Персистентное состояние воркфлоу вместо таблицы `saga_instances`.**
   В `internal/adapters/pg/model.go` нет ни `saga_instances`, ни
   `reservation_id`/`ledger_entry_ids` в самой таблице `orders` — эти
   значения существуют только как локальные переменные
   `internal/adapters/temporal/workflow.go` (`reserveOut.ReservationID`,
   `grantOut.LicenseID`) и переживают падение процесса не потому, что кто-то
   их сохранил в базу, а потому что Temporal хранит ПОЛНУЮ историю событий
   воркфлоу на своей стороне (`deploy/compose/temporal.yml`, `pg-temporal`).
   Самописная версия потребовала бы отдельной таблицы с полями
   `state`/`step`/`payload`/`compensations` и кода, который её читает
   и пишет на каждом шаге.

2. **Восстановление после падения воркера через реплей истории — и
   вытекающее из него требование детерминизма.** Если `cmd/worker` падает
   между `GrantLicense` и `CommitFunds`, следующий поднявшийся воркер
   (или тот же самый после рестарта) не начинает сагу заново и не теряет
   `reservation_id` — Temporal СНОВА исполняет функцию
   `PlaceOrderWorkflow` (`workflow.go`), но при этом ПОДСТАВЛЯЕТ результаты
   уже выполненных activity из истории вместо того, чтобы вызывать их
   заново. Отсюда требование, зафиксированное комментарием прямо над
   `PlaceOrderWorkflow`: внутри этой функции нет `time.Now()`, `rand`,
   походов в сеть или в базу — весь ввод-вывод вынесен в `activities.go`,
   которые НЕ переигрываются, а вызываются по-настоящему один раз на
   попытку. Самописная версия потребовала бы либо восстанавливать состояние
   вручную по таблице `saga_instances` при старте процесса, либо смириться
   с потерей прогресса на каждом падении воркера.

3. **`RetryPolicy` и таймауты вместо собственного цикла ретраев.**
   `workflow.go` объявляет `forwardActivityOptions` (ограниченные попытки —
   сага обязана в какой-то момент сдаться и откатиться) и
   `criticalActivityOptions` (не ограничены — `ConfirmOrder`,
   `RevokeLicense`, `ReleaseFunds` обязаны рано или поздно пройти).
   Никакого `for { ...; time.Sleep(backoff) }` в проекте нет — вся механика
   попыток, задержек и экспоненциального backoff — параметры одной
   структуры `sdktemporal.RetryPolicy`, и каждая попытка видна в
   `make wf-show` с временем, входом и результатом.

4. **Дедупликация через `workflow_id = order_id`.**
   `internal/adapters/temporal/starter.go`, `Starter.StartPlaceOrder`:
   `client.StartWorkflowOptions{ID: order.ID, WorkflowIDReusePolicy:
   REJECT_DUPLICATE}`. Повторный запуск с тем же `order_id` Temporal
   отклонит сам, без единой строчки кода в этом сервисе, проверяющей
   «а не запущена ли уже такая сага». Самописная версия проверяла бы это
   через `SELECT ... WHERE order_id = ? FOR UPDATE` в той же
   `saga_instances`.

5. **Обратный порядок компенсаций через накопленный список.**
   `workflow.go`, переменная `compensations []func(workflow.Context) error`:
   каждый успешно (или потенциально частично) выполненный шаг добавляет
   свою компенсацию в конец списка, `failOrder` проходит его С КОНЦА.
   Компенсация — НЕ откат: `RevokeLicense`/`ReleaseFunds` пишут НОВЫЕ факты
   (см. `ledger_entries` со стороны wallet), а не стирают старые — оба
   события, «зарезервировали» и «сняли резерв», остаются в истории
   воркфлоу и в `ledger_entries` навсегда. `make wf-show` показывает это
   пошагово.

## Идемпотентность activities

Все activity, которые двигают состояние других сервисов, идемпотентны по
`order_id`:

- `ReserveFunds`/`CommitFunds`/`ReleaseFunds` передают `order_id` как
  `idempotency_key` в `wallet` (`internal/adapters/grpc/wallet_saga.go`,
  контракт — `proto/gosplash/wallet/v1/wallet.proto`).
- `GrantLicense`/`RevokeLicense` используют `order_id` напрямую
  (`internal/adapters/grpc/catalog_saga.go`, контракт —
  `proto/gosplash/catalog/v1/catalog.proto`).

Это критично именно потому, что Temporal ретраит activity при ЛЮБОЙ
сетевой ошибке, включая случай «запрос дошёл и применился, но ответ
потерялся» — без идемпотентности на стороне wallet/catalog повторная
попытка активности удвоила бы эффект.

Транспортная идемпотентность (`pkg/grpcx.Dial` + `resilience.Idempotent` /
`resilience.NotIdempotent`) — отдельный, более узкий вопрос: можно ли
слепо ретраить САМ ВЫЗОВ на уровне gRPC-клиента, до того как Temporal вообще
узнает об ошибке. `GetListing` (чтение) использует `resilience.Idempotent`.
`ReserveFunds`/`CommitFunds`/`ReleaseFunds`/`GrantLicense`/`RevokeLicense`
(мутации) — `resilience.NotIdempotent`: их безопасность при повторе уже
обеспечивает `idempotency_key`/`order_id` на стороне сервиса-получателя
и `RetryPolicy` самой activity, а не слепой ретрай транспорта. Разбор —
комментарии в `internal/adapters/grpc/wallet_saga.go` и `catalog_saga.go`.

## Локальный конфиг — временное отступление

`pkg/config` не имеет секции `Order` (её пока некому туда добавить —
зона ответственности этого агента ограничена `services/order/**`).
`internal/config/config.go` читает `ORDER_DSN`, `ORDER_HTTP_ADDR`,
`ORDER_GRPC_ADDR`, `ORDER_METRICS_ADDR`, `TEMPORAL_ADDRESS`,
`TEMPORAL_NAMESPACE`, `TEMPORAL_TASK_QUEUE`, `IDEMPOTENCY_TTL`,
`WALLET_GRPC_TARGET`, `CATALOG_GRPC_TARGET` напрямую из окружения — это
осознанное и явно прокомментированное нарушение правила
«`os.Getenv` только в `pkg/config`» (docs/STYLE.md). Как только интегратор
перенесёт эти переменные в `pkg/config.Config`, пакет `internal/config`
нужно удалить, а `cmd/order`/`cmd/worker` — перевести на
`config.Load().Order`. Общие настройки (Kafka, Redis, наблюдаемость, gRPC,
outbox) уже берутся из `pkg/config.Load()`.

## Проверка руками

Нужен поднятый docker-compose стенд (`make up`, `make migrate`) и Temporal
(`deploy/compose/temporal.yml`, входит в общий `docker-compose.yml`) —
цели `mk/order.mk` предполагают это, как и аналогичные `demo-*` цели
других фаз. Тесты (`go test ./...`) docker не требуют — все внешние
зависимости заменены подделками портов, как описано в docs/STYLE.md.

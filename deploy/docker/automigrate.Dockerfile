# ─────────────────────────────────────────────────────────────────────────────
# tools/automigrate — образ ДЛЯ KUBERNETES JOB (deploy/kind/jobs/migrate.yaml),
# не для services/*. Формально задание просило Dockerfile "на сервис", но без
# накатанной схемы каждый чарт вечно сидел бы в /readyz=503 (health.Register
# "postgres"/"clickhouse" и т.д. никогда не станет ok на пустой базе) — то
# есть `make kind-up` без этого образа технически "поднимает кластер", но не
# даёт РАБОТАЮЩИЙ демо-стенд. Кладём его сюда же (deploy/docker/), а не
# куда-то отдельно: место определяется тем, что это тоже "образ для
# деплоя", а не тем, что automigrate лежит в tools/, а не в services/.
#
# tools/automigrate — ОТДЕЛЬНЫЙ go-модуль (docs/STYLE.md, раздел
# "Конфигурация": "Единственное исключение — утилиты в tools/"), поэтому
# у него собственный go.mod/go.sum, и приём с локальным replace на
# корневой gosplash здесь ПОЧТИ такой же, как в Dockerfile.media, — только
# зависимость на pkg/dbx и migrations-пакеты сервисов (см. main.go: импорт
# gosplash/services/*/migrations). Это ЕДИНСТВЕННОЕ место в проекте, которому
# разрешено импортировать migrations-пакеты сразу нескольких сервисов
# (см. package doc main.go), поэтому в контекст сборки идут ВСЕ services/*/
# migrations — но НЕ остальной internal/** каждого сервиса: DDL-пакеты
# специально вынесены из internal, чтобы это было возможно без нарушения
# границы "сервисы друг друга не импортируют" (см. docs/STYLE.md, раскладка
# "migrations/ не в internal").
# ─────────────────────────────────────────────────────────────────────────────

FROM golang:1.27 AS builder
WORKDIR /src

COPY go.mod go.sum ./
COPY pkg/ ./pkg/
COPY gen/ ./gen/

# Только то, что реально импортирует tools/automigrate/cmd/automigrate/
# main.go, ПЛЮС транзитивное замыкание внутри каждого сервиса: migrations/
# импортирует internal/adapters/pg (там лежат GORM-модели для AutoMigrate),
# тот в свою очередь — internal/domain и (у catalog/wallet/thumbnail-worker)
# internal/ports. Проверено grep'ом по факту импортов на момент написания
# Dockerfile — это НЕ "весь internal/**", а строго три подпакета из четырёх
# (adapters/pg, domain, ports), без internal/app и остальных internal/
# adapters/* (kafka, grpc, redis, s3) — миграциям они не нужны.
COPY services/media/migrations/                      ./services/media/migrations/
COPY services/media/internal/adapters/pg/             ./services/media/internal/adapters/pg/
COPY services/media/internal/domain/                  ./services/media/internal/domain/

COPY services/thumbnail-worker/migrations/                      ./services/thumbnail-worker/migrations/
COPY services/thumbnail-worker/internal/adapters/pg/             ./services/thumbnail-worker/internal/adapters/pg/
COPY services/thumbnail-worker/internal/domain/                  ./services/thumbnail-worker/internal/domain/
COPY services/thumbnail-worker/internal/ports/                   ./services/thumbnail-worker/internal/ports/

COPY services/catalog/migrations/                      ./services/catalog/migrations/
COPY services/catalog/internal/adapters/pg/             ./services/catalog/internal/adapters/pg/
COPY services/catalog/internal/domain/                  ./services/catalog/internal/domain/
COPY services/catalog/internal/ports/                   ./services/catalog/internal/ports/

COPY services/wallet/migrations/                      ./services/wallet/migrations/
COPY services/wallet/internal/adapters/pg/             ./services/wallet/internal/adapters/pg/
COPY services/wallet/internal/domain/                  ./services/wallet/internal/domain/
COPY services/wallet/internal/ports/                   ./services/wallet/internal/ports/

COPY services/order/migrations/                      ./services/order/migrations/
COPY services/order/internal/adapters/pg/             ./services/order/internal/adapters/pg/
COPY services/order/internal/domain/                  ./services/order/internal/domain/

COPY tools/automigrate/ ./tools/automigrate/

WORKDIR /src/tools/automigrate
RUN go mod edit \
      -require=gosplash@v0.0.0-00010101000000-000000000000 \
      -replace=gosplash=../../

RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/automigrate ./cmd/automigrate

# distroless, не alpine — тот же разбор, что в Dockerfile.media. НЕ :nonroot:
# Job запускается один раз до старта остальных сервисов, ничего постоянно
# не слушает и не хранит секретов дольше своего жизненного цикла — root vs
# nonroot здесь равнозначны по риску, но у Job'а нет причин НЕ соблюдать
# то же правило "непривилегированный пользователь", поэтому :nonroot всё
# равно используется — исключений из общего требования задания нет.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/automigrate /usr/local/bin/automigrate

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/automigrate"]

# ─────────────────────────────────────────────────────────────────────────────
# analytics-service — образ для Kubernetes (фаза 6).
#
# :9106 gRPC (AnalyticsService — публичная витрина отчётов, TopPhotos и
# PhotoStats), :8106 HTTP служебный (/healthz, /readyz — ClickHouse+Kafka
# ping), :8206 служебный (/metrics, /debug/pprof). Публичного REST нет —
# как и у wallet, HTTP-порт этого сервиса Kubernetes использует только для
# проб напрямую по IP пода, поэтому ClusterIP Service для HTTP в чарте не
# заводится (см. deploy/helm/analytics/values.yaml), только headless для gRPC.
#
# Собираем только основной бинарник (cmd/analytics), а не cmd/seed —
# seed-утилита предназначена для разового ручного запуска (`go run` во время
# разработки), а не для деплоя как часть Deployment.
#
# Стратегия сборки монорепы разобрана в deploy/docker/Dockerfile.media.
# ─────────────────────────────────────────────────────────────────────────────

FROM golang:1.27 AS builder
WORKDIR /src

COPY go.mod go.sum ./
COPY pkg/ ./pkg/
COPY gen/ ./gen/
COPY services/analytics/ ./services/analytics/

WORKDIR /src/services/analytics
RUN go mod edit \
      -require=gosplash@v0.0.0-00010101000000-000000000000 \
      -replace=gosplash=../../

RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/analytics ./cmd/analytics

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/analytics /usr/local/bin/analytics

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/analytics"]

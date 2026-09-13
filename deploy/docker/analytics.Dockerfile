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
# Централизованная сборка (один `go build` в ./build снаружи, а не
# builder-стадия в образе) и выбор distroless вместо alpine разобраны
# подробно в deploy/docker/media.Dockerfile.
# ─────────────────────────────────────────────────────────────────────────────

FROM gcr.io/distroless/static-debian12:nonroot

# Бинарник уже собран под linux/$(ARCH) снаружи — см. `make build-linux`
# в корневом Makefile и разбор цены в deploy/docker/media.Dockerfile.
COPY build/analytics /usr/local/bin/analytics

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/analytics"]

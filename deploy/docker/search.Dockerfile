# ─────────────────────────────────────────────────────────────────────────────
# search-service — образ для Kubernetes (фаза 6).
#
# :9107 gRPC (SearchService — публичная витрина поиска), :8107 HTTP
# служебный (/healthz, /readyz), :8207 служебный (/metrics, /debug/pprof).
# Как и analytics/wallet — публичного REST нет, поэтому ClusterIP Service
# для HTTP не заводится, только headless для gRPC (см.
# deploy/helm/search/values.yaml).
#
# Централизованная сборка (один `go build` в ./build снаружи, а не
# builder-стадия в образе) и выбор distroless вместо alpine разобраны
# подробно в deploy/docker/media.Dockerfile.
# ─────────────────────────────────────────────────────────────────────────────

FROM gcr.io/distroless/static-debian12:nonroot

# Бинарник уже собран под linux/$(ARCH) снаружи — см. `make build-linux`
# в корневом Makefile и разбор цены в deploy/docker/media.Dockerfile.
COPY build/search /usr/local/bin/search

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/search"]

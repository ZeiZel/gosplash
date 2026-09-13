# ─────────────────────────────────────────────────────────────────────────────
# catalog-service — образ для Kubernetes (фаза 6).
#
# :8102 HTTP (публичный, читает витрину), :9102 gRPC (межсервисный —
# сюда ходит order за GetListing), :8202 служебный (/metrics, /debug/pprof).
#
# Централизованная сборка (один `go build` в ./build снаружи,
# а не builder-стадия в каждом образе) и выбор distroless вместо
# alpine разобраны подробно в deploy/docker/media.Dockerfile — здесь
# ровно тот же приём, поэтому комментарий не повторяется целиком.
# ─────────────────────────────────────────────────────────────────────────────

FROM gcr.io/distroless/static-debian12:nonroot

# Бинарник уже собран под linux/$(ARCH) снаружи — см. `make build-linux`
# в корневом Makefile и разбор цены в deploy/docker/media.Dockerfile.
COPY build/catalog /usr/local/bin/catalog

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/catalog"]

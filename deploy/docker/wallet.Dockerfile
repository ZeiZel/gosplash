# ─────────────────────────────────────────────────────────────────────────────
# wallet-service — образ для Kubernetes (фаза 6).
#
# :9103 gRPC (единственный вход в деньги — публичного REST у wallet
# намеренно нет, см. package doc services/wallet/cmd/wallet/main.go),
# :8103 HTTP служебный (/healthz, /readyz), :8204 служебный (/metrics,
# /debug/pprof).
#
# Централизованная сборка (один `go build` в ./build снаружи, а не
# builder-стадия в образе) и выбор distroless вместо alpine разобраны
# подробно в deploy/docker/media.Dockerfile.
# ─────────────────────────────────────────────────────────────────────────────

FROM gcr.io/distroless/static-debian12:nonroot

# Бинарник уже собран под linux/$(ARCH) снаружи — см. `make build-linux`
# в корневом Makefile и разбор цены в deploy/docker/media.Dockerfile.
COPY build/wallet /usr/local/bin/wallet

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/wallet"]

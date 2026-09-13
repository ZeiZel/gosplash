# ─────────────────────────────────────────────────────────────────────────────
# thumbnail-worker — образ для Kubernetes (фаза 6).
#
# :8105 HTTP — ТОЛЬКО /healthz и /readyz, публичного API нет (см. package doc
# services/thumbnail-worker/cmd/thumbnail-worker/main.go). :8203 служебный
# (/metrics, /debug/pprof). Порт 8105 в Helm-чарте используется kubelet'ом
# НАПРЯМУЮ для проб (readinessProbe/livenessProbe бьют в IP пода, а не через
# Service) — поэтому у этого сервиса, как и у order-worker, k8s-объекта
# Service не заводится: наружу и другим подам ходить сюда незачем.
#
# Централизованная сборка (один `go build` в ./build снаружи, а не
# builder-стадия в образе) и выбор distroless вместо alpine разобраны
# подробно в deploy/docker/media.Dockerfile.
# ─────────────────────────────────────────────────────────────────────────────

FROM gcr.io/distroless/static-debian12:nonroot

# Бинарник уже собран под linux/$(ARCH) снаружи — см. `make build-linux`
# в корневом Makefile и разбор цены в deploy/docker/media.Dockerfile.
COPY build/thumbnail-worker /usr/local/bin/thumbnail-worker

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/thumbnail-worker"]

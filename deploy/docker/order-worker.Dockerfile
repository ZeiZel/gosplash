# ─────────────────────────────────────────────────────────────────────────────
# order-service, Temporal worker (cmd/worker) — образ для Kubernetes (фаза 6).
#
# У этого процесса НЕТ ни HTTP, ни gRPC порта: он забирает задачи из очереди
# Temporal (см. package doc services/order/cmd/worker/main.go) и ничего не
# слушает сам. Это напрямую определяет Helm-чарт (deploy/helm/order-worker):
# ни Service, ни HTTP-based liveness/readiness probes для него не заводятся —
# подробное обоснование там, в values.yaml.
#
# Почему это отдельный образ, а не ARG/CMD в order.Dockerfile — см.
# комментарий там же. Централизованная сборка (`make build-linux` в
# ./build, а не builder-стадия в образе) разобрана в media.Dockerfile.
# ─────────────────────────────────────────────────────────────────────────────

FROM gcr.io/distroless/static-debian12:nonroot

# Бинарник уже собран под linux/$(ARCH) снаружи — см. `make build-linux`
# в корневом Makefile и разбор цены в deploy/docker/media.Dockerfile.
COPY build/order-worker /usr/local/bin/order-worker

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/order-worker"]

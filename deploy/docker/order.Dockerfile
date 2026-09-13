# ─────────────────────────────────────────────────────────────────────────────
# order-service, HTTP+gRPC API (cmd/order) — образ для Kubernetes (фаза 6).
#
# :8104 HTTP (публичный — приём заказа с ключом идемпотентности), :9104 gRPC
# (межсервисный), :8205 служебный (/metrics, /debug/pprof).
#
# НЕОЧЕВИДНОЕ РЕШЕНИЕ: это ДВА РАЗНЫХ ОБРАЗА для одного go-модуля
# gosplash/services/order — этот (cmd/order, API) и deploy/docker/
# order-worker.Dockerfile (cmd/worker, Temporal worker). Дублирование
# builder-стадии — прямое следствие решения services/order/cmd/order/main.go
# (см. его package doc): это два процесса с разными профилями масштабирования
# и отказа, и в Kubernetes они обязаны быть двумя разными Deployment
# с разным числом реплик — а значит, и двумя разными образами, у каждого
# свой ENTRYPOINT. Слить их в один образ с двумя бинарниками и выбором через
# ARG/CMD усложнило бы Helm-чарт (два Deployment ссылались бы на один
# image ARG-ом, который легко перепутать при релизе) ради экономии
# нескольких строк Dockerfile — цена того не стоит.
#
# Централизованная сборка (один `go build` в ./build снаружи, а не
# builder-стадия в образе) и выбор distroless вместо alpine разобраны
# подробно в deploy/docker/media.Dockerfile.
# ─────────────────────────────────────────────────────────────────────────────

FROM gcr.io/distroless/static-debian12:nonroot

# Бинарник уже собран под linux/$(ARCH) снаружи — см. `make build-linux`
# в корневом Makefile и разбор цены в deploy/docker/media.Dockerfile.
COPY build/order /usr/local/bin/order

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/order"]

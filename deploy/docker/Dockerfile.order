# ─────────────────────────────────────────────────────────────────────────────
# order-service, HTTP+gRPC API (cmd/order) — образ для Kubernetes (фаза 6).
#
# :8104 HTTP (публичный — приём заказа с ключом идемпотентности), :9104 gRPC
# (межсервисный), :8205 служебный (/metrics, /debug/pprof).
#
# НЕОЧЕВИДНОЕ РЕШЕНИЕ: это ДВА РАЗНЫХ ОБРАЗА для одного go-модуля
# gosplash/services/order — этот (cmd/order, API) и deploy/docker/
# Dockerfile.order-worker (cmd/worker, Temporal worker). Дублирование
# builder-стадии — прямое следствие решения services/order/cmd/order/main.go
# (см. его package doc): это два процесса с разными профилями масштабирования
# и отказа, и в Kubernetes они обязаны быть двумя разными Deployment
# с разным числом реплик — а значит, и двумя разными образами, у каждого
# свой ENTRYPOINT. Слить их в один образ с двумя бинарниками и выбором через
# ARG/CMD усложнило бы Helm-чарт (два Deployment ссылались бы на один
# image ARG-ом, который легко перепутать при релизе) ради экономии
# нескольких строк Dockerfile — цена того не стоит.
#
# Стратегия сборки монорепы разобрана в deploy/docker/Dockerfile.media.
# ─────────────────────────────────────────────────────────────────────────────

FROM golang:1.27 AS builder
WORKDIR /src

COPY go.mod go.sum ./
COPY pkg/ ./pkg/
COPY gen/ ./gen/
COPY services/order/ ./services/order/

WORKDIR /src/services/order
RUN go mod edit \
      -require=gosplash@v0.0.0-00010101000000-000000000000 \
      -replace=gosplash=../../

RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/order ./cmd/order

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/order /usr/local/bin/order

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/order"]

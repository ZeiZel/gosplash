# ─────────────────────────────────────────────────────────────────────────────
# order-service, Temporal worker (cmd/worker) — образ для Kubernetes (фаза 6).
#
# У этого процесса НЕТ ни HTTP, ни gRPC порта: он забирает задачи из очереди
# Temporal (см. package doc services/order/cmd/worker/main.go) и ничего не
# слушает сам. Это напрямую определяет Helm-чарт (deploy/helm/order-worker):
# ни Service, ни HTTP-based liveness/readiness probes для него не заводятся —
# подробное обоснование там, в values.yaml.
#
# Почему это отдельный образ, а не ARG/CMD в Dockerfile.order — см.
# комментарий там же. Стратегия сборки монорепы — в Dockerfile.media.
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
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/order-worker ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/order-worker /usr/local/bin/order-worker

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/order-worker"]

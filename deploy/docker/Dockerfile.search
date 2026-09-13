# ─────────────────────────────────────────────────────────────────────────────
# search-service — образ для Kubernetes (фаза 6).
#
# :9107 gRPC (SearchService — публичная витрина поиска), :8107 HTTP
# служебный (/healthz, /readyz), :8207 служебный (/metrics, /debug/pprof).
# Как и analytics/wallet — публичного REST нет, поэтому ClusterIP Service
# для HTTP не заводится, только headless для gRPC (см.
# deploy/helm/search/values.yaml).
#
# Стратегия сборки монорепы разобрана в deploy/docker/Dockerfile.media.
# ─────────────────────────────────────────────────────────────────────────────

FROM golang:1.27 AS builder
WORKDIR /src

COPY go.mod go.sum ./
COPY pkg/ ./pkg/
COPY gen/ ./gen/
COPY services/search/ ./services/search/

WORKDIR /src/services/search
RUN go mod edit \
      -require=gosplash@v0.0.0-00010101000000-000000000000 \
      -replace=gosplash=../../

RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/search ./cmd/search

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/search /usr/local/bin/search

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/search"]

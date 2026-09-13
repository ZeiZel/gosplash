# ─────────────────────────────────────────────────────────────────────────────
# wallet-service — образ для Kubernetes (фаза 6).
#
# :9103 gRPC (единственный вход в деньги — публичного REST у wallet
# намеренно нет, см. package doc services/wallet/cmd/wallet/main.go),
# :8103 HTTP служебный (/healthz, /readyz), :8204 служебный (/metrics,
# /debug/pprof).
#
# Стратегия сборки монорепы разобрана в deploy/docker/Dockerfile.media.
# ─────────────────────────────────────────────────────────────────────────────

FROM golang:1.27 AS builder
WORKDIR /src

COPY go.mod go.sum ./
COPY pkg/ ./pkg/
COPY gen/ ./gen/
COPY services/wallet/ ./services/wallet/

WORKDIR /src/services/wallet
RUN go mod edit \
      -require=gosplash@v0.0.0-00010101000000-000000000000 \
      -replace=gosplash=../../

RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/wallet ./cmd/wallet

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/wallet /usr/local/bin/wallet

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/wallet"]

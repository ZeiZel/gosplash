# ─────────────────────────────────────────────────────────────────────────────
# catalog-service — образ для Kubernetes (фаза 6).
#
# :8102 HTTP (публичный, читает витрину), :9102 gRPC (межсервисный —
# сюда ходит order за GetListing), :8202 служебный (/metrics, /debug/pprof).
#
# Стратегия сборки монорепы (replace вместо go.work, distroless вместо
# alpine) разобрана подробно в deploy/docker/Dockerfile.media — здесь
# ровно тот же приём, поэтому комментарий не повторяется целиком.
# ─────────────────────────────────────────────────────────────────────────────

FROM golang:1.27 AS builder
WORKDIR /src

COPY go.mod go.sum ./
COPY pkg/ ./pkg/
COPY gen/ ./gen/
COPY services/catalog/ ./services/catalog/

WORKDIR /src/services/catalog
RUN go mod edit \
      -require=gosplash@v0.0.0-00010101000000-000000000000 \
      -replace=gosplash=../../

RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/catalog ./cmd/catalog

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/catalog /usr/local/bin/catalog

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/catalog"]

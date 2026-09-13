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
# Стратегия сборки монорепы разобрана в deploy/docker/Dockerfile.media.
# ─────────────────────────────────────────────────────────────────────────────

FROM golang:1.27 AS builder
WORKDIR /src

COPY go.mod go.sum ./
COPY pkg/ ./pkg/
COPY gen/ ./gen/
COPY services/thumbnail-worker/ ./services/thumbnail-worker/

WORKDIR /src/services/thumbnail-worker
RUN go mod edit \
      -require=gosplash@v0.0.0-00010101000000-000000000000 \
      -replace=gosplash=../../

RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/thumbnail-worker ./cmd/thumbnail-worker

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/thumbnail-worker /usr/local/bin/thumbnail-worker

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/thumbnail-worker"]

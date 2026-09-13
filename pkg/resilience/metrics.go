package resilience

import (
	"context"
	"log/slog"
	"sync"

	"github.com/sony/gobreaker/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meter — тот же otel.Meter, что и везде в проекте (см. pkg/httpx.Metrics):
// значения уходят в глобальный MeterProvider, который поднимает pkg/otelx,
// и наружу отдаются pull-моделью через /metrics. Если otelx.Setup ещё не
// вызван (например, в юнит-тестах пакета) — otel отдаёт no-op метр, и вызовы
// ниже просто ничего не делают, без паники и без ошибок.
var meter = otel.Meter("gosplash/resilience")

var (
	retryAttemptsTotal   metric.Int64Counter
	breakerRequestsTotal metric.Int64Counter
)

func init() {
	var err error

	retryAttemptsTotal, err = meter.Int64Counter(
		"retry_attempts_total",
		metric.WithDescription("Попытки retry по методам и результату (success/non_retryable/deadline/ctx_done/attempts_exhausted)"),
	)
	if err != nil {
		slog.Error("resilience: метрика retry_attempts_total", "error", err)
	}

	breakerRequestsTotal, err = meter.Int64Counter(
		"circuit_breaker_requests_total",
		metric.WithDescription("Запросы через circuit breaker по имени и результату (success/failure/open)"),
	)
	if err != nil {
		slog.Error("resilience: метрика circuit_breaker_requests_total", "error", err)
	}

	// ObservableGauge, а не обычный счётчик: состояние breaker — это не то,
	// что накапливается, а то, что ЕСТЬ прямо сейчас в момент скрейпа.
	// Callback читает текущее состояние всех живых breaker'ов из registry
	// ниже, поэтому отдельно вызывать Set() при каждом переходе не нужно.
	_, err = meter.Int64ObservableGauge(
		"circuit_breaker_state",
		metric.WithDescription("Состояние circuit breaker: 0 closed / 1 half-open / 2 open"),
		metric.WithInt64Callback(observeBreakerStates),
	)
	if err != nil {
		slog.Error("resilience: метрика circuit_breaker_state", "error", err)
	}
}

// registry живых breaker'ов — только для метрики состояния. Breaker'ы
// создаются по одному на зависимость на старте сервиса и живут до конца
// процесса, поэтому забывать записи из registry не нужно: их количество
// равно количеству вызываемых по gRPC сервисов, а не запросов.
var (
	breakerRegistryMu sync.Mutex
	breakerRegistry   = map[string]*Breaker{}
)

func registerBreaker(b *Breaker) {
	breakerRegistryMu.Lock()
	defer breakerRegistryMu.Unlock()
	breakerRegistry[b.name] = b
}

func observeBreakerStates(_ context.Context, o metric.Int64Observer) error {
	breakerRegistryMu.Lock()
	defer breakerRegistryMu.Unlock()
	for name, b := range breakerRegistry {
		o.Observe(stateValue(b.State()), metric.WithAttributes(attribute.String("name", name)))
	}
	return nil
}

func stateValue(s gobreaker.State) int64 {
	switch s {
	case gobreaker.StateClosed:
		return 0
	case gobreaker.StateHalfOpen:
		return 1
	case gobreaker.StateOpen:
		return 2
	default:
		return -1
	}
}

func recordRetryAttempt(ctx context.Context, method, result string) {
	retryAttemptsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("method", method),
		attribute.String("result", result),
	))
}

func recordBreakerRequest(name, result string) {
	breakerRequestsTotal.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("name", name),
		attribute.String("result", result),
	))
}

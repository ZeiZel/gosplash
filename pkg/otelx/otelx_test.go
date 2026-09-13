package otelx

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

// Этот файл существует из-за конкретной аварии, и её стоит знать.
//
// В первой версии пакета resource собирался так:
//
//	resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL, ...))
//
// resource.Merge отказывается сливать ресурсы с разными непустыми Schema URL,
// а resource.Default() помечен той версией semconv, с которой собран SDK.
// Стоило обновить go.opentelemetry.io/otel/sdk — и Setup начал возвращать
// «conflicting Schema URL» У ВСЕХ СЕМИ СЕРВИСОВ сразу, то есть ни один
// из них не поднимался.
//
// Ни один тест этого не поймал, потому что Setup не вызывал НИКТО: пакет был
// покрыт тестами ровно в тех местах, которые тестировать легко. Урок ровно
// такой: функция, которую вызывает каждый main.go первой строкой, обязана
// иметь тест, который её действительно вызывает, — даже если внутри «просто
// конфигурация».

func TestSetup_PodnimaetsyaBezKollektora(t *testing.T) {
	// Пустой OTLPEndpoint — экспортёр трейсов не создаётся, в сеть никто
	// не ходит. Всё остальное (resource, метрики, пропагатор, логгер)
	// настраивается по-настоящему, и именно там жила авария.
	shutdown, err := Setup(context.Background(), Config{
		ServiceName:      "test-service",
		Version:          "0.1.0",
		Environment:      "test",
		OTLPEndpoint:     "",
		TraceSampleRatio: 1.0,
		LogLevel:         "debug",
	})

	require.NoError(t, err, "Setup обязан подниматься без коллектора")
	require.NotNil(t, shutdown)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	assert.NoError(t, shutdown(ctx))
}

func TestBuildResource_NeKonfliktuetPoSchemaURL(t *testing.T) {
	// Прямая проверка того самого места. resource.Default() приносит свой
	// Schema URL; наш ресурс обязан слиться с ним при ЛЮБОЙ версии SDK.
	res, err := buildResource(Config{
		ServiceName: "test-service",
		Version:     "0.1.0",
		Environment: "test",
	})

	require.NoError(t, err)
	require.NotNil(t, res)

	attrs := map[string]string{}
	for _, kv := range res.Attributes() {
		attrs[string(kv.Key)] = kv.Value.String()
	}

	assert.Equal(t, "test-service", attrs["service.name"],
		"service.name — то, по чему трейсы и метрики группируются в Grafana")
	assert.Equal(t, "0.1.0", attrs["service.version"])
	assert.Equal(t, "test", attrs["deployment.environment"])
}

func TestSetupLogger_UrovenIzKonfiga(t *testing.T) {
	tests := []struct {
		name  string
		level string
		want  slog.Level
	}{
		{"debug", "debug", slog.LevelDebug},
		{"info", "info", slog.LevelInfo},
		{"warn", "warn", slog.LevelWarn},
		{"error", "error", slog.LevelError},
		// Пустая строка и мусор не должны оставлять сервис без логов вовсе.
		{"пусто — info", "", slog.LevelInfo},
		{"мусор — info", "болтливый", slog.LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setupLogger(Config{ServiceName: "test", LogLevel: tt.level})

			logger := slog.Default()
			assert.True(t, logger.Enabled(context.Background(), tt.want),
				"уровень %s должен быть включён", tt.want)
			if tt.want > slog.LevelDebug {
				assert.False(t, logger.Enabled(context.Background(), tt.want-4),
					"уровень ниже %s должен быть отключён", tt.want)
			}
		})
	}
}

func TestSetup_PropagatorNastroen(t *testing.T) {
	// Без пропагатора trace-контекст не уезжает ни в HTTP-заголовках, ни в
	// заголовках Kafka, и сквозной трейс (главный критерий фазы 0) молча
	// распадается на куски.
	shutdown, err := Setup(context.Background(), Config{
		ServiceName: "test-service", TraceSampleRatio: 1.0,
	})
	require.NoError(t, err)
	defer func() { _ = shutdown(context.Background()) }()

	fields := otel.GetTextMapPropagator().Fields()
	assert.Contains(t, fields, "traceparent", "W3C trace context обязателен")
}

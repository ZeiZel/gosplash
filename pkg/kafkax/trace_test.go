package kafkax

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestHeaderCarrier_ChitaetIPishet(t *testing.T) {
	headers := []kgo.RecordHeader{{Key: "event_id", Value: []byte("e-1")}}
	carrier := headerCarrier{headers: &headers}

	assert.Equal(t, "e-1", carrier.Get("event_id"))
	assert.Empty(t, carrier.Get("нет-такого"))

	carrier.Set("traceparent", "00-aaa-bbb-01")
	assert.Equal(t, "00-aaa-bbb-01", carrier.Get("traceparent"))
	assert.Len(t, headers, 2, "новый ключ должен добавиться")

	// Повторный Set обязан ЗАМЕНИТЬ значение, а не добавить второй заголовок
	// с тем же именем: иначе консьюмер прочитает первый попавшийся и может
	// продолжить не тот трейс.
	carrier.Set("traceparent", "00-ccc-ddd-01")
	assert.Equal(t, "00-ccc-ddd-01", carrier.Get("traceparent"))
	assert.Len(t, headers, 2, "существующий ключ должен перезаписаться")

	assert.ElementsMatch(t, []string{"event_id", "traceparent"}, carrier.Keys())
}

func TestTraceContext_PerezhivaetKafka(t *testing.T) {
	// Самый важный тест пакета: он проверяет, что трейс НЕ РВЁТСЯ на границе
	// топика. Между продюсером и консьюмером нет ни запроса, ни ответа —
	// связь держится исключительно на заголовке traceparent.
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	))

	// Сторона продюсера.
	producerCtx, span := otel.Tracer("test").Start(context.Background(), "publish")
	defer span.End()
	wantTraceID := span.SpanContext().TraceID()
	require.True(t, wantTraceID.IsValid())

	record := &kgo.Record{Topic: "media.photo.uploaded"}
	injectTrace(producerCtx, record)

	// Сообщение «уехало»: у консьюмера нет ничего, кроме заголовков.
	consumerCtx := extractTrace(context.Background(), record)
	got := trace.SpanContextFromContext(consumerCtx)

	require.True(t, got.IsValid(), "консьюмер не увидел trace-контекст — трейс порвался")
	assert.Equal(t, wantTraceID, got.TraceID(),
		"консьюмер должен продолжить ТОТ ЖЕ трейс, а не начать свой")
}

func TestExtractTrace_BezZagolovkaNePadaet(t *testing.T) {
	// Сообщение могло быть записано инструментом без трассировки
	// (kafka-console-producer, старая версия сервиса). Это не ошибка:
	// консьюмер просто начнёт новый трейс.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	ctx := extractTrace(context.Background(), &kgo.Record{Topic: "t"})
	assert.False(t, trace.SpanContextFromContext(ctx).IsValid())
}

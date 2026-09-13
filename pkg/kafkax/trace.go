package kafkax

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Трассировка через Kafka.
//
// Сама по себе она НЕ работает. HTTP-клиент кладёт заголовок traceparent
// в запрос, и сервер его подхватывает, — но между продюсером и консьюмером
// нет ни запроса, ни ответа: есть запись в логе, которую кто-то когда-то
// прочитает. Если не положить trace-контекст в заголовки сообщения руками,
// трейс оборвётся на границе топика, и цепочка «загрузка → превью → каталог»
// распадётся на три несвязанных трейса.
//
// Здесь же проходит граница между parent-child и span link. Мы связываем
// консьюмера как ПОТОМКА продюсера (parent-child): в проекте события
// обрабатываются вскоре после публикации, и один трейс на всю цепочку — это
// ровно то, что хочется видеть. Span link уместнее, когда связь есть, но
// «один родитель» неверно: например, консьюмер собрал батч из ста сообщений
// от ста разных запросов — тогда у его спана сто ссылок и ни одного родителя.

// headerCarrier — мост между заголовками Kafka и пропагатором OTel.
// Пропагатор умеет работать с любым «набором пар строк», ему всё равно,
// HTTP это или Kafka; надо лишь показать ему, как читать и писать.
type headerCarrier struct {
	headers *[]kgo.RecordHeader
}

func (c headerCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// injectTrace кладёт traceparent текущего спана в заголовки записи.
func injectTrace(ctx context.Context, record *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &record.Headers})
}

// extractTrace достаёт trace-контекст из заголовков сообщения.
func extractTrace(ctx context.Context, record *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: &record.Headers})
}

// startProduceSpan открывает спан на публикацию.
//
// Имена и атрибуты — по семантическим соглашениям OTel для messaging:
// "<topic> publish". Соблюдать их стоит не из аккуратности, а потому что
// готовые дашборды и правила в Grafana ищут именно эти атрибуты.
func startProduceSpan(ctx context.Context, topic, eventType string) (context.Context, trace.Span) {
	return otel.Tracer("gosplash/kafkax").Start(ctx, topic+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation", "publish"),
			attribute.String("gosplash.event_type", eventType),
		),
	)
}

// startConsumeSpan открывает спан на обработку одного сообщения, продолжая
// трейс продюсера.
func startConsumeSpan(ctx context.Context, record *kgo.Record, eventType, eventID string) (context.Context, trace.Span) {
	ctx = extractTrace(ctx, record)
	return otel.Tracer("gosplash/kafkax").Start(ctx, record.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.source.name", record.Topic),
			attribute.String("messaging.operation", "process"),
			attribute.Int("messaging.kafka.partition", int(record.Partition)),
			attribute.Int64("messaging.kafka.offset", record.Offset),
			attribute.String("gosplash.event_type", eventType),
			attribute.String("gosplash.event_id", eventID),
		),
	)
}

var _ propagation.TextMapCarrier = headerCarrier{}

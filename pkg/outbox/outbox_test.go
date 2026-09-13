package outbox

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/protobuf/proto"

	"gosplash/pkg/kafkax"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

func testEnvelope(t *testing.T) *eventsv1.Envelope {
	t.Helper()
	env, err := kafkax.NewEnvelope(kafkax.EventPhotoUploaded, "photo-1", &eventsv1.PhotoUploaded{
		PhotoId: "photo-1",
		UserId:  42,
	})
	require.NoError(t, err)
	return env
}

func TestBuildRow_ZapolnyaetPolya(t *testing.T) {
	env := testEnvelope(t)

	row, err := buildRow(context.Background(), "media", env, kafkax.TopicPhotoUploaded)
	require.NoError(t, err)

	assert.NotEmpty(t, row.ID)
	assert.Equal(t, "photo-1", row.AggregateID)
	assert.Equal(t, kafkax.TopicPhotoUploaded, row.Topic)
	assert.Equal(t, []byte("photo-1"), row.Key)
	assert.NotEmpty(t, row.Payload)
	assert.Nil(t, row.PublishedAt, "новая строка ещё не опубликована")

	// Payload обязан разбираться обратно в тот же конверт: relay публикует
	// его в Kafka как есть, без повторной сборки.
	var decoded eventsv1.Envelope
	require.NoError(t, proto.Unmarshal(row.Payload, &decoded))
	assert.Equal(t, env.GetEventId(), decoded.GetEventId())
}

func TestBuildRow_HeadersSoderzhatEventMetadannye(t *testing.T) {
	env := testEnvelope(t)

	row, err := buildRow(context.Background(), "media", env, kafkax.TopicPhotoUploaded)
	require.NoError(t, err)

	var headers map[string]string
	require.NoError(t, json.Unmarshal(row.Headers, &headers))

	assert.Equal(t, env.GetEventId(), headers[kafkax.HeaderEventID])
	assert.Equal(t, env.GetEventType(), headers[kafkax.HeaderEventType])
}

func TestBuildRow_ProbrasyvaetTraceparentIzKontexta(t *testing.T) {
	// Ключевое свойство: traceparent фиксируется в момент Write, а не
	// вычисляется relay'ем позже (см. комментарий в outbox.go).
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample())))

	ctx, span := otel.Tracer("test").Start(context.Background(), "http request")
	defer span.End()

	row, err := buildRow(ctx, "media", testEnvelope(t), kafkax.TopicPhotoUploaded)
	require.NoError(t, err)

	var headers map[string]string
	require.NoError(t, json.Unmarshal(row.Headers, &headers))
	assert.NotEmpty(t, headers[kafkax.HeaderTraceparent], "traceparent должен попасть в headers строки")
}

func TestBuildRow_BezActivnogoSpanaNePadaet(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	row, err := buildRow(context.Background(), "media", testEnvelope(t), kafkax.TopicPhotoUploaded)
	require.NoError(t, err)

	var headers map[string]string
	require.NoError(t, json.Unmarshal(row.Headers, &headers))
	_, hasTraceparent := headers[kafkax.HeaderTraceparent]
	assert.False(t, hasTraceparent, "без активного спана заголовка быть не должно")
}

func TestNewRowID_Unikalen(t *testing.T) {
	const n = 500
	seen := make(map[string]struct{}, n)
	for range n {
		id := newRowID()
		_, dup := seen[id]
		require.False(t, dup, "повторный id: %s", id)
		seen[id] = struct{}{}
	}
}

func TestBuildRow_ProducerFiksiruetsyaPriZapisi(t *testing.T) {
	// Атрибуция события — это «кто его породил», а не «кто его доставил».
	// Заголовок producer проставляется здесь, в момент записи в outbox;
	// relay его больше не перетирает (см. hasHeader в relay.go). Без этого
	// в gosplash врала бы атрибуция всех событий thumbnail-worker: он делит
	// шарды с media, и раньше их строки мог увезти чужой relay.
	row, err := buildRow(context.Background(), "thumbnail-worker", testEnvelope(t), kafkax.TopicPhotoThumbnailReady)
	require.NoError(t, err)

	var headers map[string]string
	require.NoError(t, json.Unmarshal(row.Headers, &headers))
	assert.Equal(t, "thumbnail-worker", headers[kafkax.HeaderProducer])
}

func TestTableFor(t *testing.T) {
	// У каждого сервиса своя очередь, даже когда база общая.
	assert.Equal(t, "outbox_media", TableFor("media"))
	assert.NotEqual(t, TableFor("media"), TableFor("thumbnail-worker"))
}

func TestTableFor_DefisStanovitsyaPodcherkivaniem(t *testing.T) {
	// Регрессия на настоящую аварию: имя "thumbnail-worker" давало таблицу
	// outbox_thumbnail-worker, и PostgreSQL читал дефис как оператор
	// вычитания — relay падал с «syntax error at or near "-"» на каждом
	// тике. Компиляция и все тесты при этом были зелёными: имя таблицы
	// попадает в сырой SQL строкой, и проверить его может только база.
	assert.Equal(t, "outbox_thumbnail_worker", TableFor("thumbnail-worker"))

	// Заодно — защита от подстановки чего угодно в SQL: имя таблицы нельзя
	// передать параметром, поэтому единственная гарантия здесь.
	assert.Equal(t, "outbox_drop_table_users___", TableFor("drop table users;--"))
	assert.Equal(t, "outbox_media", TableFor("Media"))
}

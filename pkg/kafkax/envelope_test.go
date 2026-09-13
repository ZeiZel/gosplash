package kafkax

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

func TestNewEnvelope_ZapolnyaetMetadannye(t *testing.T) {
	before := time.Now().UnixMilli()

	env, err := NewEnvelope(EventPhotoUploaded, "photo-1", &eventsv1.PhotoUploaded{
		PhotoId: "photo-1",
		UserId:  42,
	})
	require.NoError(t, err)

	assert.NotEmpty(t, env.GetEventId())
	assert.Equal(t, EventPhotoUploaded, env.GetEventType())
	assert.Equal(t, "photo-1", env.GetAggregateId())
	assert.Equal(t, int32(1), env.GetSchemaVersion())
	assert.GreaterOrEqual(t, env.GetOccurredAtUnixMs(), before)
}

func TestNewEnvelope_EventIDUnikalen(t *testing.T) {
	// Дедупликация в консьюмере держится на уникальности event_id. Если бы
	// генератор выдавал повторы, второе событие молча пропадало бы как
	// «уже обработанное» — и найти такое в проде почти невозможно.
	const n = 1000
	seen := make(map[string]struct{}, n)

	for range n {
		env, err := NewEnvelope(EventPhotoUploaded, "photo-1", &eventsv1.PhotoUploaded{PhotoId: "photo-1"})
		require.NoError(t, err)

		_, duplicate := seen[env.GetEventId()]
		require.False(t, duplicate, "повторный event_id: %s", env.GetEventId())
		seen[env.GetEventId()] = struct{}{}
	}
}

func TestNewEnvelope_EventIDRastyot(t *testing.T) {
	// UUID v7 монотонно растёт: первые 48 бит — это время. Свойство нужно
	// не для красоты, а чтобы вставка в PostgreSQL шла в «правый край»
	// B-tree, а не в случайную страницу.
	first, err := NewEnvelope(EventPhotoUploaded, "a", &eventsv1.PhotoUploaded{})
	require.NoError(t, err)

	time.Sleep(2 * time.Millisecond)

	second, err := NewEnvelope(EventPhotoUploaded, "a", &eventsv1.PhotoUploaded{})
	require.NoError(t, err)

	assert.Less(t, first.GetEventId(), second.GetEventId(),
		"event_id должен расти лексикографически вместе со временем")
}

func TestUnmarshalPayload_TudaObratno(t *testing.T) {
	original := &eventsv1.PhotoUploaded{PhotoId: "photo-7", UserId: 7}

	env, err := NewEnvelope(EventPhotoUploaded, original.GetPhotoId(), original)
	require.NoError(t, err)

	// Через сериализацию: именно в таком виде сообщение доедет до консьюмера.
	raw, err := proto.Marshal(env)
	require.NoError(t, err)

	var decoded eventsv1.Envelope
	require.NoError(t, proto.Unmarshal(raw, &decoded))

	var payload eventsv1.PhotoUploaded
	require.NoError(t, UnmarshalPayload(&decoded, &payload))

	assert.Equal(t, "photo-7", payload.GetPhotoId())
	assert.Equal(t, int64(7), payload.GetUserId())
}

func TestUnmarshalPayload_ChuzhoyTipOtvergaetsya(t *testing.T) {
	// Главная защита, которую даёт Any: попытка прочитать событие не того
	// типа — ошибка, а не молча заполненная мусором структура. Без Any
	// protobuf разобрал бы чужие байты «успешно».
	env, err := NewEnvelope(EventPhotoUploaded, "photo-1", &eventsv1.PhotoUploaded{PhotoId: "photo-1"})
	require.NoError(t, err)

	var wrong eventsv1.PhotoDeleted
	err = UnmarshalPayload(env, &wrong)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "распаковка")
}

func TestUnmarshalPayload_PustoyPayload(t *testing.T) {
	var payload eventsv1.PhotoUploaded
	err := UnmarshalPayload(&eventsv1.Envelope{EventId: "e1", EventType: "x"}, &payload)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "пустой payload")
}

func TestOccurredAt(t *testing.T) {
	env := &eventsv1.Envelope{OccurredAtUnixMs: 1_700_000_000_123}
	assert.Equal(t, int64(1_700_000_000_123), OccurredAt(env).UnixMilli())
}

func TestRetryIDLQTopic(t *testing.T) {
	assert.Equal(t, "media.photo.uploaded.retry", RetryTopic(TopicPhotoUploaded))
	assert.Equal(t, "media.photo.uploaded.dlq", DLQTopic(TopicPhotoUploaded))
}

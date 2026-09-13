package kafkax

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// PATTERN: event envelope — метаданные события отделены от его содержимого.
//
// В топик всегда пишется Envelope, а не payload напрямую. Зачем — подробно
// расписано в proto/gosplash/events/v1/events.proto; коротко: дедупликация по
// event_id без разбора payload'а, несколько типов событий в одном топике,
// осмысленный DLQ для битых сообщений и версионирование схемы.

// NewEnvelope собирает конверт вокруг payload'а.
//
// aggregateID — это и ключ сообщения в Kafka, и идентификатор сущности,
// с которой произошёл факт. Одно значение в двух ролях намеренно: так
// невозможно случайно отправить события одного фото в разные партиции и
// получить их в обработке не в том порядке.
func NewEnvelope(eventType, aggregateID string, payload proto.Message) (*eventsv1.Envelope, error) {
	// anypb.New кладёт внутрь Any не только байты, но и полное имя типа
	// ("type.googleapis.com/gosplash.events.v1.PhotoUploaded"). Именно поэтому
	// protobuf-пакетам в проекте нужен префикс gosplash — имя обязано быть
	// глобально уникальным (см. docs/adr/0001-*).
	any, err := anypb.New(payload)
	if err != nil {
		return nil, fmt.Errorf("упаковка payload %T: %w", payload, err)
	}

	return &eventsv1.Envelope{
		EventId:          newEventID(),
		EventType:        eventType,
		AggregateId:      aggregateID,
		OccurredAtUnixMs: time.Now().UnixMilli(),
		SchemaVersion:    1,
		Payload:          any,
	}, nil
}

// UnmarshalPayload достаёт payload нужного типа из конверта.
//
// Проверка типа обязательна и делается самим anypb: если в топик приехало
// событие другого типа, UnmarshalTo вернёт ошибку, а не молча заполнит
// структуру мусором. Без Any так не получилось бы — protobuf почти всегда
// «успешно» разбирает чужое сообщение, просто результат бессмысленный.
func UnmarshalPayload[T proto.Message](env *eventsv1.Envelope, dst T) error {
	if env.GetPayload() == nil {
		return fmt.Errorf("событие %s (%s): пустой payload", env.GetEventType(), env.GetEventId())
	}
	if err := env.GetPayload().UnmarshalTo(dst); err != nil {
		return fmt.Errorf("распаковка %s в %T: %w", env.GetPayload().GetTypeUrl(), dst, err)
	}
	return nil
}

// OccurredAt — время события как time.Time.
func OccurredAt(env *eventsv1.Envelope) time.Time {
	return time.UnixMilli(env.GetOccurredAtUnixMs())
}

// newEventID выдаёт UUID v7.
//
// Не v4: в v7 первые 48 бит — это миллисекунды unix-времени, поэтому значения
// монотонно растут. Для первичного ключа в PostgreSQL это принципиально —
// таблицы outbox и processed_events пишутся только вставками, и случайный v4
// разбрасывал бы их по всему B-tree, грея случайные страницы и раздувая WAL.
// С v7 вставка всегда идёт в «правый край» индекса.
//
// Если генератор откажет (это возможно только при отказе источника
// случайности), падаем на v4: лучше непоследовательный id, чем пустой.
func newEventID() string {
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	return uuid.NewString()
}

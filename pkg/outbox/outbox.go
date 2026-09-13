package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	"gosplash/pkg/kafkax"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// Write записывает событие в outbox В ТОЙ ЖЕ транзакции gorm, что и бизнес-
// изменение.
//
// Вызывающий код обязан передать tx — транзакцию, открытую ИМ, а не новое
// соединение: смысл паттерна целиком держится на том, что INSERT в outbox
// и UPDATE/INSERT бизнес-таблицы либо оба закоммитятся, либо оба
// откатятся. Пример использования (внутри db.Transaction(...)):
//
//	env, _ := kafkax.NewEnvelope(kafkax.EventPhotoUploaded, photo.ID, payload)
//	if err := repo.Create(ctx, tx, photo); err != nil { return err }
//	if err := outbox.Write(ctx, tx, "media", env, kafkax.TopicPhotoUploaded); err != nil { return err }
//
// service — имя сервиса. Оно определяет ДВЕ вещи сразу, и обе важны:
//
//   - таблицу, в которую ляжет строка (TableFor: "outbox_media"). У каждого
//     сервиса своя очередь, даже когда базу они делят;
//   - заголовок producer будущего Kafka-сообщения. Он проставляется ЗДЕСЬ,
//     в момент записи, а не в relay, потому что автор события — тот, кто его
//     записал. Relay всего лишь доставщик: он вычитывает чужие строки и не
//     обязан быть тем же процессом, что их создал.
//
// Ключ сообщения (row.Key) берётся из env.AggregateId — той же логикой,
// что и в kafkax.Producer.Publish, чтобы отложенная публикация через relay
// не отличалась по партиционированию от прямой.
func Write(ctx context.Context, tx *gorm.DB, service string, env *eventsv1.Envelope, topic string) error {
	if service == "" {
		return fmt.Errorf("outbox: не указан сервис — неизвестно, в какую таблицу писать")
	}

	row, err := buildRow(ctx, service, env, topic)
	if err != nil {
		return err
	}
	if err := tx.WithContext(ctx).Table(TableFor(service)).Create(row).Error; err != nil {
		return fmt.Errorf("outbox: запись события %s: %w", env.GetEventType(), err)
	}
	return nil
}

// Migrate создаёт таблицу outbox сервиса. Вызывается из migrations/auto.go
// каждого сервиса вместо прямого AutoMigrate(&OutboxRow{}): имя таблицы
// зависит от сервиса, и знать об этом должен пакет, а не каждый вызывающий.
func Migrate(db *gorm.DB, service string) error {
	if err := db.Table(TableFor(service)).AutoMigrate(&OutboxRow{}); err != nil {
		return fmt.Errorf("outbox: миграция %s: %w", TableFor(service), err)
	}
	return nil
}

// buildRow собирает строку outbox без единого обращения к базе — вынесена
// из Write отдельной функцией ИМЕННО ради этого: сборку заголовков и
// сериализацию можно проверить в unit-тесте, не поднимая Postgres, а весь
// код, которому Postgres действительно нужен, остаётся в Write в одну
// строку (Create).
func buildRow(ctx context.Context, service string, env *eventsv1.Envelope, topic string) (*OutboxRow, error) {
	payload, err := proto.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("outbox: marshal envelope: %w", err)
	}

	headers := map[string]string{
		kafkax.HeaderEventID:   env.GetEventId(),
		kafkax.HeaderEventType: env.GetEventType(),
		// producer фиксируется в момент ЗАПИСИ. Relay, который повезёт эту
		// строку в Kafka, может принадлежать другому процессу и другому
		// сервису — и тогда его собственное имя было бы неправдой.
		kafkax.HeaderProducer: service,
	}
	// traceparent кладём В ЗАГОЛОВКИ СТРОКИ прямо сейчас, а не оставляем
	// relay'ю разбираться: у Write есть контекст ИСХОДНОГО запроса
	// (HTTP-хендлер, Kafka-консьюмер и т. п.), а Relay — это независимый
	// процесс, который вычитает эту строку через секунды или минуты и своего
	// осмысленного контекста для неё не имеет. propagation.MapCarrier —
	// готовый TextMapCarrier поверх map[string]string, тот же интерфейс,
	// которым pkg/kafkax/trace.go пользуется для заголовков kgo.Record.
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for k, v := range carrier {
		headers[k] = v
	}

	headersJSON, err := json.Marshal(headers)
	if err != nil {
		return nil, fmt.Errorf("outbox: marshal headers: %w", err)
	}

	return &OutboxRow{
		ID:          newRowID(),
		AggregateID: env.GetAggregateId(),
		Topic:       topic,
		Key:         []byte(env.GetAggregateId()),
		Headers:     headersJSON,
		Payload:     payload,
	}, nil
}

// newRowID — UUID v7, тем же способом, что и event_id конверта (см.
// комментарий в model.go про «правый край» индекса). Не переиспользуем
// event_id конверта напрямую: это разные сущности с разным жизненным
// циклом (строка outbox может быть переиграна инструментом восстановления
// с новым id, event_id конверта при этом обязан остаться прежним, чтобы
// consumer'ы дедуплицировали правильно), и совпадение id — деталь текущей
// реализации, а не контракт, который есть смысл закреплять в коде.
func newRowID() string {
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	return uuid.NewString()
}

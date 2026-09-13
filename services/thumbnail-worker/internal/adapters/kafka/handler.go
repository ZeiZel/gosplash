// Package kafka — единственное место в thumbnail-worker, которое знает про
// pkg/kafkax и про protobuf-конверт events.v1: разбирает Envelope, вызывает
// сценарий internal/app.Service.Process и переводит его ответ на язык
// kafkax (Retryable/Permanent) — то есть решает, ехать ли сообщению
// в retry/DLQ, если сценарий вернул ошибку.
//
// Классификация ошибок живёт ЗДЕСЬ, а не в internal/app, по той же причине,
// по которой internal/app не импортирует net/http в media: сценарий не
// обязан знать, что такое Kafka, DLQ или retry-топик — он просто возвращает
// доменную ошибку (обёрнутую через %w), а конкретный транспорт решает,
// что с ней делать.
package kafka

import (
	"context"
	"errors"
	"fmt"

	"gosplash/pkg/kafkax"
	"gosplash/services/thumbnail-worker/internal/app"
	"gosplash/services/thumbnail-worker/internal/domain"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// NewHandler превращает internal/app.Service.Process в kafkax.Handler.
func NewHandler(service *app.Service) kafkax.Handler {
	return func(ctx context.Context, env *eventsv1.Envelope) error {
		var payload eventsv1.PhotoUploaded
		if err := kafkax.UnmarshalPayload(env, &payload); err != nil {
			// Битый payload — повтор не поможет, это не сетевой сбой,
			// а несовместимость данных.
			return kafkax.Permanent(fmt.Errorf("разбор PhotoUploaded: %w", err))
		}

		err := service.Process(ctx, app.ProcessInput{
			EventID:   env.GetEventId(),
			EventType: env.GetEventType(),
			// Topic — исходный топик media.photo.uploaded, а не то, откуda
			// сообщение реально прочитано (основной консьюмер или его
			// retry-версия): idempotency.Claim должен видеть одно и то же
			// значение независимо от того, сколько раз событие успело
			// съездить в <topic>.retry — это одна и та же "обработка одного
			// факта", а не разные факты.
			Topic:   kafkax.TopicPhotoUploaded,
			PhotoID: payload.GetPhotoId(),
			UserID:  payload.GetUserId(),
		})
		return classify(err)
	}
}

// classify — единственное место, где ошибка сценария превращается
// в решение kafkax: DLQ сразу (повтор бессмысленен) или retry (временный
// сбой инфраструктуры). Соответствие ровно то, что требует задание:
// не-изображение/отсутствующий оригинал/несуществующее фото — постоянные,
// всё остальное (недоступен S3 или Postgres, сеть) — retryable.
func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, domain.ErrNotAnImage) ||
		errors.Is(err, domain.ErrOriginalMissing) ||
		errors.Is(err, domain.ErrPhotoNotFound) {
		return kafkax.Permanent(err)
	}
	// Явная kafkax.Retryable, а не "просто вернуть err как есть": формально
	// это то же самое (см. pkg/kafkax/errors.go — неклассифицированная
	// ошибка по умолчанию retryable), но задание прямо перечисляет "недоступен
	// S3 или база → kafkax.Retryable(err)" как требование, а не как
	// подразумеваемое поведение, — и явная разметка здесь дешевле, чем
	// комментарий, объясняющий, что дефолт и есть нужное поведение.
	return kafkax.Retryable(err)
}

// Package app — прикладной слой каталога: наполнение витрины из событий
// и её чтение.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gosplash/pkg/kafkax"
	"gosplash/services/catalog/internal/domain"
	"gosplash/services/catalog/internal/ports"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// Indexer — то место, где Kafka, gRPC и идемпотентный консьюмер работают
// вместе.
//
// Обрабатывает ДВА топика:
//
//	media.photo.uploaded          → создать черновик карточки
//	media.photo.thumbnail-ready   → дописать превью, опубликовать
//
// Поток для первого:
//
//	media  ──[Kafka: media.photo.uploaded]──►  catalog
//	                                              │
//	                                              │ gRPC: GetPhoto(id, user_id)
//	                                              ▼
//	                                            media
//
// Событие в Kafka намеренно тонкое — только идентификаторы. Подробности
// консьюмер запрашивает по gRPC: к моменту чтения данные могли измениться,
// и «слепок» из события был бы устаревшим, а толстые события связывают
// сервисы сильнее, чем прямой вызов. Плата — один сетевой вызов на событие.
//
// PATTERN: idempotent consumer (pkg/idempotency), а не просто upsert, как
// было в фазе 0. Тогда идемпотентность возникала БЕСПЛАТНО из свойств самой
// операции: "записать то же состояние второй раз" ничего не меняет, пока
// обработка — это только "сохранить строку". Как только появилась публикация
// catalog.listing.published в outbox (HandlePhotoThumbnailReady) — повторить
// её безопасно НЕЛЬЗЯ: search (фаза 5) получит два одинаковых события,
// и хотя consumer там тоже обязан быть идемпотентным, полагаться на цепочку
// "неидемпотентный писатель + надежда, что все последующие потребители
// идемпотентны" — это ровно тот повторяющийся риск, который явная отметка
// (processed_events) устраняет один раз здесь, а не бесконечно у каждого
// будущего потребителя цепочки. Отсюда и WithClaim первым действием в обоих
// обработчиках ниже.
type Indexer struct {
	uow   ports.UnitOfWork
	media ports.PhotoFetcher
	cache ports.ListingCache
	hub   *WatchHub
}

func NewIndexer(uow ports.UnitOfWork, media ports.PhotoFetcher, cache ports.ListingCache, hub *WatchHub) *Indexer {
	return &Indexer{uow: uow, media: media, cache: cache, hub: hub}
}

// HandlePhotoUploaded обрабатывает одно событие из media.photo.uploaded:
// создаёт черновик карточки.
func (i *Indexer) HandlePhotoUploaded(ctx context.Context, env *eventsv1.Envelope) error {
	var event eventsv1.PhotoUploaded
	if err := kafkax.UnmarshalPayload(env, &event); err != nil {
		// Битый payload повторять бессмысленно: он будет ломаться вечно
		// и заблокирует партицию. Permanent — сразу в DLQ, а не тихий
		// пропуск: DLQ пакет (pkg/kafkax) в проекте уже есть, и молчаливо
		// терять такие сообщения больше нет причины.
		return kafkax.Permanent(fmt.Errorf("разбор payload media.photo.uploaded: %w", err))
	}

	applied, err := i.uow.WithClaim(ctx, env.GetEventId(), env.GetEventType(), kafkax.TopicPhotoUploaded, func(tx ports.IndexTx) error {
		// Таймаут обязателен: без него зависший media остановит партицию.
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		listing, err := i.media.Fetch(callCtx, event.GetPhotoId(), event.GetUserId())
		if err != nil {
			return err
		}

		// Заглушка отображаемого имени — см. placeholderAuthorName.
		if err := tx.UpsertAuthorSnapshot(ctx, listing.AuthorID, placeholderAuthorName(listing.AuthorID)); err != nil {
			return err
		}
		return tx.UpsertListing(ctx, listing)
	})
	if err != nil {
		return classifyMediaErr(err)
	}
	if !applied {
		slog.DebugContext(ctx, "catalog: событие уже обработано, пропускаю",
			"event_id", env.GetEventId(), "topic", kafkax.TopicPhotoUploaded)
		return nil
	}

	i.afterApply(ctx, event.GetPhotoId(), "updated", kafkax.OccurredAt(env), nil)

	slog.InfoContext(ctx, "catalog ✓ карточка создана",
		"photo_id", event.GetPhotoId(), "user_id", event.GetUserId())
	return nil
}

// HandlePhotoThumbnailReady обрабатывает одно событие из
// media.photo.thumbnail-ready: дописывает превью и переводит карточку
// в published, публикуя вслед catalog.listing.published через outbox.
func (i *Indexer) HandlePhotoThumbnailReady(ctx context.Context, env *eventsv1.Envelope) error {
	var event eventsv1.PhotoThumbnailReady
	if err := kafkax.UnmarshalPayload(env, &event); err != nil {
		return kafkax.Permanent(fmt.Errorf("разбор payload media.photo.thumbnail-ready: %w", err))
	}

	thumbnails := make(map[string]string, len(event.GetThumbnails()))
	for _, th := range event.GetThumbnails() {
		thumbnails[th.GetSize()] = th.GetStorageKey()
	}

	var published *domain.Listing
	applied, err := i.uow.WithClaim(ctx, env.GetEventId(), env.GetEventType(), kafkax.TopicPhotoThumbnailReady, func(tx ports.IndexTx) error {
		listing, err := tx.ApplyThumbnails(ctx, event.GetPhotoId(), thumbnails, kafkax.OccurredAt(env))
		if err != nil {
			return err
		}
		published = listing

		payload := &eventsv1.ListingPublished{
			ListingId:  listing.ID,
			AuthorId:   listing.AuthorID,
			AuthorName: listing.AuthorName,
			Title:      listing.Title,
			Tags:       listing.Tags,
			PriceCents: listing.PriceCents,
			Currency:   listing.Currency,
			Status:     listing.Status,
		}
		outEnv, err := kafkax.NewEnvelope(kafkax.EventListingPublished, listing.ID, payload)
		if err != nil {
			return fmt.Errorf("конверт catalog.listing.published: %w", err)
		}
		return tx.Outbox(ctx, outEnv, kafkax.TopicListingPublished)
	})
	if err != nil {
		// domain.ErrNotFound (карточка ещё не создана: thumbnail-ready
		// обогнал uploaded — топики независимы по порядку) и любые ошибки
		// БД одинаково временны: retryable, а не permanent. Постоянная
		// ошибка здесь была бы только у битого payload'а — уже отсечена
		// выше.
		return kafkax.Retryable(fmt.Errorf("применение превью %s: %w", event.GetPhotoId(), err))
	}
	if !applied {
		slog.DebugContext(ctx, "catalog: событие уже обработано, пропускаю",
			"event_id", env.GetEventId(), "topic", kafkax.TopicPhotoThumbnailReady)
		return nil
	}

	i.afterApply(ctx, event.GetPhotoId(), "published", kafkax.OccurredAt(env), published)

	slog.InfoContext(ctx, "catalog ✓ карточка опубликована",
		"photo_id", event.GetPhotoId())
	return nil
}

// afterApply — общий хвост обоих обработчиков после успешного применения:
// инвалидация кэша и уведомление подписчиков WatchListing.
func (i *Indexer) afterApply(ctx context.Context, photoID, change string, occurredAt time.Time, listing *domain.Listing) {
	// Инвалидация, а НЕ обновление кэша на месте (write-through) — см.
	// ports.ListingCache. Коротко: HandlePhotoUploaded и
	// HandlePhotoThumbnailReady работают на РАЗНЫХ consumer group и разных
	// партициях (topics.go), гарантии относительного порядка между ними
	// нет. Если бы оба писали в кэш готовое значение напрямую, более
	// старое (например, "updated" от uploaded, доставленное с опозданием
	// из retry-топика) могло бы лечь в кэш ПОСЛЕ более нового ("published")
	// и застрять там на весь TTL. Invalidate убирает ключ безусловно —
	// следующий читатель перечитает АКТУАЛЬНОЕ состояние из базы, что бы
	// в кэше ни лежало раньше.
	if err := i.cache.Invalidate(ctx, photoID); err != nil {
		slog.WarnContext(ctx, "catalog: не смог инвалидировать кэш", "photo_id", photoID, "error", err)
	}
	i.hub.Notify(photoID, listing, change, occurredAt)
}

// placeholderAuthorName — временная заглушка отображаемого имени автора.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: в проекте пока нет события с человекочитаемым именем
// автора — ни один сервис (нет auth/profile в этой фазе) его не публикует,
// PhotoUploaded несёт только user_id. Чтобы authors_snapshot и JOIN в
// GetListing/ListListings никогда не отдавали пустую строку, заводим
// заглушку по шаблону при ПЕРВОМ появлении автора. UpsertAuthorSnapshot
// делает это через ON CONFLICT DO NOTHING, поэтому настоящее имя (когда
// соответствующее событие появится в будущей фазе и придёт отдельным путём)
// не будет затёрто повторной обработкой media.photo.uploaded.
func placeholderAuthorName(authorID int64) string {
	return fmt.Sprintf("author-%d", authorID)
}

// classifyMediaErr решает, что делать с ошибкой похода в media.
//
// ErrPhotoNotFound (media ответила NotFound — фото удалено или никогда не
// существовало) — постоянная: повторять бессмысленно. Всё остальное
// (media перезапускается, таймаут, сеть) — временное.
func classifyMediaErr(err error) error {
	if errors.Is(err, domain.ErrPhotoNotFound) {
		return kafkax.Permanent(err)
	}
	return kafkax.Retryable(fmt.Errorf("gRPC GetPhoto: %w", err))
}

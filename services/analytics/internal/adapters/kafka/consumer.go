// Package kafka — адаптер analytics-сервиса к Kafka: четыре независимых
// консьюмера (analytics.photo.viewed, order.order.paid, media.photo.uploaded,
// media.photo.thumbnail-ready), каждый превращает Envelope в вызов
// internal/app.Ingest.
//
// ПОЧЕМУ КОНСЬЮМЕР НА GO, А НЕ KAFKA ENGINE — ключевое решение фазы,
// подробно разобранное в docs/adr/0016-clickhouse-kafka-engine.md. Коротко:
// в топик всегда пишется Envelope (proto/gosplash/events/v1/events.proto) —
// конверт с payload внутри google.protobuf.Any, а Kafka engine ClickHouse
// с kafka_format='Protobuf'/'ProtobufSingle' умеет разобрать ОДИН плоский
// протобаф-тип по имени сообщения, но не умеет заглянуть внутрь Any и не
// знает, что делать с полем, которое не описано в .proto, переданном движку.
// Продюсер (services/catalog) — вне зоны ответственности этой фазы и его
// нельзя переписать на публикацию голого payload. Значит, разбор конверта
// достаётся коду, а не движку БД, — отсюда этот пакет.
package kafka

import (
	"context"
	"errors"
	"fmt"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
	"gosplash/pkg/kafkax"

	"gosplash/services/analytics/internal/app"
	"gosplash/services/analytics/internal/domain"
)

// Consumers — все консьюмеры analytics-сервиса и их общий Ping/Close.
//
// Один инстанс *kafkax.Consumer на топик (то же правило, что в catalog:
// см. комментарий в services/catalog/cmd/catalog/main.go про то, почему
// НЕ одна группа на оба топика) — здесь оно даже уместнее, чем в catalog:
// объём просмотров и объём событий воронки/покупок отличается на порядки,
// и разная лаг-метрика на каждый топик — единственный способ увидеть это
// по отдельности, а не одной усреднённой цифрой.
type Consumers struct {
	views      *kafkax.Consumer
	purchases  *kafkax.Consumer
	uploads    *kafkax.Consumer
	thumbnails *kafkax.Consumer
	ingest     *app.Ingest
}

// New подключает все четыре консьюмера. group — общий префикс группы (см.
// pkg/config.AnalyticsConfig.KafkaConsumerGroup); суффиксы
// -views/-purchases/-uploads/-thumbnails разводят их так же, как catalog
// разводит -thumbnail для второго топика.
func New(brokers []string, group string, ingest *app.Ingest) (*Consumers, error) {
	views, err := kafkax.NewConsumer(brokers, group+"-views", kafkax.TopicPhotoViewed)
	if err != nil {
		return nil, fmt.Errorf("analytics: kafka consumer (photo viewed): %w", err)
	}

	purchases, err := kafkax.NewConsumer(brokers, group+"-purchases", kafkax.TopicOrderPaid)
	if err != nil {
		views.Close()
		return nil, fmt.Errorf("analytics: kafka consumer (order paid): %w", err)
	}

	uploads, err := kafkax.NewConsumer(brokers, group+"-uploads", kafkax.TopicPhotoUploaded)
	if err != nil {
		views.Close()
		purchases.Close()
		return nil, fmt.Errorf("analytics: kafka consumer (photo uploaded): %w", err)
	}

	thumbnails, err := kafkax.NewConsumer(brokers, group+"-thumbnails", kafkax.TopicPhotoThumbnailReady)
	if err != nil {
		views.Close()
		purchases.Close()
		uploads.Close()
		return nil, fmt.Errorf("analytics: kafka consumer (photo thumbnail-ready): %w", err)
	}

	return &Consumers{views: views, purchases: purchases, uploads: uploads, thumbnails: thumbnails, ingest: ingest}, nil
}

// RunViews блокирует вызывающую горутину, пока не отменят ctx — как и
// kafkax.Consumer.Run, вызывать в go func().
func (c *Consumers) RunViews(ctx context.Context) error {
	return c.views.Run(ctx, c.handlePhotoViewed)
}

func (c *Consumers) RunPurchases(ctx context.Context) error {
	return c.purchases.Run(ctx, c.handleOrderPaid)
}

func (c *Consumers) RunUploads(ctx context.Context) error {
	return c.uploads.Run(ctx, c.handlePhotoUploaded)
}

func (c *Consumers) RunThumbnails(ctx context.Context) error {
	return c.thumbnails.Run(ctx, c.handleThumbnailReady)
}

func (c *Consumers) Close() {
	c.views.Close()
	c.purchases.Close()
	c.uploads.Close()
	c.thumbnails.Close()
}

// PingViews / PingPurchases / PingUploads / PingThumbnails — для /readyz
// (httpx.Health.Register). Раздельные проверки, а не одна общая "kafka":
// один брокер может ответить на Ping одного клиента и не ответить другому
// только в теории (оба клиента смотрят в один и тот же кластер), но
// именование по образцу catalog (kafka_uploaded/kafka_thumbnail в
// services/catalog/cmd/catalog/main.go) сохраняет единообразие вывода
// /readyz по всем сервисам монорепы.
func (c *Consumers) PingViews(ctx context.Context) error      { return c.views.Ping(ctx) }
func (c *Consumers) PingPurchases(ctx context.Context) error  { return c.purchases.Ping(ctx) }
func (c *Consumers) PingUploads(ctx context.Context) error    { return c.uploads.Ping(ctx) }
func (c *Consumers) PingThumbnails(ctx context.Context) error { return c.thumbnails.Ping(ctx) }

// handlePhotoViewed разбирает Envelope и зовёт app.Ingest голыми значениями.
//
// Ошибка разбора конверта — kafkax.Permanent: битый payload не станет
// валидным после повтора (тот же принцип, что и в
// services/catalog/internal/app/indexer.go). Из ошибок самого Ingest
// permanent помечаются только ошибки валидации (classifyIngestErr): всё
// остальное (в конечном счёте — ошибка вставки в ClickHouse, см.
// internal/adapters/clickhouse/batch.go) НЕ классифицируется явно и по
// умолчанию (docs/STYLE.md, pkg/kafkax) считается retryable — это то самое
// свойство, которое избавляет от дыр в отчёте ценой редких дублей вставки,
// разобранное в комментарии Writer[T].Add.
func (c *Consumers) handlePhotoViewed(ctx context.Context, env *eventsv1.Envelope) error {
	var event eventsv1.PhotoViewed
	if err := kafkax.UnmarshalPayload(env, &event); err != nil {
		return kafkax.Permanent(fmt.Errorf("analytics.photo.viewed: %w", err))
	}

	if err := c.ingest.PhotoViewed(ctx,
		event.GetPhotoId(), event.GetAuthorId(), event.GetViewerId(), event.GetCountry(),
		kafkax.OccurredAt(env),
	); err != nil {
		return classifyIngestErr(fmt.Errorf("analytics.photo.viewed: %w", err))
	}
	return nil
}

// classifyIngestErr размечает ошибки доменного Ingest для kafkax.
//
// Пустой photo_id/order_id — свойство САМОГО события: сколько ни повторяй,
// пустая строка не станет валидным идентификатором, поэтому такое сообщение
// сразу уходит в DLQ, а не жжёт окно ретраев (и retry-топик) заведомо
// безнадёжной обработкой. Ошибки записи в ClickHouse сюда не попадают —
// они временные по определению и остаются retryable по умолчанию (см.
// комментарий handlePhotoViewed).
func classifyIngestErr(err error) error {
	if errors.Is(err, domain.ErrEmptyPhotoID) || errors.Is(err, domain.ErrEmptyOrderID) {
		return kafkax.Permanent(err)
	}
	return err
}

// handleOrderPaid разбирает Envelope и зовёт app.Ingest.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: OrderPaid.listing_id становится domain.Purchase.PhotoID
// напрямую, без похода в catalog за соответствием. Это безопасно, потому
// что в проекте listing_id и photo_id — ОДНО И ТО ЖЕ значение: индексатор
// каталога заводит карточку с ID, равным photo_id из media.photo.uploaded
// (см. services/catalog/internal/adapters/grpc/media_client.go, поле
// domain.Listing.ID = photo.GetId()), а не отдельным сгенерированным
// идентификатором. Без этого совпадения покупкам потребовался бы отдельный
// побочный запрос к catalog за фактическим photo_id по listing_id — то есть
// синхронная gRPC-зависимость на пути КАЖДОГО события покупки, которой
// разработчики каталога намеренно избежали, выбрав общий id.
func (c *Consumers) handleOrderPaid(ctx context.Context, env *eventsv1.Envelope) error {
	var event eventsv1.OrderPaid
	if err := kafkax.UnmarshalPayload(env, &event); err != nil {
		return kafkax.Permanent(fmt.Errorf("order.order.paid: %w", err))
	}

	if err := c.ingest.OrderPaid(ctx,
		event.GetOrderId(), event.GetListingId(), event.GetAuthorId(), event.GetBuyerId(), event.GetPriceCents(),
		kafkax.OccurredAt(env),
	); err != nil {
		return classifyIngestErr(fmt.Errorf("order.order.paid: %w", err))
	}
	return nil
}

// handlePhotoUploaded разбирает Envelope и зовёт app.Ingest — та же схема
// обработки ошибок, что у handlePhotoViewed (см. её комментарий): битый
// payload — Permanent, ошибки записи — retryable по умолчанию.
//
// PhotoUploaded.user_id приезжает сюда как authorID без преобразований:
// переименование — часть смысла (см. комментарий у domain.Upload), а не
// деталь протокола.
func (c *Consumers) handlePhotoUploaded(ctx context.Context, env *eventsv1.Envelope) error {
	var event eventsv1.PhotoUploaded
	if err := kafkax.UnmarshalPayload(env, &event); err != nil {
		return kafkax.Permanent(fmt.Errorf("media.photo.uploaded: %w", err))
	}

	if err := c.ingest.PhotoUploaded(ctx,
		event.GetPhotoId(), event.GetUserId(),
		kafkax.OccurredAt(env),
	); err != nil {
		return classifyIngestErr(fmt.Errorf("media.photo.uploaded: %w", err))
	}
	return nil
}

// handleThumbnailReady разбирает Envelope и зовёт app.Ingest.
//
// Из PhotoThumbnailReady используются только photo_id и время конверта:
// user_id и ключи превью (thumbnails) этому потребителю не нужны — см.
// комментарий у domain.Thumbnail, почему факт нарочно тоньше события.
func (c *Consumers) handleThumbnailReady(ctx context.Context, env *eventsv1.Envelope) error {
	var event eventsv1.PhotoThumbnailReady
	if err := kafkax.UnmarshalPayload(env, &event); err != nil {
		return kafkax.Permanent(fmt.Errorf("media.photo.thumbnail-ready: %w", err))
	}

	if err := c.ingest.ThumbnailReady(ctx,
		event.GetPhotoId(),
		kafkax.OccurredAt(env),
	); err != nil {
		return classifyIngestErr(fmt.Errorf("media.photo.thumbnail-ready: %w", err))
	}
	return nil
}

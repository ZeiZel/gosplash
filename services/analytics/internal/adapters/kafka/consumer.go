// Package kafka — адаптер analytics-сервиса к Kafka: два независимых
// консьюмера (analytics.photo.viewed, order.order.paid), каждый превращает
// Envelope в вызов internal/app.Ingest.
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
	"fmt"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
	"gosplash/pkg/kafkax"

	"gosplash/services/analytics/internal/app"
)

// Consumers — оба консьюмера analytics-сервиса и их общий Ping/Close.
//
// Один инстанс *kafkax.Consumer на топик (то же правило, что в catalog:
// см. комментарий в services/catalog/cmd/catalog/main.go про то, почему
// НЕ одна группа на оба топика) — здесь оно даже уместнее, чем в catalog:
// объём просмотров и объём покупок отличается на порядки, и разная лаг-
// метрика на каждый топик — единственный способ увидеть это по отдельности,
// а не одной усреднённой цифрой.
type Consumers struct {
	views     *kafkax.Consumer
	purchases *kafkax.Consumer
	ingest    *app.Ingest
}

// New подключает оба консьюмера. group — общий префикс группы (см.
// internal/config.Config.KafkaConsumerGroup); суффиксы -views/-purchases
// разводят их так же, как catalog разводит -thumbnail для второго топика.
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

	return &Consumers{views: views, purchases: purchases, ingest: ingest}, nil
}

// RunViews блокирует вызывающую горутину, пока не отменят ctx — как и
// kafkax.Consumer.Run, вызывать в go func().
func (c *Consumers) RunViews(ctx context.Context) error {
	return c.views.Run(ctx, c.handlePhotoViewed)
}

func (c *Consumers) RunPurchases(ctx context.Context) error {
	return c.purchases.Run(ctx, c.handleOrderPaid)
}

func (c *Consumers) Close() {
	c.views.Close()
	c.purchases.Close()
}

// PingViews / PingPurchases — для /readyz (httpx.Health.Register).
// Раздельные проверки, а не одна общая "kafka": один брокер может ответить
// на Ping одного клиента и не ответить другому только в теории (оба клиента
// смотрят в один и тот же кластер), но именование по образцу catalog
// (kafka_uploaded/kafka_thumbnail в services/catalog/cmd/catalog/main.go)
// сохраняет единообразие вывода /readyz по всем сервисам монорепы.
func (c *Consumers) PingViews(ctx context.Context) error     { return c.views.Ping(ctx) }
func (c *Consumers) PingPurchases(ctx context.Context) error { return c.purchases.Ping(ctx) }

// handlePhotoViewed разбирает Envelope и зовёт app.Ingest голыми значениями.
//
// Ошибка разбора конверта — kafkax.Permanent: битый payload не станет
// валидным после повтора (тот же принцип, что и в
// services/catalog/internal/app/indexer.go). Ошибка самого Ingest (то есть
// в конечном счёте — ошибка вставки в ClickHouse, см.
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
		return fmt.Errorf("analytics.photo.viewed: %w", err)
	}
	return nil
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
		return fmt.Errorf("order.order.paid: %w", err)
	}
	return nil
}

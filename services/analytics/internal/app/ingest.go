package app

import (
	"context"
	"fmt"
	"time"

	"gosplash/services/analytics/internal/domain"
	"gosplash/services/analytics/internal/ports"
)

// Ingest — сценарий приёма фактов из Kafka (internal/adapters/kafka).
//
// Адаптер разбирает Envelope и protobuf-payload и зовёт сюда уже голыми
// значениями (без единого protobuf-типа в аргументах) — так internal/app
// не приобретает импорт gen/go/gosplash/events/v1, а конвертация "0 значит
// анонимный просмотр" остаётся на границе, где Envelope превращается в
// domain.PhotoView (см. комментарий у domain.PhotoView).
type Ingest struct {
	views     ports.ViewSink
	purchases ports.PurchaseSink
}

func NewIngest(views ports.ViewSink, purchases ports.PurchaseSink) *Ingest {
	return &Ingest{views: views, purchases: purchases}
}

// PhotoViewed принимает один факт просмотра.
//
// viewerID == 0 конвертируется в nil здесь, а не в adapters/kafka: это
// решение о СМЫСЛЕ ("ноль — не идентификатор, а отсутствие зрителя") —
// часть домена, а не деталь разбора protobuf.
func (i *Ingest) PhotoViewed(ctx context.Context, photoID string, authorID, viewerID int64, country string, occurredAt time.Time) error {
	if photoID == "" {
		return fmt.Errorf("analytics.photo.viewed: %w", domain.ErrEmptyPhotoID)
	}

	view := domain.PhotoView{
		PhotoID:    photoID,
		AuthorID:   authorID,
		Country:    country,
		OccurredAt: occurredAt,
	}
	if viewerID != 0 {
		view.ViewerID = &viewerID
	}

	if err := i.views.Add(ctx, view); err != nil {
		return fmt.Errorf("запись просмотра %s: %w", photoID, err)
	}
	return nil
}

// OrderPaid принимает один факт покупки.
func (i *Ingest) OrderPaid(ctx context.Context, orderID, photoID string, authorID, buyerID, priceCents int64, occurredAt time.Time) error {
	if orderID == "" {
		return fmt.Errorf("order.order.paid: пустой order_id")
	}
	if photoID == "" {
		return fmt.Errorf("order.order.paid: %w", domain.ErrEmptyPhotoID)
	}

	purchase := domain.Purchase{
		OrderID:    orderID,
		PhotoID:    photoID,
		AuthorID:   authorID,
		BuyerID:    buyerID,
		PriceCents: priceCents,
		OccurredAt: occurredAt,
	}
	if err := i.purchases.Add(ctx, purchase); err != nil {
		return fmt.Errorf("запись покупки %s: %w", orderID, err)
	}
	return nil
}

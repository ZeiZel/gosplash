package pg

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
	"gosplash/pkg/kafkax"
	"gosplash/pkg/outbox"
	"gosplash/services/order/internal/domain"
)

// serviceName — владелец очереди outbox (таблица outbox_order) и значение
// заголовка producer публикуемых событий.
const serviceName = "order"

// OrderRepository реализует ОБА порта: ports.OrderRepository (для
// app.OrderService) и ports.SagaOrderRepository (для
// internal/adapters/temporal.Activities). Это один и тот же набор SQL-
// операций над одной таблицей, и разделять его на два физических типа
// пришлось бы только ради иллюзии изоляции — реальная изоляция уже есть
// на уровне интерфейсов (см. internal/ports/ports.go).
type OrderRepository struct {
	db *gorm.DB
}

func NewOrderRepository(db *gorm.DB) *OrderRepository {
	return &OrderRepository{db: db}
}

// Create вставляет заказ и order.order.placed одной транзакцией.
func (r *OrderRepository) Create(ctx context.Context, order *domain.Order, placedEvent *eventsv1.Envelope) error {
	row := toRow(order)
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(row).Error; err != nil {
			return fmt.Errorf("создание заказа: %w", err)
		}
		if err := outbox.Write(ctx, tx, serviceName, placedEvent, kafkax.TopicOrderPlaced); err != nil {
			return err
		}
		return nil
	})
}

func (r *OrderRepository) GetByID(ctx context.Context, id string) (*domain.Order, error) {
	var row OrderRow
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("чтение заказа: %w", err)
	}
	return toDomain(&row), nil
}

// UpdateStatus — простая смена статуса, без outbox. Идемпотентна
// тривиально: SET status = ? одинаков при любом числе повторов активности
// саги (см. ports.SagaOrderRepository).
func (r *OrderRepository) UpdateStatus(ctx context.Context, id, status string) error {
	err := r.db.WithContext(ctx).Model(&OrderRow{}).Where("id = ?", id).
		Update("status", status).Error
	if err != nil {
		return fmt.Errorf("обновление статуса заказа: %w", err)
	}
	return nil
}

// Complete — успешное завершение саги: статус, license_id и
// order.order.paid одной транзакцией.
func (r *OrderRepository) Complete(ctx context.Context, id, licenseID string, paidEvent *eventsv1.Envelope) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		updates := map[string]any{
			"status":     domain.StatusCompleted,
			"license_id": licenseID,
		}
		if err := tx.Model(&OrderRow{}).Where("id = ?", id).Updates(updates).Error; err != nil {
			return fmt.Errorf("завершение заказа: %w", err)
		}
		if err := outbox.Write(ctx, tx, serviceName, paidEvent, kafkax.TopicOrderPaid); err != nil {
			return err
		}
		return nil
	})
}

// Fail — заказ провален после отработавших компенсаций. Без outbox:
// order.order.* не описывает неуспех (см. proto/gosplash/events/v1) —
// узнать о провале можно только запросив сам заказ или посмотрев историю
// воркфлоу (make wf-show).
func (r *OrderRepository) Fail(ctx context.Context, id, reason string) error {
	updates := map[string]any{
		"status":         domain.StatusFailed,
		"failure_reason": reason,
	}
	if err := r.db.WithContext(ctx).Model(&OrderRow{}).Where("id = ?", id).Updates(updates).Error; err != nil {
		return fmt.Errorf("пометка заказа проваленным: %w", err)
	}
	return nil
}

func toRow(o *domain.Order) *OrderRow {
	return &OrderRow{
		ID:            o.ID,
		BuyerID:       o.BuyerID,
		ListingID:     o.ListingID,
		PriceCents:    o.PriceCents,
		Currency:      o.Currency,
		Status:        o.Status,
		FailureReason: o.FailureReason,
		LicenseID:     o.LicenseID,
	}
}

func toDomain(r *OrderRow) *domain.Order {
	return &domain.Order{
		ID:            r.ID,
		BuyerID:       r.BuyerID,
		ListingID:     r.ListingID,
		PriceCents:    r.PriceCents,
		Currency:      r.Currency,
		Status:        r.Status,
		FailureReason: r.FailureReason,
		LicenseID:     r.LicenseID,
		CreatedAt:     r.CreatedAt,
		UpdatedAt:     r.UpdatedAt,
		// AuthorID сознательно не читается из строки — её там нет,
		// см. model.go. GetByID обслуживает GetOrder, которому payee
		// автора не нужен.
	}
}

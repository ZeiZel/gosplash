// Package app — сценарии order-service. Импортирует только domain и ports
// (docs/STYLE.md): здесь нет ни net/http, ни google.golang.org/grpc, ни
// gorm, ни Temporal SDK — весь ввод-вывод остаётся за портами, поэтому
// сценарий тестируется подделками (см. order_service_test.go), без сети
// и без базы.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
	"gosplash/pkg/kafkax"
	"gosplash/services/order/internal/domain"
	"gosplash/services/order/internal/ports"
)

// idempotencyKeyPrefix — namespace ключа идемпотентности. Без префикса
// «место действия» ключа не видно ни в Redis (при отладке через redis-cli
// KEYS/SCAN), ни в таблице idempotency_keys, если её когда-нибудь придётся
// делить с другим HTTP-эндпоинтом order-service.
const idempotencyKeyPrefix = "place-order:"

// PlaceOrderCommand — вход сценария PlaceOrder. Отдельный тип команды,
// а не позиционные параметры: у сценария уже три обязательных значения,
// и добавление четвёртого не должно менять сигнатуру метода.
type PlaceOrderCommand struct {
	BuyerID        int64
	ListingID      string
	IdempotencyKey string
}

// OrderService — приём заказа: идемпотентность + создание строки + запуск
// саги. Все шаги ПОСЛЕ запуска саги (резервирование денег, выдача лицензии,
// компенсации) — не здесь, а в internal/adapters/temporal: этот сценарий
// не ждёт саму сагу, он лишь передаёт эстафету Temporal и отвечает клиенту.
type OrderService struct {
	orders    ports.OrderRepository
	idem      ports.IdempotencyStore
	listings  ports.ListingReader
	workflows ports.WorkflowStarter
}

func NewOrderService(orders ports.OrderRepository, idem ports.IdempotencyStore, listings ports.ListingReader, workflows ports.WorkflowStarter) *OrderService {
	return &OrderService{orders: orders, idem: idem, listings: listings, workflows: workflows}
}

// PlaceOrder — POST /orders.
//
// PATTERN: idempotency-key. Три исхода см. ports.IdempotencyStatus:
// свободно → делаем работу; занято параллельным запросом → 409 (через
// domain.ErrIdempotencyInProgress, перевод в код — забота адаптера);
// уже сделано → возвращаем СОХРАНЁННЫЙ ответ, не повторяя ни одного
// побочного эффекта (ни GetListing, ни Create, ни запуска саги).
func (s *OrderService) PlaceOrder(ctx context.Context, cmd PlaceOrderCommand) (*domain.Order, error) {
	if cmd.IdempotencyKey == "" {
		return nil, domain.ErrIdempotencyKeyRequired
	}

	key := idempotencyKeyPrefix + cmd.IdempotencyKey
	result, err := s.idem.Begin(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("order: начало идемпотентной операции: %w", err)
	}

	switch result.Status {
	case ports.IdempotencyDone:
		var order domain.Order
		if err := json.Unmarshal(result.Response, &order); err != nil {
			return nil, fmt.Errorf("order: разбор сохранённого ответа: %w", err)
		}
		return &order, nil
	case ports.IdempotencyInProgress:
		return nil, domain.ErrIdempotencyInProgress
	}

	// ports.IdempotencyFree — заказа с этим ключом ещё не было, работаем.
	listing, err := s.listings.GetListing(ctx, cmd.ListingID)
	if err != nil {
		return nil, err
	}
	if listing.Status != "published" {
		return nil, domain.ErrListingNotAvailable
	}

	order := &domain.Order{
		ID:         uuid.NewString(),
		BuyerID:    cmd.BuyerID,
		ListingID:  cmd.ListingID,
		PriceCents: listing.PriceCents,
		Currency:   listing.Currency,
		AuthorID:   listing.AuthorID,
		Status:     domain.StatusPending,
	}

	event, err := kafkax.NewEnvelope(kafkax.EventOrderPlaced, order.ID, &eventsv1.OrderPlaced{
		OrderId:    order.ID,
		BuyerId:    order.BuyerID,
		ListingId:  order.ListingID,
		PriceCents: order.PriceCents,
		Currency:   order.Currency,
	})
	if err != nil {
		return nil, fmt.Errorf("order: конверт order.order.placed: %w", err)
	}

	if err := s.orders.Create(ctx, order, event); err != nil {
		return nil, fmt.Errorf("order: создание заказа: %w", err)
	}

	// workflow_id = order.ID — см. ports.WorkflowStarter и
	// internal/adapters/temporal/starter.go: дедупликация запуска саги
	// достаётся бесплатно от Temporal, а не от кода этого сценария.
	if err := s.workflows.StartPlaceOrder(ctx, order); err != nil {
		return nil, fmt.Errorf("order: запуск саги: %w", err)
	}

	response, err := json.Marshal(order)
	if err != nil {
		return nil, fmt.Errorf("order: сериализация идемпотентного ответа: %w", err)
	}
	if err := s.idem.Complete(ctx, key, response); err != nil {
		// НЕ проваливаем запрос: заказ уже создан и сага уже запущена —
		// клиент обязан получить успешный ответ. Худшее, что здесь может
		// произойти — редкий повторный запрос с тем же ключом однажды
		// снова упрётся в Begin и, если и Redis, и PostgreSQL к тому
		// моменту недоступны, получит ошибку там. Это не хуже, чем
		// откатить уже сделанную (и учтённую в саге) работу.
		slog.ErrorContext(ctx, "order: не удалось зафиксировать идемпотентный ответ",
			"error", err, "order_id", order.ID)
	}

	return order, nil
}

// GetOrder — GET /orders/{id}.
func (s *OrderService) GetOrder(ctx context.Context, id string) (*domain.Order, error) {
	order, err := s.orders.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("order: чтение заказа: %w", err)
	}
	return order, nil
}

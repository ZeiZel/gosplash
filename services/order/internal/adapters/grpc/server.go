// Package grpc — реализация OrderServiceServer (contract из
// proto/gosplash/order/v1/order.proto) и клиенты order-service к catalog
// (адаптеры ports.ListingReader и ports.CatalogSaga).
//
// Клиент к wallet (ports.WalletSaga) лежит здесь же — один пакет "по
// технологии" на все gRPC-адаптеры сервиса, как и в catalog.
package grpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"

	orderv1 "gosplash/gen/go/gosplash/order/v1"
	"gosplash/pkg/grpcx"
	"gosplash/services/order/internal/app"
	"gosplash/services/order/internal/domain"
)

var errEmptyIdempotencyKey = errors.New("idempotency_key обязателен")

// Server — реализация OrderServiceServer.
//
// UnimplementedOrderServiceServer встроен намеренно (см. тот же приём
// в catalog): новый метод в .proto не ломает сборку, а отвечает
// Unimplemented.
type Server struct {
	orderv1.UnimplementedOrderServiceServer

	orders *app.OrderService
}

func NewServer(orders *app.OrderService) *Server {
	return &Server{orders: orders}
}

func (s *Server) PlaceOrder(ctx context.Context, req *orderv1.PlaceOrderRequest) (*orderv1.PlaceOrderResponse, error) {
	if req.GetIdempotencyKey() == "" {
		// codes.InvalidArgument → HTTP 400 через grpcx.CodeToHTTPStatus:
		// именно так задание "Отсутствует → 400" реализуется без единой
		// HTTP-специфичной строчки в этом методе — см. package doc
		// internal/adapters/http.
		return nil, grpcx.ToStatus(withCode(errEmptyIdempotencyKey, codes.InvalidArgument), "IDEMPOTENCY_KEY_REQUIRED")
	}

	order, err := s.orders.PlaceOrder(ctx, app.PlaceOrderCommand{
		BuyerID:        req.GetBuyerId(),
		ListingID:      req.GetListingId(),
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrIdempotencyInProgress):
			// codes.Aborted → HTTP 409: параллельный повтор с тем же ключом.
			return nil, grpcx.ToStatus(withCode(err, codes.Aborted), "IDEMPOTENCY_IN_PROGRESS")
		case errors.Is(err, domain.ErrListingNotFound):
			return nil, grpcx.ToStatus(withCode(err, codes.NotFound), "LISTING_NOT_FOUND")
		case errors.Is(err, domain.ErrListingNotAvailable):
			return nil, grpcx.ToStatus(withCode(err, codes.FailedPrecondition), "LISTING_NOT_AVAILABLE")
		default:
			return nil, grpcx.ToStatus(err, "ORDER_PLACE_FAILED")
		}
	}
	return &orderv1.PlaceOrderResponse{Order: toProto(order)}, nil
}

func (s *Server) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	order, err := s.orders.GetOrder(ctx, req.GetId())
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, grpcx.ToStatus(withCode(err, codes.NotFound), "ORDER_NOT_FOUND")
		}
		return nil, grpcx.ToStatus(err, "ORDER_GET_FAILED")
	}
	return &orderv1.GetOrderResponse{Order: toProto(order)}, nil
}

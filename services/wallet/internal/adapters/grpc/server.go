// Package grpc — реализация contract'а proto/gosplash/wallet/v1/wallet.proto
// (contract заморожен — правки в него не входят в зону этой фазы).
package grpc

import (
	"context"

	walletv1 "gosplash/gen/go/gosplash/wallet/v1"
	"gosplash/services/wallet/internal/app"
	"gosplash/services/wallet/internal/domain"
)

// Server — тонкий транспортный слой: разбор запроса, вызов app.Service,
// сборка ответа. Вся денежная логика — в internal/app, сюда она НЕ
// протекает (docs/STYLE.md: раскладка "ports & adapters").
type Server struct {
	walletv1.UnimplementedWalletServiceServer

	service *app.Service
}

func NewServer(service *app.Service) *Server {
	return &Server{service: service}
}

func toProtoMoney(m domain.Money) *walletv1.Money {
	return &walletv1.Money{AmountCents: m.AmountCents, Currency: m.Currency}
}

func (s *Server) GetBalance(ctx context.Context, req *walletv1.GetBalanceRequest) (*walletv1.GetBalanceResponse, error) {
	balance, err := s.service.GetBalance(ctx, req.GetAccountId())
	if err != nil {
		return nil, toDomainStatus(err, "WALLET_GET_BALANCE_FAILED")
	}
	return &walletv1.GetBalanceResponse{
		AccountId: balance.AccountID,
		Available: toProtoMoney(domain.Money{AmountCents: balance.Available, Currency: balance.Currency}),
		Reserved:  toProtoMoney(domain.Money{AmountCents: balance.Reserved, Currency: balance.Currency}),
	}, nil
}

func (s *Server) ReserveFunds(ctx context.Context, req *walletv1.ReserveFundsRequest) (*walletv1.ReserveFundsResponse, error) {
	amount := domain.Money{
		AmountCents: req.GetAmount().GetAmountCents(),
		Currency:    req.GetAmount().GetCurrency(),
	}

	reservation, availableAfter, err := s.service.Reserve(ctx, req.GetAccountId(), amount, req.GetIdempotencyKey())
	if err != nil {
		return nil, toDomainStatus(err, "WALLET_RESERVE_FAILED")
	}

	return &walletv1.ReserveFundsResponse{
		ReservationId:  reservation.ID,
		AvailableAfter: toProtoMoney(domain.Money{AmountCents: availableAfter, Currency: reservation.Currency}),
	}, nil
}

func (s *Server) CommitFunds(ctx context.Context, req *walletv1.CommitFundsRequest) (*walletv1.CommitFundsResponse, error) {
	ids, err := s.service.Commit(ctx, req.GetReservationId(), req.GetPayeeAccountId(), req.GetIdempotencyKey())
	if err != nil {
		return nil, toDomainStatus(err, "WALLET_COMMIT_FAILED")
	}
	return &walletv1.CommitFundsResponse{LedgerEntryIds: ids}, nil
}

func (s *Server) ReleaseFunds(ctx context.Context, req *walletv1.ReleaseFundsRequest) (*walletv1.ReleaseFundsResponse, error) {
	availableAfter, err := s.service.Release(ctx, req.GetReservationId(), req.GetReason(), req.GetIdempotencyKey())
	if err != nil {
		return nil, toDomainStatus(err, "WALLET_RELEASE_FAILED")
	}
	return &walletv1.ReleaseFundsResponse{AvailableAfter: toProtoMoney(availableAfter)}, nil
}

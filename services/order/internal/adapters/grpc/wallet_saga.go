package grpc

import (
	"context"

	walletv1 "gosplash/gen/go/gosplash/wallet/v1"
)

// WalletSagaClient — ports.WalletSaga. Единственный клиент order-service
// к wallet (order никогда не читает GetBalance — это диагностика самого
// wallet, а не часть саги). Живёт на *grpc.ClientConn с
// resilience.NotIdempotent — см. подробное обоснование в catalog_saga.go:
// то же рассуждение справедливо и здесь, ReserveFunds/CommitFunds/
// ReleaseFunds двигают деньги, и их идемпотентность держится на
// idempotency_key = order_id, а не на транспортных ретраях.
type WalletSagaClient struct {
	client walletv1.WalletServiceClient
}

func NewWalletSagaClient(client walletv1.WalletServiceClient) *WalletSagaClient {
	return &WalletSagaClient{client: client}
}

func (c *WalletSagaClient) ReserveFunds(ctx context.Context, orderID string, buyerID, amountCents int64, currency string) (string, error) {
	resp, err := c.client.ReserveFunds(ctx, &walletv1.ReserveFundsRequest{
		AccountId:      buyerID,
		Amount:         &walletv1.Money{AmountCents: amountCents, Currency: currency},
		IdempotencyKey: orderID,
	})
	if err != nil {
		return "", err
	}
	return resp.GetReservationId(), nil
}

func (c *WalletSagaClient) CommitFunds(ctx context.Context, orderID, reservationID string, payeeAccountID int64) ([]string, error) {
	resp, err := c.client.CommitFunds(ctx, &walletv1.CommitFundsRequest{
		ReservationId:  reservationID,
		PayeeAccountId: payeeAccountID,
		IdempotencyKey: orderID,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetLedgerEntryIds(), nil
}

func (c *WalletSagaClient) ReleaseFunds(ctx context.Context, orderID, reservationID, reason string) error {
	_, err := c.client.ReleaseFunds(ctx, &walletv1.ReleaseFundsRequest{
		ReservationId:  reservationID,
		Reason:         reason,
		IdempotencyKey: orderID,
	})
	return err
}

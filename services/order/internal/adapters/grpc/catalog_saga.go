package grpc

import (
	"context"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
)

// CatalogSagaClient — ports.CatalogSaga. Живёт в cmd/worker, на ОТДЕЛЬНОМ
// от CatalogReader *grpc.ClientConn с resilience.NotIdempotent (см.
// catalog_reader.go и cmd/worker/main.go): GrantLicense/RevokeLicense
// мутируют состояние catalog, и их безопасность при повторе обеспечивает
// idempotency_key = order_id НА СТОРОНЕ catalog, а не слепой ретрай
// транспорта. Ретраит эти вызовы Temporal — через RetryPolicy activity
// (см. internal/adapters/temporal/workflow.go), с видимой историей попыток
// в Temporal UI. Двойной слой ретраев (grpcx поверх RetryPolicy) означал бы
// два независимых цикла попыток поверх одного и того же вызова — Temporal
// не знает про ретраи grpcx, а grpcx не знает про already-in-flight
// activity, и итоговое число реальных попыток стало бы непредсказуемым.
//
// Ошибки возвращаются КАК ЕСТЬ (status.Error) — классификация
// retryable/non-retryable для Temporal делает
// internal/adapters/temporal.classifyGRPCError: этому пакету не обязательно
// знать про go.temporal.io/sdk, только про сам вызов.
type CatalogSagaClient struct {
	client catalogv1.CatalogServiceClient
}

func NewCatalogSagaClient(client catalogv1.CatalogServiceClient) *CatalogSagaClient {
	return &CatalogSagaClient{client: client}
}

func (c *CatalogSagaClient) GrantLicense(ctx context.Context, orderID, listingID string, buyerID int64) (string, error) {
	resp, err := c.client.GrantLicense(ctx, &catalogv1.GrantLicenseRequest{
		ListingId: listingID,
		BuyerId:   buyerID,
		OrderId:   orderID,
	})
	if err != nil {
		return "", err
	}
	return resp.GetLicenseId(), nil
}

func (c *CatalogSagaClient) RevokeLicense(ctx context.Context, orderID, reason string) (bool, error) {
	resp, err := c.client.RevokeLicense(ctx, &catalogv1.RevokeLicenseRequest{
		OrderId: orderID,
		Reason:  reason,
	})
	if err != nil {
		return false, err
	}
	return resp.GetRevoked(), nil
}

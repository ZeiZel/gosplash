package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
	"gosplash/services/order/internal/domain"
	"gosplash/services/order/internal/ports"
)

// CatalogReader — ports.ListingReader. Единственное, что app.OrderService
// знает о catalog: цена и продавец карточки в момент создания заказа.
//
// Живёт в cmd/order на СВОЁМ *grpc.ClientConn, отдельном от
// CatalogSagaClient (см. catalog_saga.go), — GetListing это чтение и
// вправе использовать pkg/grpcx.Dial с resilience.Idempotent, тогда как
// GrantLicense/RevokeLicense мутируют состояние и требуют
// resilience.NotIdempotent. Один *grpc.ClientConn с одной настройкой
// ретраев на оба класса методов означал бы либо опасные слепые ретраи
// на мутациях, либо чтения без ретраев вовсе — см. комментарий
// у pkg/grpcx.WithRetry.
type CatalogReader struct {
	client catalogv1.CatalogServiceClient
}

func NewCatalogReader(client catalogv1.CatalogServiceClient) *CatalogReader {
	return &CatalogReader{client: client}
}

func (c *CatalogReader) GetListing(ctx context.Context, listingID string) (ports.Listing, error) {
	resp, err := c.client.GetListing(ctx, &catalogv1.GetListingRequest{Id: listingID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return ports.Listing{}, fmt.Errorf("%w: %s", domain.ErrListingNotFound, listingID)
		}
		return ports.Listing{}, err
	}

	l := resp.GetListing()
	return ports.Listing{
		ID:         l.GetId(),
		AuthorID:   l.GetAuthorId(),
		PriceCents: l.GetPriceCents(),
		Currency:   l.GetCurrency(),
		Status:     l.GetStatus(),
	}, nil
}

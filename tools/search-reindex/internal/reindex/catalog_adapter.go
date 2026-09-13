package reindex

import (
	"context"
	"fmt"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
)

// CatalogAdapter — реализация CatalogClient поверх gRPC-клиента catalog.v1.
// ListListings у catalog уже фильтрует по status=published и уже отдаёт
// keyset-курсор (proto/gosplash/catalog/v1/catalog.proto) — этому адаптеру
// остаётся только перевести proto-тип в CatalogListing, ничего не решая
// самостоятельно.
type CatalogAdapter struct {
	client catalogv1.CatalogServiceClient
}

func NewCatalogAdapter(client catalogv1.CatalogServiceClient) *CatalogAdapter {
	return &CatalogAdapter{client: client}
}

func (a *CatalogAdapter) ListListings(ctx context.Context, limit int32, cursor string) ([]CatalogListing, string, error) {
	resp, err := a.client.ListListings(ctx, &catalogv1.ListListingsRequest{
		Limit:  limit,
		Cursor: cursor,
	})
	if err != nil {
		return nil, "", fmt.Errorf("catalog.ListListings: %w", err)
	}

	listings := make([]CatalogListing, 0, len(resp.GetListings()))
	for _, l := range resp.GetListings() {
		listings = append(listings, CatalogListing{
			ID:          l.GetId(),
			AuthorID:    l.GetAuthorId(),
			AuthorName:  l.GetAuthorName(),
			Title:       l.GetTitle(),
			Tags:        l.GetTags(),
			PriceCents:  l.GetPriceCents(),
			Currency:    l.GetCurrency(),
			Status:      l.GetStatus(),
			PublishedAt: l.GetPublishedAt().AsTime(),
		})
	}
	return listings, resp.GetNextCursor(), nil
}

var _ CatalogClient = (*CatalogAdapter)(nil)

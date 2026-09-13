package redis

import (
	"context"
	"time"

	"gosplash/pkg/redisx"
	"gosplash/services/catalog/internal/ports"
)

// ViewCounter — реализация ports.ViewCounter поверх pkg/redisx (Sorted Set,
// топ-N за сутки, см. pkg/redisx/topn.go).
type ViewCounter struct {
	client *redisx.Client
}

func NewViewCounter(client *redisx.Client) *ViewCounter {
	return &ViewCounter{client: client}
}

func (v *ViewCounter) IncrView(ctx context.Context, id string, at time.Time) error {
	return v.client.IncrView(ctx, id, at)
}

func (v *ViewCounter) Top(ctx context.Context, at time.Time, n int) ([]ports.TopEntry, error) {
	entries, err := v.client.Top(ctx, at, n)
	if err != nil {
		return nil, err
	}
	out := make([]ports.TopEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, ports.TopEntry{ListingID: e.Item, Views: e.Score})
	}
	return out, nil
}

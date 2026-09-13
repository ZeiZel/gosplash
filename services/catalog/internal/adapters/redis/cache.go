// Package redis — адаптер каталога к Redis: cache-aside карточки (cache.go)
// и счётчик просмотров (views.go), оба поверх pkg/redisx.
package redis

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"gosplash/pkg/redisx"
	"gosplash/services/catalog/internal/domain"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
)

// Cache — реализация ports.ListingCache.
//
// Кэшируется PROTO-представление карточки (redisx.GetOrLoadProto), а не
// JSON: contract уже описывает ровно то, что нужно отдать клиенту, и
// повторно объявлять ту же структуру для сериализации в кэш было бы чистым
// дублированием. Домен (internal/domain) при этом не знает про protobuf —
// перевод туда и обратно делает этот адаптер, а не app.
type Cache struct {
	client *redisx.Client
	ttl    time.Duration
}

func NewCache(client *redisx.Client, ttl time.Duration) *Cache {
	return &Cache{client: client, ttl: ttl}
}

func (c *Cache) key(id string) string {
	return c.client.Key("listing", id)
}

func (c *Cache) GetOrLoad(ctx context.Context, id string, load func(context.Context) (*domain.Listing, error)) (*domain.Listing, error) {
	proto, err := redisx.GetOrLoadProto[catalogv1.Listing](ctx, c.client, c.key(id), c.ttl, func(ctx context.Context) (*catalogv1.Listing, error) {
		listing, err := load(ctx)
		if err != nil {
			return nil, err
		}
		return listingToProto(listing), nil
	})
	if err != nil {
		return nil, err
	}
	return listingFromProto(proto), nil
}

func (c *Cache) Invalidate(ctx context.Context, id string) error {
	return c.client.Invalidate(ctx, c.key(id))
}

// listingToProto/listingFromProto — те же поля, что и в
// adapters/grpc/convert.go, но своя копия: адаптеры не должны зависеть друг
// от друга (redis не импортирует grpc), а стоимость дублирования — те же
// "две функции лишнего кода на сущность", которые docs/STYLE.md прямо
// разрешает ради независимости адаптеров.
func listingToProto(l *domain.Listing) *catalogv1.Listing {
	p := &catalogv1.Listing{
		Id:         l.ID,
		AuthorId:   l.AuthorID,
		AuthorName: l.AuthorName,
		Title:      l.Title,
		Tags:       l.Tags,
		StorageKey: l.StorageKey,
		Thumbnails: l.Thumbnails,
		PriceCents: l.PriceCents,
		Currency:   l.Currency,
		Status:     l.Status,
		Width:      l.Width,
		Height:     l.Height,
	}
	if !l.PublishedAt.IsZero() {
		p.PublishedAt = timestamppb.New(l.PublishedAt)
	}
	return p
}

func listingFromProto(p *catalogv1.Listing) *domain.Listing {
	l := &domain.Listing{
		ID:         p.GetId(),
		AuthorID:   p.GetAuthorId(),
		AuthorName: p.GetAuthorName(),
		Title:      p.GetTitle(),
		Tags:       p.GetTags(),
		StorageKey: p.GetStorageKey(),
		Thumbnails: p.GetThumbnails(),
		PriceCents: p.GetPriceCents(),
		Currency:   p.GetCurrency(),
		Status:     p.GetStatus(),
		Width:      p.GetWidth(),
		Height:     p.GetHeight(),
	}
	if ts := p.GetPublishedAt(); ts != nil {
		l.PublishedAt = ts.AsTime()
	}
	return l
}

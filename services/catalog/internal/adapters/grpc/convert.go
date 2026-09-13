package grpc

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
	"gosplash/services/catalog/internal/domain"
)

// toProto переводит доменную модель в контракт. Ручное отображение выглядит
// скучным, но именно оно не даёт внутренней модели протечь в публичный API:
// добавишь поле в domain.Listing — оно не появится в ответе, пока ты этого
// не захочешь здесь явно.
func toProto(l *domain.Listing) *catalogv1.Listing {
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

// toWatchResponse строит один элемент стрима WatchListing.
func toWatchResponse(listing *domain.Listing, change string, occurredAt time.Time) *catalogv1.WatchListingResponse {
	resp := &catalogv1.WatchListingResponse{
		Change:     change,
		OccurredAt: timestamppb.New(occurredAt),
	}
	if listing != nil {
		resp.Listing = toProto(listing)
	}
	return resp
}

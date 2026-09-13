package grpc

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	orderv1 "gosplash/gen/go/gosplash/order/v1"
	"gosplash/services/order/internal/domain"
)

// toProto — ручное отображение domain.Order → orderv1.Order. Скучно, но не
// даёт внутренней модели (в частности, AuthorID — payee саги) протечь
// в публичный контракт, которого proto для неё и не предусматривает.
func toProto(o *domain.Order) *orderv1.Order {
	p := &orderv1.Order{
		Id:            o.ID,
		BuyerId:       o.BuyerID,
		ListingId:     o.ListingID,
		PriceCents:    o.PriceCents,
		Currency:      o.Currency,
		Status:        o.Status,
		FailureReason: o.FailureReason,
		LicenseId:     o.LicenseID,
	}
	if !o.CreatedAt.IsZero() {
		p.CreatedAt = timestamppb.New(o.CreatedAt)
	}
	if !o.UpdatedAt.IsZero() {
		p.UpdatedAt = timestamppb.New(o.UpdatedAt)
	}
	return p
}

package grpc

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
	"gosplash/pkg/grpcx"
	"gosplash/services/catalog/internal/app"
	"gosplash/services/catalog/internal/domain"
)

var (
	errEmptyOrderID       = errors.New("order_id обязателен")
	errWatchClientTooSlow = errors.New("клиент WatchListing не успевал читать поток и был отключён")
)

// Server — реализация contract'а из proto/gosplash/catalog/v1/catalog.proto.
//
// UnimplementedCatalogServiceServer встроен намеренно (так советует сам
// gRPC): когда в .proto добавится новый метод, сервис продолжит
// компилироваться и будет отвечать Unimplemented, а не падать при сборке.
type Server struct {
	catalogv1.UnimplementedCatalogServiceServer

	listings *app.ListingService
	licenses *app.LicenseService
	hub      *app.WatchHub
}

func NewServer(listings *app.ListingService, licenses *app.LicenseService, hub *app.WatchHub) *Server {
	return &Server{listings: listings, licenses: licenses, hub: hub}
}

func (s *Server) GetListing(ctx context.Context, req *catalogv1.GetListingRequest) (*catalogv1.GetListingResponse, error) {
	listing, err := s.listings.Get(ctx, req.GetId())
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, grpcx.ToStatus(withCode(err, codes.NotFound), "LISTING_NOT_FOUND")
		}
		return nil, grpcx.ToStatus(err, "LISTING_GET_FAILED")
	}
	return &catalogv1.GetListingResponse{Listing: toProto(listing)}, nil
}

func (s *Server) ListListings(ctx context.Context, req *catalogv1.ListListingsRequest) (*catalogv1.ListListingsResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	listings, nextCursor, err := s.listings.List(ctx, limit, req.GetCursor(), req.GetTags())
	if err != nil {
		return nil, grpcx.ToStatus(err, "LISTINGS_LIST_FAILED")
	}

	resp := &catalogv1.ListListingsResponse{
		Listings:   make([]*catalogv1.Listing, 0, len(listings)),
		NextCursor: nextCursor,
	}
	for i := range listings {
		resp.Listings = append(resp.Listings, toProto(&listings[i]))
	}
	return resp, nil
}

func (s *Server) GrantLicense(ctx context.Context, req *catalogv1.GrantLicenseRequest) (*catalogv1.GrantLicenseResponse, error) {
	if req.GetOrderId() == "" {
		return nil, grpcx.ToStatus(withCode(errEmptyOrderID, codes.InvalidArgument), "ORDER_ID_REQUIRED")
	}

	license, err := s.licenses.Grant(ctx, req.GetListingId(), req.GetBuyerId(), req.GetOrderId())
	if err != nil {
		return nil, grpcx.ToStatus(err, "LICENSE_GRANT_FAILED")
	}
	var grantedAt *timestamppb.Timestamp
	if !license.GrantedAt.IsZero() {
		grantedAt = timestamppb.New(license.GrantedAt)
	}
	return &catalogv1.GrantLicenseResponse{
		LicenseId: license.ID,
		GrantedAt: grantedAt,
	}, nil
}

func (s *Server) RevokeLicense(ctx context.Context, req *catalogv1.RevokeLicenseRequest) (*catalogv1.RevokeLicenseResponse, error) {
	if req.GetOrderId() == "" {
		return nil, grpcx.ToStatus(withCode(errEmptyOrderID, codes.InvalidArgument), "ORDER_ID_REQUIRED")
	}

	revoked, err := s.licenses.Revoke(ctx, req.GetOrderId(), req.GetReason())
	if err != nil {
		return nil, grpcx.ToStatus(err, "LICENSE_REVOKE_FAILED")
	}
	// revoked=false — НЕ ошибка (см. proto и domain.License): лицензию не
	// успели выдать или её уже отозвали, компенсация обязана пережить это
	// молча.
	return &catalogv1.RevokeLicenseResponse{Revoked: revoked}, nil
}

// WatchListing — server-streaming поверх app.WatchHub: подписка на
// внутренние обновления от индексатора, без опроса базы.
func (s *Server) WatchListing(req *catalogv1.WatchListingRequest, stream catalogv1.CatalogService_WatchListingServer) error {
	ctx := stream.Context()
	id := req.GetId()

	ch, cancel := s.hub.Subscribe(id)
	defer cancel()

	// Снимок текущего состояния сразу при подключении: иначе клиент,
	// открывший WatchListing уже ПОСЛЕ публикации, ждал бы неопределённо
	// долго первого следующего изменения, хотя карточка уже готова.
	if listing, err := s.listings.Get(ctx, id); err == nil {
		if err := stream.Send(toWatchResponse(listing, "updated", time.Now())); err != nil {
			return grpcx.ToStatus(err, "WATCH_SEND_FAILED")
		}
	}

	for {
		select {
		case <-ctx.Done():
			// Обязательный выход по отмене контекста: клиент отключился
			// или сервис останавливается — RPC обязан завершиться, а не
			// держать горутину и подписку вечно.
			return nil
		case evt, ok := <-ch:
			if !ok {
				// Канал закрыт хабом: клиент отставал (см. app.WatchHub.Notify)
				// и был отключён принудительно, чтобы не тормозить индексатор.
				return grpcx.ToStatus(withCode(errWatchClientTooSlow, codes.ResourceExhausted), "WATCH_CLIENT_TOO_SLOW")
			}
			if err := stream.Send(toWatchResponse(evt.Listing, evt.Change, evt.OccurredAt)); err != nil {
				return grpcx.ToStatus(err, "WATCH_SEND_FAILED")
			}
		}
	}
}

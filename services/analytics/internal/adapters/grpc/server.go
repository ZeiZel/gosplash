// Package grpc — реализация contract'а
// proto/gosplash/analytics/v1/analytics.proto. Контракт заморожен — этот
// файл под него подстраивается, а не наоборот.
package grpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"

	analyticsv1 "gosplash/gen/go/gosplash/analytics/v1"
	"gosplash/pkg/grpcx"

	"gosplash/services/analytics/internal/app"
	"gosplash/services/analytics/internal/domain"
)

// Server — тонкий адаптер: вся логика (валидация, чтение) — в
// internal/app.StatsService, здесь только перевод proto ⇄ domain и
// domain-ошибок ⇄ codes.
type Server struct {
	analyticsv1.UnimplementedAnalyticsServiceServer

	stats *app.StatsService
}

func NewServer(stats *app.StatsService) *Server {
	return &Server{stats: stats}
}

func (s *Server) TopPhotos(ctx context.Context, req *analyticsv1.TopPhotosRequest) (*analyticsv1.TopPhotosResponse, error) {
	ranks, err := s.stats.TopPhotos(ctx, req.GetPeriod(), req.GetLimit())
	if err != nil {
		return nil, grpcx.ToStatus(toCoded(err), "TOP_PHOTOS_FAILED")
	}

	resp := &analyticsv1.TopPhotosResponse{
		Photos: make([]*analyticsv1.PhotoRank, 0, len(ranks)),
	}
	for _, r := range ranks {
		resp.Photos = append(resp.Photos, &analyticsv1.PhotoRank{
			PhotoId:   r.PhotoID,
			AuthorId:  r.AuthorID,
			Views:     r.Views,
			Purchases: r.Purchases,
		})
	}
	return resp, nil
}

func (s *Server) PhotoStats(ctx context.Context, req *analyticsv1.PhotoStatsRequest) (*analyticsv1.PhotoStatsResponse, error) {
	stats, err := s.stats.PhotoStats(ctx, req.GetPhotoId(), req.GetPeriod())
	if err != nil {
		return nil, grpcx.ToStatus(toCoded(err), "PHOTO_STATS_FAILED")
	}

	return &analyticsv1.PhotoStatsResponse{
		PhotoId:             stats.PhotoID,
		Views:               stats.Views,
		UniqueViewersApprox: stats.UniqueViewersApprox,
		Purchases:           stats.Purchases,
		RevenueCents:        stats.RevenueCents,
	}, nil
}

func (s *Server) FunnelStats(ctx context.Context, req *analyticsv1.FunnelStatsRequest) (*analyticsv1.FunnelStatsResponse, error) {
	stats, err := s.stats.FunnelStats(ctx, req.GetPeriod())
	if err != nil {
		return nil, grpcx.ToStatus(toCoded(err), "FUNNEL_STATS_FAILED")
	}

	return &analyticsv1.FunnelStatsResponse{
		Uploads:      stats.Uploads,
		Thumbnails:   stats.Thumbnails,
		ReadyPercent: stats.ReadyPercent,
		LatencyP50Ms: stats.LatencyP50Ms,
		LatencyP95Ms: stats.LatencyP95Ms,
	}, nil
}

func (s *Server) AuthorStats(ctx context.Context, req *analyticsv1.AuthorStatsRequest) (*analyticsv1.AuthorStatsResponse, error) {
	stats, err := s.stats.AuthorStats(ctx, req.GetAuthorId(), req.GetPeriod())
	if err != nil {
		return nil, grpcx.ToStatus(toCoded(err), "AUTHOR_STATS_FAILED")
	}

	return &analyticsv1.AuthorStatsResponse{
		Views:               stats.Views,
		UniqueViewersApprox: stats.UniqueViewersApprox,
		Purchases:           stats.Purchases,
		RevenueCents:        stats.RevenueCents,
		Uploads:             stats.Uploads,
	}, nil
}

// toCoded переводит доменные ошибки валидации в конкретный gRPC-код.
//
// Маппинг живёт здесь (адаптер), а не в domain — см. docs/STYLE.md и тот же
// приём в services/catalog/internal/adapters/grpc/errors.go: домен сравнивает
// ошибки через errors.Is и ничего не знает про codes.Code.
func toCoded(err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalidPeriod),
		errors.Is(err, domain.ErrInvalidLimit),
		errors.Is(err, domain.ErrEmptyPhotoID),
		errors.Is(err, domain.ErrInvalidAuthorID):
		return withCode(err, codes.InvalidArgument)
	default:
		return err
	}
}

type codedErr struct {
	error
	code codes.Code
}

func (e codedErr) GRPCCode() codes.Code { return e.code }

func withCode(err error, code codes.Code) error {
	if err == nil {
		return nil
	}
	return codedErr{error: err, code: code}
}

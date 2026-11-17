// Package app — сценарии analytics-сервиса. Импортирует только domain и
// ports (docs/STYLE.md): ни ClickHouse, ни franz-go, ни gRPC здесь не видно,
// и это значит, что StatsService и Ingest тестируются поддельными портами
// без единого контейнера (см. stats_test.go, ingest_test.go).
package app

import (
	"context"
	"fmt"

	"gosplash/services/analytics/internal/domain"
	"gosplash/services/analytics/internal/ports"
)

// StatsService — сценарии чтения: TopPhotos и PhotoStats из
// proto/gosplash/analytics/v1/analytics.proto.
type StatsService struct {
	reader ports.StatsReader
}

func NewStatsService(reader ports.StatsReader) *StatsService {
	return &StatsService{reader: reader}
}

// TopPhotos валидирует period и limit и делегирует чтение ports.StatsReader.
//
// Валидация — здесь, а не в adapters/grpc: правило "period — один из трёх
// периодов, limit — 1..1000" одинаково для gRPC и для любого другого
// транспорта, который у сервиса появится. Адаптер отвечает только за
// перевод ошибки в свой протокол (grpcx.ToStatus + codes.InvalidArgument).
func (s *StatsService) TopPhotos(ctx context.Context, periodRaw string, limit int32) ([]domain.PhotoRank, error) {
	period, err := domain.ParsePeriod(periodRaw)
	if err != nil {
		return nil, err
	}
	if err := domain.ValidateLimit(limit); err != nil {
		return nil, err
	}
	if limit == 0 {
		limit = domain.DefaultTopPhotosLimit
	}

	ranks, err := s.reader.TopPhotos(ctx, period, limit)
	if err != nil {
		return nil, fmt.Errorf("топ фото за %s: %w", period, err)
	}
	return ranks, nil
}

// PhotoStats валидирует photo_id и period и делегирует чтение.
func (s *StatsService) PhotoStats(ctx context.Context, photoID, periodRaw string) (domain.PhotoStats, error) {
	if photoID == "" {
		return domain.PhotoStats{}, domain.ErrEmptyPhotoID
	}
	period, err := domain.ParsePeriod(periodRaw)
	if err != nil {
		return domain.PhotoStats{}, err
	}

	stats, err := s.reader.PhotoStats(ctx, photoID, period)
	if err != nil {
		return domain.PhotoStats{}, fmt.Errorf("сводка по фото %s за %s: %w", photoID, period, err)
	}
	return stats, nil
}

// FunnelStats валидирует period и делегирует чтение. Параметров-идентификаторов
// у метода нет: воронка одна на всю систему — это отчёт о конвейере, а не
// о сущности.
func (s *StatsService) FunnelStats(ctx context.Context, periodRaw string) (domain.FunnelStats, error) {
	period, err := domain.ParsePeriod(periodRaw)
	if err != nil {
		return domain.FunnelStats{}, err
	}

	stats, err := s.reader.FunnelStats(ctx, period)
	if err != nil {
		return domain.FunnelStats{}, fmt.Errorf("воронка загрузки за %s: %w", period, err)
	}
	return stats, nil
}

// AuthorStats валидирует authorID и period и делегирует чтение.
func (s *StatsService) AuthorStats(ctx context.Context, authorID int64, periodRaw string) (domain.AuthorStats, error) {
	if authorID <= 0 {
		return domain.AuthorStats{}, fmt.Errorf("%w: получено %d", domain.ErrInvalidAuthorID, authorID)
	}
	period, err := domain.ParsePeriod(periodRaw)
	if err != nil {
		return domain.AuthorStats{}, err
	}

	stats, err := s.reader.AuthorStats(ctx, authorID, period)
	if err != nil {
		return domain.AuthorStats{}, fmt.Errorf("сводка по автору %d за %s: %w", authorID, period, err)
	}
	return stats, nil
}

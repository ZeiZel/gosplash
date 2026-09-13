package app_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/analytics/internal/app"
	"gosplash/services/analytics/internal/domain"
)

// fakeStatsReader — поддельный ports.StatsReader: прикладной слой
// тестируется без единого контейнера ClickHouse (docs/STYLE.md).
type fakeStatsReader struct {
	topPhotosCalled bool
	gotPeriod       domain.Period
	gotLimit        int32
	topPhotos       []domain.PhotoRank
	topPhotosErr    error

	photoStatsCalled bool
	gotPhotoID       string
	photoStats       domain.PhotoStats
	photoStatsErr    error
}

func (f *fakeStatsReader) TopPhotos(_ context.Context, period domain.Period, limit int32) ([]domain.PhotoRank, error) {
	f.topPhotosCalled = true
	f.gotPeriod = period
	f.gotLimit = limit
	return f.topPhotos, f.topPhotosErr
}

func (f *fakeStatsReader) PhotoStats(_ context.Context, photoID string, period domain.Period) (domain.PhotoStats, error) {
	f.photoStatsCalled = true
	f.gotPhotoID = photoID
	f.gotPeriod = period
	return f.photoStats, f.photoStatsErr
}

func TestStatsService_TopPhotos_NevernyPeriod(t *testing.T) {
	reader := &fakeStatsReader{}
	svc := app.NewStatsService(reader)

	_, err := svc.TopPhotos(context.Background(), "century", 10)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidPeriod)
	assert.False(t, reader.topPhotosCalled, "при невалидном периоде читать ClickHouse не за чем")
}

func TestStatsService_TopPhotos_LimitVneDiapazona(t *testing.T) {
	reader := &fakeStatsReader{}
	svc := app.NewStatsService(reader)

	_, err := svc.TopPhotos(context.Background(), "day", 100000)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidLimit)
	assert.False(t, reader.topPhotosCalled)
}

func TestStatsService_TopPhotos_NulevoyLimitZamenyaetsyaDefoltom(t *testing.T) {
	reader := &fakeStatsReader{topPhotos: []domain.PhotoRank{{PhotoID: "p1", Views: 5}}}
	svc := app.NewStatsService(reader)

	got, err := svc.TopPhotos(context.Background(), "week", 0)

	require.NoError(t, err)
	assert.Equal(t, domain.DefaultTopPhotosLimit, int(reader.gotLimit))
	assert.Equal(t, domain.PeriodWeek, reader.gotPeriod)
	assert.Equal(t, reader.topPhotos, got)
}

func TestStatsService_TopPhotos_OshibkaChiteniyaObernutaSKontekstom(t *testing.T) {
	readErr := errors.New("clickhouse недоступен")
	reader := &fakeStatsReader{topPhotosErr: readErr}
	svc := app.NewStatsService(reader)

	_, err := svc.TopPhotos(context.Background(), "day", 10)

	require.Error(t, err)
	assert.ErrorIs(t, err, readErr)
}

func TestStatsService_PhotoStats_PustoyPhotoID(t *testing.T) {
	reader := &fakeStatsReader{}
	svc := app.NewStatsService(reader)

	_, err := svc.PhotoStats(context.Background(), "", "day")

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrEmptyPhotoID)
	assert.False(t, reader.photoStatsCalled)
}

func TestStatsService_PhotoStats_NevernyPeriod(t *testing.T) {
	reader := &fakeStatsReader{}
	svc := app.NewStatsService(reader)

	_, err := svc.PhotoStats(context.Background(), "photo-1", "quarter")

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidPeriod)
}

func TestStatsService_PhotoStats_UspeshnyZapros(t *testing.T) {
	want := domain.PhotoStats{PhotoID: "photo-1", Views: 42, UniqueViewersApprox: 10, Purchases: 2, RevenueCents: 999}
	reader := &fakeStatsReader{photoStats: want}
	svc := app.NewStatsService(reader)

	got, err := svc.PhotoStats(context.Background(), "photo-1", "month")

	require.NoError(t, err)
	assert.Equal(t, "photo-1", reader.gotPhotoID)
	assert.Equal(t, domain.PeriodMonth, reader.gotPeriod)
	assert.Equal(t, want, got)
}

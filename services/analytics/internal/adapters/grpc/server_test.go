package grpc_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	analyticsv1 "gosplash/gen/go/gosplash/analytics/v1"

	analyticsgrpc "gosplash/services/analytics/internal/adapters/grpc"
	"gosplash/services/analytics/internal/app"
	"gosplash/services/analytics/internal/domain"
)

type fakeReader struct {
	topPhotos []domain.PhotoRank
	stats     domain.PhotoStats
}

func (f *fakeReader) TopPhotos(context.Context, domain.Period, int32) ([]domain.PhotoRank, error) {
	return f.topPhotos, nil
}

func (f *fakeReader) PhotoStats(context.Context, string, domain.Period) (domain.PhotoStats, error) {
	return f.stats, nil
}

func TestServer_TopPhotos_NevernyPeriodDayotInvalidArgument(t *testing.T) {
	srv := analyticsgrpc.NewServer(app.NewStatsService(&fakeReader{}))

	_, err := srv.TopPhotos(context.Background(), &analyticsv1.TopPhotosRequest{Period: "century", Limit: 10})

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestServer_TopPhotos_LimitVneDiapazonaDayotInvalidArgument(t *testing.T) {
	srv := analyticsgrpc.NewServer(app.NewStatsService(&fakeReader{}))

	_, err := srv.TopPhotos(context.Background(), &analyticsv1.TopPhotosRequest{Period: "day", Limit: 999999})

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestServer_TopPhotos_MappingVProtobuf(t *testing.T) {
	reader := &fakeReader{topPhotos: []domain.PhotoRank{
		{PhotoID: "p1", AuthorID: 10, Views: 100, Purchases: 5},
		{PhotoID: "p2", AuthorID: 20, Views: 50, Purchases: 0},
	}}
	srv := analyticsgrpc.NewServer(app.NewStatsService(reader))

	resp, err := srv.TopPhotos(context.Background(), &analyticsv1.TopPhotosRequest{Period: "week", Limit: 10})

	require.NoError(t, err)
	require.Len(t, resp.GetPhotos(), 2)
	assert.Equal(t, "p1", resp.GetPhotos()[0].GetPhotoId())
	assert.Equal(t, int64(10), resp.GetPhotos()[0].GetAuthorId())
	assert.Equal(t, int64(100), resp.GetPhotos()[0].GetViews())
	assert.Equal(t, int64(5), resp.GetPhotos()[0].GetPurchases())
	assert.Equal(t, "p2", resp.GetPhotos()[1].GetPhotoId())
}

func TestServer_PhotoStats_PustoyPhotoIdDayotInvalidArgument(t *testing.T) {
	srv := analyticsgrpc.NewServer(app.NewStatsService(&fakeReader{}))

	_, err := srv.PhotoStats(context.Background(), &analyticsv1.PhotoStatsRequest{PhotoId: "", Period: "day"})

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestServer_PhotoStats_MappingVProtobuf(t *testing.T) {
	reader := &fakeReader{stats: domain.PhotoStats{
		PhotoID: "p1", Views: 100, UniqueViewersApprox: 42, Purchases: 3, RevenueCents: 4500,
	}}
	srv := analyticsgrpc.NewServer(app.NewStatsService(reader))

	resp, err := srv.PhotoStats(context.Background(), &analyticsv1.PhotoStatsRequest{PhotoId: "p1", Period: "month"})

	require.NoError(t, err)
	assert.Equal(t, "p1", resp.GetPhotoId())
	assert.Equal(t, int64(100), resp.GetViews())
	assert.Equal(t, int64(42), resp.GetUniqueViewersApprox())
	assert.Equal(t, int64(3), resp.GetPurchases())
	assert.Equal(t, int64(4500), resp.GetRevenueCents())
}

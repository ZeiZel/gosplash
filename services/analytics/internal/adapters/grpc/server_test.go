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
	funnel    domain.FunnelStats
	author    domain.AuthorStats
}

func (f *fakeReader) TopPhotos(context.Context, domain.Period, int32) ([]domain.PhotoRank, error) {
	return f.topPhotos, nil
}

func (f *fakeReader) PhotoStats(context.Context, string, domain.Period) (domain.PhotoStats, error) {
	return f.stats, nil
}

func (f *fakeReader) FunnelStats(context.Context, domain.Period) (domain.FunnelStats, error) {
	return f.funnel, nil
}

func (f *fakeReader) AuthorStats(context.Context, int64, domain.Period) (domain.AuthorStats, error) {
	return f.author, nil
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

func TestServer_FunnelStats_NevernyPeriodDayotInvalidArgument(t *testing.T) {
	srv := analyticsgrpc.NewServer(app.NewStatsService(&fakeReader{}))

	_, err := srv.FunnelStats(context.Background(), &analyticsv1.FunnelStatsRequest{Period: "century"})

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestServer_FunnelStats_MappingVProtobuf(t *testing.T) {
	reader := &fakeReader{funnel: domain.FunnelStats{
		Uploads: 100, Thumbnails: 95, ReadyPercent: 95, LatencyP50Ms: 1500, LatencyP95Ms: 25000,
	}}
	srv := analyticsgrpc.NewServer(app.NewStatsService(reader))

	resp, err := srv.FunnelStats(context.Background(), &analyticsv1.FunnelStatsRequest{Period: "week"})

	require.NoError(t, err)
	assert.Equal(t, int64(100), resp.GetUploads())
	assert.Equal(t, int64(95), resp.GetThumbnails())
	assert.InDelta(t, 95.0, resp.GetReadyPercent(), 1e-9)
	assert.Equal(t, int64(1500), resp.GetLatencyP50Ms())
	assert.Equal(t, int64(25000), resp.GetLatencyP95Ms())
}

func TestServer_AuthorStats_NepolozhitelnyIDDayotInvalidArgument(t *testing.T) {
	srv := analyticsgrpc.NewServer(app.NewStatsService(&fakeReader{}))

	_, err := srv.AuthorStats(context.Background(), &analyticsv1.AuthorStatsRequest{AuthorId: 0, Period: "day"})

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestServer_AuthorStats_MappingVProtobuf(t *testing.T) {
	reader := &fakeReader{author: domain.AuthorStats{
		Views: 500, UniqueViewersApprox: 120, Purchases: 4, RevenueCents: 12500, Uploads: 9,
	}}
	srv := analyticsgrpc.NewServer(app.NewStatsService(reader))

	resp, err := srv.AuthorStats(context.Background(), &analyticsv1.AuthorStatsRequest{AuthorId: 42, Period: "month"})

	require.NoError(t, err)
	assert.Equal(t, int64(500), resp.GetViews())
	assert.Equal(t, int64(120), resp.GetUniqueViewersApprox())
	assert.Equal(t, int64(4), resp.GetPurchases())
	assert.Equal(t, int64(12500), resp.GetRevenueCents())
	assert.Equal(t, int64(9), resp.GetUploads())
}

package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	searchv1 "gosplash/gen/go/gosplash/search/v1"
	"gosplash/services/search/internal/app"
	"gosplash/services/search/internal/domain"
)

type fakeIndex struct {
	gotQuery domain.SearchQuery
	result   domain.SearchResult
	err      error
}

func (f *fakeIndex) Search(_ context.Context, query domain.SearchQuery) (domain.SearchResult, error) {
	f.gotQuery = query
	return f.result, f.err
}

func TestServer_Search_PerekladyvayetZaprosIOtvet(t *testing.T) {
	index := &fakeIndex{
		result: domain.SearchResult{
			Hits: []domain.SearchHit{
				{ListingID: "l1", Title: "Закат", AuthorID: 1, AuthorName: "Иван", Tags: []string{"закат"}, PriceCents: 500, Score: 1.5},
			},
			Total:      1,
			NextCursor: "cursor-1",
			TagFacets:  []domain.Facet{{Value: "закат", Count: 1}},
		},
	}
	server := NewServer(app.NewSearchService(index))

	req := &searchv1.SearchRequest{
		Query: "закат",
		Filters: &searchv1.Filters{
			Tags:          []string{"закат"},
			PriceMinCents: 100,
			PriceMaxCents: 1000,
			AuthorId:      1,
		},
		Limit:  10,
		Cursor: "",
	}

	resp, err := server.Search(context.Background(), req)
	require.NoError(t, err)

	assert.Equal(t, "закат", index.gotQuery.Query)
	assert.Equal(t, []string{"закат"}, index.gotQuery.Tags)
	assert.Equal(t, int64(1), index.gotQuery.AuthorID)

	require.Len(t, resp.GetHits(), 1)
	assert.Equal(t, "l1", resp.GetHits()[0].GetListingId())
	assert.Equal(t, int64(1), resp.GetTotal())
	assert.Equal(t, "cursor-1", resp.GetNextCursor())
	require.Len(t, resp.GetTagFacets(), 1)
	assert.Equal(t, "закат", resp.GetTagFacets()[0].GetValue())
}

func TestServer_Search_IndeksNedostupenDayetUnavailable(t *testing.T) {
	index := &fakeIndex{err: domain.ErrIndexUnavailable}
	server := NewServer(app.NewSearchService(index))

	_, err := server.Search(context.Background(), &searchv1.SearchRequest{})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "ошибка обязана быть gRPC-статусом, а не голым err")
	assert.Equal(t, codes.Unavailable, st.Code())
}

func TestServer_Search_BitiyKursorDayetInvalidArgument(t *testing.T) {
	index := &fakeIndex{err: domain.ErrInvalidDocument}
	server := NewServer(app.NewSearchService(index))

	_, err := server.Search(context.Background(), &searchv1.SearchRequest{Cursor: "мусор"})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestServer_Search_NeizvestnayaOshibkaDayetInternal(t *testing.T) {
	index := &fakeIndex{err: assertAnError{}}
	server := NewServer(app.NewSearchService(index))

	_, err := server.Search(context.Background(), &searchv1.SearchRequest{})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
}

type assertAnError struct{}

func (assertAnError) Error() string { return "что-то пошло не так" }

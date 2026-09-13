package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestSearchService_ResolveLimit(t *testing.T) {
	tests := []struct {
		name      string
		limit     int32
		wantLimit int32
	}{
		{"ноль — значение по умолчанию", 0, defaultLimit},
		{"отрицательное — значение по умолчанию", -5, defaultLimit},
		{"в пределах — берётся как есть", 10, 10},
		{"слишком большое — обрезается потолком", 10_000, maxLimit},
		{"ровно потолок — не трогается", maxLimit, maxLimit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := &fakeIndex{}
			svc := NewSearchService(index)

			_, err := svc.Search(context.Background(), domain.SearchQuery{Limit: tt.limit})
			require.NoError(t, err)
			assert.Equal(t, tt.wantLimit, index.gotQuery.Limit)
		})
	}
}

func TestSearchService_ProbrasyvaetOstalnyePolyaBezIzmeneniy(t *testing.T) {
	index := &fakeIndex{}
	svc := NewSearchService(index)

	query := domain.SearchQuery{
		Query:         "закат",
		Tags:          []string{"море"},
		PriceMinCents: 100,
		PriceMaxCents: 2000,
		AuthorID:      7,
		Cursor:        "abc",
	}
	_, err := svc.Search(context.Background(), query)
	require.NoError(t, err)

	assert.Equal(t, "закат", index.gotQuery.Query)
	assert.Equal(t, []string{"море"}, index.gotQuery.Tags)
	assert.Equal(t, int32(100), index.gotQuery.PriceMinCents)
	assert.Equal(t, int32(2000), index.gotQuery.PriceMaxCents)
	assert.Equal(t, int64(7), index.gotQuery.AuthorID)
	assert.Equal(t, "abc", index.gotQuery.Cursor)
}

func TestSearchService_ProkidyvaetOshibkuIndeksa(t *testing.T) {
	wantErr := assert.AnError
	index := &fakeIndex{err: wantErr}
	svc := NewSearchService(index)

	_, err := svc.Search(context.Background(), domain.SearchQuery{})
	assert.ErrorIs(t, err, wantErr)
}

package reindex

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCatalog — подделка CatalogClient: страницы задаются заранее, без
// единого похода в сеть (docs/STYLE.md: прикладной слой тестируется
// подделками портов).
type fakeCatalog struct {
	pages       [][]CatalogListing
	nextCursors []string
	calls       []struct {
		limit  int32
		cursor string
	}
	err error
}

func (f *fakeCatalog) ListListings(_ context.Context, limit int32, cursor string) ([]CatalogListing, string, error) {
	f.calls = append(f.calls, struct {
		limit  int32
		cursor string
	}{limit, cursor})
	if f.err != nil {
		return nil, "", f.err
	}
	idx := len(f.calls) - 1
	if idx >= len(f.pages) {
		return nil, "", nil
	}
	return f.pages[idx], f.nextCursors[idx], nil
}

// fakeES — подделка ESClient: фиксирует вызовы без реального Elasticsearch.
type fakeES struct {
	currentIndex string
	hadOld       bool
	currentErr   error

	createdIndexes []string
	createErr      error

	bulkCalls  []bulkCall
	bulkErr    error
	switchCall *switchCall
	switchErr  error
	deleted    []string
	deleteErr  error
}

type bulkCall struct {
	index string
	docs  []CatalogListing
}

type switchCall struct {
	alias, newIndex, oldIndex string
}

func (f *fakeES) CreateIndex(_ context.Context, name string) error {
	f.createdIndexes = append(f.createdIndexes, name)
	return f.createErr
}

func (f *fakeES) BulkIndex(_ context.Context, indexName string, docs []CatalogListing) error {
	f.bulkCalls = append(f.bulkCalls, bulkCall{index: indexName, docs: docs})
	return f.bulkErr
}

func (f *fakeES) CurrentIndex(_ context.Context, _ string) (string, bool, error) {
	return f.currentIndex, f.hadOld, f.currentErr
}

func (f *fakeES) SwitchAlias(_ context.Context, alias, newIndex, oldIndex string) error {
	f.switchCall = &switchCall{alias: alias, newIndex: newIndex, oldIndex: oldIndex}
	return f.switchErr
}

func (f *fakeES) DeleteIndex(_ context.Context, name string) error {
	f.deleted = append(f.deleted, name)
	return f.deleteErr
}

func fixedNow() time.Time { return time.Unix(1_700_000_000, 0) }

func TestReindexer_PeregonyaetVsePoStranicam(t *testing.T) {
	catalog := &fakeCatalog{
		pages: [][]CatalogListing{
			{{ID: "l1"}, {ID: "l2"}},
			{{ID: "l3"}},
		},
		nextCursors: []string{"cursor-1", ""},
	}
	es := &fakeES{currentIndex: "listings_1000", hadOld: true}

	r := New(catalog, es, fixedNow)
	result, err := r.Run(context.Background(), Options{Alias: "listings", BatchSize: 2})
	require.NoError(t, err)

	assert.Equal(t, "listings_1700000000", result.NewIndex)
	assert.Equal(t, "listings_1000", result.OldIndex)
	assert.Equal(t, int64(3), result.DocsIndexed)
	assert.False(t, result.DryRun)

	require.Len(t, catalog.calls, 2, "обязана быть пройдена ровно вся пагинация: последняя страница с пустым next_cursor завершает цикл")
	assert.Equal(t, "", catalog.calls[0].cursor)
	assert.Equal(t, "cursor-1", catalog.calls[1].cursor)
	for _, c := range catalog.calls {
		assert.Equal(t, int32(2), c.limit, "batch size обязан пробрасываться в каждый вызов ListListings")
	}

	require.Len(t, es.createdIndexes, 1)
	assert.Equal(t, "listings_1700000000", es.createdIndexes[0])

	require.Len(t, es.bulkCalls, 2, "каждая страница каталога — отдельный bulk-вызов В НОВЫЙ индекс")
	for _, call := range es.bulkCalls {
		assert.Equal(t, "listings_1700000000", call.index)
	}

	require.NotNil(t, es.switchCall)
	assert.Equal(t, "listings", es.switchCall.alias)
	assert.Equal(t, "listings_1700000000", es.switchCall.newIndex)
	assert.Equal(t, "listings_1000", es.switchCall.oldIndex)

	assert.Equal(t, []string{"listings_1000"}, es.deleted, "по умолчанию (без -keep-old) старый индекс удаляется")
	assert.True(t, result.OldIndexDeleted)
}

func TestReindexer_PervyyZapusk_BezStarogoIndeksa(t *testing.T) {
	catalog := &fakeCatalog{pages: [][]CatalogListing{{{ID: "l1"}}}, nextCursors: []string{""}}
	es := &fakeES{hadOld: false} // alias ещё не существует

	r := New(catalog, es, fixedNow)
	_, err := r.Run(context.Background(), Options{Alias: "listings", BatchSize: 10})
	require.NoError(t, err)

	require.NotNil(t, es.switchCall)
	assert.Empty(t, es.switchCall.oldIndex, "переключать не с чего — alias создаётся впервые")
	assert.Empty(t, es.deleted, "нечего удалять, если старого индекса не было")
}

func TestReindexer_KeepOld_NeUdalyaetStariyIndeks(t *testing.T) {
	catalog := &fakeCatalog{pages: [][]CatalogListing{{{ID: "l1"}}}, nextCursors: []string{""}}
	es := &fakeES{currentIndex: "listings_old", hadOld: true}

	r := New(catalog, es, fixedNow)
	result, err := r.Run(context.Background(), Options{Alias: "listings", BatchSize: 10, KeepOld: true})
	require.NoError(t, err)

	assert.Empty(t, es.deleted, "-keep-old обязан подавить DeleteIndex")
	assert.False(t, result.OldIndexDeleted)
	// Alias всё равно переключается — keep-old только про физическое
	// удаление, а не про сам факт переключения.
	require.NotNil(t, es.switchCall)
	assert.Equal(t, "listings_old", es.switchCall.oldIndex)
}

func TestReindexer_DryRun_NeTrogaetES(t *testing.T) {
	catalog := &fakeCatalog{
		pages:       [][]CatalogListing{{{ID: "l1"}, {ID: "l2"}}, {{ID: "l3"}}},
		nextCursors: []string{"c1", ""},
	}
	es := &fakeES{currentIndex: "listings_old", hadOld: true}

	r := New(catalog, es, fixedNow)
	result, err := r.Run(context.Background(), Options{Alias: "listings", BatchSize: 2, DryRun: true})
	require.NoError(t, err)

	assert.True(t, result.DryRun)
	assert.Equal(t, int64(3), result.DocsIndexed, "dry-run обязан честно посчитать документы, читая каталог по факту")

	assert.Empty(t, es.createdIndexes, "-dry-run не создаёт индекс")
	assert.Empty(t, es.bulkCalls, "-dry-run не пишет ни одного документа")
	assert.Nil(t, es.switchCall, "-dry-run не переключает alias")
	assert.Empty(t, es.deleted, "-dry-run не удаляет ничего")
}

func TestReindexer_OshibkaCatalog_OstanavlivayetPereindeksatsiyu(t *testing.T) {
	wantErr := errors.New("catalog недоступен")
	catalog := &fakeCatalog{err: wantErr}
	es := &fakeES{}

	r := New(catalog, es, fixedNow)
	_, err := r.Run(context.Background(), Options{Alias: "listings", BatchSize: 10})
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)

	assert.Empty(t, es.bulkCalls)
	assert.Nil(t, es.switchCall, "alias не переключается, если наполнение не завершилось")
}

func TestReindexer_OshibkaBulkIndex_OstanavlivayetPereindeksatsiyu(t *testing.T) {
	catalog := &fakeCatalog{pages: [][]CatalogListing{{{ID: "l1"}}}, nextCursors: []string{""}}
	wantErr := errors.New("elasticsearch недоступен")
	es := &fakeES{bulkErr: wantErr}

	r := New(catalog, es, fixedNow)
	_, err := r.Run(context.Background(), Options{Alias: "listings", BatchSize: 10})
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	assert.Nil(t, es.switchCall, "ошибка записи обязана остановить переиндексацию до переключения alias")
}

func TestReindexer_OshibkaSwitchAlias_StaryyIndeksNeUdalyaetsya(t *testing.T) {
	catalog := &fakeCatalog{pages: [][]CatalogListing{{{ID: "l1"}}}, nextCursors: []string{""}}
	wantErr := errors.New("сеть моргнула")
	es := &fakeES{currentIndex: "listings_old", hadOld: true, switchErr: wantErr}

	r := New(catalog, es, fixedNow)
	_, err := r.Run(context.Background(), Options{Alias: "listings", BatchSize: 10})
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	assert.Empty(t, es.deleted, "старый индекс не должен удаляться, если alias не переключился — иначе старые данные исчезнут, а новые не станут видимыми")
}

func TestReindexer_PustoyKatalog(t *testing.T) {
	catalog := &fakeCatalog{pages: [][]CatalogListing{{}}, nextCursors: []string{""}}
	es := &fakeES{}

	r := New(catalog, es, fixedNow)
	result, err := r.Run(context.Background(), Options{Alias: "listings", BatchSize: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.DocsIndexed)
	assert.Empty(t, es.bulkCalls, "пустая страница не должна порождать пустой bulk-запрос")
	require.NotNil(t, es.switchCall, "даже пустой каталог обязан завершиться переключением alias")
}

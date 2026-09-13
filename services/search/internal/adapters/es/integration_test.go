//go:build integration

package es

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tces "github.com/testcontainers/testcontainers-go/modules/elasticsearch"

	"gosplash/services/search/internal/domain"
)

// testESImage — версия не обязана совпадать с deploy/compose/elasticsearch.yml
// (8.19.11): весь код адаптера использует только стабильный REST API,
// одинаковый в 8.x и 9.x (_bulk, _search, _aliases, явный маппинг). Выбрана
// версия, уже лежащая в локальном кэше образов, — тест не должен качать
// гигабайт при каждом первом запуске на новой машине сильнее, чем должен.
const testESImage = "docker.elastic.co/elasticsearch/elasticsearch:9.5.2"

// startES поднимает контейнер ES с ВЫКЛЮЧЕННОЙ security — тот же режим,
// что и в deploy/compose/elasticsearch.yml, специально для локальной
// разработки: тесту не нужно возиться с TLS-сертификатами и Basic Auth,
// которые ES 8+ включает по умолчанию.
func startES(t *testing.T) *Client {
	t.Helper()
	ctx := context.Background()

	container, err := tces.Run(ctx, testESImage,
		testcontainers.WithEnv(map[string]string{
			"xpack.security.enabled": "false",
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	alias := fmt.Sprintf("listings-test-%d", time.Now().UnixNano())
	client, err := New([]string{container.Settings.Address}, alias)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return client.Ping(ctx) == nil
	}, 60*time.Second, 500*time.Millisecond, "кластер не поднялся за отведённое время")

	require.NoError(t, client.EnsureIndex(ctx))
	return client
}

func TestES_IndeksatsiyaIPoisk(t *testing.T) {
	client := startES(t)
	ctx := context.Background()

	indexer := NewBulkIndexer(client, 10, 50*time.Millisecond)
	defer indexer.Close()

	docs := []domain.ListingDoc{
		{ListingID: "l1", Title: "Закат над морем", AuthorID: 1, AuthorName: "Иван Петров", Tags: []string{"закат", "море"}, PriceCents: 500, Status: domain.StatusPublished},
		{ListingID: "l2", Title: "Рассвет в горах", AuthorID: 2, AuthorName: "Мария Сидорова", Tags: []string{"рассвет", "горы"}, PriceCents: 1200, Status: domain.StatusPublished},
		{ListingID: "l3", Title: "Городской пейзаж", AuthorID: 1, AuthorName: "Иван Петров", Tags: []string{"город"}, PriceCents: 300, Status: domain.StatusPublished},
	}
	for i, doc := range docs {
		require.NoError(t, indexer.IndexListing(ctx, doc, int64(1000+i)))
	}
	require.NoError(t, client.Refresh(ctx))

	search := NewSearchIndex(client)

	t.Run("точный запрос находит документ", func(t *testing.T) {
		result, err := search.Search(ctx, domain.SearchQuery{Query: "закат", Limit: 10})
		require.NoError(t, err)
		require.Len(t, result.Hits, 1)
		assert.Equal(t, "l1", result.Hits[0].ListingID)
	})

	t.Run("запрос с опечаткой находит документ благодаря fuzziness", func(t *testing.T) {
		// "заакат" — лишняя буква, расстояние Левенштейна 1 от "закат".
		result, err := search.Search(ctx, domain.SearchQuery{Query: "заакат", Limit: 10})
		require.NoError(t, err)
		require.NotEmpty(t, result.Hits, "опечатка в пределах fuzziness AUTO обязана находиться")
		assert.Equal(t, "l1", result.Hits[0].ListingID)
	})

	t.Run("фильтр по цене — filter-контекст, не влияет на текстовый поиск", func(t *testing.T) {
		result, err := search.Search(ctx, domain.SearchQuery{
			Query:         "",
			PriceMinCents: 1000,
			Limit:         10,
		})
		require.NoError(t, err)
		require.Len(t, result.Hits, 1)
		assert.Equal(t, "l2", result.Hits[0].ListingID)
	})

	t.Run("фасеты по тегам считаются по текущей выдаче", func(t *testing.T) {
		result, err := search.Search(ctx, domain.SearchQuery{AuthorID: 1, Limit: 10})
		require.NoError(t, err)
		require.Len(t, result.Hits, 2, "у автора 1 — две карточки (l1, l3)")

		facetValues := make(map[string]int64, len(result.TagFacets))
		for _, f := range result.TagFacets {
			facetValues[f.Value] = f.Count
		}
		assert.Equal(t, int64(1), facetValues["закат"])
		assert.Equal(t, int64(1), facetValues["город"])
	})

	t.Run("пагинация через search_after без потерь и дублей", func(t *testing.T) {
		seen := map[string]bool{}
		cursor := ""
		for page := 0; page < 10; page++ {
			result, err := search.Search(ctx, domain.SearchQuery{Limit: 1, Cursor: cursor})
			require.NoError(t, err)
			require.Len(t, result.Hits, 1)

			id := result.Hits[0].ListingID
			require.False(t, seen[id], "документ %s встретился на странице повторно", id)
			seen[id] = true

			if result.NextCursor == "" {
				break
			}
			cursor = result.NextCursor
		}
		assert.Len(t, seen, len(docs), "постранично обязаны пройти все документы ровно по разу")
	})
}

func TestES_UdaleniyeDokumenta(t *testing.T) {
	client := startES(t)
	ctx := context.Background()

	indexer := NewBulkIndexer(client, 10, 50*time.Millisecond)
	defer indexer.Close()

	require.NoError(t, indexer.IndexListing(ctx, domain.ListingDoc{
		ListingID: "removable", Title: "Скоро удалят", Status: domain.StatusPublished,
	}, 100))
	require.NoError(t, client.Refresh(ctx))

	search := NewSearchIndex(client)
	before, err := search.Search(ctx, domain.SearchQuery{Query: "удалят", Limit: 10})
	require.NoError(t, err)
	require.Len(t, before.Hits, 1)

	require.NoError(t, indexer.DeleteListing(ctx, "removable", 200))
	require.NoError(t, client.Refresh(ctx))

	after, err := search.Search(ctx, domain.SearchQuery{Query: "удалят", Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, after.Hits)
}

func TestES_VersionirovaniyeOtklonyaetUstarevsheeSobytiye(t *testing.T) {
	client := startES(t)
	ctx := context.Background()

	indexer := NewBulkIndexer(client, 10, 50*time.Millisecond)
	defer indexer.Close()

	// Сначала пишем версией 200 ("свежее" событие), потом пытаемся
	// перезаписать версией 100 ("устаревшее", будто бы пришло позже по
	// доставке, но раньше по факту) — external_gte обязан отклонить вторую
	// запись, оставив в индексе содержимое первой.
	require.NoError(t, indexer.IndexListing(ctx, domain.ListingDoc{
		ListingID: "versioned", Title: "Новая версия", Status: domain.StatusPublished,
	}, 200))
	err := indexer.IndexListing(ctx, domain.ListingDoc{
		ListingID: "versioned", Title: "Устаревшая версия", Status: domain.StatusPublished,
	}, 100)
	require.NoError(t, err, "version_conflict — штатный исход, а не ошибка вызывающего")
	require.NoError(t, client.Refresh(ctx))

	search := NewSearchIndex(client)
	result, err := search.Search(ctx, domain.SearchQuery{Query: "новая", Limit: 10})
	require.NoError(t, err)
	require.Len(t, result.Hits, 1, "в индексе обязана остаться версия 200, а не быть перезаписанной версией 100")
}

func TestES_EnsureIndex_Idempotentnyy(t *testing.T) {
	client := startES(t)
	ctx := context.Background()

	// Повторный вызов при уже существующем alias — no-op, без ошибки.
	require.NoError(t, client.EnsureIndex(ctx))
	require.NoError(t, client.EnsureIndex(ctx))
}

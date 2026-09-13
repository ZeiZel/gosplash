package es

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/search/internal/domain"
)

// asMap разбирает тело запроса обратно в map, чтобы проверять СТРУКТУРУ
// JSON, а не байты в байт — тест не должен ломаться от изменения порядка
// ключей, который json.Marshal для map никогда не гарантирует одинаковым.
func asMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	return m
}

func TestBuildSearchBody_PustoyZapros(t *testing.T) {
	raw, err := BuildSearchBody(domain.SearchQuery{Limit: 20})
	require.NoError(t, err)
	body := asMap(t, raw)

	boolQuery := body["query"].(map[string]any)["bool"].(map[string]any)
	must := boolQuery["must"].([]any)
	require.Len(t, must, 1)
	assert.Contains(t, must[0].(map[string]any), "match_all",
		"пустой query обязан превращаться в match_all, а не в пустой multi_match")
	assert.NotContains(t, boolQuery, "filter", "без фильтров ключ filter не нужен вовсе")

	assert.Equal(t, float64(21), body["size"], "size = limit+1, лишний документ отрежет response_parser")
	assert.NotContains(t, body, "search_after", "первая страница — без search_after")
}

func TestBuildSearchBody_TolkoFiltry(t *testing.T) {
	raw, err := BuildSearchBody(domain.SearchQuery{
		Limit:         10,
		Tags:          []string{"закат", "море"},
		PriceMinCents: 100,
		PriceMaxCents: 5000,
		AuthorID:      42,
	})
	require.NoError(t, err)
	body := asMap(t, raw)

	boolQuery := body["query"].(map[string]any)["bool"].(map[string]any)
	filter := boolQuery["filter"].([]any)
	require.Len(t, filter, 3, "tags + price range + author_id — три отдельных условия в filter")

	var sawTags, sawPrice, sawAuthor bool
	for _, f := range filter {
		clause := f.(map[string]any)
		if terms, ok := clause["terms"].(map[string]any); ok {
			sawTags = true
			assert.ElementsMatch(t, []any{"закат", "море"}, terms["tags"])
		}
		if rng, ok := clause["range"].(map[string]any); ok {
			sawPrice = true
			priceClause := rng["price_cents"].(map[string]any)
			assert.Equal(t, float64(100), priceClause["gte"])
			assert.Equal(t, float64(5000), priceClause["lte"])
		}
		if term, ok := clause["term"].(map[string]any); ok {
			sawAuthor = true
			assert.Equal(t, float64(42), term["author_id"])
		}
	}
	assert.True(t, sawTags, "фильтр по тегам не найден")
	assert.True(t, sawPrice, "фильтр по цене не найден")
	assert.True(t, sawAuthor, "фильтр по автору не найден")
}

func TestBuildSearchBody_PolnyyNabor(t *testing.T) {
	cursor, err := encodeCursor([]any{12.5, "listing-9"})
	require.NoError(t, err)

	raw, err := BuildSearchBody(domain.SearchQuery{
		Query:         "закат",
		Tags:          []string{"море"},
		PriceMinCents: 100,
		AuthorID:      7,
		Limit:         5,
		Cursor:        cursor,
	})
	require.NoError(t, err)
	body := asMap(t, raw)

	boolQuery := body["query"].(map[string]any)["bool"].(map[string]any)
	must := boolQuery["must"].([]any)
	require.Len(t, must, 1)
	multiMatch := must[0].(map[string]any)["multi_match"].(map[string]any)
	assert.Equal(t, "закат", multiMatch["query"])
	assert.Equal(t, "AUTO", multiMatch["fuzziness"],
		"fuzziness AUTO обязателен для текстового запроса")
	assert.ElementsMatch(t, []any{"title", "title.russian", "title.english", "author_name"}, multiMatch["fields"])

	filter := boolQuery["filter"].([]any)
	require.Len(t, filter, 3, "теги + цена (только gte, max не задан) + author_id")

	searchAfter, ok := body["search_after"].([]any)
	require.True(t, ok, "непустой курсор обязан попасть в search_after")
	assert.Equal(t, []any{12.5, "listing-9"}, searchAfter)

	assert.Equal(t, float64(6), body["size"])

	sort := body["sort"].([]any)
	require.Len(t, sort, 2, "score + listing_id — обязательный tie-breaker для search_after")
	assert.Equal(t, "desc", sort[0].(map[string]any)["_score"])
	assert.Equal(t, "asc", sort[1].(map[string]any)["listing_id"],
		"tie-breaker — keyword-поле listing_id, а не служебное _id (у него fielddata отключена по умолчанию)")
	assert.Equal(t, true, body["track_scores"])

	aggs := body["aggs"].(map[string]any)["tags"].(map[string]any)["terms"].(map[string]any)
	assert.Equal(t, "tags", aggs["field"])
}

func TestBuildSearchBody_BitiyKursor(t *testing.T) {
	_, err := BuildSearchBody(domain.SearchQuery{Limit: 10, Cursor: "не-курсор-совсем"})
	assert.Error(t, err)
}

func TestBuildSearchBody_ZaprosSoProbelami(t *testing.T) {
	// "   " после TrimSpace — это пустой запрос, а не multi_match по пробелу.
	raw, err := BuildSearchBody(domain.SearchQuery{Query: "   ", Limit: 10})
	require.NoError(t, err)
	body := asMap(t, raw)
	must := body["query"].(map[string]any)["bool"].(map[string]any)["must"].([]any)
	assert.Contains(t, must[0].(map[string]any), "match_all")
}

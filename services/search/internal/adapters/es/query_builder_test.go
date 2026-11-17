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

// mustMap/mustSlice/mustFloat — проверенные приведения из decoded-JSON:
// при неожиданной структуре тест падает с внятным сообщением о том, что
// именно не так, а не паникует где-то в середине цепочки `.(map[string]any)`.
func mustMap(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	require.True(t, ok, "ожидался JSON-объект, получено %T", v)
	return m
}

func mustSlice(t *testing.T, v any) []any {
	t.Helper()
	s, ok := v.([]any)
	require.True(t, ok, "ожидался JSON-массив, получено %T", v)
	return s
}

func mustFloat(t *testing.T, v any) float64 {
	t.Helper()
	f, ok := v.(float64)
	require.True(t, ok, "ожидалось число, получено %T", v)
	return f
}

func TestBuildSearchBody_PustoyZapros(t *testing.T) {
	raw, err := BuildSearchBody(domain.SearchQuery{Limit: 20})
	require.NoError(t, err)
	body := asMap(t, raw)

	boolQuery := mustMap(t, mustMap(t, body["query"])["bool"])
	must := mustSlice(t, boolQuery["must"])
	require.Len(t, must, 1)
	assert.Contains(t, mustMap(t, must[0]), "match_all",
		"пустой query обязан превращаться в match_all, а не в пустой multi_match")
	assert.NotContains(t, boolQuery, "filter", "без фильтров ключ filter не нужен вовсе")

	assert.InDelta(t, 21.0, mustFloat(t, body["size"]), 1e-9, "size = limit+1, лишний документ отрежет response_parser")
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

	boolQuery := mustMap(t, mustMap(t, body["query"])["bool"])
	filter := mustSlice(t, boolQuery["filter"])
	require.Len(t, filter, 3, "tags + price range + author_id — три отдельных условия в filter")

	var sawTags, sawPrice, sawAuthor bool
	for _, f := range filter {
		clause := mustMap(t, f)
		if terms, ok := clause["terms"].(map[string]any); ok {
			sawTags = true
			assert.ElementsMatch(t, []any{"закат", "море"}, terms["tags"])
		}
		if rng, ok := clause["range"].(map[string]any); ok {
			sawPrice = true
			priceClause := mustMap(t, rng["price_cents"])
			assert.InDelta(t, 100.0, mustFloat(t, priceClause["gte"]), 1e-9)
			assert.InDelta(t, 5000.0, mustFloat(t, priceClause["lte"]), 1e-9)
		}
		if term, ok := clause["term"].(map[string]any); ok {
			sawAuthor = true
			assert.InDelta(t, 42.0, mustFloat(t, term["author_id"]), 1e-9)
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

	boolQuery := mustMap(t, mustMap(t, body["query"])["bool"])
	must := mustSlice(t, boolQuery["must"])
	require.Len(t, must, 1)
	multiMatch := mustMap(t, mustMap(t, must[0])["multi_match"])
	assert.Equal(t, "закат", multiMatch["query"])
	assert.Equal(t, "AUTO", multiMatch["fuzziness"],
		"fuzziness AUTO обязателен для текстового запроса")
	assert.ElementsMatch(t, []any{"title", "title.russian", "title.english", "author_name"}, multiMatch["fields"])

	filter := mustSlice(t, boolQuery["filter"])
	require.Len(t, filter, 3, "теги + цена (только gte, max не задан) + author_id")

	searchAfter, ok := body["search_after"].([]any)
	require.True(t, ok, "непустой курсор обязан попасть в search_after")
	assert.Equal(t, []any{12.5, "listing-9"}, searchAfter)

	assert.InDelta(t, 6.0, mustFloat(t, body["size"]), 1e-9)

	sort := mustSlice(t, body["sort"])
	require.Len(t, sort, 2, "score + listing_id — обязательный tie-breaker для search_after")
	assert.Equal(t, "desc", mustMap(t, sort[0])["_score"])
	assert.Equal(t, "asc", mustMap(t, sort[1])["listing_id"],
		"tie-breaker — keyword-поле listing_id, а не служебное _id (у него fielddata отключена по умолчанию)")
	assert.Equal(t, true, body["track_scores"])

	aggs := mustMap(t, mustMap(t, mustMap(t, body["aggs"])["tags"])["terms"])
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
	must := mustSlice(t, mustMap(t, mustMap(t, body["query"])["bool"])["must"])
	assert.Contains(t, mustMap(t, must[0]), "match_all")
}

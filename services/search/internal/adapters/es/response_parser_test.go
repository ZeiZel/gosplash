package es

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hitJSON(id string, score float64, sortID string) string {
	return fmt.Sprintf(`{
		"_score": %v,
		"sort": [%v, %q],
		"_source": {
			"listing_id": %q,
			"title": "Заголовок %s",
			"author_id": 1,
			"author_name": "Автор",
			"tags": ["закат"],
			"price_cents": 100
		}
	}`, score, score, sortID, id, id)
}

func TestParseSearchResponse_MenshePolnoyStranitsy_BezSleduyushcheyo(t *testing.T) {
	raw := fmt.Sprintf(`{"hits":{"total":{"value":1,"relation":"eq"},"hits":[%s]}}`, hitJSON("l1", 3.5, "l1"))

	result, err := ParseSearchResponse([]byte(raw), 5)
	require.NoError(t, err)

	assert.Len(t, result.Hits, 1)
	assert.Empty(t, result.NextCursor, "хитов меньше limit — это последняя страница")
	assert.Equal(t, int64(1), result.Total)
}

func TestParseSearchResponse_PolnayaStranitsa_StroitCursor(t *testing.T) {
	// limit=2, приехало 3 (limit+1) — есть следующая страница, а лишний
	// (третий) хит наружу отдаваться не должен.
	hits := []string{hitJSON("l1", 3.0, "l1"), hitJSON("l2", 2.0, "l2"), hitJSON("l3", 1.0, "l3")}
	raw := fmt.Sprintf(`{"hits":{"total":{"value":3,"relation":"eq"},"hits":[%s]}}`, strings.Join(hits, ","))

	result, err := ParseSearchResponse([]byte(raw), 2)
	require.NoError(t, err)

	require.Len(t, result.Hits, 2, "лишний (limit+1)-й хит обязан быть отрезан")
	assert.Equal(t, "l1", result.Hits[0].ListingID)
	assert.Equal(t, "l2", result.Hits[1].ListingID)
	assert.NotEmpty(t, result.NextCursor)

	// Курсор строится по ПОСЛЕДНЕМУ хиту ОТДАВАЕМОЙ страницы (l2), а не по
	// отрезанному l3 — иначе следующая страница потеряла бы l3.
	values, err := decodeCursor(result.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, "l2", values[1])
}

func TestParseSearchResponse_Faceti(t *testing.T) {
	raw := `{
		"hits": {"total": {"value": 0}, "hits": []},
		"aggregations": {"tags": {"buckets": [
			{"key": "закат", "doc_count": 5},
			{"key": "море", "doc_count": 3}
		]}}
	}`

	result, err := ParseSearchResponse([]byte(raw), 10)
	require.NoError(t, err)

	require.Len(t, result.TagFacets, 2)
	assert.Equal(t, "закат", result.TagFacets[0].Value)
	assert.Equal(t, int64(5), result.TagFacets[0].Count)
	assert.Equal(t, "море", result.TagFacets[1].Value)
	assert.Equal(t, int64(3), result.TagFacets[1].Count)
}

func TestParseSearchResponse_TotalPoOgranicheniyu10000(t *testing.T) {
	// relation="gte" — Elasticsearch отдал нижнюю границу, а не точное
	// число (документов больше index.max_result_window). Парсер обязан
	// вернуть это значение как есть — решение "это нижняя граница, а не
	// точный ответ" остаётся на стороне вызывающего/README, не парсера.
	raw := `{"hits":{"total":{"value":10000,"relation":"gte"},"hits":[]}}`

	result, err := ParseSearchResponse([]byte(raw), 10)
	require.NoError(t, err)
	assert.Equal(t, int64(10000), result.Total)
}

func TestParseSearchResponse_BitiyJSON(t *testing.T) {
	_, err := ParseSearchResponse([]byte("не json"), 10)
	assert.Error(t, err)
}

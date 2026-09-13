package es

import (
	"encoding/json"
	"fmt"

	"gosplash/services/search/internal/domain"
)

// esSearchResponse/esHit — форма ответа Elasticsearch на POST _search.
// Отдельные типы от domain, по той же причине, что и esDoc в
// bulk_indexer.go: домен не должен знать про "_source"/"_score"/"sort" —
// это словарь протокола ES, а не словарь предметной области поиска.
type esSearchResponse struct {
	Hits struct {
		Total struct {
			Value    int64  `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []esHit `json:"hits"`
	} `json:"hits"`
	Aggregations struct {
		Tags struct {
			Buckets []struct {
				Key      string `json:"key"`
				DocCount int64  `json:"doc_count"`
			} `json:"buckets"`
		} `json:"tags"`
	} `json:"aggregations"`
}

type esHit struct {
	Score  float64 `json:"_score"`
	Sort   []any   `json:"sort"`
	Source struct {
		ListingID  string   `json:"listing_id"`
		Title      string   `json:"title"`
		AuthorID   int64    `json:"author_id"`
		AuthorName string   `json:"author_name"`
		Tags       []string `json:"tags"`
		PriceCents int64    `json:"price_cents"`
	} `json:"_source"`
}

// ParseSearchResponse превращает сырой ответ Elasticsearch в domain.SearchResult.
//
// limit — уже РАЗРЕШЁННЫЙ лимит страницы (то же значение, что было передано
// в BuildSearchBody до прибавления +1): вместе с "size: limit+1" в запросе
// это тот же приём "прочитать на одну строку больше", что и в
// services/catalog/internal/adapters/pg/cursor.go.trimPage — если приехало
// limit+1 хитов, значит страница не последняя, next_cursor строится по
// limit-му хиту (последнему в ОТДАВАЕМОЙ странице), а (limit+1)-й отрезается
// и наружу не идёт.
//
// total: Hits.Total.Value берётся как есть, независимо от Relation. По
// умолчанию Elasticsearch считает total ТОЧНО, только пока документов,
// подходящих под запрос, не больше index.max_result_window (10 000):
// дальше total.relation становится "gte", и число — это НИЖНЯЯ ГРАНИЦА
// ("нашлось хотя бы 10 000"), а не точный ответ. Для витрины фотостока это
// приемлемо: пользователю нужно "много" или "мало", а не последняя цифра.
func ParseSearchResponse(raw []byte, limit int32) (domain.SearchResult, error) {
	var resp esSearchResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return domain.SearchResult{}, fmt.Errorf("разбор ответа поиска: %w", err)
	}

	hits := resp.Hits.Hits
	nextCursor := ""
	if int32(len(hits)) > limit {
		last := hits[limit-1]
		cursor, err := encodeCursor(last.Sort)
		if err != nil {
			return domain.SearchResult{}, err
		}
		nextCursor = cursor
		hits = hits[:limit]
	}

	result := domain.SearchResult{
		Hits:       make([]domain.SearchHit, 0, len(hits)),
		Total:      resp.Hits.Total.Value,
		NextCursor: nextCursor,
	}
	for _, h := range hits {
		result.Hits = append(result.Hits, domain.SearchHit{
			ListingID:  h.Source.ListingID,
			Title:      h.Source.Title,
			AuthorID:   h.Source.AuthorID,
			AuthorName: h.Source.AuthorName,
			Tags:       h.Source.Tags,
			PriceCents: h.Source.PriceCents,
			Score:      float32(h.Score),
		})
	}

	for _, bucket := range resp.Aggregations.Tags.Buckets {
		result.TagFacets = append(result.TagFacets, domain.Facet{
			Value: bucket.Key,
			Count: bucket.DocCount,
		})
	}

	return result, nil
}

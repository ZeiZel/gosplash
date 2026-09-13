// Package grpc — реализация contract'а из proto/gosplash/search/v1/search.proto.
package grpc

import (
	searchv1 "gosplash/gen/go/gosplash/search/v1"
	"gosplash/services/search/internal/domain"
)

// toDomainQuery переводит proto-запрос в domain.SearchQuery. Limit не
// разрешается здесь — этим занимается app.SearchService, а не адаптер
// транспорта (docs/STYLE.md: адаптер переводит язык, а не решает бизнес-
// правила).
func toDomainQuery(req *searchv1.SearchRequest) domain.SearchQuery {
	filters := req.GetFilters()
	return domain.SearchQuery{
		Query:         req.GetQuery(),
		Tags:          filters.GetTags(),
		PriceMinCents: filters.GetPriceMinCents(),
		PriceMaxCents: filters.GetPriceMaxCents(),
		AuthorID:      filters.GetAuthorId(),
		Limit:         req.GetLimit(),
		Cursor:        req.GetCursor(),
	}
}

func toProtoResponse(result domain.SearchResult) *searchv1.SearchResponse {
	resp := &searchv1.SearchResponse{
		Hits:       make([]*searchv1.SearchHit, 0, len(result.Hits)),
		Total:      result.Total,
		NextCursor: result.NextCursor,
		TagFacets:  make([]*searchv1.Facet, 0, len(result.TagFacets)),
	}
	for _, h := range result.Hits {
		resp.Hits = append(resp.Hits, &searchv1.SearchHit{
			ListingId:  h.ListingID,
			Title:      h.Title,
			AuthorId:   h.AuthorID,
			AuthorName: h.AuthorName,
			Tags:       h.Tags,
			PriceCents: h.PriceCents,
			Score:      h.Score,
		})
	}
	for _, f := range result.TagFacets {
		resp.TagFacets = append(resp.TagFacets, &searchv1.Facet{
			Value: f.Value,
			Count: f.Count,
		})
	}
	return resp
}

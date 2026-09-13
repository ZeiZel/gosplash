package app

import (
	"context"

	"gosplash/services/search/internal/domain"
	"gosplash/services/search/internal/ports"
)

// Пределы limit — то же правило, что и в catalog.ListListings: 0 или
// отрицательное значение — это "клиент не задумывался", а не "верни всё
// подряд"; слишком большое — это либо ошибка клиента, либо попытка вытащить
// весь индекс одним запросом, чего сервис отдавать не обязан.
const (
	defaultLimit = 20
	maxLimit     = 100
)

// SearchService — сценарий поиска. Вся бизнес-логика здесь: как разрешать
// limit по умолчанию, как валидировать вход. Как именно устроен запрос к
// Elasticsearch (bool/filter/aggs/search_after) сервис не знает — это знает
// adapters/es, единственный, кто реализует ports.SearchIndex.
type SearchService struct {
	index ports.SearchIndex
}

func NewSearchService(index ports.SearchIndex) *SearchService {
	return &SearchService{index: index}
}

func (s *SearchService) Search(ctx context.Context, query domain.SearchQuery) (domain.SearchResult, error) {
	query.Limit = resolveLimit(query.Limit)
	return s.index.Search(ctx, query)
}

func resolveLimit(limit int32) int32 {
	if limit <= 0 {
		return defaultLimit
	}
	if limit > maxLimit {
		return maxLimit
	}
	return limit
}

package es

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"gosplash/services/search/internal/domain"
)

// SearchIndex — реализация ports.SearchIndex: единственное место, где
// BuildSearchBody, HTTP-вызов и ParseSearchResponse соединяются вместе.
// Каждая из трёх частей тестируется отдельно и без сети — здесь их просто
// некому подменить (это тонкий адаптер, а не логика), поэтому у файла нет
// собственного unit-теста, только integration_test.go (build tag integration).
type SearchIndex struct {
	client *Client
}

func NewSearchIndex(client *Client) *SearchIndex {
	return &SearchIndex{client: client}
}

func (s *SearchIndex) Search(ctx context.Context, query domain.SearchQuery) (domain.SearchResult, error) {
	body, err := BuildSearchBody(query)
	if err != nil {
		return domain.SearchResult{}, fmt.Errorf("%w: %w", domain.ErrInvalidDocument, err)
	}

	res, err := s.client.raw.Search(
		s.client.raw.Search.WithContext(ctx),
		s.client.raw.Search.WithIndex(s.client.Alias),
		s.client.raw.Search.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		return domain.SearchResult{}, classifyTransportErr("search", err)
	}
	defer res.Body.Close()

	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return domain.SearchResult{}, classifyTransportErr("search: чтение ответа", err)
	}
	if res.IsError() {
		return domain.SearchResult{}, classifyHTTPStatusErr("search", res.Status(), raw)
	}

	return ParseSearchResponse(raw, query.Limit)
}

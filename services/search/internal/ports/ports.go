// Package ports — интерфейсы, объявленные рядом с тем, кто ими пользуется
// (docs/STYLE.md): оба ниже нужны internal/app, обе реализации живут в
// internal/adapters/es.
package ports

import (
	"context"

	"gosplash/services/search/internal/domain"
)

// IndexWriter — то, что нужно app.Indexer, чтобы наполнять индекс. Версия
// передаётся отдельным параметром, а не полем ListingDoc: это НЕ часть
// документа (она никогда не попадёт в _source), а инструкция Elasticsearch
// "прими запись, только если она новее уже сохранённой" — external_gte
// версионирование, подробный разбор в app/indexer.go.
type IndexWriter interface {
	// IndexListing создаёт или полностью заменяет документ. Полная замена
	// (а не partial update), потому что catalog.listing.published — толстое
	// событие: оно всегда несёт ВСЕ поля карточки, частичный апдейт здесь
	// был бы преждевременной оптимизацией без единого пользователя.
	IndexListing(ctx context.Context, doc domain.ListingDoc, versionUnixMs int64) error
	// DeleteListing убирает документ (status=removed). Версия обязательна
	// и здесь же: без неё запоздавшее "removed" не сможет проиграть более
	// свежему "published", пришедшему из retry-топика раньше по факту, но
	// позже по offset'у.
	DeleteListing(ctx context.Context, listingID string, versionUnixMs int64) error
}

// SearchIndex — то, что нужно app.SearchService, чтобы искать.
type SearchIndex interface {
	Search(ctx context.Context, query domain.SearchQuery) (domain.SearchResult, error)
}

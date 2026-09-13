// Package domain — предметная область поиска.
//
// Здесь сознательно нет ни JSON-, ни protobuf-тегов, ни импортов
// Elasticsearch: домен описывает "что такое карточка для поиска" на языке
// самого поиска, а не на языке gRPC-контракта (proto/gosplash/search/v1)
// или транспортного формата ES-документа. Оба конца — адаптеры
// (internal/adapters/grpc, internal/adapters/es) — переводят СВОИ структуры
// в эту и обратно.
package domain

import "time"

// Статусы карточки, дублирующие proto/gosplash/events/v1/ListingPublished.
// Дублирование сознательное: домен поиска не должен ничего знать о пакете
// gosplash/gen/go/gosplash/events/v1 (это адаптер kafka/индексатора), а
// сравнивать status по голой строке "removed" по всему коду — источник опечаток.
const (
	StatusPublished = "published"
	StatusRemoved   = "removed"
)

// ListingDoc — то, что попадает в индекс Elasticsearch за одну карточку.
//
// Поля один в один соответствуют мэппингу (adapters/es/mapping.go) — это
// НЕ совпадение: маппинг явный именно потому, что документ имеет фиксированную
// форму, и ListingDoc эту форму фиксирует на стороне Go.
type ListingDoc struct {
	ListingID   string
	AuthorID    int64
	AuthorName  string
	Title       string
	Tags        []string
	PriceCents  int64
	Currency    string
	Status      string
	PublishedAt time.Time
}

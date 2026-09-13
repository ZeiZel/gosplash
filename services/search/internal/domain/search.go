package domain

// SearchQuery — запрос на поиск на языке домена: то же самое, что
// SearchRequest+Filters в proto/gosplash/search/v1/search.proto, но без
// зависимости от сгенерированного кода. Limit здесь уже РАЗРЕШЁННЫЙ
// (clamped) — приведение "0 или отрицательное → значение по умолчанию,
// слишком большое → потолок" делает app.SearchService, а не адаптер ES:
// это бизнес-правило, а не деталь Elasticsearch.
type SearchQuery struct {
	Query         string
	Tags          []string
	PriceMinCents int32
	PriceMaxCents int32
	AuthorID      int64
	Limit         int32
	// Cursor — непрозрачная строка search_after (adapters/es/cursor.go).
	// Пусто — первая страница.
	Cursor string
}

// SearchResult — то, что возвращает поиск, тоже на языке домена.
type SearchResult struct {
	Hits []SearchHit
	// Total — см. комментарий в adapters/es/response_parser.go про то, что
	// это число точное только до 10 000 документов.
	Total      int64
	NextCursor string
	TagFacets  []Facet
}

type SearchHit struct {
	ListingID  string
	Title      string
	AuthorID   int64
	AuthorName string
	Tags       []string
	PriceCents int64
	// Score — релевантность BM25, как её посчитал Elasticsearch. У пустого
	// запроса (query == "") она не несёт смысла (все документы получают
	// одинаковый score от match_all) и присутствует только ради единообразия
	// сортировки и курсора — см. adapters/es/query_builder.go.
	Score float32
}

type Facet struct {
	Value string
	Count int64
}

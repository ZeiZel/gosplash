package es

import (
	"encoding/json"
	"fmt"
	"strings"

	"gosplash/services/search/internal/domain"
)

// tagFacetSize — сколько верхних тегов возвращать в агрегации. Фиксированное
// число, а не "все теги": aggs считает bucket на КАЖДОЕ уникальное значение
// поля по ВСЕЙ выдаче, попавшей под query+filter (не только по странице
// hits), и без потолка facet-запрос по полю с длинным хвостом уникальных
// тегов превратился бы в подсчёт тысяч bucket'ов, 99% которых с doc_count=1
// и не нужны никакому UI.
const tagFacetSize = 20

// multiMatchFields — поля, по которым ищет multi_match с fuzziness.
//
// title.russian и title.english — стеммированные подполя (mapping.go):
// запрос "закаты" находит документ с "закат" через русский стеммер, запрос
// "sunsets" — документ с "sunset" через английский, независимо от того,
// на каком языке был написан заголовок конкретной карточки. Голое title
// (без анализатора-стеммера, стандартный анализатор) добавлено ради
// точных/близких к точным совпадений, которые стеммер иногда огрубляет.
var multiMatchFields = []string{"title", "title.russian", "title.english", "author_name"}

// BuildSearchBody строит тело POST _search по SearchQuery.
//
// ЧИСТАЯ функция: ни сети, ни *elasticsearch.Client — только структуры в
// JSON, поэтому тестируется без docker (query_builder_test.go).
func BuildSearchBody(q domain.SearchQuery) ([]byte, error) {
	body := map[string]any{
		"query": buildQuery(q),
		// _score + listing_id, а не просто _score — обязательный tie-breaker
		// для search_after, разбор см. в cursor.go. track_scores: true нужен
		// именно ПОТОМУ, что _score участвует в sort: по умолчанию
		// Elasticsearch не считает точный score для запросов, отсортированных
		// не по нему, а здесь он и есть поле сортировки.
		//
		// НЕОЧЕВИДНОЕ РЕШЕНИЕ: tie-breaker — listing_id (наше keyword-поле),
		// а НЕ служебное _id, хотя оно тоже уникально и напрашивается первым.
		// Сортировка по _id требует у Elasticsearch fielddata на этом поле,
		// а fielddata для _id ОТКЛЮЧЕНА по умолчанию начиная с 7.6 намеренно:
		// _id не хранится колоночно (doc_values), и построение fielddata для
		// него на лету — это загрузка в память по значению на каждый документ
		// шарда, самый частый источник OOM в кластерах, где кто-то один раз
		// отсортировал по _id "для удобства". Попытка использовать его здесь
		// падает с illegal_argument_exception прямо на первом запросе — эта
		// реализация словила это на интеграционном тесте. listing_id —
		// обычное keyword-поле, у которого doc_values включены по умолчанию,
		// оно так же гарантированно уникально (это и есть идентификатор
		// документа) и сортируется штатно, без fielddata вообще.
		"sort": []map[string]any{
			{"_score": "desc"},
			{"listing_id": "asc"},
		},
		"track_scores": true,
		// limit+1 — тот же приём "прочитать на одну строку больше", что и
		// в services/catalog/internal/adapters/pg/cursor.go: если приехало
		// limit+1 документов, значит страница не последняя, и адаптер
		// (response_parser.go) строит next_cursor по limit-му, а лишний,
		// (limit+1)-й, отрезает.
		"size": q.Limit + 1,
		"aggs": map[string]any{
			"tags": map[string]any{
				"terms": map[string]any{
					"field": "tags",
					"size":  tagFacetSize,
				},
			},
		},
		// total считается ТОЧНО по умолчанию (track_total_hits не задан —
		// действует умолчание ES: точно до 10 000 документов, дальше —
		// нижняя граница с relation="gte"). Для витрины фотостока это
		// осознанный размен: пользователю почти никогда не важно, 10 000
		// там результатов или 47 000 — важно, что "много", а платить полным
		// подсчётом по всем шардам на каждый запрос ради точности за
		// пределами первых 10 000 незачем.
	}

	cursorValues, err := decodeCursor(q.Cursor)
	if err != nil {
		return nil, err
	}
	if cursorValues != nil {
		body["search_after"] = cursorValues
	}

	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("сборка тела поиска: %w", err)
	}
	return raw, nil
}

// buildQuery собирает bool-запрос: текстовая часть в must (влияет на
// _score), структурные условия — в filter.
//
// ГЛАВНОЕ РЕШЕНИЕ этого файла: filter vs must(query) — не синонимы.
//
//	must (query-контекст)   — участвует в подсчёте _score И не кэшируется:
//	                          Elasticsearch каждый раз заново считает,
//	                          НАСКОЛЬКО документ подходит. Это то, что
//	                          нужно free-text полю: релевантность вообще
//	                          существует только здесь.
//	filter (filter-контекст) — НЕ влияет на _score (документ либо проходит
//	                          условие, либо нет, третьего не дано) и
//	                          Elasticsearch КЭШИРУЕТ битовый набор
//	                          документов, прошедших фильтр, между запросами.
//	                          Тег "закат" в фильтре — это одно и то же
//	                          множество document_id для тысячи разных
//	                          текстовых запросов подряд, и повторный расчёт
//	                          этого множества на каждый запрос был бы чистой
//	                          тратой CPU.
//
// Отсюда и раскладка ниже: tags/price/author_id — структурные условия
// "да/нет", у них нет и не может быть релевантности ("на 73% подходит по
// цене" не имеет смысла) — им место в filter. Текст заголовка и имени
// автора — единственное, где "насколько похоже" вообще имеет смысл, и
// единственное место, где применяется fuzziness.
func buildQuery(q domain.SearchQuery) map[string]any {
	must := []map[string]any{textQuery(q.Query)}

	var filter []map[string]any
	if len(q.Tags) > 0 {
		filter = append(filter, map[string]any{
			"terms": map[string]any{"tags": q.Tags},
		})
	}
	if q.PriceMinCents > 0 || q.PriceMaxCents > 0 {
		filter = append(filter, map[string]any{
			"range": map[string]any{"price_cents": priceRange(q)},
		})
	}
	if q.AuthorID != 0 {
		filter = append(filter, map[string]any{
			"term": map[string]any{"author_id": q.AuthorID},
		})
	}

	boolQuery := map[string]any{"must": must}
	if len(filter) > 0 {
		boolQuery["filter"] = filter
	}
	return map[string]any{"bool": boolQuery}
}

// textQuery — свободный текст. Пустой query превращается в match_all: по
// заданию проекта "пусто — вернутся все документы, отфильтрованные ниже"
// (proto/gosplash/search/v1/search.proto, комментарий к SearchRequest.query).
//
// fuzziness: "AUTO" на multi_match, и НИГДЕ БОЛЬШЕ. AUTO выбирает
// допустимое расстояние Левенштейна ПО ДЛИНЕ САМОГО СЛОВА в запросе:
//
//	0–2 символа  → 0 (точное совпадение обязательно)
//	3–5 символов → 1 (одна замена/вставка/удаление символа)
//	6+ символов  → 2 (до двух правок)
//
// Смысл этой прогрессии: опечатка в слове из 3 букв — это уже другое
// слово ("кот" → "кон" при fuzziness=1 неотличимо от намеренного другого
// запроса), а в слове из 10 букв одна-две опечатки почти наверняка не меняют
// намерение пользователя. Фиксированное число (например, всегда 2) было бы
// либо бесполезно строгим на длинных словах, либо катастрофически шумным
// на коротких: fuzziness=2 для двухбуквенного слова означает, что ЛЮБОЕ
// двухбуквенное сочетание считается совпадением.
//
// Почему fuzziness НЕ ставится на все поля запроса подряд (а тем более на
// filter — там его вообще нет, term/terms по определению точны): fuzzy-
// поиск устроен как раскрытие исходного термина в набор термов на
// расстоянии N — это ощутимо дороже точного совпадения по индексу, и на
// коротких/частых токенах (артикли, теги-однословники) даёт мусорные
// совпадения по чистой комбинаторике, а не по смыслу. Здесь он оправдан
// только там, где пользователь физически печатает текст руками
// (title, author_name) — то есть там, где опечатка вообще может возникнуть.
func textQuery(query string) map[string]any {
	query = strings.TrimSpace(query)
	if query == "" {
		return map[string]any{"match_all": map[string]any{}}
	}
	return map[string]any{
		"multi_match": map[string]any{
			"query":     query,
			"fields":    multiMatchFields,
			"fuzziness": "AUTO",
		},
	}
}

func priceRange(q domain.SearchQuery) map[string]any {
	r := map[string]any{}
	if q.PriceMinCents > 0 {
		r["gte"] = q.PriceMinCents
	}
	if q.PriceMaxCents > 0 {
		r["lte"] = q.PriceMaxCents
	}
	return r
}

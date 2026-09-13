package reindex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

// ESAdapter — реализация ESClient поверх настоящего Elasticsearch. В отличие
// от services/search/internal/adapters/es.Client, этот адаптер работает с
// ФИЗИЧЕСКИМИ именами индексов напрямую (а не только через alias) — это и
// есть его единственная причина существования: alias по определению не
// умеет указывать сразу на два индекса сразу (старый читается, новый
// наполняется), значит наполнение обязано идти в обход alias, по прямому
// имени.
type ESAdapter struct {
	raw *elasticsearch.Client
}

func NewESAdapter(addrs []string) (*ESAdapter, error) {
	raw, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: addrs})
	if err != nil {
		return nil, fmt.Errorf("elasticsearch client: %w", err)
	}
	return &ESAdapter{raw: raw}, nil
}

func (a *ESAdapter) CreateIndex(ctx context.Context, name string) error {
	res, err := a.raw.Indices.Create(
		name,
		a.raw.Indices.Create.WithContext(ctx),
		a.raw.Indices.Create.WithBody(strings.NewReader(indexMapping)),
	)
	if err != nil {
		return fmt.Errorf("создание индекса %s: %w", name, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("создание индекса %s: %s: %s", name, res.Status(), body)
	}
	return nil
}

// BulkIndex пишет пачку документов ОДНИМ запросом _bulk — по тем же
// причинам, что и services/search/internal/adapters/es.BulkIndexer: сеть
// амортизируется на всю пачку, а не тратится на каждый документ отдельно.
// В отличие от живого консьюмера, здесь нет конкурентных писателей в ТОТ ЖЕ
// физический индекс (alias на него ещё не указывает, пока идёт заполнение),
// поэтому строгая проверка версии не обязательна для корректности — но
// версия всё равно проставляется (published_at в мс), чтобы при повторном
// запуске той же переиндексации (например, после сбоя посередине) её
// результат не зависел от порядка получения страниц каталога: экземпляр
// с более свежим published_at выигрывает независимо от того, в каком
// порядке два запуска инструмента писали бы в один и тот же черновой
// индекс.
func (a *ESAdapter) BulkIndex(ctx context.Context, indexName string, docs []CatalogListing) error {
	if len(docs) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, doc := range docs {
		meta := map[string]any{
			"index": map[string]any{
				"_index":       indexName,
				"_id":          doc.ID,
				"version":      doc.PublishedAt.UnixMilli(),
				"version_type": "external_gte",
			},
		}
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("метаданные документа %s: %w", doc.ID, err)
		}
		buf.Write(metaBytes)
		buf.WriteByte('\n')

		source := map[string]any{
			"listing_id":   doc.ID,
			"title":        doc.Title,
			"tags":         doc.Tags,
			"author_id":    doc.AuthorID,
			"author_name":  doc.AuthorName,
			"price_cents":  doc.PriceCents,
			"currency":     doc.Currency,
			"status":       doc.Status,
			"published_at": doc.PublishedAt.UTC().Format(time.RFC3339),
		}
		docBytes, err := json.Marshal(source)
		if err != nil {
			return fmt.Errorf("документ %s: %w", doc.ID, err)
		}
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	res, err := a.raw.Bulk(bytes.NewReader(buf.Bytes()),
		a.raw.Bulk.WithContext(ctx),
		a.raw.Bulk.WithIndex(indexName),
	)
	if err != nil {
		return fmt.Errorf("bulk-запись в %s: %w", indexName, err)
	}
	defer res.Body.Close()

	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("bulk-запись в %s: чтение ответа: %w", indexName, err)
	}
	if res.IsError() {
		return fmt.Errorf("bulk-запись в %s: %s: %s", indexName, res.Status(), raw)
	}

	return firstBulkItemErr(raw)
}

// firstBulkItemErr — в отличие от services/search, инструмент не пытается
// классифицировать каждую ошибку отдельно (там ей есть куда деться —
// kafkax.Retryable/Permanent), здесь это одноразовый прогон: первая же
// проблема с документом должна остановить и громко показать оператору,
// какая карточка не легла, а не продолжать молча.
func firstBulkItemErr(raw []byte) error {
	var resp struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			ID     string `json:"_id"`
			Status int    `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("разбор ответа _bulk: %w", err)
	}
	if !resp.Errors {
		return nil
	}
	for _, item := range resp.Items {
		for _, v := range item {
			if v.Error == nil {
				continue
			}
			// version_conflict — штатный случай при повторном запуске
			// (см. комментарий BulkIndex выше), не ошибка.
			if v.Error.Type == "version_conflict_engine_exception" {
				continue
			}
			return fmt.Errorf("документ %s: %s: %s", v.ID, v.Error.Type, v.Error.Reason)
		}
	}
	return nil
}

// CurrentIndex — на какой физический индекс сейчас смотрит alias.
func (a *ESAdapter) CurrentIndex(ctx context.Context, alias string) (string, bool, error) {
	res, err := a.raw.Indices.GetAlias(
		a.raw.Indices.GetAlias.WithContext(ctx),
		a.raw.Indices.GetAlias.WithName(alias),
	)
	if err != nil {
		return "", false, fmt.Errorf("текущий индекс alias %s: %w", alias, err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return "", false, nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return "", false, fmt.Errorf("текущий индекс alias %s: %s: %s", alias, res.Status(), body)
	}

	var payload map[string]json.RawMessage
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return "", false, fmt.Errorf("разбор ответа _alias: %w", err)
	}
	for indexName := range payload {
		// В этом проекте alias всегда указывает РОВНО на один индекс —
		// именно так, как устроено само переключение ниже (add+remove
		// одним атомарным запросом). Первый ключ ответа — он и есть.
		return indexName, true, nil
	}
	return "", false, nil
}

// SwitchAlias переключает alias ОДНИМ атомарным запросом _aliases:
// действия add и remove в одном теле выполняются вместе, без промежуточного
// состояния "alias не указывает никуда" — см. комментарий пакета reindex.go.
func (a *ESAdapter) SwitchAlias(ctx context.Context, alias, newIndex, oldIndex string) error {
	actions := []map[string]any{
		{"add": map[string]any{"index": newIndex, "alias": alias}},
	}
	if oldIndex != "" {
		actions = append(actions, map[string]any{"remove": map[string]any{"index": oldIndex, "alias": alias}})
	}
	body, err := json.Marshal(map[string]any{"actions": actions})
	if err != nil {
		return fmt.Errorf("сборка запроса переключения alias: %w", err)
	}

	res, err := a.raw.Indices.UpdateAliases(
		bytes.NewReader(body),
		a.raw.Indices.UpdateAliases.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("переключение alias %s: %w", alias, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		respBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("переключение alias %s: %s: %s", alias, res.Status(), respBody)
	}
	return nil
}

func (a *ESAdapter) DeleteIndex(ctx context.Context, name string) error {
	res, err := a.raw.Indices.Delete(
		[]string{name},
		a.raw.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("удаление индекса %s: %w", name, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("удаление индекса %s: %s: %s", name, res.Status(), body)
	}
	return nil
}

var _ ESClient = (*ESAdapter)(nil)

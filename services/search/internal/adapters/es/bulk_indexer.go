package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"gosplash/services/search/internal/domain"
)

// defaultBatchSize/defaultFlushInterval — компромисс "не слишком часто
// дёргать Elasticsearch почти пустой пачкой" (интервал) и "не копить
// слишком долго, если трафик вырос" (размер) — тот же компромисс, что и
// у catalog.ViewBatcher (services/catalog/internal/adapters/kafka), только
// здесь задержка не бесплатна: она прямо влияет на "через сколько после
// публикации карточка находится поиском".
const (
	defaultBatchSize     = 100
	defaultFlushInterval = 300 * time.Millisecond

	// submitBufferFactor — во сколько раз ёмкость канала-очереди больше
	// размера пачки: небольшой запас, чтобы Submit не блокировался ровно
	// в момент, когда флашер занят сетевым вызовом к ES.
	submitBufferFactor = 2
)

// BulkIndexer — реализация ports.IndexWriter поверх Elasticsearch _bulk,
// а не Index-запроса на каждый документ.
//
// PATTERN: батчинг с синхронным ожиданием результата — гибрид между
// catalog.ViewBatcher (полностью асинхронный, Enqueue не ждёт ничего) и
// обычным синхронным вызовом на каждый документ.
//
// Почему вообще нужен bulk, а не Index на каждую карточку: каждый HTTP-
// запрос к Elasticsearch — это TCP round-trip, разбор JSON и поиск шарда
// НЕЗАВИСИМО от размера документа. Индексируя по одному, throughput
// упирается в сетевую задержку, а не в CPU кластера. Один _bulk-запрос на
// сто документов амортизирует эту задержку на все сто сразу.
//
// Почему нельзя скопировать ViewBatcher один в один (fire-and-forget,
// Enqueue без ошибки): kafkax.Consumer коммитит offset СРАЗУ после того,
// как Handler вернул nil (см. pkg/kafkax/kafka.go, processOne). Если бы
// IndexListing только клал документ в канал и возвращался, offset
// сдвинулся бы ДО того, как документ реально долетел до Elasticsearch —
// и в случае падения процесса между "положили в буфер" и "успели
// отправить" событие было бы потеряно НАВСЕГДА, потому что offset уже
// закоммичен, а at-least-once Kafka вернуть то же сообщение больше не
// обязана. Это допустимо для PhotoViewed (см. обоснование в самом
// view_batcher.go) и НЕДОПУСТИМО для поискового индекса: пропавшая
// карточка в выдаче — заметный баг, а не погрешность метрики.
//
// Поэтому здесь Submit (через IndexListing/DeleteListing) БЛОКИРУЕТ
// вызывающего до тех пор, пока пачка, в которую попал именно ЕГО документ,
// не будет реально отправлена и не вернёт результат. offset коммитится
// только после этого — at-least-once не нарушается, а bulk всё равно
// происходит, потому что пока один вызов ждёт, к тому же батчу успевают
// присоединиться другие (из других партиций топика, если их несколько,
// или следующие события той же партиции, если consumer успел уйти вперёд
// по буферу fetch'а — partitionEngine, pkg/kafkax/engine.go, разбирает
// сообщения одной партиции строго по одному, так что реальный источник
// параллелизма здесь — несколько партиций/процессов).
//
// ЦЕНА: при низком и редком трафике батч почти никогда не наполняется до
// batchSize, и каждое сообщение ждёт до flushInterval лишней задержки
// перед тем, как offset будет закоммичен, — то есть добавленная латентность
// органичена сверху flushInterval, а не растёт бесконечно. При высоком
// трафике (например, во время догоняющего чтения после простоя) батчи
// наполняются по размеру быстрее интервала, и добавленная задержка
// стремится к нулю — bulk именно тогда и даёт максимальный выигрыш.
type BulkIndexer struct {
	client        *Client
	batchSize     int
	flushInterval time.Duration

	submit chan bulkAction
	done   chan struct{}
	wg     sync.WaitGroup
}

type bulkAction struct {
	isDelete bool
	id       string
	version  int64
	doc      domain.ListingDoc
	result   chan error
}

// NewBulkIndexer запускает фоновую горутину-флашер. Close ОБЯЗАТЕЛЕН на
// graceful shutdown (симметрично catalog.ViewBatcher.Close) — иначе
// последняя неполная пачка потеряна без единого шанса долететь до ES.
func NewBulkIndexer(client *Client, batchSize int, flushInterval time.Duration) *BulkIndexer {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	if flushInterval <= 0 {
		flushInterval = defaultFlushInterval
	}

	b := &BulkIndexer{
		client:        client,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		submit:        make(chan bulkAction, batchSize*submitBufferFactor),
		done:          make(chan struct{}),
	}
	b.wg.Add(1)
	go b.run()
	return b
}

func (b *BulkIndexer) IndexListing(ctx context.Context, doc domain.ListingDoc, version int64) error {
	return b.do(ctx, bulkAction{id: doc.ListingID, version: version, doc: doc})
}

func (b *BulkIndexer) DeleteListing(ctx context.Context, listingID string, version int64) error {
	return b.do(ctx, bulkAction{isDelete: true, id: listingID, version: version})
}

func (b *BulkIndexer) do(ctx context.Context, a bulkAction) error {
	a.result = make(chan error, 1)

	select {
	case b.submit <- a:
	case <-b.done:
		return fmt.Errorf("%w: индексатор остановлен", domain.ErrIndexUnavailable)
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-a.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run — единственный читатель b.submit: копит пачку, флашит по размеру или
// по таймеру, что наступит раньше. Один в один структура catalog.ViewBatcher.run,
// разница только в том, что здесь флаш обязан разослать результат каждому
// ожидающему вызову (см. flush), а не просто "постараться и забыть".
func (b *BulkIndexer) run() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	batch := make([]bulkAction, 0, b.batchSize)
	for {
		select {
		case a := <-b.submit:
			batch = append(batch, a)
			if len(batch) >= b.batchSize {
				batch = b.flush(batch)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				batch = b.flush(batch)
			}
		case <-b.done:
			for {
				select {
				case a := <-b.submit:
					batch = append(batch, a)
				default:
					if len(batch) > 0 {
						b.flush(batch)
					}
					return
				}
			}
		}
	}
}

// flush отправляет накопленную пачку ОДНИМ запросом _bulk и разбирает
// ответ построчно: у каждого документа в пачке — свой статус, поэтому один
// невалидный документ не должен приводить к ошибке всех остальных.
func (b *BulkIndexer) flush(batch []bulkAction) []bulkAction {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	body, err := buildBulkBody(b.client.Alias, batch)
	if err != nil {
		// Ошибка сериализации — баг в коде вызывающего (например, поле,
		// которое json.Marshal в принципе не умеет сериализовать), а не
		// свойство конкретного документа: разумно считать её permanent,
		// повтор той же карточки даст ту же панику сериализации.
		b.failAll(batch, fmt.Errorf("%w: сборка bulk-тела: %w", domain.ErrInvalidDocument, err))
		return batch[:0]
	}

	res, err := b.client.raw.Bulk(bytes.NewReader(body),
		b.client.raw.Bulk.WithContext(ctx),
		b.client.raw.Bulk.WithIndex(b.client.Alias),
	)
	if err != nil {
		b.failAll(batch, classifyTransportErr("bulk", err))
		return batch[:0]
	}
	defer res.Body.Close()

	raw, err := io.ReadAll(res.Body)
	if err != nil {
		b.failAll(batch, classifyTransportErr("bulk: чтение ответа", err))
		return batch[:0]
	}
	if res.IsError() {
		b.failAll(batch, classifyHTTPStatusErr("bulk", res.Status(), raw))
		return batch[:0]
	}

	results, err := parseBulkResponse(raw)
	if err != nil {
		b.failAll(batch, classifyTransportErr("bulk: разбор ответа", err))
		return batch[:0]
	}
	if len(results) != len(batch) {
		b.failAll(batch, fmt.Errorf("%w: bulk-ответ содержит %d результатов, ожидалось %d",
			domain.ErrIndexUnavailable, len(results), len(batch)))
		return batch[:0]
	}
	for i, a := range batch {
		a.result <- results[i]
	}
	return batch[:0]
}

func (b *BulkIndexer) failAll(batch []bulkAction, err error) {
	for _, a := range batch {
		a.result <- err
	}
}

// Close останавливает флашер, дождавшись отправки того, что успело
// накопиться. Вызывается на graceful shutdown ДО остановки Kafka-консьюмера
// нельзя — наоборот, консьюмер должен быть остановлен первым (иначе новые
// Submit продолжат приходить в уже закрывающийся индексатор); порядок
// см. в cmd/search/main.go.
func (b *BulkIndexer) Close() {
	close(b.done)
	b.wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────
// Сборка тела _bulk и разбор ответа — чистые функции, не знающие про
// каналы и горутины, поэтому легко тестируются без сети (bulk_indexer_test.go
// гоняет их напрямую, а полный путь через HTTP проверяет integration_test.go).
// ─────────────────────────────────────────────────────────────────────────

// esDoc — форма документа в _source. Отдельный тип от domain.ListingDoc
// намеренно: домен ничего не знает про JSON-теги и про то, что published_at
// должен ехать строкой RFC3339, а не time.Time (encoding/json сериализовал
// бы его сам, но explicit-конвертация делает зависимость от формата видимой
// в одном месте, а не размазанной по тегам структуры домена).
type esDoc struct {
	ListingID   string   `json:"listing_id"`
	Title       string   `json:"title"`
	Tags        []string `json:"tags"`
	AuthorID    int64    `json:"author_id"`
	AuthorName  string   `json:"author_name"`
	PriceCents  int64    `json:"price_cents"`
	Currency    string   `json:"currency"`
	Status      string   `json:"status"`
	PublishedAt string   `json:"published_at"`
}

func toESDoc(doc domain.ListingDoc) esDoc {
	return esDoc{
		ListingID:   doc.ListingID,
		Title:       doc.Title,
		Tags:        doc.Tags,
		AuthorID:    doc.AuthorID,
		AuthorName:  doc.AuthorName,
		PriceCents:  doc.PriceCents,
		Currency:    doc.Currency,
		Status:      doc.Status,
		PublishedAt: doc.PublishedAt.UTC().Format(time.RFC3339),
	}
}

// buildBulkBody собирает NDJSON-тело _bulk: две строки на index-действие
// (метаданные + документ) и одна на delete (только метаданные — у удаления
// нет тела). version/version_type=external_gte в метаданных КАЖДОГО
// действия — тот самый механизм идемпотентности, разобранный в
// app/indexer.go: Elasticsearch сам отбросит действие, если в индексе уже
// лежит документ с версией не младше присланной.
func buildBulkBody(index string, batch []bulkAction) ([]byte, error) {
	var buf bytes.Buffer
	for _, a := range batch {
		op := "index"
		if a.isDelete {
			op = "delete"
		}
		meta := map[string]any{
			op: map[string]any{
				"_index":       index,
				"_id":          a.id,
				"version":      a.version,
				"version_type": "external_gte",
			},
		}
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			return nil, fmt.Errorf("метаданные %s %s: %w", op, a.id, err)
		}
		buf.Write(metaBytes)
		buf.WriteByte('\n')

		if !a.isDelete {
			docBytes, err := json.Marshal(toESDoc(a.doc))
			if err != nil {
				return nil, fmt.Errorf("документ %s: %w", a.id, err)
			}
			buf.Write(docBytes)
			buf.WriteByte('\n')
		}
	}
	return buf.Bytes(), nil
}

// bulkResponseItem/bulkResponse — то немногое, что нужно от ответа _bulk:
// статус и тип ошибки на каждый элемент. Порядок items в ответе ВСЕГДА
// совпадает с порядком действий в запросе (это гарантия самого протокола
// _bulk) — поэтому results[i] безопасно сопоставляется с batch[i].
type bulkResponseItem struct {
	Status int `json:"status"`
	Error  *struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
}

type bulkResponse struct {
	Items []map[string]bulkResponseItem `json:"items"`
}

func parseBulkResponse(raw []byte) ([]error, error) {
	var resp bulkResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("разбор ответа _bulk: %w", err)
	}

	results := make([]error, len(resp.Items))
	for i, item := range resp.Items {
		// У каждого элемента ровно один ключ — "index" или "delete",
		// смотря какое действие было отправлено.
		for _, v := range item {
			results[i] = classifyBulkItemErr(v.Status, errType(v), errReason(v))
		}
	}
	return results, nil
}

func errType(item bulkResponseItem) string {
	if item.Error == nil {
		return ""
	}
	return item.Error.Type
}

func errReason(item bulkResponseItem) string {
	if item.Error == nil {
		return ""
	}
	return item.Error.Reason
}

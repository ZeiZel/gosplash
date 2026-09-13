package es

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/search/internal/domain"
)

// ─────────────────────────────────────────────────────────────────────────
// buildBulkBody / parseBulkResponse — чистые функции, тестируются без сети.
// ─────────────────────────────────────────────────────────────────────────

func TestBuildBulkBody(t *testing.T) {
	t.Run("index-действие: две строки, версия и version_type в мета", func(t *testing.T) {
		batch := []bulkAction{{
			id:      "listing-1",
			version: 1699999999000,
			doc: domain.ListingDoc{
				ListingID:  "listing-1",
				Title:      "Закат",
				AuthorID:   1,
				AuthorName: "Иван",
				PriceCents: 500,
			},
		}}

		body, err := buildBulkBody("listings", batch)
		require.NoError(t, err)

		lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
		require.Len(t, lines, 2, "index-действие обязано занимать ровно две строки NDJSON")

		var meta map[string]map[string]any
		require.NoError(t, json.Unmarshal([]byte(lines[0]), &meta))
		idx, ok := meta["index"]
		require.True(t, ok, "первая строка обязана быть meta-объектом index")
		assert.Equal(t, "listings", idx["_index"])
		assert.Equal(t, "listing-1", idx["_id"])
		assert.Equal(t, float64(1699999999000), idx["version"])
		assert.Equal(t, "external_gte", idx["version_type"])

		var doc map[string]any
		require.NoError(t, json.Unmarshal([]byte(lines[1]), &doc))
		assert.Equal(t, "Закат", doc["title"])
	})

	t.Run("delete-действие: только одна строка, без тела документа", func(t *testing.T) {
		batch := []bulkAction{{isDelete: true, id: "listing-2", version: 42}}

		body, err := buildBulkBody("listings", batch)
		require.NoError(t, err)

		lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
		require.Len(t, lines, 1, "у delete нет тела документа — только meta-строка")

		var meta map[string]map[string]any
		require.NoError(t, json.Unmarshal([]byte(lines[0]), &meta))
		del, ok := meta["delete"]
		require.True(t, ok)
		assert.Equal(t, "listing-2", del["_id"])
		assert.Equal(t, "external_gte", del["version_type"])
	})

	t.Run("смешанная пачка сохраняет порядок", func(t *testing.T) {
		batch := []bulkAction{
			{id: "a", version: 1, doc: domain.ListingDoc{ListingID: "a"}},
			{isDelete: true, id: "b", version: 2},
			{id: "c", version: 3, doc: domain.ListingDoc{ListingID: "c"}},
		}
		body, err := buildBulkBody("listings", batch)
		require.NoError(t, err)

		lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
		// index(2 строки) + delete(1 строка) + index(2 строки) = 5.
		require.Len(t, lines, 5)
		assert.Contains(t, lines[0], `"_id":"a"`)
		assert.Contains(t, lines[2], `"_id":"b"`)
		assert.Contains(t, lines[3], `"_id":"c"`)
	})
}

func TestParseBulkResponse(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		wantNil   []bool // results[i] == nil?
		wantPerm  []bool
		wantRetry []bool
	}{
		{
			name:    "успешная индексация — нет ошибки",
			raw:     `{"items":[{"index":{"status":201}}]}`,
			wantNil: []bool{true},
		},
		{
			name:    "version_conflict — это успех, а не ошибка",
			raw:     `{"items":[{"index":{"status":409,"error":{"type":"version_conflict_engine_exception","reason":"..."}}}]}`,
			wantNil: []bool{true},
		},
		{
			name:     "mapper_parsing_exception — постоянная ошибка",
			raw:      `{"items":[{"index":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"..."}}}]}`,
			wantPerm: []bool{true},
		},
		{
			name:      "недокументированный 503 — временная ошибка по умолчанию",
			raw:       `{"items":[{"index":{"status":503,"error":{"type":"cluster_block_exception","reason":"..."}}}]}`,
			wantRetry: []bool{true},
		},
		{
			name:     "успех и ошибка в одной пачке — независимы друг от друга",
			raw:      `{"items":[{"index":{"status":201}},{"delete":{"status":400,"error":{"type":"illegal_argument_exception","reason":"..."}}}]}`,
			wantNil:  []bool{true, false},
			wantPerm: []bool{false, true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := parseBulkResponse([]byte(tt.raw))
			require.NoError(t, err)
			for i, res := range results {
				if len(tt.wantNil) > i && tt.wantNil[i] {
					assert.NoError(t, res, "элемент %d должен быть успешным", i)
				}
				if len(tt.wantPerm) > i && tt.wantPerm[i] {
					assert.True(t, errors.Is(res, domain.ErrInvalidDocument), "элемент %d должен быть permanent", i)
				}
				if len(tt.wantRetry) > i && tt.wantRetry[i] {
					assert.True(t, errors.Is(res, domain.ErrIndexUnavailable), "элемент %d должен быть retryable", i)
				}
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────
// BulkIndexer целиком — против httptest-сервера, изображающего Elasticsearch.
// Без docker: это обычный http.Server в памяти процесса, а не контейнер.
// ─────────────────────────────────────────────────────────────────────────

// fakeES — минимальная имитация _bulk-эндпоинта: копит запросы, отвечает
// заготовленным JSON или обрывает соединение, если задано errStatus.
type fakeES struct {
	mu        sync.Mutex
	requests  []string // сырые тела запросов, по одному на вызов _bulk
	response  string   // что отвечать (если errStatus == 0)
	errStatus int
}

func newFakeES(response string) *httptest.Server {
	f := &fakeES{response: response}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.requests = append(f.requests, string(body))
		status := f.errStatus
		resp := f.response
		f.mu.Unlock()

		// go-elasticsearch проверяет этот заголовок, чтобы убедиться, что
		// на том конце действительно Elasticsearch (а не Nginx с похожим
		// ответом), и без него отказывается работать — httptest.Server
		// обязан его подделать, иначе КАЖДЫЙ запрос будет падать с
		// "the server is not Elasticsearch" до того, как тело ответа
		// вообще будет разобрано.
		w.Header().Set("X-Elastic-Product", "Elasticsearch")

		if status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"boom"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(resp))
	}))
}

func newTestClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()
	client, err := New([]string{server.URL}, "listings")
	require.NoError(t, err)
	return client
}

func TestBulkIndexer_FlushPoRazmeru(t *testing.T) {
	successResponse := func(n int) string {
		items := make([]string, n)
		for i := range items {
			items[i] = `{"index":{"status":201}}`
		}
		return fmt.Sprintf(`{"errors":false,"items":[%s]}`, strings.Join(items, ","))
	}

	server := newFakeES(successResponse(2))
	defer server.Close()

	client := newTestClient(t, server)
	// Большой flushInterval — единственный способ пачка вообще уедет
	// в этом тесте это наполнение до batchSize=2.
	indexer := NewBulkIndexer(client, 2, time.Hour)
	defer indexer.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = indexer.IndexListing(ctx, domain.ListingDoc{ListingID: fmt.Sprintf("l-%d", i)}, int64(i))
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		assert.NoError(t, err)
	}
}

func TestBulkIndexer_FlushPoTaymeru(t *testing.T) {
	server := newFakeES(`{"errors":false,"items":[{"index":{"status":201}}]}`)
	defer server.Close()

	client := newTestClient(t, server)
	// batchSize огромный — единственный способ пачка уедет здесь — таймер.
	indexer := NewBulkIndexer(client, 1000, 20*time.Millisecond)
	defer indexer.Close()

	err := indexer.IndexListing(context.Background(), domain.ListingDoc{ListingID: "solo"}, 1)
	assert.NoError(t, err, "одиночный документ обязан уехать по таймеру, не дожидаясь batchSize")
}

func TestBulkIndexer_VersionConflict_NeOshibka(t *testing.T) {
	server := newFakeES(`{"errors":true,"items":[{"index":{"status":409,"error":{"type":"version_conflict_engine_exception","reason":"..."}}}]}`)
	defer server.Close()

	client := newTestClient(t, server)
	indexer := NewBulkIndexer(client, 1, 10*time.Millisecond)
	defer indexer.Close()

	err := indexer.IndexListing(context.Background(), domain.ListingDoc{ListingID: "stale"}, 1)
	assert.NoError(t, err, "устаревшая по версии запись — это штатный исход, а не ошибка")
}

func TestBulkIndexer_MapperException_Permanent(t *testing.T) {
	server := newFakeES(`{"errors":true,"items":[{"index":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"..."}}}]}`)
	defer server.Close()

	client := newTestClient(t, server)
	indexer := NewBulkIndexer(client, 1, 10*time.Millisecond)
	defer indexer.Close()

	err := indexer.IndexListing(context.Background(), domain.ListingDoc{ListingID: "bad"}, 1)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrInvalidDocument))
}

func TestBulkIndexer_ClusterNedostupen_Retryable(t *testing.T) {
	server := newFakeES("")
	server.Close() // сервер уже остановлен — соединение обязано отвалиться

	client := newTestClient(t, server)
	indexer := NewBulkIndexer(client, 1, 10*time.Millisecond)
	defer indexer.Close()

	err := indexer.IndexListing(context.Background(), domain.ListingDoc{ListingID: "x"}, 1)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrIndexUnavailable))
}

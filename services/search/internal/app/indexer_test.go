package app

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/pkg/kafkax"
	"gosplash/services/search/internal/domain"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// fakeWriter — подделка ports.IndexWriter без единого похода в сеть
// (docs/STYLE.md: прикладной слой тестируется подделками портов).
type fakeWriter struct {
	indexed     []domain.ListingDoc
	indexedVer  []int64
	deletedID   string
	deletedVer  int64
	indexErr    error
	deleteErr   error
	indexCalls  int
	deleteCalls int
}

func (f *fakeWriter) IndexListing(_ context.Context, doc domain.ListingDoc, version int64) error {
	f.indexCalls++
	if f.indexErr != nil {
		return f.indexErr
	}
	f.indexed = append(f.indexed, doc)
	f.indexedVer = append(f.indexedVer, version)
	return nil
}

func (f *fakeWriter) DeleteListing(_ context.Context, listingID string, version int64) error {
	f.deleteCalls++
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deletedID = listingID
	f.deletedVer = version
	return nil
}

func mustEnvelope(t *testing.T, event *eventsv1.ListingPublished) *eventsv1.Envelope {
	t.Helper()
	env, err := kafkax.NewEnvelope(kafkax.EventListingPublished, event.GetListingId(), event)
	require.NoError(t, err)
	return env
}

func TestIndexer_HandleListingPublished_Published(t *testing.T) {
	writer := &fakeWriter{}
	indexer := NewIndexer(writer)

	event := &eventsv1.ListingPublished{
		ListingId:  "listing-1",
		AuthorId:   42,
		AuthorName: "Иван",
		Title:      "Закат над морем",
		Tags:       []string{"закат", "море"},
		PriceCents: 1500,
		Currency:   "RUB",
		Status:     domain.StatusPublished,
	}
	env := mustEnvelope(t, event)

	err := indexer.HandleListingPublished(context.Background(), env)
	require.NoError(t, err)

	require.Len(t, writer.indexed, 1, "published-событие обязано вызвать IndexListing, а не Delete")
	assert.Equal(t, 0, writer.deleteCalls)
	doc := writer.indexed[0]
	assert.Equal(t, "listing-1", doc.ListingID)
	assert.Equal(t, int64(42), doc.AuthorID)
	assert.Equal(t, "Закат над морем", doc.Title)
	assert.ElementsMatch(t, []string{"закат", "море"}, doc.Tags)
	assert.Equal(t, int64(1500), doc.PriceCents)

	// Версия — occurred_at конверта, а не что-то ещё: это ключевое свойство
	// идемпотентности через версионирование (см. комментарий в indexer.go).
	assert.Equal(t, env.GetOccurredAtUnixMs(), writer.indexedVer[0])
}

func TestIndexer_HandleListingPublished_Removed(t *testing.T) {
	writer := &fakeWriter{}
	indexer := NewIndexer(writer)

	event := &eventsv1.ListingPublished{
		ListingId: "listing-2",
		Status:    domain.StatusRemoved,
	}
	env := mustEnvelope(t, event)

	err := indexer.HandleListingPublished(context.Background(), env)
	require.NoError(t, err)

	assert.Equal(t, 0, writer.indexCalls, "removed-событие не должно индексировать документ")
	require.Equal(t, 1, writer.deleteCalls)
	assert.Equal(t, "listing-2", writer.deletedID)
	assert.Equal(t, env.GetOccurredAtUnixMs(), writer.deletedVer)
}

func TestIndexer_HandleListingPublished_BitiyPayload(t *testing.T) {
	writer := &fakeWriter{}
	indexer := NewIndexer(writer)

	// Конверт без payload — типичный случай "битого сообщения": разбор
	// провалится сразу, и повторять его смысла нет.
	env := &eventsv1.Envelope{EventId: "e1", EventType: kafkax.EventListingPublished}

	err := indexer.HandleListingPublished(context.Background(), env)
	require.Error(t, err)
	assert.True(t, errors.Is(err, kafkax.ErrPermanent),
		"битый payload обязан классифицироваться как permanent, повтор его не починит")
	assert.Equal(t, 0, writer.indexCalls)
	assert.Equal(t, 0, writer.deleteCalls)
}

func TestClassifyESErr(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantRetry bool
		wantPerma bool
	}{
		{
			name:      "индекс недоступен — временная ошибка",
			err:       domain.ErrIndexUnavailable,
			wantRetry: true,
		},
		{
			name:      "документ не прошёл маппинг — постоянная ошибка",
			err:       domain.ErrInvalidDocument,
			wantPerma: true,
		},
		{
			name:      "неизвестная ошибка — retryable по умолчанию",
			err:       errors.New("что-то пошло не так"),
			wantRetry: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyESErr(tt.err)
			assert.Equal(t, tt.wantRetry, errors.Is(got, kafkax.ErrRetryable))
			assert.Equal(t, tt.wantPerma, errors.Is(got, kafkax.ErrPermanent))
		})
	}
}

func TestIndexer_HandleListingPublished_OshibkiPishutsyaCherezClassify(t *testing.T) {
	writer := &fakeWriter{indexErr: domain.ErrInvalidDocument}
	indexer := NewIndexer(writer)

	env := mustEnvelope(t, &eventsv1.ListingPublished{
		ListingId: "listing-3",
		Status:    domain.StatusPublished,
	})

	err := indexer.HandleListingPublished(context.Background(), env)
	require.Error(t, err)
	assert.True(t, errors.Is(err, kafkax.ErrPermanent))
}

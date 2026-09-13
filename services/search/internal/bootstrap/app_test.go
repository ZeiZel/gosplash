package bootstrap_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/pkg/kafkax"
	"gosplash/services/search/internal/bootstrap"
	"gosplash/services/search/internal/domain"
)

// fakeSearchIndex/fakeIndexWriter — подделки ports.SearchIndex/IndexWriter
// (docs/STYLE.md: «прикладной слой тестируется подделками портов, без
// контейнеров»). Их методы в этом тесте не вызываются: Run поднимает
// серверы и сразу останавливается по отменённому ctx, не обслужив ни
// одного запроса.
type fakeSearchIndex struct{}

func (fakeSearchIndex) Search(context.Context, domain.SearchQuery) (domain.SearchResult, error) {
	return domain.SearchResult{}, nil
}

type fakeIndexWriter struct{}

func (fakeIndexWriter) IndexListing(context.Context, domain.ListingDoc, int64) error { return nil }
func (fakeIndexWriter) DeleteListing(context.Context, string, int64) error           { return nil }

// fakeConsumer — Consumer без Kafka: Run блокируется до отмены ctx, как
// настоящие kafkax.Consumer.Run/RetryConsumer.Run, но не открывает ни
// одного соединения — находка задания: kafkax.NewConsumer открывает
// kgo.Client СРАЗУ (см. её комментарий в internal/bootstrap/app.go),
// поэтому Deps принимает интерфейс, а не *kafkax.Consumer.
type fakeConsumer struct{ closed bool }

func (c *fakeConsumer) Run(ctx context.Context, _ kafkax.Handler) error { <-ctx.Done(); return nil }
func (c *fakeConsumer) Close()                                          { c.closed = true }
func (c *fakeConsumer) Ping(context.Context) error                      { return nil }

func validDeps() bootstrap.Deps {
	return bootstrap.Deps{
		SearchIndex:   fakeSearchIndex{},
		IndexWriter:   fakeIndexWriter{},
		Consumer:      &fakeConsumer{},
		RetryConsumer: &fakeConsumer{},
		GRPCAddr:      ":0",
		HTTPAddr:      ":0",
		MetricsAddr:   ":0",
	}
}

// TestNewApp_TrebuetObyazatelnyeZavisimosti проверяет отказ без похода
// в сеть: без любого из четырёх портов App собираться не должен.
func TestNewApp_TrebuetObyazatelnyeZavisimosti(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*bootstrap.Deps)
	}{
		{"без SearchIndex", func(d *bootstrap.Deps) { d.SearchIndex = nil }},
		{"без IndexWriter", func(d *bootstrap.Deps) { d.IndexWriter = nil }},
		{"без Consumer", func(d *bootstrap.Deps) { d.Consumer = nil }},
		{"без RetryConsumer", func(d *bootstrap.Deps) { d.RetryConsumer = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deps := validDeps()
			tc.mutate(&deps)
			_, err := bootstrap.NewApp(deps)
			assert.Error(t, err, "App не должен собираться без обязательной зависимости")
		})
	}
}

// TestApp_RunOstanavlivaetsyaPoOtmeneKontekstaICloseIdempotenten — главный
// тест композиционного корня: App собирается из подделок (без Elasticsearch
// и Kafka), Run быстро возвращается по отменённому контексту (не зависает,
// как зависал бы kafkax.Consumer.Close без AllowRebalance — см. задание),
// а Close можно звать сколько угодно раз подряд.
//
// ServeMetricsAndPprof регистрирует "GET /metrics" в http.DefaultServeMux —
// повторная регистрация в том же процессе паникует, поэтому Run в этом
// пакете вызывается РОВНО ОДИН РАЗ на весь тестовый бинарник.
func TestApp_RunOstanavlivaetsyaPoOtmeneKontekstaICloseIdempotenten(t *testing.T) {
	consumer := &fakeConsumer{}
	retryConsumer := &fakeConsumer{}
	deps := validDeps()
	deps.Consumer = consumer
	deps.RetryConsumer = retryConsumer

	application, err := bootstrap.NewApp(deps)
	require.NoError(t, err, "App обязан собраться из одних подделок портов")

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- application.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-runErr:
		assert.NoError(t, err, "остановка по отменённому контексту не должна возвращать ошибку")
	case <-time.After(5 * time.Second):
		t.Fatal("Run не вернулся за 5 секунд после отмены контекста — похоже на зависший shutdown")
	}

	assert.False(t, consumer.closed, "App.Close не обязан закрывать чужой Consumer — им владеет run() в main.go")
	assert.False(t, retryConsumer.closed, "то же для RetryConsumer")

	assert.NotPanics(t, func() {
		require.NoError(t, application.Close())
		require.NoError(t, application.Close())
	}, "Close обязан быть идемпотентным и не паниковать при повторном вызове")
}

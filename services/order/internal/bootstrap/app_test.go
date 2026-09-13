package bootstrap_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
	"gosplash/services/order/internal/bootstrap"
	"gosplash/services/order/internal/domain"
	"gosplash/services/order/internal/ports"
)

// fakeRelay — Relay без сети, см. её комментарий в app.go: Deps.Relay —
// интерфейс именно затем, чтобы outbox.New (который открывает пулы
// и продюсер Kafka СРАЗУ) не понадобился этому тесту.
type fakeRelay struct{ closed bool }

func (r *fakeRelay) Run(ctx context.Context) error { <-ctx.Done(); return nil }
func (r *fakeRelay) Close()                        { r.closed = true }
func (r *fakeRelay) Ping(context.Context) error    { return nil }

// fakeOrderRepo/fakeIdemStore/fakeListingReader/fakeStarter — подделки
// портов app.NewOrderService (docs/STYLE.md: «прикладной слой тестируется
// подделками портов, без контейнеров» — то же самое годится и для
// композиционного корня, который эти подделки просто прокидывает дальше).
// Их методы в этом тесте не вызываются вовсе: Run поднимает серверы
// и сразу останавливается по отменённому ctx, не обслужив ни одного
// запроса, — но тип обязан реализовать интерфейс целиком.
type fakeOrderRepo struct{}

func (fakeOrderRepo) Create(context.Context, *domain.Order, *eventsv1.Envelope) error { return nil }
func (fakeOrderRepo) GetByID(context.Context, string) (*domain.Order, error)          { return nil, nil }

type fakeIdemStore struct{}

func (fakeIdemStore) Begin(context.Context, string) (ports.IdempotencyResult, error) {
	return ports.IdempotencyResult{}, nil
}
func (fakeIdemStore) Complete(context.Context, string, []byte) error { return nil }

type fakeListingReader struct{}

func (fakeListingReader) GetListing(context.Context, string) (ports.Listing, error) {
	return ports.Listing{}, nil
}

type fakeStarter struct{}

func (fakeStarter) StartPlaceOrder(context.Context, *domain.Order) error { return nil }

func validDeps() bootstrap.Deps {
	return bootstrap.Deps{
		OrderRepo:     fakeOrderRepo{},
		IdemStore:     fakeIdemStore{},
		ListingReader: fakeListingReader{},
		Starter:       fakeStarter{},
		Relay:         &fakeRelay{},
		GRPCAddr:      ":0",
		HTTPAddr:      ":0",
		MetricsAddr:   ":0",
	}
}

// TestNewApp_TrebuetObyazatelnyeZavisimosti проверяет отказ без похода
// в сеть: NewApp обязан отвергнуть Deps с любой отсутствующей зависимостью
// сразу, а не уронить процесс позже на первом обращении к nil-порту.
func TestNewApp_TrebuetObyazatelnyeZavisimosti(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*bootstrap.Deps)
	}{
		{"без OrderRepo", func(d *bootstrap.Deps) { d.OrderRepo = nil }},
		{"без IdemStore", func(d *bootstrap.Deps) { d.IdemStore = nil }},
		{"без ListingReader", func(d *bootstrap.Deps) { d.ListingReader = nil }},
		{"без Starter", func(d *bootstrap.Deps) { d.Starter = nil }},
		{"без Relay", func(d *bootstrap.Deps) { d.Relay = nil }},
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
// тест композиционного корня API-процесса: App собирается целиком из
// подделок портов (без Postgres/Redis/catalog/Temporal), Run быстро
// возвращается по отменённому контексту, а Close можно звать сколько
// угодно раз подряд.
//
// ServeMetricsAndPprof регистрирует "GET /metrics" в http.DefaultServeMux
// (pkg/httpx.ServeMetricsAndPprof) — повторная регистрация в том же
// процессе паникует, поэтому Run в этом пакете вызывается РОВНО ОДИН РАЗ
// на весь тестовый бинарник (см. тот же приём в wallet/internal/bootstrap).
func TestApp_RunOstanavlivaetsyaPoOtmeneKontekstaICloseIdempotenten(t *testing.T) {
	relay := &fakeRelay{}
	deps := validDeps()
	deps.Relay = relay

	application, err := bootstrap.NewApp(deps)
	require.NoError(t, err, "App обязан собраться из одних подделок портов")

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- application.Run(ctx) }()

	// Даём серверам реально подняться на ":0", прежде чем просить остановиться.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-runErr:
		assert.NoError(t, err, "остановка по отменённому контексту не должна возвращать ошибку")
	case <-time.After(5 * time.Second):
		t.Fatal("Run не вернулся за 5 секунд после отмены контекста — похоже на зависший shutdown")
	}

	assert.False(t, relay.closed, "App.Close не обязан закрывать чужой Relay — им владеет run() в main.go")

	assert.NotPanics(t, func() {
		require.NoError(t, application.Close())
		require.NoError(t, application.Close())
	}, "Close обязан быть идемпотентным и не паниковать при повторном вызове")
}

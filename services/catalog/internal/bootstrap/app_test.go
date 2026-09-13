package bootstrap_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
	"gosplash/pkg/kafkax"

	"gosplash/services/catalog/internal/app"
	"gosplash/services/catalog/internal/bootstrap"
	"gosplash/services/catalog/internal/domain"
	"gosplash/services/catalog/internal/ports"
)

// Тесты этого файла доказывают, что приложение собирается и корректно
// останавливается без единого живого соединения — Postgres, Redis, Kafka.
// Бизнес-логика сценариев (Indexer, ListingService, LicenseService) уже
// покрыта в internal/app/*_test.go — здесь она не проверяется повторно.
//
// НАХОДКА (см. package doc в app.go): *dbx.DB, *redisx.Client,
// *kafkax.Consumer и *outbox.Relay нельзя было бы передать в NewApp
// напрямую и остаться тестируемыми без докера — их конструкторы дозваниваются
// до живой инфраструктуры немедленно. Поэтому Deps принимает уже готовые
// ports-интерфейсы (для сценариев) и узкие Consumer/Relay (для фоновых
// процессов) — и то, и другое легко подделать здесь, без единого сетевого
// вызова.

type fakeListingRepo struct{}

func (fakeListingRepo) GetByID(context.Context, string) (*domain.Listing, error) {
	return nil, domain.ErrNotFound
}
func (fakeListingRepo) List(context.Context, int, string, []string) ([]domain.Listing, string, error) {
	return nil, "", nil
}
func (fakeListingRepo) CountOn(context.Context, string) (int64, error) { return 0, nil }

type fakeViewCounter struct{}

func (fakeViewCounter) IncrView(context.Context, string, time.Time) error { return nil }
func (fakeViewCounter) Top(context.Context, time.Time, int) ([]ports.TopEntry, error) {
	return nil, nil
}

type fakeViewPublisher struct{}

func (fakeViewPublisher) Enqueue(string, int64, int64, string) {}

type fakeCache struct{}

func (fakeCache) GetOrLoad(ctx context.Context, _ string, load func(context.Context) (*domain.Listing, error)) (*domain.Listing, error) {
	return load(ctx)
}
func (fakeCache) Invalidate(context.Context, string) error { return nil }

type fakeUnitOfWork struct{}

func (fakeUnitOfWork) WithClaim(context.Context, string, string, string, func(ports.IndexTx) error) (bool, error) {
	return false, nil
}

type fakeMediaFetcher struct{}

func (fakeMediaFetcher) Fetch(context.Context, string, int64) (*domain.Listing, error) {
	return nil, domain.ErrPhotoNotFound
}

type fakeLicenseRepo struct{}

func (fakeLicenseRepo) Grant(context.Context, string, int64, string) (*domain.License, error) {
	return &domain.License{}, nil
}
func (fakeLicenseRepo) Revoke(context.Context, string, string) (bool, error) { return false, nil }

// fakeConsumer — подделка bootstrap.Consumer: не открывает ни одного
// соединения и выходит ровно тогда, когда отменяют ctx.
type fakeConsumer struct {
	runs atomic.Int32
}

func (c *fakeConsumer) Run(ctx context.Context, _ kafkax.Handler) error {
	c.runs.Add(1)
	<-ctx.Done()
	return nil
}

// fakeRelay — подделка bootstrap.Relay, то же поведение.
type fakeRelay struct {
	runs atomic.Int32
}

func (r *fakeRelay) Run(ctx context.Context) error {
	r.runs.Add(1)
	<-ctx.Done()
	return nil
}

func testTimeouts() bootstrap.Timeouts {
	return bootstrap.Timeouts{
		DrainDelay:      10 * time.Millisecond,
		ShutdownTimeout: time.Second,
	}
}

func newTestServices() (*app.ListingService, *app.LicenseService, *app.WatchHub) {
	listingService := app.NewListingService(fakeListingRepo{}, fakeCache{}, fakeViewCounter{}, fakeViewPublisher{})
	licenseService := app.NewLicenseService(fakeLicenseRepo{})
	hub := app.NewWatchHub()
	return listingService, licenseService, hub
}

func newTestApp(t *testing.T, consumers []bootstrap.ConsumerSpec, relay bootstrap.Relay) *bootstrap.App {
	t.Helper()

	listingService, licenseService, hub := newTestServices()

	a, err := bootstrap.NewApp(bootstrap.Deps{
		ServiceName:    "catalog-test",
		HTTPAddr:       ":0",
		GRPCAddr:       ":0",
		MetricsAddr:    ":0",
		ListingService: listingService,
		LicenseService: licenseService,
		Hub:            hub,
		Consumers:      consumers,
		Relay:          relay,
		Timeouts:       testTimeouts(),
	})
	require.NoError(t, err, "NewApp обязан собираться без единого живого соединения")
	require.NotNil(t, a)
	return a
}

func TestNewApp_TrebuetSzenariiIRelay(t *testing.T) {
	listingService, licenseService, hub := newTestServices()

	_, err := bootstrap.NewApp(bootstrap.Deps{ListingService: listingService, LicenseService: licenseService, Hub: hub})
	assert.Error(t, err, "без Relay собрать приложение нельзя — читать outbox некому")

	_, err = bootstrap.NewApp(bootstrap.Deps{Relay: &fakeRelay{}})
	assert.Error(t, err, "без сценариев собрать приложение нельзя")
}

// TestRun_OstanavlivaetsyaBystroPoOtmeneKonteksta — главное свойство,
// которое требует задание: App.Run обязан вернуться быстро после отмены
// ctx. Ровно один вызов Run на весь тестовый бинарь пакета: она поднимает
// /metrics на http.DefaultServeMux (см. такой же комментарий в
// services/media/internal/bootstrap/app_test.go).
func TestRun_OstanavlivaetsyaBystroPoOtmeneKonteksta(t *testing.T) {
	indexer := app.NewIndexer(fakeUnitOfWork{}, fakeMediaFetcher{}, fakeCache{}, app.NewWatchHub())

	uploaded := &fakeConsumer{}
	thumbnail := &fakeConsumer{}
	uploadedRetry := &fakeConsumer{}
	thumbnailRetry := &fakeConsumer{}
	relay := &fakeRelay{}

	handleUploaded := func(ctx context.Context, env *eventsv1.Envelope) error {
		return indexer.HandlePhotoUploaded(ctx, env)
	}

	a := newTestApp(t, []bootstrap.ConsumerSpec{
		{Name: "uploaded", Consumer: uploaded, Handle: handleUploaded, AwaitOnShutdown: true},
		{Name: "thumbnail-ready", Consumer: thumbnail, Handle: handleUploaded, AwaitOnShutdown: true},
		{Name: "uploaded-retry", Consumer: uploadedRetry, Handle: handleUploaded, AwaitOnShutdown: false},
		{Name: "thumbnail-ready-retry", Consumer: thumbnailRetry, Handle: handleUploaded, AwaitOnShutdown: false},
	}, relay)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-runErr:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run не вернулась в течение 3 секунд после отмены контекста")
	}

	assert.Equal(t, int32(1), uploaded.runs.Load())
	assert.Equal(t, int32(1), thumbnail.runs.Load())
	assert.Equal(t, int32(1), uploadedRetry.runs.Load())
	assert.Equal(t, int32(1), thumbnailRetry.runs.Load())
	assert.Equal(t, int32(1), relay.runs.Load())

	require.NoError(t, a.Close())
}

// TestClose_IdempotentnaBezZapuska — Close не паникует и не требует
// предварительного Run.
func TestClose_IdempotentnaBezZapuska(t *testing.T) {
	a := newTestApp(t, nil, &fakeRelay{})

	assert.NotPanics(t, func() {
		require.NoError(t, a.Close())
		require.NoError(t, a.Close())
	})
}

package bootstrap_test

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/media/internal/app"
	"gosplash/services/media/internal/bootstrap"
	"gosplash/services/media/internal/domain"
)

// Тесты этого файла доказывают ровно то, что просит рефакторинг
// композиционного корня: приложение собирается и останавливается без
// поднятой инфраструктуры. Бизнес-логика (Upload/Delete/...) уже покрыта
// в internal/app/service_test.go — здесь она не проверяется.
//
// НАХОДКА (см. package doc в app.go): подделать удалось PhotoRepository,
// ObjectStorage (оба — уже интерфейсы ports) и outbox-relay (свежий
// интерфейс bootstrap.Relay). А вот *dbx.Shards и *outbox.Relay
// напрямую подделать было бы НЕЛЬЗЯ — их конструкторы дозваниваются до
// живого Postgres/Kafka немедленно (см. package doc), поэтому Deps.Service
// и Deps.Relay в App принимают уже готовые сценарий/интерфейс, а не сырые
// адреса подключения.

type fakePhotoRepo struct{}

func (fakePhotoRepo) Create(context.Context, *domain.Photo) error { return nil }
func (fakePhotoRepo) GetByID(context.Context, int64, string) (*domain.Photo, error) {
	return nil, domain.ErrNotFound
}
func (fakePhotoRepo) ListByUser(context.Context, int64, int) ([]domain.Photo, error) { return nil, nil }
func (fakePhotoRepo) Delete(context.Context, int64, string) error                    { return nil }
func (fakePhotoRepo) ShardOf(int64) int                                              { return 0 }
func (fakePhotoRepo) CountByShard(context.Context) ([]int64, error)                  { return []int64{0}, nil }

type fakeObjectStorage struct{}

func (fakeObjectStorage) Put(context.Context, string, string, io.Reader, int64, string) error {
	return nil
}
func (fakeObjectStorage) PresignedURL(context.Context, string, string, time.Duration) (string, error) {
	return "https://example.test/fake", nil
}

// fakeRelay — подделка bootstrap.Relay: не открывает ни одного соединения
// и выходит ровно тогда, когда отменяют ctx — то же поведение, которого
// docs/STYLE.md и pkg/kafkax требуют от настоящего outbox.Relay.Run.
type fakeRelay struct {
	runCalls   atomic.Int32
	closeCalls atomic.Int32
}

func (r *fakeRelay) Run(ctx context.Context) error {
	r.runCalls.Add(1)
	<-ctx.Done()
	return nil
}

func (r *fakeRelay) Close() { r.closeCalls.Add(1) }

func (r *fakeRelay) Ping(context.Context) error { return nil }

// testTimeouts — короткие тайминги: тест не обязан ждать боевые 15+2
// секунды, чтобы убедиться, что порядок остановки соблюдается.
func testTimeouts() bootstrap.Timeouts {
	return bootstrap.Timeouts{
		DrainDelay:           10 * time.Millisecond,
		ShutdownTimeout:      time.Second,
		RelayShutdownTimeout: time.Second,
	}
}

func newTestApp(t *testing.T, relay bootstrap.Relay) *bootstrap.App {
	t.Helper()

	service := app.NewPhotoService(fakePhotoRepo{}, fakeObjectStorage{}, "originals")

	// ":0" — ОС сама выбирает свободный порт: тест не должен зависеть от
	// того, что порты 8101/9101/8201 сейчас свободны на машине, где он
	// запущен (и не должен мешать реально запущенному сервису).
	a, err := bootstrap.NewApp(bootstrap.Deps{
		ServiceName:  "media-test",
		HTTPAddr:     ":0",
		GRPCAddr:     ":0",
		MetricsAddr:  ":0",
		Service:      service,
		MaxBodyBytes: 1 << 20,
		PresignedTTL: time.Minute,
		Relay:        relay,
		Timeouts:     testTimeouts(),
	})
	require.NoError(t, err, "NewApp обязан собираться без единого живого соединения")
	require.NotNil(t, a)
	return a
}

func TestNewApp_TrebuetSzenariyIRelay(t *testing.T) {
	_, err := bootstrap.NewApp(bootstrap.Deps{Relay: &fakeRelay{}})
	assert.Error(t, err, "без Service собрать приложение нельзя")

	service := app.NewPhotoService(fakePhotoRepo{}, fakeObjectStorage{}, "originals")
	_, err = bootstrap.NewApp(bootstrap.Deps{Service: service})
	assert.Error(t, err, "без Relay собрать приложение нельзя — читать outbox некому")
}

// TestRun_OstanavlivaetsyaBystroPoOtmeneKonteksta — главное свойство,
// которое требует задание: App.Run обязан вернуться быстро после отмены
// ctx, а не зависнуть на боевых таймаутах graceful shutdown.
//
// Run вызывается ровно ОДИН раз на весь тестовый бинарь пакета: она
// поднимает /metrics на http.DefaultServeMux (pkg/httpx.ServeMetricsAndPprof),
// а повторная регистрация того же пути в том же процессе паникует —
// это ограничение самого http.ServeMux, не связанное с рефакторингом.
func TestRun_OstanavlivaetsyaBystroPoOtmeneKonteksta(t *testing.T) {
	relay := &fakeRelay{}
	a := newTestApp(t, relay)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	// Даём Run реально стартовать серверы, прежде чем просить остановиться —
	// иначе тест проверял бы отмену контекста ДО начала работы, а не
	// собственно быстрый graceful shutdown.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-runErr:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run не вернулась в течение 3 секунд после отмены контекста")
	}

	assert.Equal(t, int32(1), relay.runCalls.Load(), "relay обязан быть запущен ровно один раз")

	// App.Close не трогает relay — она его не открывала (см. package doc):
	// закрытие relay после того, как Run дождался его остановки, — забота
	// run() в cmd/media/main.go, обычным defer, как dbInstance в эталоне.
	require.NoError(t, a.Close())
}

// TestClose_IdempotentnaBezZapuska — Close не паникует и не требует
// предварительного Run: NewApp могла собрать приложение, которое решили
// не запускать (например, ошибка чуть выше по стеку), и в этом случае
// освобождать нечего, но и падать тоже нельзя.
func TestClose_IdempotentnaBezZapuska(t *testing.T) {
	a := newTestApp(t, &fakeRelay{})

	assert.NotPanics(t, func() {
		require.NoError(t, a.Close())
		require.NoError(t, a.Close())
	})
}

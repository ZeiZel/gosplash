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

	"gosplash/services/thumbnail-worker/internal/bootstrap"
)

// Тесты этого файла доказывают, что приложение собирается и корректно
// останавливается без единого живого соединения — Postgres, Redis, Kafka, S3.
// Бизнес-логика (генерация превью) уже покрыта в internal/app/service_test.go.
//
// НАХОДКА (см. package doc в app.go): *dbx.Shards, *kafkax.Consumer и
// *outbox.Relay нельзя было бы передать в NewApp напрямую и остаться
// тестируемыми без докера — их конструкторы дозваниваются до живой
// инфраструктуры немедленно. Поэтому Deps принимает узкие интерфейсы
// Consumer/Relay, которым легко подсунуть подделку.

// fakeConsumer — подделка bootstrap.Consumer: не открывает ни одного
// соединения и выходит ровно тогда, когда отменяют ctx — то же поведение,
// которого docs/STYLE.md и pkg/kafkax требуют от настоящего *kafkax.Consumer
// (обязательный AllowRebalance на выходе живёт внутри самого pkg/kafkax и
// здесь не воспроизводится: подделка не открывает соединения, которое
// нужно было бы освобождать).
type fakeConsumer struct {
	runs atomic.Int32
}

func (c *fakeConsumer) Run(ctx context.Context, _ kafkax.Handler) error {
	c.runs.Add(1)
	<-ctx.Done()
	return nil
}

type fakeRelay struct {
	runs atomic.Int32
}

func (r *fakeRelay) Run(ctx context.Context) error {
	r.runs.Add(1)
	<-ctx.Done()
	return nil
}

func noopHandle(context.Context, *eventsv1.Envelope) error { return nil }

func testTimeouts() bootstrap.Timeouts {
	return bootstrap.Timeouts{
		DrainDelay:      10 * time.Millisecond,
		ShutdownTimeout: time.Second,
	}
}

func newTestApp(t *testing.T, consumer, retryConsumer bootstrap.Consumer, relay bootstrap.Relay) *bootstrap.App {
	t.Helper()

	a, err := bootstrap.NewApp(bootstrap.Deps{
		ServiceName:   "thumbnail-worker-test",
		HTTPAddr:      ":0",
		MetricsAddr:   ":0",
		Consumer:      consumer,
		RetryConsumer: retryConsumer,
		Handle:        noopHandle,
		Relay:         relay,
		Timeouts:      testTimeouts(),
	})
	require.NoError(t, err, "NewApp обязан собираться без единого живого соединения")
	require.NotNil(t, a)
	return a
}

func TestNewApp_TrebuetVseZavisimosti(t *testing.T) {
	full := bootstrap.Deps{
		Consumer:      &fakeConsumer{},
		RetryConsumer: &fakeConsumer{},
		Handle:        noopHandle,
		Relay:         &fakeRelay{},
	}

	withoutConsumer := full
	withoutConsumer.Consumer = nil
	_, err := bootstrap.NewApp(withoutConsumer)
	assert.Error(t, err, "без основного консьюмера собрать приложение нельзя")

	withoutRetry := full
	withoutRetry.RetryConsumer = nil
	_, err = bootstrap.NewApp(withoutRetry)
	assert.Error(t, err, "без retry-консьюмера собрать приложение нельзя")

	withoutHandle := full
	withoutHandle.Handle = nil
	_, err = bootstrap.NewApp(withoutHandle)
	assert.Error(t, err, "без обработчика собрать приложение нельзя")

	withoutRelay := full
	withoutRelay.Relay = nil
	_, err = bootstrap.NewApp(withoutRelay)
	assert.Error(t, err, "без outbox-relay собрать приложение нельзя")
}

// TestRun_OstanavlivaetsyaBystroPoOtmeneKonteksta — главное свойство,
// которое требует задание: App.Run обязан вернуться быстро после отмены
// ctx. Ровно один вызов Run на весь тестовый бинарь пакета: она поднимает
// /metrics на http.DefaultServeMux (см. такой же комментарий в
// services/media/internal/bootstrap/app_test.go).
func TestRun_OstanavlivaetsyaBystroPoOtmeneKonteksta(t *testing.T) {
	consumer := &fakeConsumer{}
	retryConsumer := &fakeConsumer{}
	relay := &fakeRelay{}
	a := newTestApp(t, consumer, retryConsumer, relay)

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

	assert.Equal(t, int32(1), consumer.runs.Load())
	assert.Equal(t, int32(1), retryConsumer.runs.Load())
	assert.Equal(t, int32(1), relay.runs.Load())

	require.NoError(t, a.Close())
}

// TestClose_IdempotentnaBezZapuska — Close не паникует и не требует
// предварительного Run.
func TestClose_IdempotentnaBezZapuska(t *testing.T) {
	a := newTestApp(t, &fakeConsumer{}, &fakeConsumer{}, &fakeRelay{})

	assert.NotPanics(t, func() {
		require.NoError(t, a.Close())
		require.NoError(t, a.Close())
	})
}

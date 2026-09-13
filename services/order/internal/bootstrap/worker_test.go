package bootstrap_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
	"gosplash/services/order/internal/bootstrap"
)

// fakeWalletSaga/fakeCatalogSaga/fakeSagaOrderRepo — подделки портов
// temporal.NewActivities, тем же приёмом, что и в app_test.go: их методы
// здесь не вызываются, WorkerApp.Run в этом тесте не успевает дойти
// до исполнения ни одной activity.
type fakeWalletSaga struct{}

func (fakeWalletSaga) ReserveFunds(context.Context, string, int64, int64, string) (string, error) {
	return "", nil
}
func (fakeWalletSaga) CommitFunds(context.Context, string, string, int64) ([]string, error) {
	return nil, nil
}
func (fakeWalletSaga) ReleaseFunds(context.Context, string, string, string) error { return nil }

type fakeCatalogSaga struct{}

func (fakeCatalogSaga) GrantLicense(context.Context, string, string, int64) (string, error) {
	return "", nil
}
func (fakeCatalogSaga) RevokeLicense(context.Context, string, string) (bool, error) {
	return false, nil
}

type fakeSagaOrderRepo struct{}

func (fakeSagaOrderRepo) UpdateStatus(context.Context, string, string) error { return nil }
func (fakeSagaOrderRepo) Complete(context.Context, string, string, *eventsv1.Envelope) error {
	return nil
}
func (fakeSagaOrderRepo) Fail(context.Context, string, string) error { return nil }

// newLazyTemporalClient — client.Client БЕЗ похода в сеть при
// конструировании (см. комментарий WorkerDeps.TemporalClient в worker.go):
// sdkclient.NewLazyClient откладывает подключение до первого реального
// вызова, а NewWorkerApp/sdkworker.New этот вызов не делают — регистрация
// workflow/activities — чистая работа с локальными мапами.
func newLazyTemporalClient(t *testing.T) sdkclient.Client {
	t.Helper()
	c, err := sdkclient.NewLazyClient(sdkclient.Options{HostPort: "127.0.0.1:1"})
	require.NoError(t, err, "ленивый temporal-клиент обязан конструироваться без сети")
	return c
}

func validWorkerDeps(t *testing.T) bootstrap.WorkerDeps {
	return bootstrap.WorkerDeps{
		WalletSaga:     fakeWalletSaga{},
		CatalogSaga:    fakeCatalogSaga{},
		OrderRepo:      fakeSagaOrderRepo{},
		TemporalClient: newLazyTemporalClient(t),
		TaskQueue:      "test-task-queue",
	}
}

// TestNewWorkerApp_TrebuetObyazatelnyeZavisimosti — как и у API-приложения,
// отсутствующая зависимость обязана провалить сборку сразу.
func TestNewWorkerApp_TrebuetObyazatelnyeZavisimosti(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*bootstrap.WorkerDeps)
	}{
		{"без WalletSaga", func(d *bootstrap.WorkerDeps) { d.WalletSaga = nil }},
		{"без CatalogSaga", func(d *bootstrap.WorkerDeps) { d.CatalogSaga = nil }},
		{"без OrderRepo", func(d *bootstrap.WorkerDeps) { d.OrderRepo = nil }},
		{"без TemporalClient", func(d *bootstrap.WorkerDeps) { d.TemporalClient = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deps := validWorkerDeps(t)
			tc.mutate(&deps)
			_, err := bootstrap.NewWorkerApp(deps)
			assert.Error(t, err, "WorkerApp не должен собираться без обязательной зависимости")
		})
	}
}

// TestWorkerApp_RunOstanavlivaetsyaPoOtmeneKontekstaICloseIdempotenten —
// тот же контракт, что и у App: Run блокируется до отмены ctx и завершается
// после неё, Close идемпотентен. Воркер сконструирован на ленивом клиенте
// без живого Temporal-сервера — Worker.Run() в таком режиме продолжает
// РЕТРАИТЬ подключение к incoming task queue на фоне (это поведение самого
// Temporal SDK, не этого проекта), а WorkerApp.Run обязана прервать его
// вызовом Stop() и дождаться реального возврата — см. её комментарий.
func TestWorkerApp_RunOstanavlivaetsyaPoOtmeneKontekstaICloseIdempotenten(t *testing.T) {
	application, err := bootstrap.NewWorkerApp(validWorkerDeps(t))
	require.NoError(t, err, "WorkerApp обязан собраться из подделок портов и ленивого temporal-клиента")

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- application.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-runErr:
		// Ошибка здесь возможна (например, ErrWorkerShutdown) — проверяем
		// только то, что Run вообще вернулся, а не её конкретное значение:
		// это уже поведение Temporal SDK, а не композиционного корня.
	case <-time.After(10 * time.Second):
		t.Fatal("Run не вернулся за 10 секунд после отмены контекста — Stop() не остановил воркер")
	}

	assert.NotPanics(t, func() {
		require.NoError(t, application.Close())
		require.NoError(t, application.Close())
	}, "Close обязан быть идемпотентным и не паниковать при повторном вызове")
}

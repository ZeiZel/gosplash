package bootstrap

import (
	"context"
	"fmt"
	"log/slog"

	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"

	"gosplash/services/order/internal/adapters/temporal"
	"gosplash/services/order/internal/ports"
)

// WorkerDeps — то, чем пользуется temporal-worker процесс.
//
// WalletSaga/CatalogSaga/OrderRepo — те же ports.WalletSaga/CatalogSaga/
// SagaOrderRepository, которых просит temporal.NewActivities: их
// реализации (ordergrpc.NewWalletSagaClient и т.п.) оборачивают
// *grpc.ClientConn, а grpcx.Dial не ходит в сеть при вызове (grpc.NewClient
// лениво резолвит цель при первом RPC) — поэтому их можно было бы строить
// и внутри NewWorkerApp. Порты оставлены явными зависимостями всё равно:
// так тест подделывает саги без единого grpc.ClientConn, а WorkerApp
// не обязан знать, что за портами стоит именно gRPC.
//
// TemporalClient — единственная по-настоящему ленивая зависимость этого
// процесса: sdkclient.Dial (см. cmd/worker/main.go) подключается СРАЗУ
// и является намеренным fail-fast, а sdkclient.NewLazyClient — тем же
// типом client.Client, но без похода в сеть при конструировании (см.
// worker_test.go) — и то, и другое строится СНАРУЖИ, WorkerApp сам клиента
// не открывает.
type WorkerDeps struct {
	WalletSaga  ports.WalletSaga
	CatalogSaga ports.CatalogSaga
	OrderRepo   ports.SagaOrderRepository

	TemporalClient sdkclient.Client
	TaskQueue      string
}

// WorkerApp — собранный temporal-worker процесс: РЕГИСТРИРУЕТ workflow
// и activities, ничего не запускает до Run. Отдельный от App тип — см.
// package doc app.go про то, почему не флаг у одного App.
type WorkerApp struct {
	worker sdkworker.Worker
}

// NewWorkerApp регистрирует workflow/activities на переданном клиенте.
// Сама регистрация — только запись в локальные мапы sdkworker.Worker,
// без единого похода в сеть (сеть — при первом Run, когда воркер начинает
// поллить очередь), поэтому конструктор безопасен для теста.
func NewWorkerApp(deps WorkerDeps) (*WorkerApp, error) {
	if deps.WalletSaga == nil {
		return nil, fmt.Errorf("bootstrap: order-worker: WalletSaga обязателен")
	}
	if deps.CatalogSaga == nil {
		return nil, fmt.Errorf("bootstrap: order-worker: CatalogSaga обязателен")
	}
	if deps.OrderRepo == nil {
		return nil, fmt.Errorf("bootstrap: order-worker: OrderRepo обязателен")
	}
	if deps.TemporalClient == nil {
		return nil, fmt.Errorf("bootstrap: order-worker: TemporalClient обязателен")
	}

	activities := temporal.NewActivities(deps.WalletSaga, deps.CatalogSaga, deps.OrderRepo)

	w := sdkworker.New(deps.TemporalClient, deps.TaskQueue, sdkworker.Options{})
	w.RegisterWorkflow(temporal.PlaceOrderWorkflow)
	// Регистрация СТРУКТУРЫ целиком: Temporal SDK сам находит каждый
	// экспортированный метод *Activities и регистрирует его как activity
	// по имени метода — см. package doc internal/adapters/temporal/activities.go.
	w.RegisterActivity(activities)

	return &WorkerApp{worker: w}, nil
}

// Run переводит Temporal SDK-шный Worker.Run(interruptCh)/Stop() на общий
// для проекта контракт "блокируется до отмены ctx, потом сама останавливается".
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: у Worker.Run() своя модель остановки — канал вместо
// context.Context (см. package doc adapters/temporal и старый main.go,
// использовавший sdkworker.InterruptCh() — API самого SDK, а не этого
// проекта). Run(nil) здесь вместо InterruptCh(): второй сам подписывается
// на SIGINT/SIGTERM, а сигналы уже ловит run() в main.go через
// signal.NotifyContext — тот же источник правды, что и у остальных
// сервисов монорепы. Двум разным подписчикам на одни и те же сигналы
// незачем существовать одновременно.
func (a *WorkerApp) Run(ctx context.Context) error {
	runErr := make(chan error, 1)
	go func() { runErr <- a.worker.Run(nil) }()

	select {
	case <-ctx.Done():
		slog.Info("order-worker: останавливаюсь…")
		a.worker.Stop()
	case err := <-runErr:
		return err
	}

	// Stop() асинхронно завершает Run — дожидаемся её реального возврата,
	// а не выходим сразу после вызова Stop(): иначе run() в main.go может
	// закрыть grpc-соединения к wallet/catalog раньше, чем воркер доделал
	// текущую activity поверх них.
	return <-runErr
}

// Close ничего не закрывает: gRPC-соединения к wallet/catalog и клиент
// Temporal App не открывал — они принадлежат run() в cmd/worker/main.go.
func (a *WorkerApp) Close() error {
	return nil
}

// Package bootstrap — композиционный корень order-service.
//
// Это НЕ internal/app: там сценарий (app.OrderService), который импортирует
// только domain и ports (см. docs/STYLE.md). Здесь — ровно наоборот: как
// сценарий связывается с транспортом (gRPC/HTTP) и инфраструктурой (relay,
// health) в работающий процесс.
//
// ДВА ПРОЦЕССА — ДВА ТИПА App, А НЕ ОДИН С ФЛАГОМ:
//
// cmd/order (App, этот файл) и cmd/worker (WorkerApp, worker.go) — разные
// процессы с разным жизненным циклом (см. package doc бывших main.go,
// перенесённый туда же): у API есть HTTP/gRPC-серверы, outbox-relay
// и readiness; у воркера — только temporal-worker, без единого HTTP-порта
// и без readiness в привычном смысле (Temporal сам решает, кому раздавать
// задачи). Общий App с полем "режим" означал бы, что каждый метод (Run,
// Close) начинается с if a.mode == worker { ... } else { ... } — то есть
// два независимых жизненных цикла, притворяющихся одним типом. Два разных
// типа с одинаковой ФОРМОЙ (NewX, Run, Close) дают ту же согласованность
// с остальными сервисами монорепы, но без ветвления внутри методов.
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/grpc"

	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"

	orderv1 "gosplash/gen/go/gosplash/order/v1"
	ordergrpc "gosplash/services/order/internal/adapters/grpc"
	orderhttp "gosplash/services/order/internal/adapters/http"
	"gosplash/services/order/internal/app"
	"gosplash/services/order/internal/ports"
)

const serviceName = "order"

const (
	// drainDelay — пауза между NotReady и остановкой серверов: балансировщику
	// нужно успеть узнать о смене готовности прежде, чем соединения начнут рваться.
	drainDelay          = 2 * time.Second
	httpShutdownTimeout = 15 * time.Second
	grpcShutdownTimeout = 15 * time.Second
	relayJoinTimeout    = 15 * time.Second
)

// Relay — узкий срез *outbox.Relay: Run/Close/Ping. Интерфейс — чтобы тест
// App мог подставить fake-релей без живых Kafka и Postgres: outbox.New
// открывает пулы и продюсер СРАЗУ (см. pkg/outbox/relay.go), поэтому
// настоящий Relay в принципе нельзя получить без сети.
type Relay interface {
	Run(ctx context.Context) error
	Close()
	Ping(ctx context.Context) error
}

// Deps — то, чем пользуется API-процесс. OrderRepo/IdemStore/ListingReader/
// Starter — это РОВНО ports.OrderRepository/IdempotencyStore/ListingReader/
// WorkflowStarter, которых просит app.NewOrderService: NewApp принимает их
// уже готовыми, а не строит из *gorm.DB/*redisx.Client/*grpc.ClientConn
// сама.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: почему на уровне портов, а не «дайте нам DB и Redis,
// остальное соберём сами», как в wallet. У wallet все внешние зависимости
// (dbx.Open, redisx.New, grpcx.Dial) ленивые — объект строится без похода
// в сеть, поэтому App может собрать репозиторий из сырого *gorm.DB сама.
// У order одна зависимость категорически НЕ ленивая: sdkclient.Dial (клиент
// Temporal) сам подключается СРАЗУ и возвращает ошибку, если сервер
// недоступен (см. cmd/order/main.go — это осознанный fail-fast). Собирать
// ordertemporal.Starter внутри NewApp значило бы, что NewApp тоже не может
// собраться без живого Temporal — то есть ровно та находка, о которой
// предупреждает задание. Решение — поднять точку сборки на уровень портов:
// run() открывает Temporal (и заодно DB/Redis/catalog-conn, которые от
// этого не страдают) и заворачивает их в ports.WorkflowStarter/OrderRepository/
// IdempotencyStore/ListingReader — а NewApp работает уже с интерфейсами,
// которые тест подделывает без единого сетевого вызова.
type Deps struct {
	OrderRepo     ports.OrderRepository
	IdemStore     ports.IdempotencyStore
	ListingReader ports.ListingReader
	Starter       ports.WorkflowStarter

	Relay Relay

	// PostgresPing/RedisPing — health-проверки без прямой зависимости от
	// *gorm.DB/*redisx.Client: то же соображение, что и выше про Starter,
	// только для /readyz, а не для бизнес-порта.
	PostgresPing httpx.Check
	RedisPing    httpx.Check

	JWTSecret      string
	DefaultTimeout time.Duration

	GRPCAddr    string
	HTTPAddr    string
	MetricsAddr string
}

// App — собранный API-процесс order: HTTP+gRPC приём заказов и outbox-relay.
// Ничего не слушает и не ловит сигналы — этим занимается Run.
type App struct {
	deps Deps

	grpcServer *grpc.Server
	grpcHealth *grpcx.Health

	httpServer    *http.Server
	metricsServer *http.Server
	health        *httpx.Health
}

// NewApp связывает сценарий с транспортом. Ошибка — только не заданные
// обязательные зависимости; дальше конструирование в памяти, без сети.
func NewApp(deps Deps) (*App, error) {
	if deps.OrderRepo == nil {
		return nil, fmt.Errorf("bootstrap: order: OrderRepo обязателен")
	}
	if deps.IdemStore == nil {
		return nil, fmt.Errorf("bootstrap: order: IdemStore обязателен")
	}
	if deps.ListingReader == nil {
		return nil, fmt.Errorf("bootstrap: order: ListingReader обязателен")
	}
	if deps.Starter == nil {
		return nil, fmt.Errorf("bootstrap: order: Starter обязателен")
	}
	if deps.Relay == nil {
		return nil, fmt.Errorf("bootstrap: order: Relay обязателен")
	}

	orderService := app.NewOrderService(deps.OrderRepo, deps.IdemStore, deps.ListingReader, deps.Starter)

	orderServer := ordergrpc.NewServer(orderService)
	grpcHealth := grpcx.NewHealth()

	grpcServer := grpcx.NewServer(grpcx.ServerParams{
		ServiceName:    serviceName,
		JWTSecret:      []byte(deps.JWTSecret),
		DefaultTimeout: deps.DefaultTimeout,
	})
	orderv1.RegisterOrderServiceServer(grpcServer, orderServer)
	grpcHealth.Register(grpcServer)

	health := httpx.NewHealth()
	if deps.PostgresPing != nil {
		health.Register("postgres", deps.PostgresPing)
	}
	if deps.RedisPing != nil {
		health.Register("redis", deps.RedisPing)
	}
	health.Register("outbox", deps.Relay.Ping)

	router := http.NewServeMux()
	orderhttp.Register(router, orderServer)
	health.Handle(router, serviceName)

	httpServer := httpx.NewServer(
		httpx.DefaultServerConfig(deps.HTTPAddr),
		httpx.Chain(router, httpx.Default(serviceName)...),
	)

	return &App{
		deps:       deps,
		grpcServer: grpcServer,
		grpcHealth: grpcHealth,
		httpServer: httpServer,
		health:     health,
	}, nil
}

// Run поднимает серверы и relay, блокируется до отмены ctx или до первого
// фатального сбоя gRPC-сервера, затем проводит graceful shutdown.
//
// Порядок остановки выверен на живом кластере и переносится без изменений
// из прежнего main.go: снять readiness → пауза → остановить серверы →
// дождаться relay.
func (a *App) Run(ctx context.Context) error {
	a.metricsServer = httpx.ServeMetricsAndPprof(a.deps.MetricsAddr)
	go httpx.Serve(a.httpServer, "public")

	grpcErr := make(chan error, 1)
	go func() {
		grpcErr <- grpcx.Serve(a.grpcServer, a.deps.GRPCAddr, serviceName)
	}()

	relayDone := make(chan error, 1)
	go func() {
		relayDone <- a.deps.Relay.Run(ctx)
	}()

	var runErr error
	select {
	case <-ctx.Done():
		slog.Info("order: останавливаюсь…")
	case err := <-grpcErr:
		if err != nil {
			runErr = fmt.Errorf("order: gRPC остановлен: %w", err)
		}
	}

	a.health.NotReady()
	a.grpcHealth.NotServing()
	time.Sleep(drainDelay)

	httpx.Shutdown(ctx, httpShutdownTimeout, a.httpServer, a.metricsServer)
	grpcx.Shutdown(a.grpcServer, grpcShutdownTimeout)

	// Дожидаемся relay ПЕРЕД тем, как run() в main.go закроет его пулы:
	// Relay.Close требует, чтобы Run уже вернул управление (см. её
	// комментарий в pkg/outbox) — то же правило, что и для kafka-консьюмеров
	// в analytics/search.
	select {
	case err := <-relayDone:
		if err != nil && runErr == nil {
			runErr = fmt.Errorf("order: outbox relay остановлен: %w", err)
		}
	case <-time.After(relayJoinTimeout):
		slog.Warn("order: outbox relay не остановился вовремя")
	}

	if runErr == nil {
		slog.Info("order: остановлен")
	}
	return runErr
}

// Close ничего не закрывает: серверы уже остановлены внутри Run, а DB/Redis/
// catalog-conn/Temporal-клиент App не открывал — их закрывает run() в
// main.go, которому они принадлежат. Сигнатура согласована с остальными
// сервисами монорепы (см. wallet/internal/bootstrap.App.Close).
func (a *App) Close() error {
	return nil
}

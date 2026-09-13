// Package bootstrap — композиционный корень search-service.
//
// Это НЕ internal/app: там сценарии (Indexer, SearchService), которые
// импортируют только domain и ports (см. docs/STYLE.md). Здесь — ровно
// наоборот: как сценарии связываются с транспортом (gRPC, Kafka-консьюмеры)
// и инфраструктурой (health) в работающий процесс.
//
// App принимает SearchIndex и IndexWriter уже готовыми портами, а Consumer/
// RetryConsumer — узкими интерфейсами поверх *kafkax.Consumer/RetryConsumer:
// es.New и kafkax.NewConsumer открывают сетевые соединения СРАЗУ (см. их
// комментарии), поэтому единственный способ собрать App без живых
// Elasticsearch и Kafka — принять то, что от них нужно, уже обёрнутым —
// тот же приём, что и Relay в wallet/order, Consumers в analytics.
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/grpc"

	searchv1 "gosplash/gen/go/gosplash/search/v1"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"
	"gosplash/pkg/kafkax"

	searchgrpc "gosplash/services/search/internal/adapters/grpc"
	"gosplash/services/search/internal/app"
	"gosplash/services/search/internal/ports"
)

const serviceName = "search"

const (
	// drainDelay — пауза между NotReady и остановкой серверов: балансировщику
	// нужно успеть узнать о смене готовности прежде, чем соединения начнут рваться.
	drainDelay          = 2 * time.Second
	httpShutdownTimeout = 15 * time.Second
	grpcShutdownTimeout = 15 * time.Second
	consumerJoinTimeout = 15 * time.Second
)

// Consumer — узкий срез *kafkax.Consumer и *kafkax.RetryConsumer: у обоих
// одинаковый набор Run(ctx, Handler)/Close()/Ping(ctx) (см. pkg/kafkax/
// kafka.go и retry.go), поэтому один интерфейс годится для обоих полей Deps.
type Consumer interface {
	Run(ctx context.Context, handle kafkax.Handler) error
	Close()
	Ping(ctx context.Context) error
}

// Deps — уже открытые снаружи ресурсы и адреса, на которых слушать.
type Deps struct {
	SearchIndex ports.SearchIndex
	IndexWriter ports.IndexWriter

	Consumer      Consumer
	RetryConsumer Consumer

	// ElasticsearchPing — health-проверка без прямой зависимости App от
	// *es.Client: то же соображение, что и с Relay в wallet/order — App
	// не открывал этот клиент, ему достаточно функции для /readyz.
	ElasticsearchPing httpx.Check

	JWTSecret      string
	DefaultTimeout time.Duration

	GRPCAddr    string
	HTTPAddr    string
	MetricsAddr string
}

// App — собранный процесс search: gRPC-поиск, консьюмер и retry-консьюмер
// индексации. Ничего не слушает и не ловит сигналы — этим занимается Run.
type App struct {
	deps Deps

	indexer *app.Indexer

	grpcServer *grpc.Server
	grpcHealth *grpcx.Health

	httpServer    *http.Server
	metricsServer *http.Server
	health        *httpx.Health
}

// NewApp связывает сценарии с транспортом. Ошибка — только не заданные
// обязательные зависимости; дальше конструирование в памяти, без сети.
func NewApp(deps Deps) (*App, error) {
	if deps.SearchIndex == nil {
		return nil, fmt.Errorf("bootstrap: search: SearchIndex обязателен")
	}
	if deps.IndexWriter == nil {
		return nil, fmt.Errorf("bootstrap: search: IndexWriter обязателен")
	}
	if deps.Consumer == nil {
		return nil, fmt.Errorf("bootstrap: search: Consumer обязателен")
	}
	if deps.RetryConsumer == nil {
		return nil, fmt.Errorf("bootstrap: search: RetryConsumer обязателен")
	}

	indexer := app.NewIndexer(deps.IndexWriter)
	searchService := app.NewSearchService(deps.SearchIndex)

	searchServer := searchgrpc.NewServer(searchService)
	grpcHealth := grpcx.NewHealth()

	grpcServer := grpcx.NewServer(grpcx.ServerParams{
		ServiceName: serviceName,
		JWTSecret:   []byte(deps.JWTSecret),
		// Поиск — публичная витрина, как чтение каталога: браузер и
		// grpcurl обязаны достучаться без токена.
		PublicMethods: []string{
			searchv1.SearchService_Search_FullMethodName,
		},
		DefaultTimeout: deps.DefaultTimeout,
	})
	searchv1.RegisterSearchServiceServer(grpcServer, searchServer)
	grpcHealth.Register(grpcServer)

	health := httpx.NewHealth()
	if deps.ElasticsearchPing != nil {
		health.Register("elasticsearch", deps.ElasticsearchPing)
	}
	health.Register("kafka_consumer", deps.Consumer.Ping)

	router := http.NewServeMux()
	health.Handle(router, serviceName)

	httpServer := httpx.NewServer(
		httpx.DefaultServerConfig(deps.HTTPAddr),
		httpx.Chain(router, httpx.Default(serviceName)...),
	)

	return &App{
		deps:       deps,
		indexer:    indexer,
		grpcServer: grpcServer,
		grpcHealth: grpcHealth,
		httpServer: httpServer,
		health:     health,
	}, nil
}

// Run поднимает серверы, консьюмер и retry-консьюмер, блокируется до
// отмены ctx или до первого фатального сбоя gRPC-сервера, затем проводит
// graceful shutdown.
//
// Порядок остановки выверен на живом кластере и переносится без изменений
// из прежнего main.go: снять readiness → пауза → остановить серверы →
// дождаться консьюмера (retry-консьюмеру отдельного ожидания не было
// и в прежнем main.go — см. его комментарий про BulkIndexer.Close ниже).
func (a *App) Run(ctx context.Context) error {
	a.metricsServer = httpx.ServeMetricsAndPprof(a.deps.MetricsAddr)
	go httpx.Serve(a.httpServer, "public")

	grpcErr := make(chan error, 1)
	go func() {
		grpcErr <- grpcx.Serve(a.grpcServer, a.deps.GRPCAddr, serviceName)
	}()

	consumerDone := make(chan error, 1)
	go func() { consumerDone <- a.deps.Consumer.Run(ctx, a.indexer.HandleListingPublished) }()

	retryDone := make(chan error, 1)
	go func() { retryDone <- a.deps.RetryConsumer.Run(ctx, a.indexer.HandleListingPublished) }()

	var runErr error
	select {
	case <-ctx.Done():
		slog.Info("search: останавливаюсь…")
	case err := <-grpcErr:
		if err != nil {
			runErr = fmt.Errorf("search: gRPC остановлен: %w", err)
		}
	}

	a.health.NotReady()
	a.grpcHealth.NotServing()
	time.Sleep(drainDelay)

	httpx.Shutdown(ctx, httpShutdownTimeout, a.httpServer, a.metricsServer)
	grpcx.Shutdown(a.grpcServer, grpcShutdownTimeout)

	// Консьюмеру даём доработать текущее сообщение — тот же приём, что и
	// в catalog/cmd/catalog/main.go. Оба консьюмера ждём одинаково: у
	// retry-консьюмера тот же контракт Run/AllowRebalance, что и у основного
	// (pkg/kafkax), и не ждать его было бы тем же классом бага, из-за
	// которого поды зависали на 80 секунд (см. docs задания).
	deadline := time.After(consumerJoinTimeout)
	for _, done := range []<-chan error{consumerDone, retryDone} {
		select {
		case err := <-done:
			if err != nil && runErr == nil {
				runErr = fmt.Errorf("search: консьюмер остановлен: %w", err)
			}
		case <-deadline:
			slog.Warn("search: консьюмер не остановился вовремя")
		}
	}

	if runErr == nil {
		slog.Info("search: остановлен")
	}
	return runErr
}

// Close ничего не закрывает: серверы уже остановлены внутри Run, а
// Elasticsearch-клиент, BulkIndexer и консьюмеры App не открывал — их
// закрывает run() в main.go, которому они принадлежат. Сигнатура
// согласована с остальными сервисами монорепы (см.
// wallet/internal/bootstrap.App.Close).
func (a *App) Close() error {
	return nil
}

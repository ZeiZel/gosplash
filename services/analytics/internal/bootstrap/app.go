// Package bootstrap — композиционный корень analytics-service.
//
// Это НЕ internal/app: там сценарии (StatsService, Ingest), которые
// импортируют только domain и ports (см. docs/STYLE.md). Здесь — ровно
// наоборот: как сценарии связываются с транспортом (gRPC, Kafka-консьюмеры)
// и инфраструктурой (health) в работающий процесс.
//
// App принимает StatsReader и Consumers уже готовыми, а не строит их из
// *clickhouse.Client сама: clickhouse.New открывает native-соединение
// СРАЗУ (см. её комментарий в internal/adapters/clickhouse/client.go),
// поэтому единственный способ собрать App без живого ClickHouse — принять
// уже обёрнутые в порты/интерфейс реализации, как и в остальных сервисах
// монорепы (wallet/order — Relay, тот же приём).
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	analyticsv1 "gosplash/gen/go/gosplash/analytics/v1"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"

	analyticsgrpc "gosplash/services/analytics/internal/adapters/grpc"
	"gosplash/services/analytics/internal/app"
	"gosplash/services/analytics/internal/ports"
)

const serviceName = "analytics"

const (
	// drainDelay — пауза между NotReady и остановкой серверов: балансировщику
	// нужно успеть узнать о смене готовности прежде, чем соединения начнут рваться.
	drainDelay           = 2 * time.Second
	httpShutdownTimeout  = 15 * time.Second
	grpcShutdownTimeout  = 15 * time.Second
	consumersJoinTimeout = 15 * time.Second
)

// Consumers — узкий срез *kafka.Consumers (адаптер analytics к Kafka):
// Run*/Ping* на каждый из четырёх топиков и Close. Интерфейс — чтобы
// тест App мог подставить fake-консьюмеры без живой Kafka.
type Consumers interface {
	RunViews(ctx context.Context) error
	RunPurchases(ctx context.Context) error
	RunUploads(ctx context.Context) error
	RunThumbnails(ctx context.Context) error
	PingViews(ctx context.Context) error
	PingPurchases(ctx context.Context) error
	PingUploads(ctx context.Context) error
	PingThumbnails(ctx context.Context) error
	Close()
}

// Deps — уже открытые снаружи ресурсы и адреса, на которых слушать.
// StatsReader — ровно ports.StatsReader, которого просит app.NewStatsService:
// его реализация (*clickhouse.Repository) собирается в run() из уже
// открытого *clickhouse.Client, а сюда приходит готовым портом.
type Deps struct {
	StatsReader ports.StatsReader
	Consumers   Consumers

	// ClickHousePing — health-проверка без прямой зависимости App от
	// *clickhouse.Client: то же соображение, что и с Relay в wallet/order,
	// перенесённое на функцию, а не интерфейс, потому что Ping — единственное,
	// что App нужно от клиента помимо StatsReader/Consumers.
	ClickHousePing httpx.Check

	JWTSecret      string
	DefaultTimeout time.Duration

	// GRPCReflectionEnabled — та же пара строк, что в bootstrap media:
	// grpcurl-демо (make demo-funnel, grpc-reflect) без reflection требует
	// .proto под руками, локально это никому не нужно.
	GRPCReflectionEnabled bool

	GRPCAddr    string
	HTTPAddr    string
	MetricsAddr string
}

// App — собранный процесс analytics: gRPC-сервер отчётов и четыре
// независимых Kafka-консьюмера (просмотры, покупки, загрузки, превью).
// Ничего не слушает и не ловит сигналы — этим занимается Run.
type App struct {
	deps Deps

	grpcServer *grpc.Server
	grpcHealth *grpcx.Health

	httpServer    *http.Server
	metricsServer *http.Server
	health        *httpx.Health
}

// NewApp связывает сценарии с транспортом. Ошибка — только не заданные
// обязательные зависимости; дальше конструирование в памяти, без сети.
func NewApp(deps Deps) (*App, error) {
	if deps.StatsReader == nil {
		return nil, fmt.Errorf("bootstrap: analytics: StatsReader обязателен")
	}
	if deps.Consumers == nil {
		return nil, fmt.Errorf("bootstrap: analytics: Consumers обязателен")
	}

	statsService := app.NewStatsService(deps.StatsReader)
	analyticsServer := analyticsgrpc.NewServer(statsService)
	grpcHealth := grpcx.NewHealth()

	grpcServer := grpcx.NewServer(grpcx.ServerParams{
		ServiceName: serviceName,
		JWTSecret:   []byte(deps.JWTSecret),
		// Все методы читают только СВОДКИ — публичная витрина отчётов,
		// а не персональные данные пользователя, поэтому она, как и чтение
		// каталога в catalog-service, доступна без токена.
		PublicMethods: []string{
			analyticsv1.AnalyticsService_TopPhotos_FullMethodName,
			analyticsv1.AnalyticsService_PhotoStats_FullMethodName,
			analyticsv1.AnalyticsService_FunnelStats_FullMethodName,
			analyticsv1.AnalyticsService_AuthorStats_FullMethodName,
		},
		DefaultTimeout: deps.DefaultTimeout,
	})
	analyticsv1.RegisterAnalyticsServiceServer(grpcServer, analyticsServer)
	grpcHealth.Register(grpcServer)
	// Reflection — только по явному флагу: тот же комментарий, что у media
	// (services/media/internal/bootstrap/app.go), причина та же — она
	// отдаёт контракт целиком без аутентификации.
	if deps.GRPCReflectionEnabled {
		reflection.Register(grpcServer)
	}

	health := httpx.NewHealth()
	if deps.ClickHousePing != nil {
		health.Register("clickhouse", deps.ClickHousePing)
	}
	health.Register("kafka_views", deps.Consumers.PingViews)
	health.Register("kafka_purchases", deps.Consumers.PingPurchases)
	health.Register("kafka_uploads", deps.Consumers.PingUploads)
	health.Register("kafka_thumbnails", deps.Consumers.PingThumbnails)

	// Публичного HTTP API у сервиса нет (наружу — только gRPC), поэтому на
	// публичном порту — только health-хендлеры; /metrics и /debug/pprof —
	// на служебном порту ниже, в отдельном DefaultServeMux.
	router := http.NewServeMux()
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

// Run поднимает серверы и все консьюмеры, блокируется до отмены ctx или до
// первого фатального сбоя gRPC-сервера, затем проводит graceful shutdown.
//
// Порядок остановки выверен на живом кластере и переносится без изменений
// из прежнего main.go: снять readiness → пауза → остановить серверы →
// дождаться ВСЕХ консьюмеров.
func (a *App) Run(ctx context.Context) error {
	a.metricsServer = httpx.ServeMetricsAndPprof(a.deps.MetricsAddr)
	go httpx.Serve(a.httpServer, "public")

	grpcErr := make(chan error, 1)
	go func() {
		grpcErr <- grpcx.Serve(a.grpcServer, a.deps.GRPCAddr, serviceName)
	}()

	viewsDone := make(chan error, 1)
	go func() { viewsDone <- a.deps.Consumers.RunViews(ctx) }()

	purchasesDone := make(chan error, 1)
	go func() { purchasesDone <- a.deps.Consumers.RunPurchases(ctx) }()

	uploadsDone := make(chan error, 1)
	go func() { uploadsDone <- a.deps.Consumers.RunUploads(ctx) }()

	thumbnailsDone := make(chan error, 1)
	go func() { thumbnailsDone <- a.deps.Consumers.RunThumbnails(ctx) }()

	var runErr error
	select {
	case <-ctx.Done():
		slog.Info("analytics: останавливаюсь…")
	case err := <-grpcErr:
		if err != nil {
			runErr = fmt.Errorf("analytics: gRPC остановлен: %w", err)
		}
	}

	a.health.NotReady()
	a.grpcHealth.NotServing()
	time.Sleep(drainDelay)

	httpx.Shutdown(ctx, httpShutdownTimeout, a.httpServer, a.metricsServer)
	grpcx.Shutdown(a.grpcServer, grpcShutdownTimeout)

	// Дожидаемся ВСЕХ консьюмеров ПЕРЕД тем, как run() в main.go закроет их
	// (Consumers.Close вызывает kafkax.Consumer.Close, который требует, чтобы
	// Run уже вернул управление — см. docs задания про AllowRebalance и
	// pkg/kafkax/shutdown_test.go).
	deadline := time.After(consumersJoinTimeout)
	for _, done := range []<-chan error{viewsDone, purchasesDone, uploadsDone, thumbnailsDone} {
		select {
		case err := <-done:
			if err != nil && runErr == nil {
				runErr = fmt.Errorf("analytics: консьюмер остановлен: %w", err)
			}
		case <-deadline:
			slog.Warn("analytics: консьюмер не остановился вовремя")
		}
	}

	if runErr == nil {
		slog.Info("analytics: остановлен")
	}
	return runErr
}

// Close ничего не закрывает: серверы уже остановлены внутри Run, а
// ClickHouse-клиент и Consumers App не открывал — их закрывает run() в
// main.go, которому они принадлежат. Сигнатура согласована с остальными
// сервисами монорепы (см. wallet/internal/bootstrap.App.Close).
func (a *App) Close() error {
	return nil
}

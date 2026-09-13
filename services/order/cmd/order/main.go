// order-service, HTTP+gRPC API — приём заказов с ключом идемпотентности
// и запуск саги PlaceOrderWorkflow в Temporal. Фаза 3 в docs/PLAN.md.
//
// ПОЧЕМУ ДВА ПРОЦЕССА (этот и cmd/worker), А НЕ ОДИН:
//
//	этот процесс (API)   — принимает HTTP/gRPC-запрос, отвечает клиенту за
//	                        миллисекунды. Его причина масштабироваться —
//	                        количество покупателей, нажимающих «купить»
//	                        одновременно. Его профиль отказа — недоступность
//	                        Redis/PostgreSQL/catalog В МОМЕНТ ПРИЁМА заказа;
//	                        если он падает ПОСЛЕ того, как воркфлоу запущен,
//	                        сага НИЧЕГО не замечает и продолжается — она
//	                        живёт в Temporal, а не в памяти этого процесса.
//	cmd/worker (Temporal
//	worker)              — исполняет шаги саги МИНУТАМИ, а не миллисекундами
//	                        (ретраи с backoff, ожидание ответа от wallet
//	                        и catalog). Его причина масштабироваться —
//	                        количество ОДНОВРЕМЕННО ИСПОЛНЯЮЩИХСЯ САГ, а не
//	                        количество входящих HTTP-запросов: соотношение
//	                        между ними произвольное (одна купленная лицензия
//	                        — один запрос, но несколько минут работы саги
//	                        при недоступном wallet). Его профиль отказа —
//	                        падение ПОСРЕДИ саги: Temporal просто передаёт
//	                        задачу другому воркеру и продолжает с того шага,
//	                        на котором остановились (см. package doc
//	                        internal/adapters/temporal и docs/adr/0004).
//
// Слить их в один процесс означало бы либо держать HTTP-обработчики
// заблокированными на время выполнения саги (недопустимо — покупатель ждёт
// секунды, а не минуты), либо изобретать внутри одного процесса ту же
// границу асинхронности, которую Temporal уже проводит между клиентом
// и воркером, — то есть тот же самый рефакторинг, но без пользы от
// готовой инфраструктуры.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	sdkclient "go.temporal.io/sdk/client"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
	pkgconfig "gosplash/pkg/config"
	"gosplash/pkg/dbx"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"
	"gosplash/pkg/otelx"
	"gosplash/pkg/outbox"
	"gosplash/pkg/redisx"
	"gosplash/pkg/resilience"

	orderv1 "gosplash/gen/go/gosplash/order/v1"
	ordergrpc "gosplash/services/order/internal/adapters/grpc"
	orderhttp "gosplash/services/order/internal/adapters/http"
	"gosplash/services/order/internal/adapters/idempotency"
	"gosplash/services/order/internal/adapters/pg"
	ordertemporal "gosplash/services/order/internal/adapters/temporal"
	"gosplash/services/order/internal/app"
)

const serviceName = "order"

func main() {
	// Общее (Kafka, Redis, Observe, GRPC, Outbox) — из pkg/config, как
	// требует docs/STYLE.md. Своё (ORDER_*, TEMPORAL_*, ...) — из
	conf := pkgconfig.Load()
	orderConf := conf.Order

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownOtel, err := otelx.Setup(ctx, otelx.Config{
		ServiceName:      serviceName,
		Version:          "0.1.0",
		Environment:      conf.Observe.Environment,
		OTLPEndpoint:     conf.Observe.OTLPEndpoint,
		TraceSampleRatio: conf.Observe.SampleRatio,
		LogLevel:         conf.Observe.LogLevel,
	})
	if err != nil {
		slog.Error("order: наблюдаемость", "error", err)
		os.Exit(1)
	}

	if conf.GRPC.JWTSecret == "" {
		slog.Warn("order: JWT_SECRET пуст — gRPC-аутентификация выключена, это нормально только для локальной разработки")
	}

	// ── База ─────────────────────────────────────────────────────────────
	database, err := dbx.Open(orderConf.DSN)
	if err != nil {
		slog.Error("order: postgres", "error", err)
		os.Exit(1)
	}

	// ── Redis: быстрый путь идемпотентности (см. ADR 0018) ──────────────────
	redisClient, err := redisx.New(redisx.Config{
		Addr:        conf.Redis.Addr,
		Password:    conf.Redis.Password,
		DB:          conf.Redis.DB,
		PoolSize:    conf.Redis.PoolSize,
		MaxRetries:  conf.Redis.MaxRetries,
		DialTimeout: conf.Redis.DialTimeout,
		ReadTimeout: conf.Redis.ReadTimeout,
		ServiceName: serviceName,
	})
	if err != nil {
		slog.Error("order: redis", "error", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	idemStore := idempotency.New(redisClient, database, orderConf.IdempotencyTTL)

	// ── gRPC-клиент к catalog: ТОЛЬКО чтение (GetListing) ───────────────────
	// resilience.Idempotent — безопасно ретраить на транспорте (задание,
	// блок 5; см. также internal/adapters/grpc/catalog_reader.go).
	catalogConn, err := grpcx.Dial(orderConf.CatalogGRPCTarget,
		grpcx.WithRetry(resilience.DefaultRetryConfig(), resilience.Idempotent),
		grpcx.WithBreaker(resilience.DefaultBreakerConfig("catalog")),
	)
	if err != nil {
		slog.Error("order: gRPC к catalog", "error", err)
		os.Exit(1)
	}
	defer catalogConn.Close()
	listingReader := ordergrpc.NewCatalogReader(catalogv1.NewCatalogServiceClient(catalogConn))

	// ── Temporal: клиент запускает сагу, не исполняет её (см. package doc) ──
	temporalClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  orderConf.TemporalAddress,
		Namespace: orderConf.TemporalNamespace,
	})
	if err != nil {
		slog.Error("order: подключение к temporal", "error", err)
		os.Exit(1)
	}
	defer temporalClient.Close()
	starter := ordertemporal.NewStarter(temporalClient, orderConf.TemporalTaskQueue)

	// ── Outbox-relay: order.order.placed / order.order.paid → Kafka ─────────
	// ConfirmOrder (шаг саги, исполняется В cmd/worker) пишет
	// order.order.paid в ТУ ЖЕ таблицу outbox_order той же базы ORDER_DSN —
	// relay здесь вычитывает обе записи независимо от того, какой процесс
	// их создал (см. pkg/outbox: relay — просто доставщик чужих строк).
	relay, err := outbox.New(outbox.Config{
		ServiceName:  serviceName,
		Brokers:      conf.Kafka.Brokers,
		DSNs:         []string{orderConf.DSN},
		PollInterval: conf.Outbox.PollInterval,
		BatchSize:    conf.Outbox.BatchSize,
	})
	if err != nil {
		slog.Error("order: outbox relay", "error", err)
		os.Exit(1)
	}
	defer relay.Close()

	// ── Слои ─────────────────────────────────────────────────────────────
	orderRepo := pg.NewOrderRepository(database)
	orderService := app.NewOrderService(orderRepo, idemStore, listingReader, starter)

	// ── gRPC-сервер ──────────────────────────────────────────────────────
	orderServer := ordergrpc.NewServer(orderService)
	grpcHealth := grpcx.NewHealth()

	grpcServer := grpcx.NewServer(grpcx.ServerParams{
		ServiceName:    serviceName,
		JWTSecret:      []byte(conf.GRPC.JWTSecret),
		DefaultTimeout: conf.GRPC.DefaultTimeout,
	})
	orderv1.RegisterOrderServiceServer(grpcServer, orderServer)
	grpcHealth.Register(grpcServer)

	// ── Готовность ───────────────────────────────────────────────────────
	health := httpx.NewHealth()
	health.Register("postgres", func(ctx context.Context) error { return dbx.Ping(ctx, database) })
	health.Register("redis", redisClient.Ping)
	health.Register("outbox", relay.Ping)

	// ── HTTP ─────────────────────────────────────────────────────────────
	router := http.NewServeMux()
	orderhttp.Register(router, orderServer)
	health.Handle(router, serviceName)

	httpServer := httpx.NewServer(
		httpx.DefaultServerConfig(orderConf.HTTPAddr),
		httpx.Chain(router, httpx.Default(serviceName)...),
	)

	// ── Запуск ───────────────────────────────────────────────────────────
	metricsServer := httpx.ServeMetricsAndPprof(orderConf.MetricsAddr)
	go httpx.Serve(httpServer, "public")

	grpcDone := make(chan struct{})
	go func() {
		defer close(grpcDone)
		if err := grpcx.Serve(grpcServer, orderConf.GRPCAddr, serviceName); err != nil {
			slog.Error("order: gRPC остановлен", "error", err)
		}
	}()

	go func() {
		if err := relay.Run(ctx); err != nil {
			slog.Error("order: outbox relay остановлен", "error", err)
		}
	}()

	// ── Остановка ────────────────────────────────────────────────────────
	<-ctx.Done()
	slog.Info("order: останавливаюсь…")

	health.NotReady()
	grpcHealth.NotServing()
	time.Sleep(2 * time.Second)

	httpx.Shutdown(ctx, 15*time.Second, httpServer, metricsServer)
	grpcx.Shutdown(grpcServer, 15*time.Second)

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := shutdownOtel(shutdownCtx); err != nil {
		slog.Error("order: otel shutdown", "error", err)
	}
	slog.Info("order: остановлен")
}

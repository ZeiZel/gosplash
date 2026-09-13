// order-service, Temporal worker — исполняет PlaceOrderWorkflow и её
// activities. Фаза 3 в docs/PLAN.md.
//
// Почему это отдельный процесс от cmd/order — см. package doc там же.
// Коротко: у него другая единица масштабирования (число одновременных
// саг, а не входящих HTTP-запросов) и другой профиль отказа (падение
// посреди activity — Temporal переигрывает шаг на любом воркере, который
// поднимется следующим, а не теряет прогресс).
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
	walletv1 "gosplash/gen/go/gosplash/wallet/v1"
	pkgconfig "gosplash/pkg/config"
	"gosplash/pkg/dbx"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/otelx"
	"gosplash/pkg/resilience"

	ordergrpc "gosplash/services/order/internal/adapters/grpc"
	"gosplash/services/order/internal/adapters/pg"
	ordertemporal "gosplash/services/order/internal/adapters/temporal"
)

const serviceName = "order-worker"

func main() {
	// Общее (Observe) — из pkg/config, как в cmd/order. Kafka/Redis/GRPC/
	// Outbox воркеру не нужны: он не публикует в Kafka напрямую, ничего не
	// кэширует в Redis и не поднимает свой gRPC-сервер — outbox relay
	// и gRPC API живут в cmd/order.
	conf := pkgconfig.Load()
	orderConf := conf.Order

	ctx := context.Background()
	shutdownOtel, err := otelx.Setup(ctx, otelx.Config{
		ServiceName:      serviceName,
		Version:          "0.1.0",
		Environment:      conf.Observe.Environment,
		OTLPEndpoint:     conf.Observe.OTLPEndpoint,
		TraceSampleRatio: conf.Observe.SampleRatio,
		LogLevel:         conf.Observe.LogLevel,
	})
	if err != nil {
		slog.Error("order-worker: наблюдаемость", "error", err)
		os.Exit(1)
	}

	// ── База: activities читают/пишут ту же таблицу orders, что и API ───────
	database, err := dbx.Open(orderConf.DSN)
	if err != nil {
		slog.Error("order-worker: postgres", "error", err)
		os.Exit(1)
	}

	// ── gRPC-клиенты к wallet и catalog: ТОЛЬКО мутирующие методы саги ──────
	// resilience.NotIdempotent — их безопасность обеспечивает
	// idempotency_key = order_id на стороне wallet/catalog и RetryPolicy
	// activity (см. internal/adapters/temporal/workflow.go), а не слепой
	// ретрай транспорта (см. internal/adapters/grpc/wallet_saga.go
	// и catalog_saga.go — там разбор подробно).
	walletConn, err := grpcx.Dial(orderConf.WalletGRPCTarget,
		grpcx.WithRetry(resilience.DefaultRetryConfig(), resilience.NotIdempotent),
		grpcx.WithBreaker(resilience.DefaultBreakerConfig("wallet")),
	)
	if err != nil {
		slog.Error("order-worker: gRPC к wallet", "error", err)
		os.Exit(1)
	}
	defer walletConn.Close()

	catalogConn, err := grpcx.Dial(orderConf.CatalogGRPCTarget,
		grpcx.WithRetry(resilience.DefaultRetryConfig(), resilience.NotIdempotent),
		grpcx.WithBreaker(resilience.DefaultBreakerConfig("catalog")),
	)
	if err != nil {
		slog.Error("order-worker: gRPC к catalog", "error", err)
		os.Exit(1)
	}
	defer catalogConn.Close()

	walletSaga := ordergrpc.NewWalletSagaClient(walletv1.NewWalletServiceClient(walletConn))
	catalogSaga := ordergrpc.NewCatalogSagaClient(catalogv1.NewCatalogServiceClient(catalogConn))
	orderRepo := pg.NewOrderRepository(database)

	activities := ordertemporal.NewActivities(walletSaga, catalogSaga, orderRepo)

	// ── Temporal worker ──────────────────────────────────────────────────
	temporalClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  orderConf.TemporalAddress,
		Namespace: orderConf.TemporalNamespace,
	})
	if err != nil {
		slog.Error("order-worker: подключение к temporal", "error", err)
		os.Exit(1)
	}
	defer temporalClient.Close()

	w := sdkworker.New(temporalClient, orderConf.TemporalTaskQueue, sdkworker.Options{})
	w.RegisterWorkflow(ordertemporal.PlaceOrderWorkflow)
	// Регистрация СТРУКТУРЫ целиком: Temporal SDK сам находит каждый
	// экспортированный метод *Activities и регистрирует его как activity
	// по имени метода (ReserveFunds, GrantLicense, ...) — см. package doc
	// internal/adapters/temporal/activities.go.
	w.RegisterActivity(activities)

	slog.Info("order-worker: запущен", "task_queue", orderConf.TemporalTaskQueue, "namespace", orderConf.TemporalNamespace)

	// w.Run блокирует до SIGINT/SIGTERM (InterruptCh сам на них подписан)
	// и разбирается с graceful stop сам — в отличие от grpcx/httpx серверов
	// в cmd/order, здесь нет своего signal.NotifyContext: это API самого
	// Temporal SDK, а не этого проекта.
	runErr := w.Run(sdkworker.InterruptCh())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdownOtel(shutdownCtx); err != nil {
		slog.Error("order-worker: otel shutdown", "error", err)
	}

	if runErr != nil {
		slog.Error("order-worker: остановлен с ошибкой", "error", runErr)
		os.Exit(1)
	}
	slog.Info("order-worker: остановлен")
}

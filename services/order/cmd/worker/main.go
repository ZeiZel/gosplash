// order-service, Temporal worker — исполняет PlaceOrderWorkflow и её
// activities. Фаза 3 в docs/PLAN.md.
//
// Почему это отдельный процесс от cmd/order, и почему это отдельный тип
// App (bootstrap.WorkerApp, а не bootstrap.App с флагом) — см. package doc
// cmd/order/main.go и internal/bootstrap/app.go. Коротко: у этого процесса
// другая единица масштабирования (число одновременных саг, а не входящих
// HTTP-запросов) и другой профиль отказа (падение посреди activity —
// Temporal переигрывает шаг на любом воркере, который поднимется
// следующим, а не теряет прогресс).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	sdkclient "go.temporal.io/sdk/client"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
	walletv1 "gosplash/gen/go/gosplash/wallet/v1"
	pkgconfig "gosplash/pkg/config"
	"gosplash/pkg/dbx"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/otelx"
	"gosplash/pkg/resilience"

	ordergrpc "gosplash/services/order/internal/adapters/grpc"
	"gosplash/services/order/internal/adapters/pg"
	"gosplash/services/order/internal/bootstrap"
)

const serviceName = "order-worker"

// otelShutdownTimeout — сколько ждём выгрузки последних трейсов/метрик
// после того, как воркер уже остановлен.
const otelShutdownTimeout = 5 * time.Second

// run — жизненный цикл процесса воркера: конфиг, соединения, запуск
// WorkerApp, ожидание сигнала, graceful shutdown. Возвращает ошибку вместо
// os.Exit — по тем же причинам, что и в cmd/order/main.go (gocritic:
// exitAfterDefer): defer здесь закрывают DB и gRPC-соединения к wallet
// и catalog, и обязаны отработать независимо от того, что вернул Run.
//
// ctx ловит SIGINT/SIGTERM через signal.NotifyContext — так же, как во
// всех остальных процессах монорепы, а НЕ через sdkworker.InterruptCh()
// (API самого Temporal SDK), которым пользовался прежний main.go: два
// разных подписчика на одни и те же сигналы в одном процессе не нужны.
// bootstrap.WorkerApp.Run переводит эту отмену в Worker.Run()/Stop() —
// см. её комментарий.
func run() error {
	// Общее (Observe) — из pkg/config, как в cmd/order. Kafka/Redis/GRPC/
	// Outbox воркеру не нужны: он не публикует в Kafka напрямую, ничего не
	// кэширует в Redis и не поднимает свой gRPC-сервер — outbox relay
	// и gRPC API живут в cmd/order.
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
		return fmt.Errorf("order-worker: наблюдаемость: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), otelShutdownTimeout)
		defer cancel()
		if err := shutdownOtel(shutdownCtx); err != nil {
			slog.Error("order-worker: otel shutdown", "error", err)
		}
	}()

	// ── База: activities читают/пишут ту же таблицу orders, что и API ───────
	database, err := dbx.Open(orderConf.DSN)
	if err != nil {
		return fmt.Errorf("order-worker: postgres: %w", err)
	}
	defer func() {
		if sqlDB, err := database.DB(); err == nil {
			if err := sqlDB.Close(); err != nil {
				slog.Error("order-worker: не удалось закрыть пул соединений", "error", err)
			}
		}
	}()

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
		return fmt.Errorf("order-worker: gRPC к wallet: %w", err)
	}
	defer func() {
		if err := walletConn.Close(); err != nil {
			slog.Error("order-worker: закрытие соединения с wallet", "error", err)
		}
	}()

	catalogConn, err := grpcx.Dial(orderConf.CatalogGRPCTarget,
		grpcx.WithRetry(resilience.DefaultRetryConfig(), resilience.NotIdempotent),
		grpcx.WithBreaker(resilience.DefaultBreakerConfig("catalog")),
	)
	if err != nil {
		return fmt.Errorf("order-worker: gRPC к catalog: %w", err)
	}
	defer func() {
		if err := catalogConn.Close(); err != nil {
			slog.Error("order-worker: закрытие соединения с catalog", "error", err)
		}
	}()

	walletSaga := ordergrpc.NewWalletSagaClient(walletv1.NewWalletServiceClient(walletConn))
	catalogSaga := ordergrpc.NewCatalogSagaClient(catalogv1.NewCatalogServiceClient(catalogConn))
	orderRepo := pg.NewOrderRepository(database)

	// ── Temporal worker ──────────────────────────────────────────────────
	temporalClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  orderConf.TemporalAddress,
		Namespace: orderConf.TemporalNamespace,
	})
	if err != nil {
		return fmt.Errorf("order-worker: подключение к temporal: %w", err)
	}
	defer temporalClient.Close()

	application, err := bootstrap.NewWorkerApp(bootstrap.WorkerDeps{
		WalletSaga:     walletSaga,
		CatalogSaga:    catalogSaga,
		OrderRepo:      orderRepo,
		TemporalClient: temporalClient,
		TaskQueue:      orderConf.TemporalTaskQueue,
	})
	if err != nil {
		return fmt.Errorf("order-worker: сборка приложения: %w", err)
	}
	defer func() {
		if err := application.Close(); err != nil {
			slog.Error("order-worker: закрытие приложения", "error", err)
		}
	}()

	slog.Info("order-worker: запущен", "task_queue", orderConf.TemporalTaskQueue, "namespace", orderConf.TemporalNamespace)

	return application.Run(ctx)
}

// main — три строки: вызвать run(), при ошибке залогировать и os.Exit(1).
// os.Exit здесь безопасен: run() к этому моменту уже вернула управление,
// и все её defer (закрытие DB, gRPC-соединений, temporal-клиента, otel)
// успели отработать до этой строки, а не обрываются вызовом os.Exit.
func main() {
	if err := run(); err != nil {
		slog.Error("order-worker: остановлен с ошибкой", "error", err.Error())
		os.Exit(1)
	}
}

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
// готовой инфраструктуры. По той же причине это ДВА РАЗНЫХ типа App
// (bootstrap.App здесь, bootstrap.WorkerApp в cmd/worker) — см. package doc
// internal/bootstrap/app.go.
//
// Файл разделён на bootstrap.App (чистая сборка зависимостей), run()
// (конфиг/соединения/сигналы/graceful shutdown) и main() — см. STYLE.md
// и internal/bootstrap про причину.
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
	pkgconfig "gosplash/pkg/config"
	"gosplash/pkg/dbx"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/otelx"
	"gosplash/pkg/outbox"
	"gosplash/pkg/redisx"
	"gosplash/pkg/resilience"

	ordergrpc "gosplash/services/order/internal/adapters/grpc"
	"gosplash/services/order/internal/adapters/idempotency"
	"gosplash/services/order/internal/adapters/pg"
	ordertemporal "gosplash/services/order/internal/adapters/temporal"
	"gosplash/services/order/internal/bootstrap"
)

const serviceName = "order"

// otelShutdownTimeout — сколько ждём выгрузки последних трейсов/метрик
// после того, как серверы уже остановлены.
const otelShutdownTimeout = 5 * time.Second

// run — жизненный цикл процесса: конфиг, соединения, запуск App, ожидание
// сигнала, graceful shutdown. Возвращает ошибку вместо os.Exit — все defer
// (закрытие DB/Redis/catalog-conn/temporal-клиента/relay, выгрузка otel)
// успевают отработать независимо от того, на каком шаге всё пошло не так.
// Раньше os.Exit(1) внутри main() обрывал их (gocritic: exitAfterDefer),
// и соединения оставались висеть до убийства процесса супервизором.
func run() error {
	// Общее (Kafka, Redis, Observe, GRPC, Outbox) — из pkg/config, как
	// требует docs/STYLE.md. Своё (ORDER_*, TEMPORAL_*, ...) — из него же.
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
		return fmt.Errorf("order: наблюдаемость: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), otelShutdownTimeout)
		defer cancel()
		if err := shutdownOtel(shutdownCtx); err != nil {
			slog.Error("order: otel shutdown", "error", err)
		}
	}()

	if conf.GRPC.JWTSecret == "" {
		slog.Warn("order: JWT_SECRET пуст — gRPC-аутентификация выключена, это нормально только для локальной разработки")
	}

	// ── База ─────────────────────────────────────────────────────────────
	database, err := dbx.Open(orderConf.DSN)
	if err != nil {
		return fmt.Errorf("order: postgres: %w", err)
	}
	defer func() {
		if sqlDB, err := database.DB(); err == nil {
			if err := sqlDB.Close(); err != nil {
				slog.Error("order: не удалось закрыть пул соединений", "error", err)
			}
		}
	}()

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
		return fmt.Errorf("order: redis: %w", err)
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			slog.Error("order: закрытие redis", "error", err)
		}
	}()

	idemStore := idempotency.New(redisClient, database, orderConf.IdempotencyTTL)

	// ── gRPC-клиент к catalog: ТОЛЬКО чтение (GetListing) ───────────────────
	// resilience.Idempotent — безопасно ретраить на транспорте (задание,
	// блок 5; см. также internal/adapters/grpc/catalog_reader.go).
	catalogConn, err := grpcx.Dial(orderConf.CatalogGRPCTarget,
		grpcx.WithRetry(resilience.DefaultRetryConfig(), resilience.Idempotent),
		grpcx.WithBreaker(resilience.DefaultBreakerConfig("catalog")),
	)
	if err != nil {
		return fmt.Errorf("order: gRPC к catalog: %w", err)
	}
	defer func() {
		if err := catalogConn.Close(); err != nil {
			slog.Error("order: закрытие соединения с catalog", "error", err)
		}
	}()
	listingReader := ordergrpc.NewCatalogReader(catalogv1.NewCatalogServiceClient(catalogConn))

	// ── Temporal: клиент запускает сагу, не исполняет её (см. package doc) ──
	temporalClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  orderConf.TemporalAddress,
		Namespace: orderConf.TemporalNamespace,
	})
	if err != nil {
		return fmt.Errorf("order: подключение к temporal: %w", err)
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
		return fmt.Errorf("order: outbox relay: %w", err)
	}
	defer relay.Close()

	// ── Слои: DB/Redis/catalog-conn/Temporal уже открыты — заворачиваем их
	// в порты, которых просит bootstrap.NewApp (см. её DEPS про причину) ────
	orderRepo := pg.NewOrderRepository(database)

	application, err := bootstrap.NewApp(bootstrap.Deps{
		OrderRepo:      orderRepo,
		IdemStore:      idemStore,
		ListingReader:  listingReader,
		Starter:        starter,
		Relay:          relay,
		PostgresPing:   func(ctx context.Context) error { return dbx.Ping(ctx, database) },
		RedisPing:      redisClient.Ping,
		JWTSecret:      conf.GRPC.JWTSecret,
		DefaultTimeout: conf.GRPC.DefaultTimeout,
		GRPCAddr:       orderConf.GRPCAddr,
		HTTPAddr:       orderConf.HTTPAddr,
		MetricsAddr:    orderConf.MetricsAddr,
	})
	if err != nil {
		return fmt.Errorf("order: сборка приложения: %w", err)
	}
	defer func() {
		if err := application.Close(); err != nil {
			slog.Error("order: закрытие приложения", "error", err)
		}
	}()

	// Run блокируется до отмены ctx (сигнал) или фатального сбоя сервера
	// и сама проводит graceful shutdown серверов и relay.
	return application.Run(ctx)
}

// main — три строки: вызвать run(), при ошибке залогировать и os.Exit(1).
// os.Exit здесь безопасен: он самый внешний вызов в процессе, и к этому
// моменту run() уже вернула управление — все её defer (закрытие DB, Redis,
// catalog-conn, Temporal-клиента, relay, otel) успели отработать ДО этой
// строки, а не обрываются им.
func main() {
	if err := run(); err != nil {
		slog.Error("order: приложение остановлено с ошибкой", "error", err.Error())
		os.Exit(1)
	}
}

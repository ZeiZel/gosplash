// analytics-service — агрегаты просмотров и покупок поверх ClickHouse.
// Фаза 4 (docs/PLAN.md).
//
// Несколько независимых занятий в одном процессе:
//
//	gRPC :9106        — AnalyticsService (proto/gosplash/analytics/v1):
//	                    TopPhotos и PhotoStats, читает из ClickHouse.
//	Kafka-консьюмеры  — слушают analytics.photo.viewed и order.order.paid,
//	                    батчами пишут в photo_views и purchases
//	                    (internal/adapters/kafka, internal/adapters/clickhouse).
//	                    Почему консьюмер на Go, а не Kafka engine ClickHouse —
//	                    docs/adr/0016-clickhouse-kafka-engine.md.
//	HTTP :8106        — /healthz, /readyz (ClickHouse ping + Kafka ping).
//	HTTP :8206        — служебный: /metrics и /debug/pprof.
//
// Про cmd/seed (соседний бинарник этого сервиса) — см. его package doc:
// это одноразовая утилита засева демо-данных, а не второй процесс рантайма,
// поэтому она НЕ проходит через internal/bootstrap (там нечего запускать
// и не с чем graceful-стопиться — только один синхронный проход и выход).
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

	"gosplash/pkg/config"
	"gosplash/pkg/otelx"

	analyticsch "gosplash/services/analytics/internal/adapters/clickhouse"
	analyticskafka "gosplash/services/analytics/internal/adapters/kafka"
	"gosplash/services/analytics/internal/app"
	"gosplash/services/analytics/internal/bootstrap"
	"gosplash/services/analytics/migrations"
)

const serviceName = "analytics"

// otelShutdownTimeout — сколько ждём выгрузки последних трейсов/метрик
// после того, как серверы уже остановлены.
const otelShutdownTimeout = 5 * time.Second

// migrateTimeout — миграции ClickHouse выполняются один раз на старте,
// см. migrations/auto.go.
const migrateTimeout = 30 * time.Second

// run — жизненный цикл процесса: конфиг, соединения, запуск App, ожидание
// сигнала, graceful shutdown. Возвращает ошибку вместо os.Exit — все defer
// (закрытие ClickHouse-клиента, консьюмеров, выгрузка otel) успевают
// отработать независимо от того, на каком шаге всё пошло не так. Раньше
// os.Exit(1) внутри main() обрывал их (gocritic: exitAfterDefer).
func run() error {
	// Вся конфигурация — из pkg/config: единственное место в проекте, где
	// вызывается os.Getenv (docs/STYLE.md).
	conf := config.Load()
	local := conf.Analytics

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
		return fmt.Errorf("analytics: наблюдаемость: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), otelShutdownTimeout)
		defer cancel()
		if err := shutdownOtel(shutdownCtx); err != nil {
			slog.Error("analytics: otel shutdown", "error", err)
		}
	}()

	if conf.GRPC.JWTSecret == "" {
		slog.Warn("analytics: JWT_SECRET пуст — gRPC-аутентификация выключена, это нормально только для локальной разработки")
	}

	// ── ClickHouse ─────────────────────────────────────────────────────────
	chClient, err := analyticsch.New(analyticsch.Config{
		Addr:        local.ClickHouse.Addr,
		Database:    local.ClickHouse.Database,
		User:        local.ClickHouse.User,
		Password:    local.ClickHouse.Password,
		DialTimeout: local.ClickHouse.DialTimeout,
		ReadTimeout: local.ClickHouse.ReadTimeout,
	})
	if err != nil {
		return fmt.Errorf("analytics: clickhouse: %w", err)
	}
	defer chClient.Close()

	// Миграции — idempotent-но при каждом старте, см. migrations/auto.go
	// (почему обычным SQL, а не GORM) и services/catalog/migrations/auto.go
	// (тот же приём для DDL, которого нет в словаре GORM).
	migrateCtx, cancelMigrate := context.WithTimeout(ctx, migrateTimeout)
	err = migrations.Migrate(migrateCtx, chClient.Conn)
	cancelMigrate()
	if err != nil {
		return fmt.Errorf("analytics: миграции clickhouse: %w", err)
	}

	// ── Слои: ClickHouse уже открыт — заворачиваем его в порты, которых
	// просит bootstrap.NewApp (см. её Deps про причину) ─────────────────────
	repository := analyticsch.NewRepository(chClient.Conn)
	viewWriter := analyticsch.NewViewWriter(chClient.Conn, local.ViewBatchSize, local.ViewBatchFlushInterval)
	purchaseWriter := analyticsch.NewPurchaseWriter(chClient.Conn, local.PurchaseBatchSize, local.PurchaseBatchFlushInterval)
	ingest := app.NewIngest(viewWriter, purchaseWriter)

	// ── Kafka: два независимых консьюмера ───────────────────────────────────
	consumers, err := analyticskafka.New(conf.Kafka.Brokers, local.KafkaConsumerGroup, ingest)
	if err != nil {
		return fmt.Errorf("analytics: kafka: %w", err)
	}
	defer consumers.Close()

	application, err := bootstrap.NewApp(bootstrap.Deps{
		StatsReader:    repository,
		Consumers:      consumers,
		ClickHousePing: chClient.Ping,
		JWTSecret:      conf.GRPC.JWTSecret,
		DefaultTimeout: conf.GRPC.DefaultTimeout,
		GRPCAddr:       local.GRPCAddr,
		HTTPAddr:       local.HTTPAddr,
		MetricsAddr:    local.MetricsAddr,
	})
	if err != nil {
		return fmt.Errorf("analytics: сборка приложения: %w", err)
	}
	defer func() {
		if err := application.Close(); err != nil {
			slog.Error("analytics: закрытие приложения", "error", err)
		}
	}()

	// Run блокируется до отмены ctx (сигнал) или фатального сбоя сервера
	// и сама проводит graceful shutdown серверов и консьюмеров.
	return application.Run(ctx)
}

// main — три строки: вызвать run(), при ошибке залогировать и os.Exit(1).
// os.Exit здесь безопасен: run() к этому моменту уже вернула управление,
// и все её defer (закрытие ClickHouse, консьюмеров, otel) успели
// отработать до этой строки, а не обрываются вызовом os.Exit.
func main() {
	if err := run(); err != nil {
		slog.Error("analytics: приложение остановлено с ошибкой", "error", err.Error())
		os.Exit(1)
	}
}

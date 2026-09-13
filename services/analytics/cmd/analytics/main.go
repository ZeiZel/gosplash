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
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	analyticsv1 "gosplash/gen/go/gosplash/analytics/v1"
	"gosplash/pkg/config"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"
	"gosplash/pkg/otelx"

	analyticsch "gosplash/services/analytics/internal/adapters/clickhouse"
	analyticsgrpc "gosplash/services/analytics/internal/adapters/grpc"
	analyticskafka "gosplash/services/analytics/internal/adapters/kafka"
	"gosplash/services/analytics/internal/app"
	"gosplash/services/analytics/migrations"
)

const serviceName = "analytics"

func main() {
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
		slog.Error("analytics: наблюдаемость", "error", err)
		os.Exit(1)
	}

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
		slog.Error("analytics: clickhouse", "error", err)
		os.Exit(1)
	}
	defer chClient.Close()

	// Миграции — idempotent-но при каждом старте, см. migrations/auto.go
	// (почему обычным SQL, а не GORM) и services/catalog/migrations/auto.go
	// (тот же приём для DDL, которого нет в словаре GORM).
	migrateCtx, cancelMigrate := context.WithTimeout(ctx, 30*time.Second)
	err = migrations.Migrate(migrateCtx, chClient.Conn)
	cancelMigrate()
	if err != nil {
		slog.Error("analytics: миграции clickhouse", "error", err)
		os.Exit(1)
	}

	// ── Слои ─────────────────────────────────────────────────────────────────
	repository := analyticsch.NewRepository(chClient.Conn)
	viewWriter := analyticsch.NewViewWriter(chClient.Conn, local.ViewBatchSize, local.ViewBatchFlushInterval)
	purchaseWriter := analyticsch.NewPurchaseWriter(chClient.Conn, local.PurchaseBatchSize, local.PurchaseBatchFlushInterval)

	statsService := app.NewStatsService(repository)
	ingest := app.NewIngest(viewWriter, purchaseWriter)

	// ── Kafka: два независимых консьюмера ─────────────────────────────────────
	consumers, err := analyticskafka.New(conf.Kafka.Brokers, local.KafkaConsumerGroup, ingest)
	if err != nil {
		slog.Error("analytics: kafka", "error", err)
		os.Exit(1)
	}
	defer consumers.Close()

	// ── gRPC-сервер ──────────────────────────────────────────────────────────
	analyticsServer := analyticsgrpc.NewServer(statsService)
	grpcHealth := grpcx.NewHealth()

	grpcServer := grpcx.NewServer(grpcx.ServerParams{
		ServiceName: serviceName,
		JWTSecret:   []byte(conf.GRPC.JWTSecret),
		// Оба метода читают только СВОДКИ (см. комментарий сервиса в
		// proto/gosplash/analytics/v1/analytics.proto) — публичная витрина
		// отчётов, а не персональные данные пользователя, поэтому она,
		// как и чтение каталога в catalog-service, доступна без токена.
		PublicMethods: []string{
			analyticsv1.AnalyticsService_TopPhotos_FullMethodName,
			analyticsv1.AnalyticsService_PhotoStats_FullMethodName,
		},
		DefaultTimeout: conf.GRPC.DefaultTimeout,
	})
	analyticsv1.RegisterAnalyticsServiceServer(grpcServer, analyticsServer)
	grpcHealth.Register(grpcServer)

	// ── Готовность ───────────────────────────────────────────────────────────
	health := httpx.NewHealth()
	health.Register("clickhouse", chClient.Ping)
	health.Register("kafka_views", consumers.PingViews)
	health.Register("kafka_purchases", consumers.PingPurchases)

	// Публичного HTTP API у сервиса нет (см. proto: наружу — только gRPC),
	// поэтому на публичном порту — только health-хендлеры, как заготовка
	// под /healthz и /readyz; /metrics и /debug/pprof — на служебном порту
	// ниже (httpx.ServeMetricsAndPprof), в отдельном DefaultServeMux.
	router := http.NewServeMux()
	health.Handle(router, serviceName)

	httpServer := httpx.NewServer(
		httpx.DefaultServerConfig(local.HTTPAddr),
		httpx.Chain(router, httpx.Default(serviceName)...),
	)

	// ── Запуск ───────────────────────────────────────────────────────────────
	metricsServer := httpx.ServeMetricsAndPprof(local.MetricsAddr)
	go httpx.Serve(httpServer, "public")

	grpcDone := make(chan struct{})
	go func() {
		defer close(grpcDone)
		if err := grpcx.Serve(grpcServer, local.GRPCAddr, serviceName); err != nil {
			slog.Error("analytics: gRPC остановлен", "error", err)
		}
	}()

	viewsDone := make(chan struct{})
	go func() {
		defer close(viewsDone)
		if err := consumers.RunViews(ctx); err != nil {
			slog.Error("analytics: консьюмер просмотров остановлен", "error", err)
		}
	}()

	purchasesDone := make(chan struct{})
	go func() {
		defer close(purchasesDone)
		if err := consumers.RunPurchases(ctx); err != nil {
			slog.Error("analytics: консьюмер покупок остановлен", "error", err)
		}
	}()

	// ── Остановка ────────────────────────────────────────────────────────────
	<-ctx.Done()
	slog.Info("analytics: останавливаюсь…")

	health.NotReady()
	grpcHealth.NotServing()
	time.Sleep(2 * time.Second)

	httpx.Shutdown(ctx, 15*time.Second, httpServer, metricsServer)
	grpcx.Shutdown(grpcServer, 15*time.Second)

	waitAll := func(timeout time.Duration, chans ...<-chan struct{}) {
		deadline := time.After(timeout)
		for _, ch := range chans {
			select {
			case <-ch:
			case <-deadline:
				slog.Warn("analytics: консьюмер не остановился вовремя")
				return
			}
		}
	}
	waitAll(15*time.Second, viewsDone, purchasesDone)

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := shutdownOtel(shutdownCtx); err != nil {
		slog.Error("analytics: otel shutdown", "error", err)
	}
	slog.Info("analytics: остановлен")
}

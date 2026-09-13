// wallet-service — счета, двойная запись в ledger, резервирование средств.
// Фаза 3 в docs/PLAN.md.
//
// Состав процесса:
//
//	gRPC :9103   — WalletService (proto/gosplash/wallet/v1): GetBalance,
//	              ReserveFunds, CommitFunds, ReleaseFunds. Единственный вход
//	              в деньги — саги дергают эти методы напрямую по gRPC.
//	Outbox-relay — публикует wallet.account.debited в Kafka.
//	HTTP :8204   — служебный: /healthz, /readyz, /metrics, /debug/pprof.
//
// Публичного REST у wallet НЕТ намеренно (см. ниже, «Готовность»): деньгами
// управляет только сага через gRPC, а не браузер или curl.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	walletv1 "gosplash/gen/go/gosplash/wallet/v1"
	"gosplash/pkg/config"
	"gosplash/pkg/dbx"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"
	"gosplash/pkg/otelx"
	"gosplash/pkg/outbox"

	walletgrpc "gosplash/services/wallet/internal/adapters/grpc"
	"gosplash/services/wallet/internal/adapters/pg"
	"gosplash/services/wallet/internal/app"
)

const serviceName = "wallet"

func main() {
	// Общее (Kafka, Redis, Observe, GRPC, Outbox) — из настоящего
	// pkg/config, как у всех сервисов. WALLET_* — из локального конфига
	conf := config.Load()
	walletConf := conf.Wallet

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── Наблюдаемость первой: всё, что случится дальше при старте, обязано
	// попасть в трейсы и логи с trace_id ─────────────────────────────────────
	shutdownOtel, err := otelx.Setup(ctx, otelx.Config{
		ServiceName:      serviceName,
		Version:          "0.1.0",
		Environment:      conf.Observe.Environment,
		OTLPEndpoint:     conf.Observe.OTLPEndpoint,
		TraceSampleRatio: conf.Observe.SampleRatio,
		LogLevel:         conf.Observe.LogLevel,
	})
	if err != nil {
		slog.Error("wallet: наблюдаемость", "error", err)
		os.Exit(1)
	}

	if conf.GRPC.JWTSecret == "" {
		slog.Warn("wallet: JWT_SECRET пуст — gRPC-аутентификация выключена, это нормально только для локальной разработки")
	}

	// ── База: одна, не шардирована ────────────────────────────────────────────
	// Денежная операция (CommitFunds) обязана быть ОДНОЙ ACID-транзакцией
	// над двумя счетами и парой проводок — а транзакция между шардами
	// невозможна в принципе (см. docs/adr/0002-*: то же ограничение, из-за
	// которого media шардирован ПО ПОЛЬЗОВАТЕЛЮ, а не как попало). Поэтому
	// у wallet один DSN, а не Shards, как у media.
	database, err := dbx.Open(walletConf.DSN)
	if err != nil {
		slog.Error("wallet: postgres", "error", err)
		os.Exit(1)
	}

	// ── Слои ─────────────────────────────────────────────────────────────────
	store := pg.NewStore(database)
	service := app.NewService(store)
	walletServer := walletgrpc.NewServer(service)

	// ── Outbox-relay: wallet.account.debited → Kafka ──────────────────────────
	// DSN один (не шарды, см. выше) — тот же самый, на который пишет
	// CommitFunds внутри своей транзакции.
	relay, err := outbox.New(outbox.Config{
		ServiceName:  serviceName,
		Brokers:      conf.Kafka.Brokers,
		DSNs:         []string{walletConf.DSN},
		PollInterval: conf.Outbox.PollInterval,
		BatchSize:    conf.Outbox.BatchSize,
	})
	if err != nil {
		slog.Error("wallet: outbox relay", "error", err)
		os.Exit(1)
	}
	defer relay.Close()

	// ── gRPC-сервер ──────────────────────────────────────────────────────────
	grpcServer := grpcx.NewServer(grpcx.ServerParams{
		ServiceName: serviceName,
		JWTSecret:   []byte(conf.GRPC.JWTSecret),
		// PublicMethods пуст намеренно: все четыре метода двигают или
		// показывают деньги, публичных среди них нет — в отличие от
		// catalog, у wallet нет витрины.
		DefaultTimeout: conf.GRPC.DefaultTimeout,
	})
	walletv1.RegisterWalletServiceServer(grpcServer, walletServer)
	grpcHealth := grpcx.NewHealth()
	grpcHealth.Register(grpcServer)

	// ── Готовность ───────────────────────────────────────────────────────────
	health := httpx.NewHealth()
	health.Register("postgres", func(ctx context.Context) error { return dbx.Ping(ctx, database) })
	health.Register("outbox", relay.Ping)

	// ── HTTP: ТОЛЬКО служебный, публичного REST нет ───────────────────────────
	// НЕОЧЕВИДНОЕ РЕШЕНИЕ: у media и catalog HTTP — тонкий фасад поверх
	// gRPC для браузера/curl. У wallet его нет вообще: единственный законный
	// вызывающий — сага заказа (Temporal-воркер), которая уже говорит по
	// gRPC и не нуждается в HTTP-обёртке, а открывать деньгами управляющий
	// REST для отладки means новая незащищённая поверхность там, где цена
	// ошибки — реальные копейки, а не просмотр карточки. Публичный
	// GRPC-порт остаётся ЕДИНСТВЕННЫМ входом в деньги.
	router := http.NewServeMux()
	health.Handle(router, serviceName)

	httpServer := httpx.NewServer(
		httpx.DefaultServerConfig(walletConf.HTTPAddr),
		httpx.Chain(router, httpx.Default(serviceName)...),
	)

	// ── Запуск ───────────────────────────────────────────────────────────────
	metricsServer := httpx.ServeMetricsAndPprof(walletConf.MetricsAddr)
	go httpx.Serve(httpServer, "public")

	grpcDone := make(chan struct{})
	go func() {
		defer close(grpcDone)
		if err := grpcx.Serve(grpcServer, walletConf.GRPCAddr, serviceName); err != nil {
			slog.Error("wallet: gRPC остановлен", "error", err)
		}
	}()

	go func() {
		if err := relay.Run(ctx); err != nil {
			slog.Error("wallet: outbox relay остановлен", "error", err)
		}
	}()

	// ── Остановка ────────────────────────────────────────────────────────────
	<-ctx.Done()
	slog.Info("wallet: останавливаюсь…")

	health.NotReady()
	grpcHealth.NotServing()
	time.Sleep(2 * time.Second)

	httpx.Shutdown(ctx, 15*time.Second, httpServer, metricsServer)
	grpcx.Shutdown(grpcServer, 15*time.Second)

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := shutdownOtel(shutdownCtx); err != nil {
		slog.Error("wallet: otel shutdown", "error", err)
	}
	slog.Info("wallet: остановлен")
}

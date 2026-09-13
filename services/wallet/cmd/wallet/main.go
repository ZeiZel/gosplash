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
// Публичного REST у wallet НЕТ намеренно (см. internal/bootstrap): деньгами
// управляет только сага через gRPC, а не браузер или curl.
//
// Файл разделён на три части (см. internal/bootstrap про причину):
// bootstrap.App — чистая сборка зависимостей, run() — конфиг/соединения/
// сигналы/graceful shutdown, main() — вызов run() и os.Exit по её ошибке.
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
	"gosplash/pkg/dbx"
	"gosplash/pkg/otelx"
	"gosplash/pkg/outbox"

	"gosplash/services/wallet/internal/bootstrap"
)

const serviceName = "wallet"

// otelShutdownTimeout — сколько ждём выгрузки последних трейсов/метрик
// после того, как серверы уже остановлены.
const otelShutdownTimeout = 5 * time.Second

// run — жизненный цикл процесса: конфиг, соединения, запуск App, ожидание
// сигнала, graceful shutdown. Возвращает ошибку вместо os.Exit, поэтому
// все defer (закрытие DB, relay, otel) успевают отработать независимо от
// того, на каком шаге всё пошло не так — раньше os.Exit(1) внутри main()
// обрывал их (gocritic: exitAfterDefer), и соединения с Postgres/Kafka
// оставались висеть до убийства процесса супервизором.
func run() error {
	// Общее (Kafka, Redis, Observe, GRPC, Outbox) — из настоящего
	// pkg/config, как у всех сервисов. WALLET_* — из локального конфига.
	conf := config.Load()
	walletConf := conf.Wallet

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── Наблюдаемость первой: всё, что случится дальше при старте, обязано
	// попасть в трейсы и логи с trace_id ────────────────────────────────────
	shutdownOtel, err := otelx.Setup(ctx, otelx.Config{
		ServiceName:      serviceName,
		Version:          "0.1.0",
		Environment:      conf.Observe.Environment,
		OTLPEndpoint:     conf.Observe.OTLPEndpoint,
		TraceSampleRatio: conf.Observe.SampleRatio,
		LogLevel:         conf.Observe.LogLevel,
	})
	if err != nil {
		return fmt.Errorf("wallet: наблюдаемость: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), otelShutdownTimeout)
		defer cancel()
		if err := shutdownOtel(shutdownCtx); err != nil {
			slog.Error("wallet: otel shutdown", "error", err)
		}
	}()

	if conf.GRPC.JWTSecret == "" {
		slog.Warn("wallet: JWT_SECRET пуст — gRPC-аутентификация выключена, это нормально только для локальной разработки")
	}

	// ── База: одна, не шардирована ──────────────────────────────────────────
	// Денежная операция (CommitFunds) обязана быть ОДНОЙ ACID-транзакцией
	// над двумя счетами и парой проводок — а транзакция между шардами
	// невозможна в принципе (см. docs/adr/0002-*: то же ограничение, из-за
	// которого media шардирован ПО ПОЛЬЗОВАТЕЛЮ, а не как попало). Поэтому
	// у wallet один DSN, а не Shards, как у media.
	database, err := dbx.Open(walletConf.DSN)
	if err != nil {
		return fmt.Errorf("wallet: postgres: %w", err)
	}
	defer func() {
		if sqlDB, err := database.DB(); err == nil {
			if err := sqlDB.Close(); err != nil {
				slog.Error("wallet: не удалось закрыть пул соединений", "error", err)
			}
		}
	}()

	// ── Outbox-relay: wallet.account.debited → Kafka ────────────────────────
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
		return fmt.Errorf("wallet: outbox relay: %w", err)
	}
	defer relay.Close()

	// ── Сборка приложения: чистая композиция, без сети ──────────────────────
	application, err := bootstrap.NewApp(bootstrap.Deps{
		DB:             database,
		Relay:          relay,
		JWTSecret:      conf.GRPC.JWTSecret,
		DefaultTimeout: conf.GRPC.DefaultTimeout,
		GRPCAddr:       walletConf.GRPCAddr,
		HTTPAddr:       walletConf.HTTPAddr,
		MetricsAddr:    walletConf.MetricsAddr,
	})
	if err != nil {
		return fmt.Errorf("wallet: сборка приложения: %w", err)
	}
	defer func() {
		if err := application.Close(); err != nil {
			slog.Error("wallet: закрытие приложения", "error", err)
		}
	}()

	// Run блокируется до отмены ctx (сигнал) или фатального сбоя сервера
	// и сама проводит graceful shutdown серверов и relay.
	return application.Run(ctx)
}

// main — три строки: вызвать run(), при ошибке залогировать и os.Exit(1).
// os.Exit здесь безопасен: он самый внешний вызов в процессе, и к этому
// моменту run() уже вернула управление — все её defer (закрытие DB, relay,
// otel) успели отработать ДО этой строки, а не обрываются им.
func main() {
	if err := run(); err != nil {
		slog.Error("wallet: приложение остановлено с ошибкой", "error", err.Error())
		os.Exit(1)
	}
}

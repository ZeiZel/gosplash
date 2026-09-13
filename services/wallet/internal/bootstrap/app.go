// Package bootstrap — композиционный корень wallet-service.
//
// Это НЕ internal/app: там сценарии (прикладной слой, ничего не знающий про
// gorm/grpc/kafka), а здесь ровно наоборот — только про то, как поднятые
// снаружи соединения складываются в работающий процесс. Смешивать две эти
// ответственности в одном пакете значило бы, что internal/app перестаёт
// собираться без сети (см. docs/STYLE.md, «Раскладка сервиса»), поэтому
// композиция вынесена отдельно.
//
// App — чистая сборка зависимостей: NewApp ничего не слушает и не ловит
// сигналов, поэтому его можно вызвать из теста напрямую. Жизненным циклом
// (конфиг, соединения, os/signal, graceful shutdown) занимается run() в
// cmd/wallet/main.go.
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"gorm.io/gorm"

	walletv1 "gosplash/gen/go/gosplash/wallet/v1"
	"gosplash/pkg/dbx"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"

	walletgrpc "gosplash/services/wallet/internal/adapters/grpc"
	"gosplash/services/wallet/internal/adapters/pg"
	"gosplash/services/wallet/internal/app"

	"google.golang.org/grpc"
)

const serviceName = "wallet"

const (
	// drainDelay — пауза между NotReady и остановкой серверов: балансировщику
	// нужно успеть узнать о смене готовности прежде, чем соединения начнут рваться.
	drainDelay          = 2 * time.Second
	httpShutdownTimeout = 15 * time.Second
	grpcShutdownTimeout = 15 * time.Second
	relayJoinTimeout    = 15 * time.Second
)

// Relay — узкий срез *outbox.Relay: Run/Close/Ping. Интерфейс, а не
// конкретный тип, — чтобы тест App мог подставить fake-релей без живых
// Kafka и Postgres: outbox.New (см. pkg/outbox/relay.go) открывает пулы
// и продюсер СРАЗУ, поэтому настоящий Relay в принципе нельзя получить
// без сети, а App это и не должен требовать.
type Relay interface {
	Run(ctx context.Context) error
	Close()
	Ping(ctx context.Context) error
}

// Deps — уже открытые снаружи ресурсы и адреса, на которых слушать.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: DB и Relay приходят готовыми, а не открываются
// внутри NewApp. dbx.Open и outbox.New — это поход в сеть (см. их
// комментарии), а App обязан собираться без сети: время жизни соединений —
// забота run(), у которой есть контекст процесса и defer на закрытие,
// а не композиции слоёв.
type Deps struct {
	DB    *gorm.DB
	Relay Relay

	JWTSecret      string
	DefaultTimeout time.Duration

	GRPCAddr    string
	HTTPAddr    string
	MetricsAddr string
}

// App — собранный процесс wallet: gRPC-сервер денежных операций и служебный
// HTTP (health/metrics/pprof). Публичного REST нет намеренно — см. package
// doc бывшего cmd/wallet/main.go, перенесённый туда же в новом файле.
type App struct {
	deps Deps

	grpcServer *grpc.Server
	grpcHealth *grpcx.Health

	httpServer    *http.Server
	metricsServer *http.Server
	health        *httpx.Health
}

// NewApp связывает слои. Ошибка возможна одна — не заданы обязательные
// зависимости; дальше идёт только конструирование объектов в памяти,
// без единого похода в сеть, поэтому NewApp можно звать из теста.
func NewApp(deps Deps) (*App, error) {
	if deps.DB == nil {
		return nil, fmt.Errorf("bootstrap: wallet: DB обязателен")
	}
	if deps.Relay == nil {
		return nil, fmt.Errorf("bootstrap: wallet: Relay обязателен")
	}

	store := pg.NewStore(deps.DB)
	service := app.NewService(store)
	walletServer := walletgrpc.NewServer(service)

	grpcServer := grpcx.NewServer(grpcx.ServerParams{
		ServiceName: serviceName,
		JWTSecret:   []byte(deps.JWTSecret),
		// PublicMethods пуст намеренно: все четыре метода двигают или
		// показывают деньги, публичных среди них нет — в отличие от
		// catalog, у wallet нет витрины.
		DefaultTimeout: deps.DefaultTimeout,
	})
	walletv1.RegisterWalletServiceServer(grpcServer, walletServer)
	grpcHealth := grpcx.NewHealth()
	grpcHealth.Register(grpcServer)

	health := httpx.NewHealth()
	health.Register("postgres", func(ctx context.Context) error { return dbx.Ping(ctx, deps.DB) })
	health.Register("outbox", deps.Relay.Ping)

	// ── HTTP: ТОЛЬКО служебный, публичного REST нет ─────────────────────────
	// Единственный законный вызывающий деньги — сага заказа по gRPC, а не
	// браузер или curl; открывать управляющий деньгами REST для отладки —
	// новая незащищённая поверхность там, где цена ошибки — реальные копейки.
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

// Run поднимает серверы и relay, затем блокируется до отмены ctx (сигнал
// ловит run() в main.go) либо до первого фатального сбоя одного из них.
//
// Порядок остановки выверен на живом кластере (переносится без изменений
// из прежнего main.go): снять readiness → пауза → остановить серверы →
// дождаться relay → сообщить наружу.
func (a *App) Run(ctx context.Context) error {
	a.metricsServer = httpx.ServeMetricsAndPprof(a.deps.MetricsAddr)
	go httpx.Serve(a.httpServer, "public")

	grpcErr := make(chan error, 1)
	go func() {
		grpcErr <- grpcx.Serve(a.grpcServer, a.deps.GRPCAddr, serviceName)
	}()

	relayDone := make(chan error, 1)
	go func() {
		relayDone <- a.deps.Relay.Run(ctx)
	}()

	var runErr error
	select {
	case <-ctx.Done():
		slog.Info("wallet: останавливаюсь…")
	case err := <-grpcErr:
		if err != nil {
			runErr = fmt.Errorf("wallet: gRPC остановлен: %w", err)
		}
	}

	a.health.NotReady()
	a.grpcHealth.NotServing()
	time.Sleep(drainDelay)

	httpx.Shutdown(ctx, httpShutdownTimeout, a.httpServer, a.metricsServer)
	grpcx.Shutdown(a.grpcServer, grpcShutdownTimeout)

	// Дожидаемся relay ПЕРЕД тем, как run() в main.go закроет его пулы
	// (Relay.Close требует, чтобы Run уже вернул управление, см. её
	// комментарий в pkg/outbox) — то же правило, что и для kafka-консьюмеров
	// в analytics/search: закрывать соединение раньше, чем горутина, которая
	// им пользуется, вышла, — источник паник и утечек, а не экономия времени.
	select {
	case err := <-relayDone:
		if err != nil && runErr == nil {
			runErr = fmt.Errorf("wallet: outbox relay остановлен: %w", err)
		}
	case <-time.After(relayJoinTimeout):
		slog.Warn("wallet: outbox relay не остановился вовремя")
	}

	if runErr == nil {
		slog.Info("wallet: остановлен")
	}
	return runErr
}

// Close отпускает то, что App построил сам (серверы уже остановлены внутри
// Run). DB и Relay App не открывал — их закрывает run() в main.go, которому
// они и принадлежат; вызывать их Close отсюда значило бы закрывать чужой
// ресурс дважды. Метод пуст сегодня, но оставлен в форме, согласованной
// с параллельным сервисом (media/catalog/thumbnail-worker): у них Close
// действительно на что-то закрывает, и одинаковая сигнатура App дешевле
// для читателя, чем разные контракты у соседних сервисов.
func (a *App) Close() error {
	return nil
}

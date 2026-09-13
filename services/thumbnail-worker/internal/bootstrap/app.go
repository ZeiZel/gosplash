// Package bootstrap — композиционный корень thumbnail-worker'а.
//
// Такое же разделение, что и в services/media и services/catalog (см.
// подробный разбор в services/media/internal/bootstrap) и в эталоне
// ZeiZel/gomple/cmd/main.go:
//
//	NewApp(deps) — чистая сборка HTTP-сервера (только health — публичного
//	               API у воркера нет) из уже готового обработчика Kafka-
//	               сообщений. Ничего не запускает — тестируется без докера
//	               (app_test.go).
//	App.Run(ctx) — жизненный цикл: HTTP-сервер, основной и retry-консьюмеры,
//	               outbox-relay, блокировка до отмены ctx, остановка в
//	               порядке, который был в cmd/thumbnail-worker/main.go ДО
//	               рефакторинга (см. комментарии внутри Run).
//	App.Close()  — закрывает только то, что создала сама App: http.Server.
//	               Kafka-консьюмеры и outbox-relay App не открывала — их
//	               закрывает run() в cmd/thumbnail-worker/main.go обычным
//	               defer, как и до рефакторинга.
//
// Имя пакета — bootstrap, не app: internal/app здесь уже занят сценарием
// (Service — генерация превью, docs/STYLE.md), а три сервиса, прошедших
// этот рефакторинг, используют одно и то же имя композиционного пакета.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: Deps не принимает *dbx.Shards, *redisx.Client,
// *kafkax.Consumer или *outbox.Relay напрямую — их конструкторы дозваниваются
// до живого Postgres/Redis/Kafka немедленно, и с ними NewApp нельзя было бы
// вызвать в тесте без докера (задание требует именно такого теста):
//   - health-проверки приходят уже готовыми функциями (httpx.Check);
//   - консьюмеры и outbox-relay — узкими интерфейсами Consumer/Relay,
//     которым легко подсунуть подделку, ничего не поднимая.
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"gosplash/pkg/httpx"
	"gosplash/pkg/kafkax"
)

// Consumer — то немногое, что App-у нужно от *kafkax.Consumer и
// *kafkax.RetryConsumer: запустить обработку и вернуть управление, когда
// её остановят (по отменённому ctx). Close сюда намеренно не входит —
// см. package doc: consumer.Close() остаётся за run().
type Consumer interface {
	Run(ctx context.Context, handle kafkax.Handler) error
}

// Relay — то немногое, что App-у нужно от *outbox.Relay: запустить.
type Relay interface {
	Run(ctx context.Context) error
}

// Timeouts — тайминги жизненного цикла, вынесены отдельно, чтобы тест мог
// их сократить и не ждать боевые 15+2 секунды на каждый прогон.
type Timeouts struct {
	DrainDelay      time.Duration
	ShutdownTimeout time.Duration
}

func DefaultTimeouts() Timeouts {
	return Timeouts{
		DrainDelay:      2 * time.Second,
		ShutdownTimeout: 15 * time.Second,
	}
}

// Deps — всё необходимое для сборки App.
type Deps struct {
	ServiceName string
	HTTPAddr    string
	MetricsAddr string

	HealthChecks map[string]httpx.Check

	// Consumer/RetryConsumer читают ОДИН И ТОТ ЖЕ Handle (тот же
	// kafkax.Handler, что main.go до рефакторинга строил один раз через
	// thumbkafka.NewHandler(service) и передавал обоим).
	Consumer      Consumer
	RetryConsumer Consumer
	Handle        kafkax.Handler

	Relay Relay

	Timeouts Timeouts
}

// App — собранное приложение: все зависимости связаны, ничего не запущено.
type App struct {
	serviceName string
	metricsAddr string

	httpServer *http.Server
	health     *httpx.Health

	consumer      Consumer
	retryConsumer Consumer
	handle        kafkax.Handler
	relay         Relay
	timeouts      Timeouts

	closeOnce sync.Once
}

// NewApp собирает thumbnail-worker: HTTP-сервер с health-ручками (у воркера
// нет публичного API — только /healthz и /readyz) и держит ссылки на
// фоновые процессы для последующего Run. Не открывает ни одного сетевого
// соединения и не запускает ни одного сервера — см. package doc.
func NewApp(deps Deps) (*App, error) {
	if deps.Consumer == nil {
		return nil, fmt.Errorf("bootstrap: не задан основной консьюмер (Deps.Consumer)")
	}
	if deps.RetryConsumer == nil {
		return nil, fmt.Errorf("bootstrap: не задан retry-консьюмер (Deps.RetryConsumer)")
	}
	if deps.Handle == nil {
		return nil, fmt.Errorf("bootstrap: не задан обработчик сообщений (Deps.Handle)")
	}
	if deps.Relay == nil {
		return nil, fmt.Errorf("bootstrap: не задан outbox-relay (Deps.Relay)")
	}
	if deps.ServiceName == "" {
		deps.ServiceName = "thumbnail-worker"
	}
	timeouts := deps.Timeouts
	if timeouts == (Timeouts{}) {
		timeouts = DefaultTimeouts()
	}

	// ── Готовность ───────────────────────────────────────────────────────────
	health := httpx.NewHealth()
	for name, check := range deps.HealthChecks {
		health.Register(name, check)
	}

	// ── HTTP (только health — публичного API у воркера нет) ──────────────────
	router := http.NewServeMux()
	health.Handle(router, deps.ServiceName)
	httpServer := httpx.NewServer(
		httpx.DefaultServerConfig(deps.HTTPAddr),
		httpx.Chain(router, httpx.Default(deps.ServiceName)...),
	)

	return &App{
		serviceName:   deps.ServiceName,
		metricsAddr:   deps.MetricsAddr,
		httpServer:    httpServer,
		health:        health,
		consumer:      deps.Consumer,
		retryConsumer: deps.RetryConsumer,
		handle:        deps.Handle,
		relay:         deps.Relay,
		timeouts:      timeouts,
	}, nil
}

// Run запускает всё и блокируется до отмены ctx, затем корректно
// останавливается. Порядок — тот же, что был в
// cmd/thumbnail-worker/main.go до рефакторинга:
//
//  1. снять readiness;
//  2. пауза DrainDelay — балансировщику/оркестратору нужно время узнать об этом;
//  3. остановить HTTP-сервер, дав доработать активным запросам;
//  4. дождаться, пока консьюмер, retry-консьюмер и outbox-relay доработают
//     текущую итерацию — у каждого СВОЙ независимый бюджет ShutdownTimeout
//     (а не общий на всех троих), ровно как в исходном waitFor.
//
// Ошибки фоновых процессов логируются на месте и не прерывают Run.
func (a *App) Run(ctx context.Context) error {
	metricsServer := httpx.ServeMetricsAndPprof(a.metricsAddr)
	go httpx.Serve(a.httpServer, "public")

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		if err := a.consumer.Run(ctx, a.handle); err != nil {
			slog.Error(a.serviceName+": консьюмер остановлен", "error", err)
		}
	}()

	retryDone := make(chan struct{})
	go func() {
		defer close(retryDone)
		if err := a.retryConsumer.Run(ctx, a.handle); err != nil {
			slog.Error(a.serviceName+": retry-консьюмер остановлен", "error", err)
		}
	}()

	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		if err := a.relay.Run(ctx); err != nil {
			slog.Error(a.serviceName+": outbox relay остановлен", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info(a.serviceName + ": останавливаюсь…")

	a.health.NotReady()
	time.Sleep(a.timeouts.DrainDelay)

	httpx.Shutdown(ctx, a.timeouts.ShutdownTimeout, a.httpServer, metricsServer)

	// Каждому фоновому циклу даём доработать текущую итерацию, а не рвём
	// по живому: у консьюмеров это текущее сообщение (offset не сдвинут,
	// значит, при обрыве оно просто приедет снова — не смертельно, но
	// лишняя повторная обработка), у relay — текущий батч.
	waitFor := func(name string, done <-chan struct{}) {
		select {
		case <-done:
		case <-time.After(a.timeouts.ShutdownTimeout):
			slog.Warn(a.serviceName+": не остановился за отведённое время", "component", name)
		}
	}
	waitFor("consumer", consumerDone)
	waitFor("retry-consumer", retryDone)
	waitFor("outbox-relay", relayDone)

	slog.Info(a.serviceName + ": остановлен")
	return nil
}

// Close освобождает ресурсы, которые App создала сама: принудительно
// закрывает HTTP-сервер. Kafka-консьюмеры и outbox-relay App не закрывает —
// не она их открывала (см. package doc). Идемпотентен.
func (a *App) Close() error {
	a.closeOnce.Do(func() {
		if a.httpServer != nil {
			_ = a.httpServer.Close()
		}
	})
	return nil
}

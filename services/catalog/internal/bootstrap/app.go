// Package bootstrap — композиционный корень catalog-сервиса.
//
// Такое же разделение, что и в services/media/internal/bootstrap (см.
// подробный разбор там) и в эталоне ZeiZel/gomple/cmd/main.go:
//
//	NewApp(deps) — чистая сборка HTTP/gRPC серверов из уже готовых сценариев
//	               и уже открытых фоновых процессов. Ничего не запускает —
//	               тестируется без докера (app_test.go).
//	App.Run(ctx) — жизненный цикл: серверы, Kafka-консьюмеры, outbox-relay,
//	               блокировка до отмены ctx, остановка в порядке, который
//	               был в cmd/catalog/main.go ДО рефакторинга (см. комментарии
//	               внутри Run — они дословно те же, поведение не менялось).
//	App.Close()  — закрывает только то, что создала сама App: http.Server
//	               и grpc.Server. outbox-relay и Kafka-консьюмеры App не
//	               открывала — их закрывает run() в cmd/catalog/main.go
//	               обычным defer, как и до рефакторинга.
//
// Имя пакета — bootstrap, не app: internal/app здесь уже занят
// сценариями (Indexer, ListingService, LicenseService — docs/STYLE.md), а
// три сервиса, прошедших этот рефакторинг, используют одно и то же имя
// композиционного пакета, чтобы приём узнавался с первого взгляда.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: Deps не принимает *dbx.DB, *redisx.Client,
// *kafkax.Consumer или *outbox.Relay напрямую — их конструкторы дозваниваются
// до живого Postgres/Redis/Kafka немедленно, и с ними NewApp нельзя было бы
// вызвать в тесте без докера (задание требует именно такого теста):
//   - health-проверки приходят уже готовыми функциями (httpx.Check);
//   - Kafka-консьюмеры и outbox-relay — узкими интерфейсами Consumer/Relay,
//     которым легко подсунуть подделку, ничего не поднимая.
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"
	"gosplash/pkg/kafkax"

	cataloggrpc "gosplash/services/catalog/internal/adapters/grpc"
	cataloghttp "gosplash/services/catalog/internal/adapters/http"
	"gosplash/services/catalog/internal/app"
)

// Consumer — то немногое, что App-у нужно от *kafkax.Consumer и
// *kafkax.RetryConsumer: запустить обработку и вернуть управление, когда
// её остановят (по отменённому ctx). Оба типа пакета kafkax удовлетворяют
// этому интерфейсу структурно, без обёртки. Close сюда намеренно не
// входит — см. package doc: consumer.Close() остаётся за run().
type Consumer interface {
	Run(ctx context.Context, handle kafkax.Handler) error
}

// Relay — то немногое, что App-у нужно от *outbox.Relay: запустить.
// Как и Consumer, не включает Close/Ping — см. package doc.
type Relay interface {
	Run(ctx context.Context) error
}

// ConsumerSpec — один Kafka-консьюмер вместе со своим обработчиком и
// правилом остановки.
type ConsumerSpec struct {
	// Name — только для логов (что именно не остановилось вовремя).
	Name     string
	Consumer Consumer
	Handle   kafkax.Handler
	// AwaitOnShutdown — ждать ли остановки ЭТОГО консьюмера, прежде чем
	// Run() вернёт управление. В catalog ДО рефакторинга так ждали только
	// основные консьюмеры (uploaded, thumbnail-ready) — у retry-консьюмеров
	// (uploaded-retry, thumbnail-ready-retry) done-канала не было вовсе, и
	// graceful shutdown возвращался, не дожидаясь их. Это поведение
	// сохранено как есть (задание запрещает менять поведение), хотя оно и
	// несимметрично: ретраи могут доработать текущее сообщение уже после
	// того, как процесс формально "остановился".
	AwaitOnShutdown bool
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
	GRPCAddr    string
	MetricsAddr string

	JWTSecret     string
	GRPCTimeout   time.Duration
	PublicMethods []string

	// Сценарии (app.*) собираются снаружи — в run() из pg/redis/grpc-клиента
	// в проде, из подделок ports.* в тесте.
	ListingService *app.ListingService
	LicenseService *app.LicenseService
	Hub            *app.WatchHub

	HealthChecks map[string]httpx.Check

	Consumers []ConsumerSpec
	Relay     Relay

	Timeouts Timeouts
}

// App — собранное приложение: все зависимости связаны, ничего не запущено.
type App struct {
	serviceName string
	grpcAddr    string
	metricsAddr string

	httpServer *http.Server
	grpcServer *grpc.Server
	grpcHealth *grpcx.Health
	health     *httpx.Health

	consumers []ConsumerSpec
	relay     Relay
	timeouts  Timeouts

	closeOnce sync.Once
}

// NewApp собирает catalog-сервис: связывает сценарии с HTTP- и gRPC-
// адаптерами и настраивает health-проверки. Не открывает ни одного
// сетевого соединения и не запускает ни одного сервера — см. package doc.
func NewApp(deps Deps) (*App, error) {
	if deps.ListingService == nil {
		return nil, fmt.Errorf("bootstrap: не задан ListingService")
	}
	if deps.LicenseService == nil {
		return nil, fmt.Errorf("bootstrap: не задан LicenseService")
	}
	if deps.Hub == nil {
		return nil, fmt.Errorf("bootstrap: не задан WatchHub")
	}
	if deps.Relay == nil {
		return nil, fmt.Errorf("bootstrap: не задан outbox-relay (Deps.Relay)")
	}
	if deps.ServiceName == "" {
		deps.ServiceName = "catalog"
	}
	timeouts := deps.Timeouts
	if timeouts == (Timeouts{}) {
		timeouts = DefaultTimeouts()
	}

	if deps.JWTSecret == "" {
		// docs/STYLE.md и pkg/config: пустой секрет означает выключенную
		// проверку токена — допустимо только локально, сервис обязан
		// сказать об этом в лог, а не выключить аутентификацию молча.
		slog.Warn(deps.ServiceName + ": JWT_SECRET пуст — gRPC-аутентификация выключена, это нормально только для локальной разработки")
	}

	// ── gRPC ─────────────────────────────────────────────────────────────────
	catalogServer := cataloggrpc.NewServer(deps.ListingService, deps.LicenseService, deps.Hub)
	grpcHealth := grpcx.NewHealth()
	grpcServer := grpcx.NewServer(grpcx.ServerParams{
		ServiceName:    deps.ServiceName,
		JWTSecret:      []byte(deps.JWTSecret),
		PublicMethods:  deps.PublicMethods,
		DefaultTimeout: deps.GRPCTimeout,
	})
	catalogv1.RegisterCatalogServiceServer(grpcServer, catalogServer)
	grpcHealth.Register(grpcServer)

	// ── Готовность ───────────────────────────────────────────────────────────
	health := httpx.NewHealth()
	for name, check := range deps.HealthChecks {
		health.Register(name, check)
	}

	// ── HTTP ─────────────────────────────────────────────────────────────────
	router := http.NewServeMux()
	cataloghttp.Register(router, catalogServer, deps.ListingService)
	health.Handle(router, deps.ServiceName)

	httpServer := httpx.NewServer(
		httpx.DefaultServerConfig(deps.HTTPAddr),
		httpx.Chain(router, httpx.Default(deps.ServiceName)...),
	)

	return &App{
		serviceName: deps.ServiceName,
		grpcAddr:    deps.GRPCAddr,
		metricsAddr: deps.MetricsAddr,
		httpServer:  httpServer,
		grpcServer:  grpcServer,
		grpcHealth:  grpcHealth,
		health:      health,
		consumers:   deps.Consumers,
		relay:       deps.Relay,
		timeouts:    timeouts,
	}, nil
}

// Run запускает всё и блокируется до отмены ctx, затем корректно
// останавливается. Порядок — тот же, что был в cmd/catalog/main.go до
// рефакторинга:
//
//  1. снять readiness (HTTP и gRPC);
//  2. пауза DrainDelay — балансировщику нужно время узнать об этом;
//  3. остановить HTTP/gRPC серверы, дав доработать активным запросам;
//  4. дождаться ТОЛЬКО тех консьюмеров, у которых AwaitOnShutdown — то есть
//     основных (uploaded, thumbnail-ready), но не retry-консьюмеров и не
//     outbox-relay: так было в исходном main.go, и это сохранено как есть.
//
// Ошибки фоновых процессов логируются на месте и не прерывают Run.
func (a *App) Run(ctx context.Context) error {
	metricsServer := httpx.ServeMetricsAndPprof(a.metricsAddr)
	go httpx.Serve(a.httpServer, "public")

	go func() {
		if err := grpcx.Serve(a.grpcServer, a.grpcAddr, a.serviceName); err != nil {
			slog.Error(a.serviceName+": gRPC остановлен", "error", err)
		}
	}()

	go func() {
		if err := a.relay.Run(ctx); err != nil {
			slog.Error(a.serviceName+": outbox relay остановлен", "error", err)
		}
	}()

	var awaited []<-chan struct{}
	for _, spec := range a.consumers {
		if !spec.AwaitOnShutdown {
			go func() {
				if err := spec.Consumer.Run(ctx, spec.Handle); err != nil {
					slog.Error(a.serviceName+": консьюмер остановлен", "consumer", spec.Name, "error", err)
				}
			}()
			continue
		}

		done := make(chan struct{})
		awaited = append(awaited, done)
		go func() {
			defer close(done)
			if err := spec.Consumer.Run(ctx, spec.Handle); err != nil {
				slog.Error(a.serviceName+": консьюмер остановлен", "consumer", spec.Name, "error", err)
			}
		}()
	}

	<-ctx.Done()
	slog.Info(a.serviceName + ": останавливаюсь…")

	a.health.NotReady()
	a.grpcHealth.NotServing()
	time.Sleep(a.timeouts.DrainDelay)

	httpx.Shutdown(ctx, a.timeouts.ShutdownTimeout, a.httpServer, metricsServer)
	grpcx.Shutdown(a.grpcServer, a.timeouts.ShutdownTimeout)

	// Консьюмерам даём доработать текущее сообщение. Оборвать его на
	// середине не смертельно (offset не сдвинут, сообщение приедет снова),
	// но каждый такой обрыв — это лишняя повторная обработка после
	// рестарта.
	deadline := time.After(a.timeouts.ShutdownTimeout)
	for _, done := range awaited {
		select {
		case <-done:
		case <-deadline:
			slog.Warn(a.serviceName + ": консьюмер не остановился вовремя")
			return nil
		}
	}

	slog.Info(a.serviceName + ": остановлен")
	return nil
}

// Close освобождает ресурсы, которые App создала сама: принудительно
// закрывает HTTP- и gRPC-серверы. Kafka-консьюмеры и outbox-relay App не
// закрывает — не она их открывала (см. package doc). Идемпотентен.
func (a *App) Close() error {
	a.closeOnce.Do(func() {
		if a.httpServer != nil {
			_ = a.httpServer.Close()
		}
		if a.grpcServer != nil {
			a.grpcServer.Stop()
		}
	})
	return nil
}

// Package bootstrap — композиционный корень media-сервиса.
//
// Раньше вся сборка зависимостей и весь жизненный цикл жили одним куском
// в cmd/media/main.go (249 строк) — и были непроверяемы: package main
// нельзя импортировать из теста, лежащего рядом с ним (см. эталон
// ZeiZel/gomple/cmd/main.go + auth_test.go). Пакет называется bootstrap,
// а не app: internal/app в этом сервисе уже занят сценарием (PhotoService,
// docs/STYLE.md — "internal/app/ сценарии"), и второй пакет с тем же именем
// на другом уровне только путал бы импорт. Во всех трёх сервисах,
// прошедших этот рефакторинг (media, catalog, thumbnail-worker), пакет
// называется одинаково — bootstrap, — чтобы приём искался одним и тем
// же способом, а не угадывался заново в каждом сервисе.
//
// Три части, как в эталоне:
//
//	NewApp(deps) — чистая сборка: связывает уже открытые соединения и уже
//	               готовый сценарий (app.PhotoService) в HTTP- и gRPC-серверы.
//	               Ничего не слушает и не запускает — поэтому её можно
//	               позвать из теста без докера (см. app_test.go).
//	App.Run(ctx) — жизненный цикл: поднимает серверы и outbox-relay,
//	               блокируется до отмены ctx, затем останавливает всё в
//	               том порядке, который выверен на живом кластере (см.
//	               комментарии внутри Run).
//	App.Close()  — освобождает то, что создала сама App: http.Server и
//	               grpc.Server. Соединения, которые App только ИСПОЛЬЗУЕТ
//	               (outbox-relay, БД, Redis), она не открывала — их
//	               закрывает run() в cmd/media/main.go обычным defer,
//	               ровно как *dbx.DB в эталоне ZeiZel/gomple/cmd/main.go.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: Deps НЕ принимает *dbx.Shards, *redisx.Client или
// *outbox.Relay напрямую. dbx.NewShards и outbox.New дозваниваются до
// живой инфраструктуры ПРЯМО В КОНСТРУКТОРЕ (см. их doc-комментарии) —
// с ними NewApp нельзя было бы вызвать в тесте без поднятого Postgres/Kafka,
// а задание требует именно такого теста. Поэтому:
//   - health-проверки приходят уже готовыми функциями (httpx.Check),
//     собранными в cmd/media/main.go из реальных Ping;
//   - outbox-relay приходит как узкий интерфейс Relay — двум методам,
//     которые App использует (Run — запустить и дождаться остановки,
//     плюс Ping для health), легко подсунуть подделку, ничего не поднимая.
//
// Это ровно тот случай, о котором предупреждает задание рефакторинга:
// раз собрать App без живых соединений было нельзя — зависимости,
// которые открывают их сами, ушли из Deps в узкие интерфейсы или в готовые
// значения снаружи.
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	mediav1 "gosplash/gen/go/gosplash/media/v1"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"

	mediagrpc "gosplash/services/media/internal/adapters/grpc"
	mediahttp "gosplash/services/media/internal/adapters/http"
	"gosplash/services/media/internal/app"
)

// Relay — то немногое, что App-у нужно от *outbox.Relay: запустить и
// дождаться штатной остановки по отменённому ctx. Узкий интерфейс, а не
// конкретный тип (см. package doc про то, почему outbox.New нельзя вызвать
// без живой Kafka и Postgres) и не более широкий интерфейс: Close и Ping
// App не вызывает — Close остаётся за run() (см. cmd/media/main.go),
// а health-проверки приходят уже готовыми функциями через Deps.HealthChecks.
type Relay interface {
	Run(ctx context.Context) error
}

// Timeouts — тайминги жизненного цикла. Вынесены в структуру со значениями
// по умолчанию (DefaultTimeouts), чтобы тест мог сократить их и не ждать
// реальные 15+2 секунды на каждый прогон.
type Timeouts struct {
	// DrainDelay — пауза между NotReady/NotServing и остановкой серверов:
	// балансировщику нужно время узнать, что сервис уходит из ротации
	// (см. комментарий у httpx.Shutdown).
	DrainDelay time.Duration
	// ShutdownTimeout — сколько ждём завершения активных HTTP/gRPC запросов.
	ShutdownTimeout time.Duration
	// RelayShutdownTimeout — сколько ждём, пока outbox-relay доработает
	// текущий батч, прежде чем идти дальше и логировать предупреждение.
	RelayShutdownTimeout time.Duration
}

// DefaultTimeouts — то же самое, что было захардкожено в main.go до
// рефакторинга (2с паузы, 15с на остановку).
func DefaultTimeouts() Timeouts {
	return Timeouts{
		DrainDelay:           2 * time.Second,
		ShutdownTimeout:      15 * time.Second,
		RelayShutdownTimeout: 15 * time.Second,
	}
}

// Deps — всё, что нужно для сборки App: конфигурация адресов и таймаутов,
// уже готовый сценарий и уже открытые (или подделанные в тесте) фоновые
// зависимости.
type Deps struct {
	ServiceName string
	HTTPAddr    string
	GRPCAddr    string
	MetricsAddr string

	// HTTPReadTimeout/HTTPWriteTimeout — media держит их больше общих
	// (см. cmd/media/main.go до рефакторинга): загрузка 50 МБ по медленному
	// каналу не должна обрываться сервером. Нулевые значения означают
	// "взять httpx.DefaultServerConfig как есть".
	HTTPReadTimeout  time.Duration
	HTTPWriteTimeout time.Duration

	// JWTSecret — пустая строка отключает проверку подписи gRPC ВСЕМ
	// методам, кроме PublicMethods. Допустимо только локально; NewApp
	// предупреждает об этом в лог, а не молчит (см. вызов ниже).
	JWTSecret     string
	GRPCTimeout   time.Duration
	PublicMethods []string

	// Service — уже собранный сценарий (app.NewPhotoService). Собирается
	// снаружи (в cmd/media или в тесте) именно потому, что для этого нужны
	// ports.PhotoRepository и ports.ObjectStorage — в проде это pg+s3x,
	// в тесте — подделки из internal/app/service_test.go же устройства.
	Service *app.PhotoService

	// Далее — то, что раньше main.go передавал прямо в mediahttp.Deps.
	Limiter                mediahttp.RateLimiter
	MaxBodyBytes           int64
	PresignedTTL           time.Duration
	UploadRateCapacity     int
	UploadRateRefillPerSec float64

	// HealthChecks — уже готовые функции проверки готовности (обычно
	// shards.Ping и т.п.). Ключ — имя проверки в ответе /readyz.
	HealthChecks map[string]httpx.Check

	Relay Relay

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

	relay    Relay
	timeouts Timeouts

	closeOnce sync.Once
}

// NewApp собирает media-сервис: связывает сценарий с HTTP- и gRPC-адаптерами
// и настраивает health-проверки. Не открывает ни одного сетевого соединения
// и не запускает ни одного сервера — см. package doc.
func NewApp(deps Deps) (*App, error) {
	if deps.Service == nil {
		return nil, fmt.Errorf("bootstrap: не задан сценарий (Deps.Service)")
	}
	if deps.Relay == nil {
		return nil, fmt.Errorf("bootstrap: не задан outbox-relay (Deps.Relay)")
	}
	if deps.ServiceName == "" {
		deps.ServiceName = "media"
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

	// ── HTTP ─────────────────────────────────────────────────────────────────
	router := http.NewServeMux()
	mediahttp.Register(router, mediahttp.Deps{
		Service:                deps.Service,
		MaxBodyBytes:           deps.MaxBodyBytes,
		PresignedTTL:           deps.PresignedTTL,
		Limiter:                deps.Limiter,
		UploadRateCapacity:     deps.UploadRateCapacity,
		UploadRateRefillPerSec: deps.UploadRateRefillPerSec,
	})
	health.Handle(router, deps.ServiceName)

	httpCfg := httpx.DefaultServerConfig(deps.HTTPAddr)
	if deps.HTTPReadTimeout > 0 {
		httpCfg.ReadTimeout = deps.HTTPReadTimeout
	}
	if deps.HTTPWriteTimeout > 0 {
		httpCfg.WriteTimeout = deps.HTTPWriteTimeout
	}
	httpServer := httpx.NewServer(httpCfg, httpx.Chain(router, httpx.Default(deps.ServiceName)...))

	// ── gRPC ─────────────────────────────────────────────────────────────────
	if deps.JWTSecret == "" {
		// Пустой секрет отключает проверку подписи ВСЕМ методам, кроме
		// PublicMethods (pkg/grpcx) — не тихо, а с явным предупреждением,
		// чтобы выключенная аутентификация не уехала в прод незамеченной.
		slog.Warn(deps.ServiceName + ": JWT_SECRET пуст, gRPC-аутентификация выключена — допустимо только локально")
	}

	grpcHealth := grpcx.NewHealth()
	grpcServer := grpcx.NewServer(grpcx.ServerParams{
		ServiceName:    deps.ServiceName,
		JWTSecret:      []byte(deps.JWTSecret),
		PublicMethods:  deps.PublicMethods,
		DefaultTimeout: deps.GRPCTimeout,
	})
	mediav1.RegisterMediaServiceServer(grpcServer, mediagrpc.NewServer(deps.Service))
	grpcHealth.Register(grpcServer)
	// Reflection позволяет grpcurl вызывать методы без .proto-файла под
	// рукой. В проде обычно выключают, локально — незаменимо.
	reflection.Register(grpcServer)

	return &App{
		serviceName: deps.ServiceName,
		grpcAddr:    deps.GRPCAddr,
		metricsAddr: deps.MetricsAddr,
		httpServer:  httpServer,
		grpcServer:  grpcServer,
		grpcHealth:  grpcHealth,
		health:      health,
		relay:       deps.Relay,
		timeouts:    timeouts,
	}, nil
}

// Run запускает всё и блокируется до отмены ctx, затем корректно
// останавливается. Порядок остановки выверен на живом кластере (было в
// cmd/media/main.go до рефакторинга) и сохранён без изменений:
//
//  1. снять readiness (HTTP и gRPC) — балансировщик перестаёт слать новый трафик;
//  2. пауза DrainDelay — балансировщику нужно время узнать об этом;
//  3. остановить HTTP/gRPC серверы, дав доработать активным запросам;
//  4. дождаться outbox-relay (он доработает текущий батч сам, см. Relay.Run);
//
// Ошибки фоновых серверов (Serve вернул не ErrServerClosed) логируются на
// месте и не прерывают Run — так было и в исходном main.go: единственный
// штатный путь выхода — отмена ctx.
func (a *App) Run(ctx context.Context) error {
	metricsServer := httpx.ServeMetricsAndPprof(a.metricsAddr)
	go httpx.Serve(a.httpServer, "public")

	go func() {
		if err := grpcx.Serve(a.grpcServer, a.grpcAddr, a.serviceName); err != nil {
			slog.Error(a.serviceName+": grpc", "error", err)
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
	a.grpcHealth.NotServing()
	time.Sleep(a.timeouts.DrainDelay)

	httpx.Shutdown(ctx, a.timeouts.ShutdownTimeout, a.httpServer, metricsServer)
	grpcx.Shutdown(a.grpcServer, a.timeouts.ShutdownTimeout)

	// Relay'ю даём доработать текущий батч (см. комментарий к Relay.Run —
	// он и сам не оборвёт его на середине): ждём relayDone здесь, а не
	// закрываем relay сразу же, иначе последний батч останется без
	// соединений на середине публикации.
	select {
	case <-relayDone:
	case <-time.After(a.timeouts.RelayShutdownTimeout):
		slog.Warn(a.serviceName + ": outbox relay не остановился вовремя")
	}

	slog.Info(a.serviceName + ": остановлен")
	return nil
}

// Close освобождает ресурсы, которые App СОЗДАЛА САМА: принудительно
// закрывает HTTP- и gRPC-серверы. Outbox-relay App не закрывает — она его
// не открывала (см. package doc), это забота run() в cmd/media/main.go,
// обычным defer сразу после успешного outbox.New, как и everywhere в
// эталоне ZeiZel/gomple. Идемпотентен: повторный вызов, а также вызов без
// предшествующего Run, ничего не ломает — http.Server.Close и
// grpc.Server.Stop документированы как безопасные при повторном вызове,
// а sync.Once — дополнительная гарантия на случай, если это когда-нибудь
// перестанет быть так.
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

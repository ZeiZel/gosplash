package grpcx

import (
	"fmt"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"gosplash/pkg/resilience"
)

// dialConfig собирается функциональными опциями Dial. Отдельный тип, а не
// набор параметров функции, потому что часть настроек (breaker, retry)
// нужны не всем клиентам с одинаковыми значениями: клиент к wallet и клиент
// к media разумно настраивать разными таймаутами и порогами.
type dialConfig struct {
	insecureCreds bool
	keepalive     keepalive.ClientParameters
	retry         resilience.RetryConfig
	idempotency   resilience.Idempotency
	breaker       resilience.BreakerConfig
	extraDialOpts []grpc.DialOption
}

// DialOption настраивает Dial.
type DialOption func(*dialConfig)

// WithKeepalive переопределяет параметры keepalive-пингов клиента.
func WithKeepalive(p keepalive.ClientParameters) DialOption {
	return func(c *dialConfig) { c.keepalive = p }
}

// WithRetry переопределяет конфиг retry и ОБЯЗЫВАЕТ вызывающий код явно
// назвать идемпотентность оборачиваемых вызовов — см. resilience.NewRetrier.
// Если метод дергает несколько разных RPC с разной идемпотентностью
// (например, GetBalance и CommitFunds), это повод завести ДВА разных
// *grpc.ClientConn с разными Dial-опциями, а не один клиент "на всякий
// случай неидемпотентный": иначе идемпотентные чтения platят задержкой
// ретраев наравне с записями, для которых ретраи вообще запрещены.
func WithRetry(cfg resilience.RetryConfig, idempotency resilience.Idempotency) DialOption {
	return func(c *dialConfig) {
		c.retry = cfg
		c.idempotency = idempotency
	}
}

// WithBreaker переопределяет конфиг circuit breaker.
func WithBreaker(cfg resilience.BreakerConfig) DialOption {
	return func(c *dialConfig) { c.breaker = cfg }
}

// WithDialOptions добавляет произвольные grpc.DialOption — путь для TLS-кредов
// или специфичных для сервиса настроек, которые не стоит протаскивать через
// весь API grpcx.
func WithDialOptions(opts ...grpc.DialOption) DialOption {
	return func(c *dialConfig) { c.extraDialOpts = append(c.extraDialOpts, opts...) }
}

func defaultDialConfig(target string) dialConfig {
	return dialConfig{
		insecureCreds: true,
		keepalive: keepalive.ClientParameters{
			Time:                20 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		},
		retry:       resilience.DefaultRetryConfig(),
		idempotency: resilience.NotIdempotent, // безопасный дефолт: без явного WithRetry(..., Idempotent) ретраев нет
		breaker:     resilience.DefaultBreakerConfig(target),
	}
}

// Dial открывает клиентское соединение до target с балансировкой, keepalive,
// трассировкой и устойчивостью (retry + circuit breaker) из pkg/resilience.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: почему round_robin, и почему это не работает без
// headless Service в Kubernetes (см. также docs/adr/0015).
//
// gRPC работает поверх HTTP/2, а HTTP/2 мультиплексирует МНОЖЕСТВО запросов
// в ОДНОМ TCP-соединении. Обычный Kubernetes Service (ClusterIP) — это
// балансировщик УРОВНЯ СОЕДИНЕНИЯ: kube-proxy выбирает под один раз, в
// момент установления TCP-соединения, и дальше все запросы этого клиента,
// сколько бы их ни было, идут в ОДИН И ТОТ ЖЕ под, пока соединение живо
// (а gRPC-клиент держит его открытым намеренно, это и есть смысл HTTP/2).
// При трёх репликах сервиса результат — вся нагрузка одного клиента на
// одном поде, а не поровну на трёх.
//
// round_robin — это балансировка НА СТОРОНЕ КЛИЕНТА, между вызовами, а не
// между соединениями: клиент сам открывает соединение к КАЖДОМУ адресу,
// которые ему возвращает резолвер, и раздаёт запросы по кругу. Чтобы это
// сработало, резолвер обязан вернуть адреса ВСЕХ подов, а не один
// виртуальный IP — а обычный ClusterIP как раз и есть один виртуальный IP.
// Отсюда headless Service (ClusterIP: None): DNS-запрос к нему возвращает
// A-записи всех подов напрямую, и встроенный DNS-резолвер grpc клиента
// получает список адресов, по которому и работает round_robin.
//
// grpc.WithDefaultServiceConfig ниже задаёт round_robin ТОЛЬКО как service
// config по умолчанию — если target-сервис вернёт свой service config
// (например, через DNS TXT-запись), тот победит. Для этого проекта такое
// не настраивается, но так ведёт себя сам grpc-go, и это стоит знать.
func Dial(target string, opts ...DialOption) (*grpc.ClientConn, error) {
	cfg := defaultDialConfig(target)
	for _, opt := range opts {
		opt(&cfg)
	}

	retrier := resilience.NewRetrier(cfg.retry, cfg.idempotency)
	breaker := resilience.NewBreaker(cfg.breaker)

	dialOpts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
		grpc.WithKeepaliveParams(cfg.keepalive),
		// StatsHandler, а не interceptor: он единственный видит СЫРУЮ
		// передачу данных (в том числе стримы) и умеет мерить размер
		// сообщений — так же, как на сервере в текущих main.go сервисов.
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		// Порядок важен: breaker СНАРУЖИ retry. Если сервис уже признан
		// мёртвым (Open), запрос обязан провалиться мгновенно ДО того, как
		// retry успеет начать умножать его на MaxAttempts с паузами между
		// ними — иначе "мгновенный 503" на деле занял бы несколько секунд
		// ожидания между ретраями поверх мгновенных отказов breaker'а.
		// Обратный порядок (retry снаружи breaker) означал бы, что каждая
		// повторная попытка внутри retry ЗАНОВО спрашивает breaker, и общая
		// задержка запроса всё равно складывается из пауз retry.
		grpc.WithChainUnaryInterceptor(
			breakerUnaryClientInterceptor(breaker),
			retryUnaryClientInterceptor(retrier),
		),
	}

	if cfg.insecureCreds {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	// extraDialOpts — в конце: если вызывающий код передал свои TransportCredentials
	// через WithDialOptions, они обязаны победить дефолтный insecure.
	dialOpts = append(dialOpts, cfg.extraDialOpts...)

	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("grpcx: dial %s: %w", target, err)
	}
	return conn, nil
}

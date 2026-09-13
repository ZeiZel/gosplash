// Package redisx — единственная точка входа в Redis для сервисов проекта.
//
// Пакет закрывает пять паттернов, которые в фазе 2 нужны разным сервисам:
// cache-aside со стемпед-защитой (cache.go), rate limiting на Lua (ratelimit.go),
// распределённый лок (lock.go), idempotency-key (idempotency.go), топ-N через
// Sorted Set (topn.go) и приблизительный подсчёт уникальных через HyperLogLog
// (hll.go). Он инфраструктурный: бизнес-правил («сколько стоит лицензия»,
// «что считать просмотром») здесь нет и не будет — они остаются в сервисах,
// пакет лишь предоставляет примитивы поверх go-redis.
//
// Ключевое решение: ни один метод пакета не принимает голый ключ. Единственный
// способ получить ключ — Client.Key(...), который всегда строит его в виде
// "gosplash:<service>:<parts...>". Без этого правила разные сервисы рано или
// поздно случайно наступят на чужие ключи (catalog положит что-то по ключу
// "user:42", который media уже использует под свои нужды) — а на namespace
// в общем инстансе Redis (см. deploy/compose/redis.yml) полагается вся
// логическая изоляция между сервисами.
package redisx

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/singleflight"
)

// Config — параметры подключения. Сознательно НЕ импортирует pkg/config:
// пакет обязан собираться и тестироваться независимо от остального проекта,
// а разбор переменных окружения — забота исключительно pkg/config (см.
// docs/STYLE.md, раздел «Конфигурация»).
type Config struct {
	Addr     string
	Password string
	DB       int

	PoolSize   int
	MaxRetries int

	DialTimeout time.Duration
	ReadTimeout time.Duration

	// ServiceName — тот же namespace, что во всех остальных pkg/*x: попадает
	// и в ключи (gosplash:<service>:...), и в label service метрик hit/miss.
	// Обязателен: без него ключи всех сервисов легли бы в один namespace
	// "gosplash::..." и стали бы неразличимы при инвалидации по SCAN.
	ServiceName string
}

// Client — обёртка над *redis.Client с namespace'ом ключей и метриками
// кэша. Один Client на процесс, как и *gorm.DB в pkg/dbx.
type Client struct {
	rdb     *redis.Client
	service string

	sf singleflight.Group

	hits   metric.Int64Counter
	misses metric.Int64Counter
}

// New поднимает клиента: TCP-пул с явными таймаутами и повторами, трассировку
// через redisotel (спан на каждую команду — без него запрос в Redis невидим
// в трейсе, и «почему запрос к каталогу занял 40 мс» не разбирается) и
// метрики пула через тот же redisotel (соединения, ошибки — то, что не
// связано ни с одним конкретным ключом и потому не влезает в hits/misses).
func New(cfg Config) (*Client, error) {
	if cfg.ServiceName == "" {
		return nil, fmt.Errorf("redisx: ServiceName обязателен — иначе ключи " +
			"всех сервисов лягут в один namespace")
	}
	if cfg.Addr == "" {
		return nil, fmt.Errorf("redisx: Addr пуст")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:        cfg.Addr,
		Password:    cfg.Password,
		DB:          cfg.DB,
		PoolSize:    cfg.PoolSize,
		MaxRetries:  cfg.MaxRetries,
		DialTimeout: cfg.DialTimeout,
		ReadTimeout: cfg.ReadTimeout,
	})

	// Трассировка: спан на Eval/Get/Set и т. п. с traceparent, унаследованным
	// из ctx запроса — так HTTP-спан из pkg/httpx и спан похода в Redis
	// оказываются в одном трейсе.
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		return nil, fmt.Errorf("redisx: трассировка: %w", err)
	}
	// Метрики пула и длительности команд — хуками go-redis, а не руками:
	// go-redis сам знает, когда команда реально ушла в сеть, а когда обошлась
	// локальным кэшем клиента.
	if err := redisotel.InstrumentMetrics(rdb); err != nil {
		return nil, fmt.Errorf("redisx: метрики: %w", err)
	}

	meter := otel.Meter("gosplash/redisx")
	hits, err := meter.Int64Counter(
		"redis_cache_hits_total",
		metric.WithDescription("Попадания в кэш cache-aside (pkg/redisx)"),
	)
	if err != nil {
		return nil, fmt.Errorf("redisx: счётчик hits: %w", err)
	}
	misses, err := meter.Int64Counter(
		"redis_cache_misses_total",
		metric.WithDescription("Промахи кэша cache-aside (pkg/redisx)"),
	)
	if err != nil {
		return nil, fmt.Errorf("redisx: счётчик misses: %w", err)
	}

	return &Client{
		rdb:     rdb,
		service: cfg.ServiceName,
		hits:    hits,
		misses:  misses,
	}, nil
}

// Key строит единственный легальный вид ключа в проекте:
// gosplash:<service>:<parts, склеенные через ":">.
//
// Первый элемент parts трактуется как «вид» значения (kind) и используется
// как одноимённый label в метриках hits/misses — так по одному ключу можно
// понять, что именно кэшируется, не заводя для этого отдельный параметр
// в каждом методе пакета (см. kindFromKey в cache.go).
func (c *Client) Key(parts ...string) string {
	all := make([]string, 0, len(parts)+2)
	all = append(all, "gosplash", c.service)
	all = append(all, parts...)
	return strings.Join(all, ":")
}

// kindFromKey достаёт «вид» значения из ключа, построенного через Key: третий
// сегмент, gosplash:<service>:<kind>:... Ключ, собранный НЕ через Key (в
// пакете таких быть не должно, но тест на это есть), даёт "unknown" —
// потерять метрику не страшно, а вот запаниковать на чужом формате ключа
// в проде — очень.
func kindFromKey(key string) string {
	parts := strings.SplitN(key, ":", 4)
	if len(parts) < 3 {
		return "unknown"
	}
	return parts[2]
}

func (c *Client) recordHit(ctx context.Context, key string) {
	c.hits.Add(ctx, 1, metric.WithAttributes(
		attribute.String("service", c.service),
		attribute.String("kind", kindFromKey(key)),
	))
}

func (c *Client) recordMiss(ctx context.Context, key string) {
	c.misses.Add(ctx, 1, metric.WithAttributes(
		attribute.String("service", c.service),
		attribute.String("kind", kindFromKey(key)),
	))
}

// Ping — для /readyz (см. pkg/httpx.Health.Register). Промах Redis не должен
// валить сервис насмерть (кэш — это оптимизация), но readiness обязан
// показать деградацию: без кэша каждый запрос идёт мимо в базу, и это то,
// что оператор должен увидеть раньше, чем база начнёт захлёбываться.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Close закрывает пул соединений. Вызывается при graceful shutdown.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Raw отдаёт обёрнутый *redis.Client для команд, которых в pkg/redisx
// сознательно нет (в инфраструктурном пакете невозможно предугадать каждую
// нужную сервису команду). Не escape hatch от namespace: ключи для Raw
// собираются через Key(...) точно так же, как и для остальных методов —
// иначе смысл namespace теряется в первом же вызове.
func (c *Client) Raw() *redis.Client {
	return c.rdb
}

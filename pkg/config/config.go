// Package config — единственное место, где проект читает переменные окружения.
//
// Правило: os.Getenv вызывается ТОЛЬКО здесь. Дальше по коду ходит уже
// разобранная структура. Иначе через полгода на вопрос «какие переменные нужны
// сервису» придётся отвечать grep'ом по всему репозиторию.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Media     MediaConfig
	Catalog   CatalogConfig
	Thumbnail ThumbnailConfig
	Outbox    OutboxConfig
	Kafka     KafkaConfig
	S3        S3Config
	Wallet    WalletConfig
	Order     OrderConfig
	Analytics AnalyticsConfig
	Search    SearchConfig
	Redis     RedisConfig
	Observe   ObserveConfig
	GRPC      GRPCConfig
}

// WalletConfig — кошелёк. Одна база, без шардов и без реплик.
//
// Шардировать счета нельзя в принципе: перевод между двумя счетами на разных
// серверах перестал бы быть транзакцией. Это не «медленнее» — это неверно,
// и никакая сага здесь не спасает, потому что деньги ВНУТРИ одного сервиса
// обязаны двигаться атомарно.
//
// Реплики тоже нет: баланс читают ровно перед тем, как его изменить, и
// отставшая реплика дала бы разрешение потратить уже потраченное.
type WalletConfig struct {
	DSN         string
	HTTPAddr    string
	GRPCAddr    string
	MetricsAddr string
}

// AnalyticsConfig — аналитика на ClickHouse.
type AnalyticsConfig struct {
	GRPCAddr    string
	HTTPAddr    string
	MetricsAddr string

	ClickHouse ClickHouseConfig

	KafkaConsumerGroup string

	// Батчи вставки. Построчная вставка в ClickHouse — антипаттерн: каждая
	// вставка создаёт отдельный парт на диске, а фоновые мержи за потоком
	// мелких партов не успевают. Просмотров и покупок разное количество на
	// порядки, поэтому пределы у них свои.
	ViewBatchSize              int
	ViewBatchFlushInterval     time.Duration
	PurchaseBatchSize          int
	PurchaseBatchFlushInterval time.Duration
}

type ClickHouseConfig struct {
	// Addr — НАТИВНЫЙ протокол (9000 внутри сети, 59440 с хоста), а не HTTP:
	// им ходит clickhouse-go, и он заметно экономнее по трафику.
	Addr     string
	Database string
	User     string
	Password string

	DialTimeout time.Duration
	ReadTimeout time.Duration
}

// SearchConfig — поиск на Elasticsearch.
type SearchConfig struct {
	Addrs []string
	// IndexAlias — имя, по которому сервис ходит в индекс. Именно ALIAS,
	// а не индекс: переиндексация создаёт новый индекс и атомарно
	// переключает на него алиас, и клиенты этого не замечают.
	IndexAlias string

	GRPCAddr    string
	HTTPAddr    string
	MetricsAddr string

	KafkaConsumerGroup string
}

// OrderConfig — заказы и сага.
type OrderConfig struct {
	DSN         string
	HTTPAddr    string
	GRPCAddr    string
	MetricsAddr string

	// Temporal. Адрес С ХОСТА — localhost:57233, а не 7233: порты проекта
	// вынесены в диапазон 5xxxx. Внутри docker-сети это temporal:7233,
	// и на этой разнице спотыкаются все.
	TemporalAddress   string
	TemporalNamespace string
	TemporalTaskQueue string

	// IdempotencyTTL — сколько живёт ключ идемпотентности. Сутки — это ответ
	// на вопрос «в течение какого времени повтор запроса всё ещё считается
	// тем же самым запросом», а не технический параметр кэша.
	IdempotencyTTL time.Duration

	// Кого зовёт сага.
	WalletGRPCTarget  string
	CatalogGRPCTarget string
}

// GRPCConfig — общее для всех gRPC-серверов проекта.
type GRPCConfig struct {
	// JWTSecret — симметричный ключ для проверки подписи токенов в
	// интерсепторе аутентификации. Симметричный, потому что токены выпускает
	// и проверяет один и тот же контур; как только появится внешний
	// издатель, ключ станет асимметричным, и это будет отдельное решение.
	//
	// Пустое значение означает, что проверка выключена, — допустимо только
	// локально. Сервис обязан сказать об этом в лог при старте, иначе
	// выключенная аутентификация уедет в прод незамеченной.
	JWTSecret string

	// DefaultTimeout — дедлайн, который сервер ставит сам, если клиент
	// не прислал свой. Без него запрос от клиента без дедлайна может висеть
	// вечно, занимая горутину и соединение.
	DefaultTimeout time.Duration
}

// MediaConfig — media-сервис, шардированное хранилище.
type MediaConfig struct {
	// ShardDSNs — по одному DSN на шард, порядок важен: индекс в этом срезе
	// и есть номер шарда. Поменяешь порядок местами — половина фотографий
	// «потеряется», потому что hash(user_id) будет указывать не туда.
	ShardDSNs   []string
	HTTPAddr    string
	GRPCAddr    string
	MetricsAddr string

	// Лимит загрузок на пользователя: token bucket в Redis.
	// Capacity — размер «вёдра», то есть допустимый всплеск; RefillPerSec —
	// устойчивая скорость. Два числа, а не одно, потому что «10 в минуту»
	// не отвечает на вопрос, можно ли сделать 10 подряд.
	UploadRateCapacity     int
	UploadRateRefillPerSec float64
}

// CatalogConfig — catalog-сервис, реплицированное хранилище.
type CatalogConfig struct {
	// PrimaryDSN — сюда идут все записи (их делает Kafka-консьюмер).
	PrimaryDSN string
	// ReplicaDSN — сюда идут все чтения (их делает публичный HTTP API).
	// Если пусто, чтения пойдут в primary: с одной базой всё тоже работает,
	// просто без разделения нагрузки.
	ReplicaDSN  string
	HTTPAddr    string
	GRPCAddr    string
	MetricsAddr string
	// CacheTTL — базовый срок жизни карточки в кэше. К нему добавляется
	// джиттер: одинаковый TTL у тысячи ключей даёт синхронное протухание
	// и залп в базу.
	CacheTTL time.Duration

	// Батчер просмотров. Событий analytics.photo.viewed на порядки больше,
	// чем всех остальных вместе, поэтому они копятся в памяти и уезжают
	// пачками. Два предела, а не один: по размеру — чтобы буфер не рос
	// бесконечно под нагрузкой, по времени — чтобы при редких просмотрах
	// событие не лежало в буфере часами.
	ViewBatchSize     int
	ViewFlushInterval time.Duration
	// MediaGRPCTarget — адрес media-сервиса. Получив событие из Kafka,
	// catalog идёт сюда за подробностями о фото.
	MediaGRPCTarget string
}

// ThumbnailConfig — воркер превью.
type ThumbnailConfig struct {
	HTTPAddr    string
	MetricsAddr string

	// Размеры по длинной стороне, в пикселях.
	Sizes []int
	// Размер пула воркеров. Ноль — runtime.NumCPU(). Ресайз CPU-bound:
	// горутина на сообщение даст тысячу параллельных ресайзов и OOM.
	Workers int
	// LockTTL — срок жизни лока на photo_id. Должен быть заметно больше
	// типичного времени обработки, иначе watchdog не успеет продлить.
	LockTTL time.Duration
}

// OutboxConfig — relay. Общий для всех сервисов, у которых есть outbox.
type OutboxConfig struct {
	PollInterval time.Duration
	BatchSize    int
}

type KafkaConfig struct {
	Brokers []string
	// Группы консьюмеров. Все процессы с одинаковой группой ДЕЛЯТ партиции
	// между собой; с разными — каждый читает весь топик целиком.
	CatalogGroup   string
	ThumbnailGroup string
}

type S3Config struct {
	Endpoint         string
	AccessKey        string
	SecretKey        string
	UseSSL           bool
	BucketOriginals  string
	BucketThumbnails string
	PresignedTTL     time.Duration
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	PoolSize int

	// Таймауты и ретраи задаются явно по той же причине, что и у PostgreSQL:
	// значения по умолчанию у клиента щедрые, и подвисший Redis превращается
	// в подвисший сервис. Кэш обязан отваливаться быстро — это кэш, а не
	// источник правды.
	MaxRetries  int
	DialTimeout time.Duration
	ReadTimeout time.Duration
}

// ObserveConfig — наблюдаемость. Одинакова для всех сервисов, поэтому лежит
// отдельно от их конфигов.
type ObserveConfig struct {
	// OTLPEndpoint — host:port коллектора без схемы. Пусто — трейсы никуда
	// не отправляются, но сервис работает: отсутствие коллектора не должно
	// мешать локальному запуску.
	OTLPEndpoint string
	Environment  string
	// SampleRatio — доля трассируемых запросов. Локально 1.0.
	SampleRatio float64
	// LogLevel — debug | info | warn | error.
	LogLevel string
}

// Load читает .env (если он есть) и собирает конфиг.
//
// Отсутствие .env — не ошибка: в docker или CI переменные приходят из окружения,
// а файла нет. Ошибка — отсутствие обязательного значения, и тогда лучше упасть
// на старте, чем через час словить панику на nil-соединении.
func Load() *Config {
	if err := godotenv.Load(); err != nil {
		slog.Info("config: .env не найден, читаю переменные окружения")
	}

	return &Config{
		Media: MediaConfig{
			ShardDSNs:   shardDSNs(),
			HTTPAddr:    env("MEDIA_HTTP_ADDR", ":8101"),
			GRPCAddr:    env("MEDIA_GRPC_ADDR", ":9101"),
			MetricsAddr: env("MEDIA_METRICS_ADDR", ":8201"),

			UploadRateCapacity:     integer("MEDIA_UPLOAD_RATE_CAPACITY", 10),
			UploadRateRefillPerSec: ratioAny("MEDIA_UPLOAD_RATE_REFILL_PER_SEC", 0.5),
		},
		Catalog: CatalogConfig{
			PrimaryDSN:  env("CATALOG_DSN", ""),
			ReplicaDSN:  env("CATALOG_REPLICA_DSN", ""),
			HTTPAddr:    env("CATALOG_HTTP_ADDR", ":8102"),
			GRPCAddr:    env("CATALOG_GRPC_ADDR", ":9102"),
			MetricsAddr: env("CATALOG_METRICS_ADDR", ":8202"),
			CacheTTL:    duration("CACHE_TTL", 10*time.Minute),

			ViewBatchSize:     integer("CATALOG_VIEW_BATCH_SIZE", 200),
			ViewFlushInterval: duration("CATALOG_VIEW_FLUSH_INTERVAL", 2*time.Second),
			MediaGRPCTarget:   env("MEDIA_GRPC_TARGET", "localhost:9101"),
		},
		Thumbnail: ThumbnailConfig{
			HTTPAddr:    env("THUMBNAIL_HTTP_ADDR", ":8105"),
			MetricsAddr: env("THUMBNAIL_METRICS_ADDR", ":8203"),
			Sizes:       ints("THUMBNAIL_SIZES", []int{320, 800, 1600}),
			Workers:     integer("THUMBNAIL_WORKERS", 0),
			LockTTL:     duration("THUMBNAIL_LOCK_TTL", 30*time.Second),
		},
		Outbox: OutboxConfig{
			PollInterval: duration("OUTBOX_POLL_INTERVAL", time.Second),
			BatchSize:    integer("OUTBOX_BATCH_SIZE", 100),
		},
		Kafka: KafkaConfig{
			Brokers:        strings.Split(env("KAFKA_BROKERS", "localhost:59094"), ","),
			CatalogGroup:   env("KAFKA_CONSUMER_GROUP_CATALOG", "catalog"),
			ThumbnailGroup: env("KAFKA_CONSUMER_GROUP_THUMBNAIL", "thumbnail-worker"),
		},
		S3: S3Config{
			Endpoint:         env("S3_ENDPOINT", "localhost:59000"),
			AccessKey:        env("S3_ACCESS_KEY", "gosplash"),
			SecretKey:        env("S3_SECRET_KEY", "gosplash"),
			UseSSL:           env("S3_USE_SSL", "false") == "true",
			BucketOriginals:  env("S3_BUCKET_ORIGINALS", "gosplash-originals"),
			BucketThumbnails: env("S3_BUCKET_THUMBNAILS", "gosplash-thumbnails"),
			PresignedTTL:     duration("S3_PRESIGNED_TTL", 15*time.Minute),
		},
		Wallet: WalletConfig{
			DSN:         env("WALLET_DSN", ""),
			HTTPAddr:    env("WALLET_HTTP_ADDR", ":8103"),
			GRPCAddr:    env("WALLET_GRPC_ADDR", ":9103"),
			MetricsAddr: env("WALLET_METRICS_ADDR", ":8204"),
		},
		Order: OrderConfig{
			DSN:               env("ORDER_DSN", ""),
			HTTPAddr:          env("ORDER_HTTP_ADDR", ":8104"),
			GRPCAddr:          env("ORDER_GRPC_ADDR", ":9104"),
			MetricsAddr:       env("ORDER_METRICS_ADDR", ":8205"),
			TemporalAddress:   env("TEMPORAL_ADDRESS", "localhost:57233"),
			TemporalNamespace: env("TEMPORAL_NAMESPACE", "gosplash"),
			TemporalTaskQueue: env("TEMPORAL_TASK_QUEUE", "order-saga"),
			IdempotencyTTL:    duration("IDEMPOTENCY_TTL", 24*time.Hour),
			WalletGRPCTarget:  env("WALLET_GRPC_TARGET", "localhost:9103"),
			CatalogGRPCTarget: env("CATALOG_GRPC_TARGET", "localhost:9102"),
		},
		Analytics: AnalyticsConfig{
			GRPCAddr:    env("ANALYTICS_GRPC_ADDR", ":9106"),
			HTTPAddr:    env("ANALYTICS_HTTP_ADDR", ":8106"),
			MetricsAddr: env("ANALYTICS_METRICS_ADDR", ":8206"),
			ClickHouse: ClickHouseConfig{
				Addr:        env("CLICKHOUSE_ADDR", "localhost:59440"),
				Database:    env("CLICKHOUSE_DB", "gosplash"),
				User:        env("CLICKHOUSE_USER", "gosplash"),
				Password:    env("CLICKHOUSE_PASSWORD", "gosplash"),
				DialTimeout: duration("CLICKHOUSE_DIAL_TIMEOUT", 5*time.Second),
				ReadTimeout: duration("CLICKHOUSE_READ_TIMEOUT", 30*time.Second),
			},
			KafkaConsumerGroup:         env("KAFKA_CONSUMER_GROUP_ANALYTICS", "analytics"),
			ViewBatchSize:              integer("ANALYTICS_CH_BATCH_SIZE", 1000),
			ViewBatchFlushInterval:     duration("ANALYTICS_CH_FLUSH_INTERVAL", 5*time.Second),
			PurchaseBatchSize:          integer("ANALYTICS_CH_PURCHASE_BATCH_SIZE", 100),
			PurchaseBatchFlushInterval: duration("ANALYTICS_CH_PURCHASE_FLUSH_INTERVAL", 5*time.Second),
		},
		Search: SearchConfig{
			Addrs:              strings.Split(env("ELASTICSEARCH_ADDRS", "http://localhost:59200"), ","),
			IndexAlias:         env("SEARCH_INDEX_ALIAS", "listings"),
			GRPCAddr:           env("SEARCH_GRPC_ADDR", ":9107"),
			HTTPAddr:           env("SEARCH_HTTP_ADDR", ":8107"),
			MetricsAddr:        env("SEARCH_METRICS_ADDR", ":8207"),
			KafkaConsumerGroup: env("KAFKA_CONSUMER_GROUP_SEARCH", "search"),
		},
		Redis: RedisConfig{
			Addr:        env("REDIS_ADDR", "localhost:56379"),
			Password:    env("REDIS_PASSWORD", ""),
			DB:          integer("REDIS_DB", 0),
			PoolSize:    integer("REDIS_POOL_SIZE", 20),
			MaxRetries:  integer("REDIS_MAX_RETRIES", 3),
			DialTimeout: duration("REDIS_DIAL_TIMEOUT", 2*time.Second),
			ReadTimeout: duration("REDIS_READ_TIMEOUT", 500*time.Millisecond),
		},
		GRPC: GRPCConfig{
			JWTSecret:      env("JWT_SECRET", ""),
			DefaultTimeout: duration("GRPC_DEFAULT_TIMEOUT", 5*time.Second),
		},
		Observe: ObserveConfig{
			OTLPEndpoint: env("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:54317"),
			Environment:  env("ENVIRONMENT", "local"),
			SampleRatio:  ratio("OTEL_TRACE_SAMPLE_RATIO", 1.0),
			LogLevel:     env("LOG_LEVEL", "info"),
		},
	}
}

// shardDSNs собирает MEDIA_SHARD_0_DSN, MEDIA_SHARD_1_DSN, … пока они идут
// подряд. Добавить третий шард — это дописать строку в .env, а не править код.
func shardDSNs() []string {
	var dsns []string
	for i := 0; ; i++ {
		dsn := os.Getenv(fmt.Sprintf("MEDIA_SHARD_%d_DSN", i))
		if dsn == "" {
			return dsns
		}
		dsns = append(dsns, dsn)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func duration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("config: не разобрал длительность, беру значение по умолчанию",
			"key", key, "value", v, "fallback", fallback)
		return fallback
	}
	return d
}

func integer(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn("config: не разобрал число, беру значение по умолчанию",
			"key", key, "value", v, "fallback", fallback)
		return fallback
	}
	return n
}

func ratio(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 || f > 1 {
		slog.Warn("config: доля должна быть числом от 0 до 1, беру значение по умолчанию",
			"key", key, "value", v, "fallback", fallback)
		return fallback
	}
	return f
}

// ints разбирает список чисел через запятую: "320,800,1600".
func ints(key string, fallback []int) []int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}

	parts := strings.Split(v, ",")
	result := make([]int, 0, len(parts))
	for _, part := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n <= 0 {
			slog.Warn("config: список чисел испорчен, беру значение по умолчанию",
				"key", key, "value", v, "fallback", fallback)
			return fallback
		}
		result = append(result, n)
	}
	return result
}

// ratioAny — дробное число без ограничения диапазоном 0..1.
// Отдельно от ratio намеренно: там ограничение осмысленно (доля не бывает
// больше единицы), здесь оно мешало бы задать скорость пополнения больше
// одного токена в секунду.
func ratioAny(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		slog.Warn("config: ожидалось положительное число, беру значение по умолчанию",
			"key", key, "value", v, "fallback", fallback)
		return fallback
	}
	return f
}

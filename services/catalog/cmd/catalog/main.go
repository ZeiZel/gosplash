// catalog-service — публичный read-API, gRPC-контракт и индексатор каталога.
//
// Несколько независимых занятий в одном процессе:
//
//	gRPC :9102        — CatalogService (proto/gosplash/catalog/v1): читает
//	                    с РЕПЛИКИ, лицензии — с primary.
//	HTTP :8102        — тот же контракт тонким REST-фасадом поверх gRPC
//	                    (см. internal/adapters/http) плюс /catalog/replication
//	                    и /catalog/top, которых в contract'е нет.
//	Kafka-консьюмеры  — слушают media.photo.uploaded и
//	                    media.photo.thumbnail-ready, идемпотентно наполняют
//	                    витрину и публикуют catalog.listing.published через
//	                    outbox.
//	Retry-консьюмеры  — kafkax.NewRetryConsumer по каждому из топиков.
//	Outbox-relay      — публикует catalog.listing.published в Kafka.
//	View-batcher      — пакетно публикует analytics.photo.viewed (без outbox,
//	                    см. internal/adapters/kafka/view_batcher.go).
//	HTTP :8202        — служебный: /metrics и /debug/pprof.
//
// main.go разделён на три части (PATTERN: composition root, по образцу
// ZeiZel/gomple/cmd/main.go):
//
//	bootstrap.NewApp — чистая сборка серверов из уже готовых сценариев,
//	                   живёт в internal/bootstrap и потому тестируется
//	                   без докера (см. internal/bootstrap/app_test.go).
//	run              — жизненный цикл: конфиг, соединения, запуск, сигнал,
//	                   graceful shutdown. Возвращает ошибку, а не зовёт
//	                   os.Exit — лекарство от gocritic exitAfterDefer: пока
//	                   os.Exit не вызван НИ РАЗУ до возврата из run, все
//	                   defer гарантированно отрабатывают.
//	main             — три строки: run, лог ошибки, os.Exit(1).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
	mediav1 "gosplash/gen/go/gosplash/media/v1"
	"gosplash/pkg/config"
	"gosplash/pkg/dbx"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"
	"gosplash/pkg/kafkax"
	"gosplash/pkg/otelx"
	"gosplash/pkg/outbox"
	"gosplash/pkg/redisx"
	"gosplash/pkg/resilience"

	cataloggrpc "gosplash/services/catalog/internal/adapters/grpc"
	catalogkafka "gosplash/services/catalog/internal/adapters/kafka"
	"gosplash/services/catalog/internal/adapters/pg"
	catalogredis "gosplash/services/catalog/internal/adapters/redis"
	"gosplash/services/catalog/internal/app"
	"gosplash/services/catalog/internal/bootstrap"
)

const serviceName = "catalog"

func run() error {
	conf := config.Load()

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
		return err
	}

	// ── База: primary + реплика за одним соединением ─────────────────────────
	database, err := dbx.OpenWithReplica(conf.Catalog.PrimaryDSN, conf.Catalog.ReplicaDSN)
	if err != nil {
		return err
	}
	if conf.Catalog.ReplicaDSN == "" {
		slog.Warn("catalog: реплика не настроена, читаю из primary")
	}

	// ── Redis: кэш карточек и счётчик просмотров ──────────────────────────────
	redisClient, err := redisx.New(redisx.Config{
		Addr:        conf.Redis.Addr,
		Password:    conf.Redis.Password,
		DB:          conf.Redis.DB,
		PoolSize:    conf.Redis.PoolSize,
		MaxRetries:  conf.Redis.MaxRetries,
		DialTimeout: conf.Redis.DialTimeout,
		ReadTimeout: conf.Redis.ReadTimeout,
		ServiceName: serviceName,
	})
	if err != nil {
		return err
	}
	defer redisClient.Close()

	// ── gRPC-клиент к media: ТОЛЬКО чтение (GetPhoto) ───────────────────────
	// grpcx.Dial, а не голый grpc.NewClient: keepalive-пинги, round_robin,
	// retry и circuit breaker — единый путь всех клиентов монорепы (так же,
	// как services/order/cmd/order/main.go ходит в catalog).
	// resilience.Idempotent — безопасно ретраить на транспорте: catalog зовёт
	// у media только GetPhoto, это чтение. Соединение остаётся ленивым (внутри
	// grpcx.Dial тот же grpc.NewClient): подключится при первом вызове, поэтому
	// порядок запуска сервисов не важен — catalog спокойно стартует раньше media.
	mediaConn, err := grpcx.Dial(conf.Catalog.MediaGRPCTarget,
		grpcx.WithRetry(resilience.DefaultRetryConfig(), resilience.Idempotent),
		grpcx.WithBreaker(resilience.DefaultBreakerConfig("media")),
	)
	if err != nil {
		return fmt.Errorf("catalog: gRPC к media: %w", err)
	}
	defer func() {
		if err := mediaConn.Close(); err != nil {
			slog.Error("catalog: закрытие соединения с media", "error", err)
		}
	}()

	// ── Kafka: продюсер (просмотры) и консьюмеры ──────────────────────────────
	producer, err := kafkax.NewProducer(conf.Kafka.Brokers, serviceName)
	if err != nil {
		return err
	}
	defer producer.Close()

	// Два топика — ДВЕ разные consumer group, не одна общая "catalog".
	//
	// kafkax.NewConsumer привязывает один kgo.Client к ОДНОМУ топику. Если бы
	// оба консьюмера присоединились к ОДНОЙ группе "catalog", но с разной
	// подпиской (uploaded vs thumbnail-ready), участники группы декларировали
	// бы РАЗНЫЕ наборы топиков — формально допустимо в протоколе Kafka, но
	// объединяет ребалансировку и метрики лага двух НИКАК не связанных между
	// собой потоков в одну учётную единицу без единой выгоды: партиции
	// uploaded и партиции thumbnail-ready всё равно не пересекаются и не
	// должны делиться друг с другом. Отдельная группа на топик — это чистый
	// домен ребалансировки и чистая метрика лага для каждого потока.
	thumbnailGroup := conf.Kafka.CatalogGroup + "-thumbnail"

	uploadedConsumer, err := kafkax.NewConsumer(conf.Kafka.Brokers, conf.Kafka.CatalogGroup, kafkax.TopicPhotoUploaded)
	if err != nil {
		return err
	}
	defer uploadedConsumer.Close()

	thumbnailConsumer, err := kafkax.NewConsumer(conf.Kafka.Brokers, thumbnailGroup, kafkax.TopicPhotoThumbnailReady)
	if err != nil {
		return err
	}
	defer thumbnailConsumer.Close()

	uploadedRetry, err := kafkax.NewRetryConsumer(conf.Kafka.Brokers, conf.Kafka.CatalogGroup+"-retry", kafkax.TopicPhotoUploaded)
	if err != nil {
		return err
	}
	defer uploadedRetry.Close()

	thumbnailRetry, err := kafkax.NewRetryConsumer(conf.Kafka.Brokers, thumbnailGroup+"-retry", kafkax.TopicPhotoThumbnailReady)
	if err != nil {
		return err
	}
	defer thumbnailRetry.Close()

	// ── Outbox-relay: catalog.listing.published → Kafka ───────────────────────
	relay, err := outbox.New(outbox.Config{
		ServiceName:     serviceName,
		Brokers:         conf.Kafka.Brokers,
		DSNs:            []string{conf.Catalog.PrimaryDSN},
		PollInterval:    conf.Outbox.PollInterval,
		BatchSize:       conf.Outbox.BatchSize,
		Retention:       conf.Outbox.Retention,
		CleanupInterval: conf.Outbox.CleanupInterval,
	})
	if err != nil {
		return err
	}
	defer relay.Close()

	// ── Слои ─────────────────────────────────────────────────────────────────
	listingRepo := pg.NewListingRepository(database)
	licenseRepo := pg.NewLicenseRepository(database)
	uow := pg.NewUnitOfWork(database)

	mediaClient := cataloggrpc.NewMediaClient(mediav1.NewMediaServiceClient(mediaConn))
	cache := catalogredis.NewCache(redisClient, conf.Catalog.CacheTTL)
	viewCounter := catalogredis.NewViewCounter(redisClient)
	// Close вызывается явно на graceful shutdown ниже (а не через defer),
	// чтобы буфер флашился ПОСЛЕ того, как консьюмеры остановились (иначе
	// новые просмотры могли бы прийти уже после флаша), но ДО закрытия
	// producer'а (defer выше закроет его позже) — иначе накопленные, но ещё
	// не отправленные события просмотров были бы потеряны молча вместо
	// честного флаша.
	viewBatcher := catalogkafka.NewViewBatcher(producer,
		conf.Catalog.ViewBatchSize, conf.Catalog.ViewFlushInterval)

	hub := app.NewWatchHub()
	indexer := app.NewIndexer(uow, mediaClient, cache, hub)
	listingService := app.NewListingService(listingRepo, cache, viewCounter, viewBatcher)
	licenseService := app.NewLicenseService(licenseRepo)

	// ── Готовность ───────────────────────────────────────────────────────────
	healthChecks := map[string]httpx.Check{
		"postgres":        func(ctx context.Context) error { return dbx.Ping(ctx, database) },
		"kafka_uploaded":  uploadedConsumer.Ping,
		"kafka_thumbnail": thumbnailConsumer.Ping,
		"kafka_producer":  producer.Ping,
		"redis":           redisClient.Ping,
		"outbox":          relay.Ping,
	}

	// ── Сборка приложения ────────────────────────────────────────────────────
	catalogApp, err := bootstrap.NewApp(bootstrap.Deps{
		ServiceName: serviceName,
		HTTPAddr:    conf.Catalog.HTTPAddr,
		GRPCAddr:    conf.Catalog.GRPCAddr,
		MetricsAddr: conf.Catalog.MetricsAddr,

		JWTSecret:   conf.GRPC.JWTSecret,
		GRPCTimeout: conf.GRPC.DefaultTimeout,
		// Чтение каталога — публичная витрина: браузер и grpcurl обязаны
		// достучаться до неё без токена, как до любого сайта фотостока.
		// GrantLicense/RevokeLicense остаются под аутентификацией — это
		// шаги саги заказа, а не витрина.
		PublicMethods: []string{
			catalogv1.CatalogService_GetListing_FullMethodName,
			catalogv1.CatalogService_ListListings_FullMethodName,
			catalogv1.CatalogService_WatchListing_FullMethodName,
		},

		ListingService: listingService,
		LicenseService: licenseService,
		Hub:            hub,

		HealthChecks: healthChecks,

		Consumers: []bootstrap.ConsumerSpec{
			{Name: "uploaded", Consumer: uploadedConsumer, Handle: indexer.HandlePhotoUploaded, AwaitOnShutdown: true},
			{Name: "thumbnail-ready", Consumer: thumbnailConsumer, Handle: indexer.HandlePhotoThumbnailReady, AwaitOnShutdown: true},
			// Retry-консьюмеры ДО рефакторинга не имели done-канала и не
			// дожидались остановки — поведение сохранено (AwaitOnShutdown: false).
			{Name: "uploaded-retry", Consumer: uploadedRetry, Handle: indexer.HandlePhotoUploaded, AwaitOnShutdown: false},
			{Name: "thumbnail-ready-retry", Consumer: thumbnailRetry, Handle: indexer.HandlePhotoThumbnailReady, AwaitOnShutdown: false},
		},
		Relay: relay,
	})
	if err != nil {
		return err
	}
	// App.Close подчищает HTTP/gRPC серверы, которые собрала сама NewApp —
	// ставим defer сразу после успешной сборки, чтобы он сработал на любом
	// пути выхода из run, включая ошибку самого Run ниже.
	defer func() {
		if err := catalogApp.Close(); err != nil {
			slog.Error("catalog: закрытие приложения", "error", err)
		}
	}()

	runErr := catalogApp.Run(ctx)

	// Буфер просмотров сбрасывается ДО закрытия producer'а (defer выше
	// закроет его позже уже после return) — иначе накопленные, но ещё не
	// отправленные события просмотров были бы потеряны молча вместо
	// честного флаша.
	viewBatcher.Close()

	// Наблюдаемость гасится ПОСЛЕДНЕЙ: иначе спаны и метрики самой остановки
	// не успеют уехать, а это ровно то время, которое интересно смотреть,
	// когда выкатка прошла плохо.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := shutdownOtel(shutdownCtx); err != nil {
		slog.Error("catalog: otel shutdown", "error", err)
	}

	return runErr
}

// main — три строки. os.Exit здесь безопасен: он выполняется ПОСЛЕ того,
// как run() вернул управление, то есть после того, как отработали все её
// defer (закрытие БД, Redis, Kafka-клиентов, outbox-relay). Если бы os.Exit
// стоял внутри run на путях ошибок (как было до рефакторинга), эти defer
// пропускались бы — это и есть gocritic exitAfterDefer, который ловит CI.
func main() {
	if err := run(); err != nil {
		slog.Error("catalog: приложение остановлено с ошибкой", "error", err)
		os.Exit(1)
	}
}

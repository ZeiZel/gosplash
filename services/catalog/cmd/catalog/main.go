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
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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

	cataloggrpc "gosplash/services/catalog/internal/adapters/grpc"
	cataloghttp "gosplash/services/catalog/internal/adapters/http"
	catalogkafka "gosplash/services/catalog/internal/adapters/kafka"
	"gosplash/services/catalog/internal/adapters/pg"
	catalogredis "gosplash/services/catalog/internal/adapters/redis"
	"gosplash/services/catalog/internal/app"
)

const serviceName = "catalog"

func main() {
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
		slog.Error("catalog: наблюдаемость", "error", err)
		os.Exit(1)
	}

	if conf.GRPC.JWTSecret == "" {
		// docs/STYLE.md и pkg/config: пустой секрет означает выключенную
		// проверку токена — допустимо только локально, и сервис обязан
		// сказать об этом в лог, а не выключить аутентификацию молча.
		slog.Warn("catalog: JWT_SECRET пуст — gRPC-аутентификация выключена, это нормально только для локальной разработки")
	}

	// ── База: primary + реплика за одним соединением ─────────────────────────
	database, err := dbx.OpenWithReplica(conf.Catalog.PrimaryDSN, conf.Catalog.ReplicaDSN)
	if err != nil {
		slog.Error("catalog: postgres", "error", err)
		os.Exit(1)
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
		slog.Error("catalog: redis", "error", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	// ── gRPC-клиент к media ──────────────────────────────────────────────────
	// grpc.NewClient не устанавливает соединение немедленно: он создаёт
	// «ленивый» канал, который подключится при первом вызове и сам будет
	// переподключаться. Поэтому порядок запуска сервисов не важен —
	// catalog спокойно стартует раньше media.
	mediaConn, err := grpc.NewClient(
		conf.Catalog.MediaGRPCTarget,
		// Без TLS: внутренняя сеть локальной разработки (pkg/grpcx, раздел
		// «чего сознательно нет»).
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		slog.Error("catalog: gRPC к media", "error", err)
		os.Exit(1)
	}
	defer mediaConn.Close()

	// ── Kafka: продюсер (просмотры) и консьюмеры ──────────────────────────────
	producer, err := kafkax.NewProducer(conf.Kafka.Brokers, serviceName)
	if err != nil {
		slog.Error("catalog: kafka producer", "error", err)
		os.Exit(1)
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
		slog.Error("catalog: kafka consumer (uploaded)", "error", err)
		os.Exit(1)
	}
	defer uploadedConsumer.Close()

	thumbnailConsumer, err := kafkax.NewConsumer(conf.Kafka.Brokers, thumbnailGroup, kafkax.TopicPhotoThumbnailReady)
	if err != nil {
		slog.Error("catalog: kafka consumer (thumbnail-ready)", "error", err)
		os.Exit(1)
	}
	defer thumbnailConsumer.Close()

	uploadedRetry, err := kafkax.NewRetryConsumer(conf.Kafka.Brokers, conf.Kafka.CatalogGroup+"-retry", kafkax.TopicPhotoUploaded)
	if err != nil {
		slog.Error("catalog: kafka retry-consumer (uploaded)", "error", err)
		os.Exit(1)
	}
	defer uploadedRetry.Close()

	thumbnailRetry, err := kafkax.NewRetryConsumer(conf.Kafka.Brokers, thumbnailGroup+"-retry", kafkax.TopicPhotoThumbnailReady)
	if err != nil {
		slog.Error("catalog: kafka retry-consumer (thumbnail-ready)", "error", err)
		os.Exit(1)
	}
	defer thumbnailRetry.Close()

	// ── Outbox-relay: catalog.listing.published → Kafka ───────────────────────
	relay, err := outbox.New(outbox.Config{
		ServiceName:  serviceName,
		Brokers:      conf.Kafka.Brokers,
		DSNs:         []string{conf.Catalog.PrimaryDSN},
		PollInterval: conf.Outbox.PollInterval,
		BatchSize:    conf.Outbox.BatchSize,
	})
	if err != nil {
		slog.Error("catalog: outbox relay", "error", err)
		os.Exit(1)
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
	// чтобы буфер флашился ДО остановки producer'а, а не в произвольном
	// порядке вместе с остальными defer'ами main().
	viewBatcher := catalogkafka.NewViewBatcher(producer,
		conf.Catalog.ViewBatchSize, conf.Catalog.ViewFlushInterval)

	hub := app.NewWatchHub()
	indexer := app.NewIndexer(uow, mediaClient, cache, hub)
	listingService := app.NewListingService(listingRepo, cache, viewCounter, viewBatcher)
	licenseService := app.NewLicenseService(licenseRepo)

	// ── gRPC-сервер ──────────────────────────────────────────────────────────
	catalogServer := cataloggrpc.NewServer(listingService, licenseService, hub)
	grpcHealth := grpcx.NewHealth()

	grpcServer := grpcx.NewServer(grpcx.ServerParams{
		ServiceName: serviceName,
		JWTSecret:   []byte(conf.GRPC.JWTSecret),
		// Чтение каталога — публичная витрина: браузер и grpcurl обязаны
		// достучаться до неё без токена, как до любого сайта фотостока.
		// GrantLicense/RevokeLicense остаются под аутентификацией — это
		// шаги саги заказа, а не витрина.
		PublicMethods: []string{
			catalogv1.CatalogService_GetListing_FullMethodName,
			catalogv1.CatalogService_ListListings_FullMethodName,
			catalogv1.CatalogService_WatchListing_FullMethodName,
		},
		DefaultTimeout: conf.GRPC.DefaultTimeout,
	})
	catalogv1.RegisterCatalogServiceServer(grpcServer, catalogServer)
	grpcHealth.Register(grpcServer)

	// ── Готовность ───────────────────────────────────────────────────────────
	health := httpx.NewHealth()
	health.Register("postgres", func(ctx context.Context) error { return dbx.Ping(ctx, database) })
	health.Register("kafka_uploaded", uploadedConsumer.Ping)
	health.Register("kafka_thumbnail", thumbnailConsumer.Ping)
	health.Register("kafka_producer", producer.Ping)
	health.Register("redis", redisClient.Ping)
	health.Register("outbox", relay.Ping)

	// ── HTTP ─────────────────────────────────────────────────────────────────
	router := http.NewServeMux()
	cataloghttp.Register(router, catalogServer, listingService)
	health.Handle(router, serviceName)

	httpServer := httpx.NewServer(
		httpx.DefaultServerConfig(conf.Catalog.HTTPAddr),
		httpx.Chain(router, httpx.Default(serviceName)...),
	)

	// ── Запуск ───────────────────────────────────────────────────────────────
	metricsServer := httpx.ServeMetricsAndPprof(conf.Catalog.MetricsAddr)
	go httpx.Serve(httpServer, "public")

	grpcDone := make(chan struct{})
	go func() {
		defer close(grpcDone)
		if err := grpcx.Serve(grpcServer, conf.Catalog.GRPCAddr, serviceName); err != nil {
			slog.Error("catalog: gRPC остановлен", "error", err)
		}
	}()

	go func() {
		if err := relay.Run(ctx); err != nil {
			slog.Error("catalog: outbox relay остановлен", "error", err)
		}
	}()

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		if err := uploadedConsumer.Run(ctx, indexer.HandlePhotoUploaded); err != nil {
			slog.Error("catalog: консьюмер uploaded остановлен", "error", err)
		}
	}()

	thumbnailDone := make(chan struct{})
	go func() {
		defer close(thumbnailDone)
		if err := thumbnailConsumer.Run(ctx, indexer.HandlePhotoThumbnailReady); err != nil {
			slog.Error("catalog: консьюмер thumbnail-ready остановлен", "error", err)
		}
	}()

	go func() {
		if err := uploadedRetry.Run(ctx, indexer.HandlePhotoUploaded); err != nil {
			slog.Error("catalog: retry-консьюмер uploaded остановлен", "error", err)
		}
	}()

	go func() {
		if err := thumbnailRetry.Run(ctx, indexer.HandlePhotoThumbnailReady); err != nil {
			slog.Error("catalog: retry-консьюмер thumbnail-ready остановлен", "error", err)
		}
	}()

	// ── Остановка ────────────────────────────────────────────────────────────
	<-ctx.Done()
	slog.Info("catalog: останавливаюсь…")

	health.NotReady()
	grpcHealth.NotServing()
	time.Sleep(2 * time.Second)

	httpx.Shutdown(ctx, 15*time.Second, httpServer, metricsServer)
	grpcx.Shutdown(grpcServer, 15*time.Second)

	// Консьюмерам даём доработать текущее сообщение. Оборвать его на середине
	// не смертельно (offset не сдвинут, сообщение приедет снова), но каждый
	// такой обрыв — это лишняя повторная обработка после рестарта.
	waitAll := func(timeout time.Duration, chans ...<-chan struct{}) {
		deadline := time.After(timeout)
		for _, ch := range chans {
			select {
			case <-ch:
			case <-deadline:
				slog.Warn("catalog: консьюмер не остановился вовремя")
				return
			}
		}
	}
	waitAll(15*time.Second, consumerDone, thumbnailDone)

	// Буфер просмотров сбрасывается ДО закрытия producer'а (defer выше
	// закроет его позже) — иначе накопленные, но ещё не отправленные
	// события просмотров были бы потеряны молча вместо честного флаша.
	viewBatcher.Close()

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := shutdownOtel(shutdownCtx); err != nil {
		slog.Error("catalog: otel shutdown", "error", err)
	}
	slog.Info("catalog: остановлен")
}

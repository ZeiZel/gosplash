// media-service — приём загрузок.
//
// Один процесс поднимает четыре занятия:
//
//	HTTP :8101       — загрузка файлов и публичные ручки. Сюда проксирует NGINX.
//	gRPC :9101       — межсервисный API. Наружу не выставлен, ходит только catalog.
//	outbox.Relay     — фоновый процесс, публикующий в Kafka то, что HTTP-ручки
//	                   и gRPC записали в таблицу outbox (docs/adr/0008-*).
//	HTTP :8201       — служебный: /metrics для Prometheus и /debug/pprof.
//
// Композиция зависимостей собирается здесь и только здесь: main видит всю
// программу целиком, а пакеты ниже не знают, откуда берутся их зависимости,
// и потому легко подменяются в тестах. Никакого DI-контейнера — обычные
// конструкторы.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc/reflection"

	mediav1 "gosplash/gen/go/gosplash/media/v1"
	"gosplash/pkg/config"
	"gosplash/pkg/dbx"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"
	"gosplash/pkg/otelx"
	"gosplash/pkg/outbox"
	"gosplash/pkg/redisx"
	"gosplash/pkg/s3x"

	mediagrpc "gosplash/services/media/internal/adapters/grpc"
	mediahttp "gosplash/services/media/internal/adapters/http"
	"gosplash/services/media/internal/adapters/pg"
	"gosplash/services/media/internal/app"
)

const serviceName = "media"

func main() {
	conf := config.Load()

	// ── Наблюдаемость поднимается ПЕРВОЙ ─────────────────────────────────────
	// До неё нет ни структурированных логов, ни трейсов, поэтому ошибки старта
	// всего остального были бы не видны там, где их будут искать.
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
		slog.Error("media: наблюдаемость", "error", err)
		os.Exit(1)
	}

	// ── Инфраструктура ───────────────────────────────────────────────────────
	shards, err := dbx.NewShards(conf.Media.ShardDSNs)
	if err != nil {
		slog.Error("media: postgres", "error", err)
		os.Exit(1)
	}
	defer shards.Close()
	slog.Info("media: подключено шардов", "count", shards.Count())

	storage, err := s3x.New(conf.S3.Endpoint, conf.S3.AccessKey, conf.S3.SecretKey, conf.S3.UseSSL)
	if err != nil {
		slog.Error("media: s3", "error", err)
		os.Exit(1)
	}

	// redisx.New не пытается подключиться немедленно (пул go-redis ленивый),
	// поэтому ошибка отсюда — это ошибка КОНФИГУРАЦИИ (пустой ServiceName,
	// пустой Addr), а не временная недоступность Redis. Временную
	// недоступность сервис обязан переживать сам — см. fail-open в
	// mediahttp.Handler.allowUpload — и падать на старте здесь неуместно.
	redisClient, err := redisx.New(redisx.Config{
		ServiceName: serviceName,
		Addr:        conf.Redis.Addr,
		Password:    conf.Redis.Password,
		DB:          conf.Redis.DB,
		PoolSize:    conf.Redis.PoolSize,
		MaxRetries:  conf.Redis.MaxRetries,
		DialTimeout: conf.Redis.DialTimeout,
		ReadTimeout: conf.Redis.ReadTimeout,
	})
	if err != nil {
		slog.Error("media: redis", "error", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	// outbox.Relay — отдельный процесс ВНУТРИ этого же бинарника (не
	// отдельный сервис): открывает СВОИ pgx-пулы на те же DSN шардов и свой
	// Kafka-продюсер, поэтому конфигурация не пересекается с shards/gRPC
	// выше. ShardDSNs передаётся как есть — Relay обходит их ВСЕ на каждом
	// тике (см. комментарий к pollAllShards в pkg/outbox), а не выбирает
	// один по ключу: у relay нет ключа шардирования, он вычитывает то, что
	// уже лежит неопубликованным, независимо от того, чьё это фото.
	relay, err := outbox.New(outbox.Config{
		ServiceName:  serviceName,
		Brokers:      conf.Kafka.Brokers,
		DSNs:         conf.Media.ShardDSNs,
		PollInterval: conf.Outbox.PollInterval,
		BatchSize:    conf.Outbox.BatchSize,
	})
	if err != nil {
		slog.Error("media: outbox relay", "error", err)
		os.Exit(1)
	}

	// ── Слои ─────────────────────────────────────────────────────────────────
	repository := pg.NewPhotoRepository(shards)
	service := app.NewPhotoService(repository, storage, conf.S3.BucketOriginals)

	// ── Готовность ───────────────────────────────────────────────────────────
	// НЕОЧЕВИДНОЕ РЕШЕНИЕ: здесь нет ни "kafka", ни "redis".
	//
	// "kafka" пропала не случайно: раньше media публиковала события В Kafka
	// НАПРЯМУЮ (kafkax.Producer), и его Ping имело смысл проверять — прямая
	// публикация была на критическом пути запроса. Теперь публикация ушла
	// в outbox.Relay, и критический путь запроса заканчивается на commit
	// транзакции Postgres (см. adapters/pg.PhotoRepository.Create); успеет
	// ли Relay доставить событие В ЭТУ секунду — вопрос отдельной метрики
	// (outbox_pending, pkg/outbox), а не /readyz. Совмещать их значило бы
	// не отличать "сервис не может принять запрос" от "фоновый процесс
	// временно отстаёт", а это ровно те две проблемы, ради которых liveness
	// и readiness вообще разделяют (см. комментарий в pkg/httpx.Health).
	//
	// "redis" отсутствует по симметричной причине: token bucket в Redis —
	// это единственное, для чего media использует Redis, и он FAIL-OPEN
	// (mediahttp.Handler.allowUpload) — недоступность Redis НЕ должна мешать
	// приёму загрузок. Зарегистрировать его здесь значило бы объявить
	// сервис неготовым принимать трафик именно тогда, когда он готов
	// принимать загрузки без ограничения — то есть свести на нет весь смысл
	// fail-open одной строчкой в main.
	health := httpx.NewHealth()
	health.Register("postgres", shards.Ping)

	// ── HTTP ─────────────────────────────────────────────────────────────────
	router := http.NewServeMux()
	mediahttp.Register(router, mediahttp.Deps{
		Service:                service,
		MaxBodyBytes:           50 << 20,
		PresignedTTL:           conf.S3.PresignedTTL,
		Limiter:                redisClient,
		UploadRateCapacity:     conf.Media.UploadRateCapacity,
		UploadRateRefillPerSec: conf.Media.UploadRateRefillPerSec,
	})
	health.Handle(router, serviceName)

	httpCfg := httpx.DefaultServerConfig(conf.Media.HTTPAddr)
	// Загрузка 50 МБ по медленному каналу не должна обрываться сервером,
	// поэтому у media таймауты на чтение и запись тела больше общих.
	httpCfg.ReadTimeout = 5 * time.Minute
	httpCfg.WriteTimeout = 5 * time.Minute
	httpServer := httpx.NewServer(httpCfg, httpx.Chain(router, httpx.Default(serviceName)...))

	// ── gRPC ─────────────────────────────────────────────────────────────────
	if conf.GRPC.JWTSecret == "" {
		// Пустой секрет отключает проверку подписи ВСЕМ методам, кроме
		// PublicMethods (см. authUnaryInterceptor в pkg/grpcx), — не тихо,
		// а с явным предупреждением, чтобы выключенная аутентификация не
		// уехала в прод незамеченной.
		slog.Warn("media: JWT_SECRET пуст, gRPC-аутентификация выключена — допустимо только локально")
	}

	grpcHealth := grpcx.NewHealth()
	grpcServer := grpcx.NewServer(grpcx.ServerParams{
		ServiceName: serviceName,
		JWTSecret:   []byte(conf.GRPC.JWTSecret),
		// GetPhoto пока публичный: аутентификация запросов между сервисами
		// (catalog → media) появится вместе с service-to-service токенами,
		// это отдельная задача. Полное имя метода, а не аннотация в .proto —
		// pkg/grpcx не читает protobuf-опции (см. комментарий в interceptors.go).
		PublicMethods:  []string{"/gosplash.media.v1.MediaService/GetPhoto"},
		DefaultTimeout: conf.GRPC.DefaultTimeout,
	})
	mediav1.RegisterMediaServiceServer(grpcServer, mediagrpc.NewServer(service))
	grpcHealth.Register(grpcServer)
	// Reflection позволяет grpcurl вызывать методы без .proto-файла под рукой:
	//   grpcurl -plaintext localhost:9101 list
	// В проде обычно выключают, локально — незаменимо.
	reflection.Register(grpcServer)

	// ── Запуск ───────────────────────────────────────────────────────────────
	metricsServer := httpx.ServeMetricsAndPprof(conf.Media.MetricsAddr)
	go httpx.Serve(httpServer, "public")

	go func() {
		if err := grpcx.Serve(grpcServer, conf.Media.GRPCAddr, serviceName); err != nil {
			slog.Error("media: grpc", "error", err)
		}
	}()

	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		if err := relay.Run(ctx); err != nil {
			slog.Error("media: outbox relay остановлен", "error", err)
		}
	}()

	// ── Остановка ────────────────────────────────────────────────────────────
	<-ctx.Done()
	slog.Info("media: останавливаюсь…")

	// Сначала перестаём быть ready, потом ждём, пока об этом узнает
	// балансировщик, и только затем закрываем соединения. Подробности —
	// в комментарии к httpx.Shutdown.
	health.NotReady()
	grpcHealth.NotServing()
	time.Sleep(2 * time.Second)

	httpx.Shutdown(ctx, 15*time.Second, httpServer, metricsServer)
	grpcx.Shutdown(grpcServer, 15*time.Second)

	// Relay'ю даём доработать текущий батч (см. комментарий к Relay.Run —
	// он и сам не оборвёт его на середине), а закрываем его пулы и продюсер
	// уже ПОСЛЕ того, как Run гарантированно вернул управление — иначе
	// последний батч останется без соединений на середине публикации.
	select {
	case <-relayDone:
	case <-time.After(15 * time.Second):
		slog.Warn("media: outbox relay не остановился за 15с")
	}
	relay.Close()

	// Наблюдаемость гасится ПОСЛЕДНЕЙ: иначе спаны и метрики самой остановки
	// не успеют уехать, а это ровно то время, которое интересно смотреть,
	// когда выкатка прошла плохо.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := shutdownOtel(shutdownCtx); err != nil {
		slog.Error("media: otel shutdown", "error", err)
	}
	slog.Info("media: остановлен")
}

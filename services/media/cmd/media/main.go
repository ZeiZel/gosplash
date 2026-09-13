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
// main.go разделён на три части (PATTERN: composition root, по образцу
// ZeiZel/gomple/cmd/main.go):
//
//	bootstrap.NewApp — чистая сборка серверов из уже открытых соединений,
//	                   живёт в internal/bootstrap и потому тестируется
//	                   без докера (см. internal/bootstrap/app_test.go).
//	run              — жизненный цикл: конфиг, соединения, запуск, сигнал,
//	                   graceful shutdown. Возвращает ошибку, а не зовёт
//	                   os.Exit — это и есть лекарство от gocritic
//	                   exitAfterDefer: пока os.Exit не вызван НИ РАЗУ до
//	                   возврата из run, все defer гарантированно отрабатывают.
//	main             — три строки: run, лог ошибки, os.Exit(1).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gosplash/pkg/config"
	"gosplash/pkg/dbx"
	"gosplash/pkg/httpx"
	"gosplash/pkg/otelx"
	"gosplash/pkg/outbox"
	"gosplash/pkg/redisx"
	"gosplash/pkg/s3x"

	"gosplash/services/media/internal/adapters/pg"
	"gosplash/services/media/internal/app"
	"gosplash/services/media/internal/bootstrap"
)

const serviceName = "media"

func run() error {
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
		return err
	}

	// ── Инфраструктура ───────────────────────────────────────────────────────
	// Каждое успешно открытое соединение сразу получает defer на закрытие:
	// возврат ошибки ниже (в отличие от os.Exit) гарантированно проходит
	// через все уже зарегистрированные defer, поэтому частично поднятая
	// инфраструктура не течёт при неудачном старте.
	shards, err := dbx.NewShards(conf.Media.ShardDSNs)
	if err != nil {
		return err
	}
	defer shards.Close()
	slog.Info("media: подключено шардов", "count", shards.Count())

	storage, err := s3x.New(conf.S3.Endpoint, conf.S3.AccessKey, conf.S3.SecretKey, conf.S3.UseSSL)
	if err != nil {
		return err
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
		return err
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
		return err
	}
	// App.Run дожидается остановки relay (relayDone) ПЕРЕД тем, как
	// вернуть управление, — так что к моменту, когда сработает этот defer,
	// Run гарантированно уже вернул управление, и закрывать пулы/продюсер
	// relay безопасно (см. комментарий к outbox.Relay.Close).
	defer relay.Close()

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
	healthChecks := map[string]httpx.Check{
		"postgres": shards.Ping,
	}

	// ── Сборка приложения ────────────────────────────────────────────────────
	mediaApp, err := bootstrap.NewApp(bootstrap.Deps{
		ServiceName: serviceName,
		HTTPAddr:    conf.Media.HTTPAddr,
		GRPCAddr:    conf.Media.GRPCAddr,
		MetricsAddr: conf.Media.MetricsAddr,

		// Загрузка 50 МБ по медленному каналу не должна обрываться сервером,
		// поэтому у media таймауты на чтение и запись тела больше общих.
		HTTPReadTimeout:  5 * time.Minute,
		HTTPWriteTimeout: 5 * time.Minute,

		JWTSecret:   conf.GRPC.JWTSecret,
		GRPCTimeout: conf.GRPC.DefaultTimeout,
		// GetPhoto пока публичный: аутентификация запросов между сервисами
		// (catalog → media) появится вместе с service-to-service токенами,
		// это отдельная задача. Полное имя метода, а не аннотация в .proto —
		// pkg/grpcx не читает protobuf-опции (см. комментарий в interceptors.go).
		PublicMethods: []string{"/gosplash.media.v1.MediaService/GetPhoto"},

		Service:                service,
		MaxBodyBytes:           50 << 20,
		PresignedTTL:           conf.S3.PresignedTTL,
		Limiter:                redisClient,
		UploadRateCapacity:     conf.Media.UploadRateCapacity,
		UploadRateRefillPerSec: conf.Media.UploadRateRefillPerSec,

		HealthChecks: healthChecks,
		Relay:        relay,
	})
	if err != nil {
		return err
	}
	// App.Close подчищает HTTP/gRPC серверы, которые собрала сама NewApp —
	// ставим defer сразу после успешной сборки, чтобы он сработал на любом
	// пути выхода из run, включая ошибку самого Run ниже.
	defer func() {
		if err := mediaApp.Close(); err != nil {
			slog.Error("media: закрытие приложения", "error", err)
		}
	}()

	runErr := mediaApp.Run(ctx)

	// Наблюдаемость гасится ПОСЛЕДНЕЙ: иначе спаны и метрики самой остановки
	// не успеют уехать, а это ровно то время, которое интересно смотреть,
	// когда выкатка прошла плохо.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := shutdownOtel(shutdownCtx); err != nil {
		slog.Error("media: otel shutdown", "error", err)
	}

	return runErr
}

// main — три строки. os.Exit здесь безопасен: он выполняется ПОСЛЕ того,
// как run() вернул управление, то есть после того, как отработали все её
// defer (закрытие БД, Redis, outbox-relay). Если бы os.Exit стоял внутри
// run на путях ошибок (как было до рефакторинга), defer'ы этой функции
// пропускались бы — это и есть gocritic exitAfterDefer, который ловит CI.
func main() {
	if err := run(); err != nil {
		slog.Error("media: приложение остановлено с ошибкой", "error", err)
		os.Exit(1)
	}
}

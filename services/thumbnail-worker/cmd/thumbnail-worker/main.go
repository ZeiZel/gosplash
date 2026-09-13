// thumbnail-worker — генерация превью из Kafka.
//
// Один процесс поднимает:
//
//	Kafka-консьюмер        — слушает media.photo.uploaded, генерирует три
//	                         превью (320/800/1600 по длинной стороне,
//	                         JPEG q85 — см. internal/adapters/imagex про
//	                         выбор формата) и кладёт их в gosplash-thumbnails.
//	Kafka retry-консьюмер  — media.photo.uploaded.retry, тот же обработчик.
//	Outbox relay           — публикует media.photo.thumbnail-ready, которое
//	                         internal/adapters/pg записывает в ту же
//	                         транзакцию, что и обновление статуса фото.
//	HTTP :8105             — /healthz, /readyz.
//	HTTP :8203             — служебный: /metrics и /debug/pprof.
//
// main.go разделён на три части (PATTERN: composition root, по образцу
// ZeiZel/gomple/cmd/main.go):
//
//	bootstrap.NewApp — чистая сборка HTTP-сервера и держателя фоновых
//	                   процессов, живёт в internal/bootstrap и потому
//	                   тестируется без докера (internal/bootstrap/app_test.go).
//	run              — жизненный цикл: конфиг, соединения, запуск, сигнал,
//	                   graceful shutdown. Возвращает ошибку, а не зовёт
//	                   os.Exit — лекарство от gocritic exitAfterDefer: пока
//	                   os.Exit не вызван НИ РАЗУ до возврата из run, все
//	                   defer гарантированно отрабатывают.
//	main             — три строки: run, лог ошибки, os.Exit(1).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"gosplash/pkg/config"
	"gosplash/pkg/dbx"
	"gosplash/pkg/httpx"
	"gosplash/pkg/kafkax"
	"gosplash/pkg/otelx"
	"gosplash/pkg/outbox"
	"gosplash/pkg/redisx"

	"gosplash/services/thumbnail-worker/internal/adapters/imagex"
	thumbkafka "gosplash/services/thumbnail-worker/internal/adapters/kafka"
	"gosplash/services/thumbnail-worker/internal/adapters/pg"
	thumbredis "gosplash/services/thumbnail-worker/internal/adapters/redis"
	thumbs3 "gosplash/services/thumbnail-worker/internal/adapters/s3"
	"gosplash/services/thumbnail-worker/internal/app"
	"gosplash/services/thumbnail-worker/internal/bootstrap"
)

const serviceName = "thumbnail-worker"

func run() error {
	conf := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── Наблюдаемость поднимается ПЕРВОЙ ─────────────────────────────────────
	// До неё нет ни структурированных логов, ни трейсов — ошибки старта
	// всего остального были бы не видны там, где их будут искать.
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

	// ── PostgreSQL ────────────────────────────────────────────────────────────
	//
	// НЕОЧЕВИДНОЕ РЕШЕНИЕ: своей базы у thumbnail-worker нет — используются
	// ТЕ ЖЕ шарды, что и у media (conf.Media.ShardDSNs), с тем же выбором
	// шарда по hash(user_id) (pkg/dbx.Shards). Это не временный костыль,
	// а прямое следствие того, что processed_events (идемпотентность,
	// pkg/idempotency) и обновление photos.status ОБЯЗАНЫ жить в ОДНОЙ
	// транзакции — а транзакция в PostgreSQL не бывает между двумя разными
	// серверами. Заводить thumbnail-worker'у отдельную базу означало бы либо
	// потерять эту гарантию, либо городить distributed transaction ради
	// задачи, которая на общих шардах решается сама собой: данные фото и
	// отметка о его обработке физически лежат рядом. Правило проекта
	// "os.Getenv только в pkg/config" при этом не оставляет способа завести
	// новую переменную THUMBNAIL_DSN, не трогая pkg/config (вне зоны этой
	// задачи) — так что даже если бы отдельная база была желательна, её
	// сейчас неоткуда было бы сконфигурировать. Подробный разбор цены этого
	// решения (таблицы processed_events/outbox на шардах media сейчас
	// фактически принадлежат только thumbnail-worker'у) — в doc-комментарии
	// internal/adapters/pg/repository.go.
	shards, err := dbx.NewShards(conf.Media.ShardDSNs)
	if err != nil {
		return err
	}
	defer shards.Close()
	slog.Info("thumbnail-worker: подключено шардов", "count", shards.Count())

	// ── S3 ────────────────────────────────────────────────────────────────────
	// Свой клиент, а не pkg/s3x: тому не хватает Get (media его не читает,
	// а нам обязательно нужно прочитать оригинал) — см. package doc
	// internal/adapters/s3.
	storage, err := thumbs3.New(conf.S3.Endpoint, conf.S3.AccessKey, conf.S3.SecretKey, conf.S3.UseSSL)
	if err != nil {
		return err
	}

	// ── Redis (лок) ──────────────────────────────────────────────────────────
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

	// ── Слои ─────────────────────────────────────────────────────────────────
	repository := pg.NewPhotoRepository(shards)
	locker := thumbredis.NewLocker(redisClient)
	processor := imagex.New()

	// Workers=0 → runtime.NumCPU(). Разрешение дефолта — забота композиции
	// (main видит железо, на котором запущен), а не сценария: internal/app
	// принимает уже готовое положительное число и просто ограничивает им
	// число одновременных CPU-bound ресайзов (см. package doc internal/app —
	// "горутина на сообщение даст тысячу параллельных ресайзов и OOM").
	workers := conf.Thumbnail.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	slog.Info("thumbnail-worker: пул ресайза", "workers", workers)

	service := app.NewService(
		repository, storage, locker, processor,
		conf.S3.BucketOriginals, conf.S3.BucketThumbnails,
		conf.Thumbnail.Sizes, conf.Thumbnail.LockTTL, workers,
	)
	handler := thumbkafka.NewHandler(service)

	// ── Kafka: основной консьюмер + retry ────────────────────────────────────
	consumer, err := kafkax.NewConsumer(conf.Kafka.Brokers, conf.Kafka.ThumbnailGroup, kafkax.TopicPhotoUploaded)
	if err != nil {
		return err
	}
	defer consumer.Close()

	// Группа "<group>-retry" — та же конвенция имён, что уже использует
	// mk/kafka.mk (KAFKA_GROUPS) для отображения лага: своя группа для
	// retry-топика, отдельная от основной, иначе Kafka делила бы партиции
	// основного и retry-топика между одними и теми же членами консьюмер-
	// группы, а это два разных топика с разными требованиями к задержке.
	retryConsumer, err := kafkax.NewRetryConsumer(conf.Kafka.Brokers, conf.Kafka.ThumbnailGroup+"-retry", kafkax.TopicPhotoUploaded)
	if err != nil {
		return err
	}
	defer retryConsumer.Close()

	// ── Outbox relay ─────────────────────────────────────────────────────────
	// Обходит ТЕ ЖЕ шарды media (см. комментарий про PostgreSQL выше) —
	// строки outbox, которые пишет internal/adapters/pg.CommitReady, лежат
	// именно там.
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
	defer relay.Close()

	// ── Готовность ───────────────────────────────────────────────────────────
	healthChecks := map[string]httpx.Check{
		"postgres": repository.Ping,
		"kafka":    consumer.Ping,
		"redis":    func(ctx context.Context) error { return redisClient.Ping(ctx) },
		"s3":       func(ctx context.Context) error { return storage.Ping(ctx, conf.S3.BucketOriginals) },
		"outbox":   relay.Ping,
	}

	// ── Сборка приложения ────────────────────────────────────────────────────
	thumbnailApp, err := bootstrap.NewApp(bootstrap.Deps{
		ServiceName:   serviceName,
		HTTPAddr:      conf.Thumbnail.HTTPAddr,
		MetricsAddr:   conf.Thumbnail.MetricsAddr,
		HealthChecks:  healthChecks,
		Consumer:      consumer,
		RetryConsumer: retryConsumer,
		Handle:        handler,
		Relay:         relay,
	})
	if err != nil {
		return err
	}
	// App.Close подчищает HTTP-сервер, который собрала сама NewApp —
	// ставим defer сразу после успешной сборки, чтобы он сработал на любом
	// пути выхода из run, включая ошибку самого Run ниже.
	defer func() {
		if err := thumbnailApp.Close(); err != nil {
			slog.Error("thumbnail-worker: закрытие приложения", "error", err)
		}
	}()

	runErr := thumbnailApp.Run(ctx)

	// Наблюдаемость гасится ПОСЛЕДНЕЙ: иначе спаны и метрики самой остановки
	// не успеют уехать, а это ровно то время, которое интересно смотреть,
	// когда выкатка прошла плохо.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := shutdownOtel(shutdownCtx); err != nil {
		slog.Error("thumbnail-worker: otel shutdown", "error", err)
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
		slog.Error("thumbnail-worker: приложение остановлено с ошибкой", "error", err)
		os.Exit(1)
	}
}

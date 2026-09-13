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
// Композиция зависимостей собирается здесь и только здесь: main видит всю
// программу целиком, а пакеты ниже не знают, откуда берутся их зависимости.
package main

import (
	"context"
	"log/slog"
	"net/http"
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
)

const serviceName = "thumbnail-worker"

func main() {
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
		slog.Error("thumbnail-worker: наблюдаемость", "error", err)
		os.Exit(1)
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
		slog.Error("thumbnail-worker: postgres", "error", err)
		os.Exit(1)
	}
	defer shards.Close()
	slog.Info("thumbnail-worker: подключено шардов", "count", shards.Count())

	// ── S3 ────────────────────────────────────────────────────────────────────
	// Свой клиент, а не pkg/s3x: тому не хватает Get (media его не читает,
	// а нам обязательно нужно прочитать оригинал) — см. package doc
	// internal/adapters/s3.
	storage, err := thumbs3.New(conf.S3.Endpoint, conf.S3.AccessKey, conf.S3.SecretKey, conf.S3.UseSSL)
	if err != nil {
		slog.Error("thumbnail-worker: s3", "error", err)
		os.Exit(1)
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
		slog.Error("thumbnail-worker: redis", "error", err)
		os.Exit(1)
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
		slog.Error("thumbnail-worker: kafka consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	// Группа "<group>-retry" — та же конвенция имён, что уже использует
	// mk/kafka.mk (KAFKA_GROUPS) для отображения лага: своя группа для
	// retry-топика, отдельная от основной, иначе Kafka делила бы партиции
	// основного и retry-топика между одними и теми же членами консьюмер-
	// группы, а это два разных топика с разными требованиями к задержке.
	retryConsumer, err := kafkax.NewRetryConsumer(conf.Kafka.Brokers, conf.Kafka.ThumbnailGroup+"-retry", kafkax.TopicPhotoUploaded)
	if err != nil {
		slog.Error("thumbnail-worker: kafka retry-consumer", "error", err)
		os.Exit(1)
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
		slog.Error("thumbnail-worker: outbox relay", "error", err)
		os.Exit(1)
	}
	defer relay.Close()

	// ── Готовность ───────────────────────────────────────────────────────────
	health := httpx.NewHealth()
	health.Register("postgres", repository.Ping)
	health.Register("kafka", consumer.Ping)
	health.Register("redis", func(ctx context.Context) error { return redisClient.Ping(ctx) })
	health.Register("s3", func(ctx context.Context) error { return storage.Ping(ctx, conf.S3.BucketOriginals) })
	health.Register("outbox", relay.Ping)

	// ── HTTP (только health — публичного API у воркера нет) ──────────────────
	router := http.NewServeMux()
	health.Handle(router, serviceName)
	httpServer := httpx.NewServer(
		httpx.DefaultServerConfig(conf.Thumbnail.HTTPAddr),
		httpx.Chain(router, httpx.Default(serviceName)...),
	)

	// ── Запуск ───────────────────────────────────────────────────────────────
	metricsServer := httpx.ServeMetricsAndPprof(conf.Thumbnail.MetricsAddr)
	go httpx.Serve(httpServer, "public")

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		if err := consumer.Run(ctx, handler); err != nil {
			slog.Error("thumbnail-worker: консьюмер остановлен", "error", err)
		}
	}()

	retryDone := make(chan struct{})
	go func() {
		defer close(retryDone)
		if err := retryConsumer.Run(ctx, handler); err != nil {
			slog.Error("thumbnail-worker: retry-консьюмер остановлен", "error", err)
		}
	}()

	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		if err := relay.Run(ctx); err != nil {
			slog.Error("thumbnail-worker: outbox relay остановлен", "error", err)
		}
	}()

	// ── Остановка ────────────────────────────────────────────────────────────
	<-ctx.Done()
	slog.Info("thumbnail-worker: останавливаюсь…")

	// Сначала перестаём быть ready, потом даём балансировщику/оркестратору
	// время узнать об этом, и только затем гасим сервера — подробности
	// в комментарии к httpx.Shutdown.
	health.NotReady()
	time.Sleep(2 * time.Second)

	httpx.Shutdown(ctx, 15*time.Second, httpServer, metricsServer)

	// Каждому фоновому циклу даём доработать текущую итерацию, а не рвём
	// по живому: у консьюмеров это текущее сообщение (offset не сдвинут,
	// значит, при обрыве оно просто приедет снова — не смертельно, но
	// лишняя повторная обработка), у relay — текущий батч (см. комментарий
	// к outbox.Relay.Run про то, почему обрыв батча хуже, чем секунда
	// ожидания).
	waitFor := func(name string, done <-chan struct{}) {
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			slog.Warn("thumbnail-worker: не остановился за 15с", "component", name)
		}
	}
	waitFor("consumer", consumerDone)
	waitFor("retry-consumer", retryDone)
	waitFor("outbox-relay", relayDone)

	// Наблюдаемость гасится ПОСЛЕДНЕЙ: иначе спаны и метрики самой остановки
	// не успеют уехать, а это ровно то время, которое интересно смотреть,
	// когда выкатка прошла плохо.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := shutdownOtel(shutdownCtx); err != nil {
		slog.Error("thumbnail-worker: otel shutdown", "error", err)
	}
	slog.Info("thumbnail-worker: остановлен")
}

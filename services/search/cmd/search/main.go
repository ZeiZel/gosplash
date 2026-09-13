// search-service — полнотекстовый поиск по каталогу в Elasticsearch (фаза 5).
//
// Несколько независимых занятий в одном процессе, как и у catalog:
//
//	gRPC :9107        — SearchService (proto/gosplash/search/v1): поиск.
//	HTTP :8107        — служебный: /healthz, /readyz.
//	HTTP :8207        — /metrics и /debug/pprof.
//	Kafka-консьюмер   — слушает catalog.listing.published, идемпотентно
//	                    (через версионирование, internal/app/indexer.go)
//	                    наполняет индекс listings пачками через _bulk.
//	Retry-консьюмер   — kafkax.NewRetryConsumer по тому же топику.
//
// При старте сервис проверяет, что alias SEARCH_INDEX_ALIAS указывает на
// реальный индекс, и создаёт его с явным маппингом, если это самый первый
// запуск (internal/adapters/es.Client.EnsureIndex). Смена схемы индекса
// после этого — работа tools/search-reindex, не перезапуска сервиса.
//
// Файл разделён на bootstrap.App (чистая сборка зависимостей), run()
// (конфиг/соединения/сигналы/graceful shutdown) и main() — см. STYLE.md
// и internal/bootstrap про причину.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gosplash/pkg/config"
	"gosplash/pkg/kafkax"
	"gosplash/pkg/otelx"

	"gosplash/services/search/internal/adapters/es"
	"gosplash/services/search/internal/bootstrap"
)

const serviceName = "search"

// Размер и период флаша BulkIndexer — константы, а не переменные окружения:
// подходящих переменных для них в .env/.env.example для этой фазы не заведено
// (см. задание, п.5 — список переменных закрытый), а заводить новые не входит
// в её объём. Значения подобраны так же, как defaultFlushSize/defaultFlushInterval
// в catalog.ViewBatcher — компромисс между задержкой индексации и размером
// пачки, разобранный в internal/adapters/es/bulk_indexer.go.
const (
	bulkBatchSize     = 100
	bulkFlushInterval = 300 * time.Millisecond
)

// otelShutdownTimeout — сколько ждём выгрузки последних трейсов/метрик
// после того, как серверы уже остановлены.
const otelShutdownTimeout = 5 * time.Second

// run — жизненный цикл процесса: конфиг, соединения, запуск App, ожидание
// сигнала, graceful shutdown. Возвращает ошибку вместо os.Exit — все defer
// (закрытие консьюмеров, BulkIndexer, выгрузка otel) успевают отработать
// независимо от того, на каком шаге всё пошло не так. Раньше os.Exit(1)
// внутри main() обрывал их (gocritic: exitAfterDefer).
func run() error {
	conf := config.Load()
	searchConf := conf.Search

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
		return fmt.Errorf("search: наблюдаемость: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), otelShutdownTimeout)
		defer cancel()
		if err := shutdownOtel(shutdownCtx); err != nil {
			slog.Error("search: otel shutdown", "error", err)
		}
	}()

	if conf.GRPC.JWTSecret == "" {
		slog.Warn("search: JWT_SECRET пуст — gRPC-аутентификация выключена, это нормально только для локальной разработки")
	}

	// ── Elasticsearch ────────────────────────────────────────────────────
	esClient, err := es.New(searchConf.Addrs, searchConf.IndexAlias)
	if err != nil {
		return fmt.Errorf("search: elasticsearch client: %w", err)
	}
	if err := esClient.EnsureIndex(ctx); err != nil {
		// Не просто предупреждение: без индекса ни поиск, ни консьюмер не
		// смогут работать вообще, и сервис не имеет смысла поднимать
		// в состоянии "почти готов".
		return fmt.Errorf("search: не смог обеспечить индекс: %w", err)
	}

	bulkIndexer := es.NewBulkIndexer(esClient, bulkBatchSize, bulkFlushInterval)
	// BulkIndexer закрывается ПОСЛЕ остановки обоих консьюмеров (а не через
	// defer вперемешку с остальными): application.Run ниже блокируется, пока
	// оба консьюмера не вернут управление (см. её комментарий), и только
	// после этого возвращает управление сюда — то есть к моменту, когда
	// сработает вот этот defer, новых Submit в bulkIndexer уже точно не
	// будет, и Close успевает дождаться реальной отправки последней пачки
	// в Elasticsearch, а не оборвать её на середине.
	defer bulkIndexer.Close()

	searchIndex := es.NewSearchIndex(esClient)

	// ── Kafka: консьюмер + retry-консьюмер ────────────────────────────────
	consumer, err := kafkax.NewConsumer(conf.Kafka.Brokers, searchConf.KafkaConsumerGroup, kafkax.TopicListingPublished)
	if err != nil {
		return fmt.Errorf("search: kafka consumer: %w", err)
	}
	defer consumer.Close()

	retryConsumer, err := kafkax.NewRetryConsumer(conf.Kafka.Brokers, searchConf.KafkaConsumerGroup+"-retry", kafkax.TopicListingPublished)
	if err != nil {
		return fmt.Errorf("search: kafka retry-consumer: %w", err)
	}
	defer retryConsumer.Close()

	application, err := bootstrap.NewApp(bootstrap.Deps{
		SearchIndex:       searchIndex,
		IndexWriter:       bulkIndexer,
		Consumer:          consumer,
		RetryConsumer:     retryConsumer,
		ElasticsearchPing: esClient.Ping,
		JWTSecret:         conf.GRPC.JWTSecret,
		DefaultTimeout:    conf.GRPC.DefaultTimeout,
		GRPCAddr:          searchConf.GRPCAddr,
		HTTPAddr:          searchConf.HTTPAddr,
		MetricsAddr:       searchConf.MetricsAddr,
	})
	if err != nil {
		return fmt.Errorf("search: сборка приложения: %w", err)
	}
	defer func() {
		if err := application.Close(); err != nil {
			slog.Error("search: закрытие приложения", "error", err)
		}
	}()

	// Run блокируется до отмены ctx (сигнал) или фатального сбоя сервера
	// и сама проводит graceful shutdown серверов и обоих консьюмеров.
	return application.Run(ctx)
}

// main — три строки: вызвать run(), при ошибке залогировать и os.Exit(1).
// os.Exit здесь безопасен: run() к этому моменту уже вернула управление,
// и все её defer (закрытие консьюмеров, BulkIndexer, otel) успели
// отработать до этой строки, а не обрываются вызовом os.Exit.
func main() {
	if err := run(); err != nil {
		slog.Error("search: приложение остановлено с ошибкой", "error", err.Error())
		os.Exit(1)
	}
}

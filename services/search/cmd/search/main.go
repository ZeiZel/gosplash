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
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gosplash/pkg/config"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"
	"gosplash/pkg/kafkax"
	"gosplash/pkg/otelx"

	searchv1 "gosplash/gen/go/gosplash/search/v1"

	"gosplash/services/search/internal/adapters/es"
	searchgrpc "gosplash/services/search/internal/adapters/grpc"
	"gosplash/services/search/internal/app"
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

func main() {
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
		slog.Error("search: наблюдаемость", "error", err)
		os.Exit(1)
	}

	if conf.GRPC.JWTSecret == "" {
		slog.Warn("search: JWT_SECRET пуст — gRPC-аутентификация выключена, это нормально только для локальной разработки")
	}

	// ── Elasticsearch ──────────────────────────────────────────────────────
	esClient, err := es.New(searchConf.Addrs, searchConf.IndexAlias)
	if err != nil {
		slog.Error("search: elasticsearch client", "error", err)
		os.Exit(1)
	}
	if err := esClient.EnsureIndex(ctx); err != nil {
		// Не просто предупреждение: без индекса ни поиск, ни консьюмер не
		// смогут работать вообще, и сервис не имеет смысла поднимать
		// в состоянии "почти готов".
		slog.Error("search: не смог обеспечить индекс", "error", err)
		os.Exit(1)
	}

	bulkIndexer := es.NewBulkIndexer(esClient, bulkBatchSize, bulkFlushInterval)
	searchIndex := es.NewSearchIndex(esClient)

	// ── Kafka: консьюмер + retry-консьюмер ───────────────────────────────────
	consumer, err := kafkax.NewConsumer(conf.Kafka.Brokers, searchConf.KafkaConsumerGroup, kafkax.TopicListingPublished)
	if err != nil {
		slog.Error("search: kafka consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	retryConsumer, err := kafkax.NewRetryConsumer(conf.Kafka.Brokers, searchConf.KafkaConsumerGroup+"-retry", kafkax.TopicListingPublished)
	if err != nil {
		slog.Error("search: kafka retry-consumer", "error", err)
		os.Exit(1)
	}
	defer retryConsumer.Close()

	// ── Слои ─────────────────────────────────────────────────────────────────
	indexer := app.NewIndexer(bulkIndexer)
	searchService := app.NewSearchService(searchIndex)

	// ── gRPC-сервер ──────────────────────────────────────────────────────────
	searchServer := searchgrpc.NewServer(searchService)
	grpcHealth := grpcx.NewHealth()

	grpcServer := grpcx.NewServer(grpcx.ServerParams{
		ServiceName: serviceName,
		JWTSecret:   []byte(conf.GRPC.JWTSecret),
		// Поиск — публичная витрина, как чтение каталога: браузер и
		// grpcurl обязаны достучаться без токена.
		PublicMethods: []string{
			searchv1.SearchService_Search_FullMethodName,
		},
		DefaultTimeout: conf.GRPC.DefaultTimeout,
	})
	searchv1.RegisterSearchServiceServer(grpcServer, searchServer)
	grpcHealth.Register(grpcServer)

	// ── Готовность ───────────────────────────────────────────────────────────
	health := httpx.NewHealth()
	health.Register("elasticsearch", esClient.Ping)
	health.Register("kafka_consumer", consumer.Ping)

	router := http.NewServeMux()
	health.Handle(router, serviceName)

	httpServer := httpx.NewServer(
		httpx.DefaultServerConfig(searchConf.HTTPAddr),
		httpx.Chain(router, httpx.Default(serviceName)...),
	)

	// ── Запуск ───────────────────────────────────────────────────────────────
	metricsServer := httpx.ServeMetricsAndPprof(searchConf.MetricsAddr)
	go httpx.Serve(httpServer, "public")

	grpcDone := make(chan struct{})
	go func() {
		defer close(grpcDone)
		if err := grpcx.Serve(grpcServer, searchConf.GRPCAddr, serviceName); err != nil {
			slog.Error("search: gRPC остановлен", "error", err)
		}
	}()

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		if err := consumer.Run(ctx, indexer.HandleListingPublished); err != nil {
			slog.Error("search: консьюмер остановлен", "error", err)
		}
	}()

	go func() {
		if err := retryConsumer.Run(ctx, indexer.HandleListingPublished); err != nil {
			slog.Error("search: retry-консьюмер остановлен", "error", err)
		}
	}()

	// ── Остановка ────────────────────────────────────────────────────────────
	<-ctx.Done()
	slog.Info("search: останавливаюсь…")

	health.NotReady()
	grpcHealth.NotServing()
	time.Sleep(2 * time.Second)

	httpx.Shutdown(ctx, 15*time.Second, httpServer, metricsServer)
	grpcx.Shutdown(grpcServer, 15*time.Second)

	// Консьюмеру даём доработать текущее сообщение — тот же приём, что и
	// в catalog/cmd/catalog/main.go.
	select {
	case <-consumerDone:
	case <-time.After(15 * time.Second):
		slog.Warn("search: консьюмер не остановился вовремя")
	}

	// BulkIndexer закрывается ПОСЛЕ остановки консьюмера (а не через defer
	// вперемешку с остальными): к этому моменту новых Submit уже точно не
	// будет, и Close успевает дождаться реальной отправки последней пачки
	// в Elasticsearch, а не оборвать её на середине.
	bulkIndexer.Close()

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := shutdownOtel(shutdownCtx); err != nil {
		slog.Error("search: otel shutdown", "error", err)
	}
	slog.Info("search: остановлен")
}

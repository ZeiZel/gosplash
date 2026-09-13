// Package otelx — наблюдаемость: трейсы, метрики и логи, настроенные одинаково
// во всех сервисах проекта.
//
// Три сигнала отвечают на три разных вопроса, и подменять один другим дорого:
//
//	Метрики — «плохо ли сейчас и насколько». Дёшевы, агрегированы, хранятся
//	          долго. По ним строят алерты. Ответить «почему плохо» не могут:
//	          в метрике нет конкретного запроса.
//	Трейсы  — «где именно в цепочке вызовов время». Показывают один запрос
//	          целиком, через все сервисы и топики. Дороги, поэтому в проде
//	          семплируются.
//	Логи    — «что именно произошло в этой точке». Самые подробные и самые
//	          бесполезные без первых двух: тысяча строк в секунду без trace_id
//	          не даёт ответить ни на один вопрос.
//
// Связывает их trace_id: он есть в спане, попадает в каждую строку лога
// (см. slog-handler ниже) и позволяет из графика в Grafana провалиться в
// конкретный трейс, а из трейса — в логи именно этого запроса.
package otelx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
)

// Config — что нужно знать пакету о сервисе, который его поднимает.
type Config struct {
	// ServiceName попадает в атрибут service.name. По нему трейсы и метрики
	// группируются в Grafana, поэтому имя должно совпадать с именем сервиса
	// в compose и в Helm, а не быть «media-svc-2».
	ServiceName string
	Version     string
	Environment string

	// OTLPEndpoint — адрес коллектора, host:port без схемы (grpc).
	// Пусто — трейсы не экспортируются, но API работает: код с трассировкой
	// не должен падать оттого, что коллектор не поднят.
	OTLPEndpoint string

	// TraceSampleRatio — доля трассируемых запросов. Локально 1.0 (все),
	// в проде обычно сотые доли: трейс стоит дороже метрики на три порядка.
	TraceSampleRatio float64

	// LogLevel — уровень логирования ("debug", "info", "warn", "error").
	// Приходит параметром, а не читается из окружения здесь: os.Getenv в
	// проекте вызывается только в pkg/config (docs/STYLE.md). Пустая строка
	// означает info.
	LogLevel string
}

// Shutdown останавливает экспортёры. Вызывать ОБЯЗАТЕЛЬНО и с таймаутом:
// в буфере лежат неотправленные спаны, и без flush последние секунды жизни
// сервиса — те самые, где обычно и случилась авария, — в трейсах не появятся.
type Shutdown func(context.Context) error

// Setup поднимает глобальные провайдеры трейсов и метрик и подменяет
// стандартный slog-логгер на JSON с trace_id.
//
// Провайдеры именно глобальные (otel.SetTracerProvider). В библиотеке так
// делать не стоит, но здесь пакет вызывается один раз из main каждого
// сервиса, а взамен любая инструментированная библиотека — otelhttp, otelgrpc,
// otelgorm — начинает работать без передачи провайдера через полпроекта.
func Setup(ctx context.Context, cfg Config) (Shutdown, error) {
	res, err := buildResource(cfg)
	if err != nil {
		return nil, fmt.Errorf("otelx: resource: %w", err)
	}

	var shutdowns []Shutdown

	// ── Трейсы ───────────────────────────────────────────────────────────────
	if cfg.OTLPEndpoint != "" {
		exporter, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
			// Без TLS: коллектор стоит в той же сети. В проде здесь были бы
			// креды, и грубая ошибка — оставить insecure «на время».
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("otelx: otlp exporter: %w", err)
		}

		tp := sdktrace.NewTracerProvider(
			// Batcher, а не SimpleSpanProcessor: тот отправляет каждый спан
			// синхронно и превращает трассировку в тормоз на горячем пути.
			sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(5*time.Second)),
			sdktrace.WithResource(res),
			// ParentBased важнее, чем кажется: если родительский сервис решил
			// трассировать запрос, дочерний обязан согласиться, иначе трейс
			// получится дырявым. Своё решение принимается только для запросов
			// без родителя.
			sdktrace.WithSampler(sdktrace.ParentBased(
				sdktrace.TraceIDRatioBased(cfg.TraceSampleRatio),
			)),
		)
		otel.SetTracerProvider(tp)
		shutdowns = append(shutdowns, tp.Shutdown)
	}

	// Пропагатор — то, что кладёт trace-контекст в исходящие запросы и
	// достаёт из входящих. W3C traceparent понимают все: и otelhttp,
	// и otelgrpc, и наши Kafka-заголовки (см. pkg/kafkax).
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// ── Метрики ──────────────────────────────────────────────────────────────
	// Метрики НЕ идут в коллектор: Prometheus сам приходит за ними на /metrics.
	// Pull-модель здесь удобнее push'а — по факту скрейпа сразу видно, жив ли
	// процесс, и не нужно доверять доставке.
	promExp, err := promexporter.New()
	if err != nil {
		return nil, fmt.Errorf("otelx: prometheus exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExp),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	shutdowns = append(shutdowns, mp.Shutdown)

	// ── Логи ─────────────────────────────────────────────────────────────────
	setupLogger(cfg)

	// Ошибки внутри самого OTel (не смог отправить батч и т. п.) по умолчанию
	// уходят в никуда. Пусть лучше будут видны.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		slog.Error("otel", "error", err)
	}))

	return func(ctx context.Context) error {
		var errs []error
		for _, fn := range shutdowns {
			errs = append(errs, fn(ctx))
		}
		return errors.Join(errs...)
	}, nil
}

// buildResource собирает описание сервиса: service.name, версия, окружение
// плюс то, что SDK определил сам (хост, ОС, процесс).
//
// НЕОЧЕВИДНОЕ МЕСТО — resource.NewSchemaless, а не resource.NewWithAttributes
// с semconv.SchemaURL.
//
// У каждого Resource есть Schema URL — версия словаря семантических
// соглашений, по которому названы атрибуты. resource.Merge отказывается
// сливать два ресурса с РАЗНЫМИ непустыми Schema URL и возвращает ошибку
// «conflicting Schema URL». А resource.Default() внутри SDK помечен той
// версией semconv, с которой собран сам SDK, — и она меняется при каждом
// его обновлении. Стоит поднять go.opentelemetry.io/otel/sdk на версию,
// где semconv новее нашего импорта, и Setup начинает падать на старте
// у ВСЕХ сервисов разом, причём по причине, которая не имеет отношения
// ни к телеметрии, ни к коду сервиса.
//
// Schemaless-ресурс не объявляет версию словаря вовсе, поэтому сливается
// с чем угодно. Мы при этом ничего не теряем: типизированные хелперы
// semconv.ServiceName и semconv.ServiceVersion по-прежнему дают правильные
// имена атрибутов, а Schema URL нужен потребителю телеметрии только для
// автоматической миграции старых имён — задача, которой у локального стенда
// нет.
func buildResource(cfg Config) (*resource.Resource, error) {
	attrs := resource.NewSchemaless(
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.Version),
		attribute.String("deployment.environment", cfg.Environment),
	)

	res, err := resource.Merge(resource.Default(), attrs)
	if err != nil {
		// Сюда попасть уже не должно, но если однажды попадём — сервис
		// обязан подняться. Телеметрия без атрибута окружения хуже полной,
		// но несопоставимо лучше, чем сервис, не стартовавший из-за версии
		// словаря имён.
		slog.Warn("otelx: не смог слить resource, беру значения по умолчанию", "error", err)
		return resource.Default(), nil
	}
	return res, nil
}

// Tracer — трейсер для ручной инструментации там, где нет готовой библиотеки.
func Tracer(name string) trace.Tracer { return otel.Tracer(name) }

// ─────────────────────────────────────────────────────────────────────────────
// ЛОГИ
// ─────────────────────────────────────────────────────────────────────────────

// setupLogger делает slog.Default() структурированным JSON-логгером, который
// в каждую запись подмешивает trace_id и span_id из контекста.
func setupLogger(cfg Config) {
	var level slog.Level
	// UnmarshalText принимает "debug"/"info"/"warn"/"error" в любом регистре.
	// Не разобралось — остаёмся на info: неверно записанный уровень не повод
	// остаться без логов вовсе.
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		level = slog.LevelInfo
	}

	// JSON, а не текст: логи собирает promtail и кладёт в Loki, а тот ищет
	// по полям. Человекочитаемость приносится в жертву осознанно — читать
	// логи глазами приходится на порядок реже, чем искать по ним.
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})

	slog.SetDefault(slog.New(&traceHandler{
		Handler: base.WithAttrs([]slog.Attr{
			slog.String("service", cfg.ServiceName),
		}),
	}))
}

// traceHandler — обёртка над обычным хендлером slog, добавляющая trace_id.
//
// Это и есть то место, где логи «склеиваются» с трейсами: в Grafana по
// trace_id из лога открывается трейс, и наоборот. Без этого поля лог остаётся
// просто потоком строк, а вопрос «что происходило с ЭТИМ запросом» решается
// grep'ом по времени, то есть никак.
type traceHandler struct {
	slog.Handler
}

func (h *traceHandler) Handle(ctx context.Context, record slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		record.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, record)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithGroup(name)}
}

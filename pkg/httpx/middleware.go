package httpx

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Middleware — функция, оборачивающая один http.Handler в другой.
type Middleware func(http.Handler) http.Handler

// Chain собирает цепочку. Порядок аргументов — порядок ВЫПОЛНЕНИЯ:
// Chain(h, A, B) даёт A(B(h)), то есть A видит запрос первым и ответ последним.
//
// Это важно ровно для одного случая: Recover обязан стоять снаружи всех
// остальных, иначе паника в middleware уронит процесс до того, как её
// кто-нибудь поймает.
func Chain(h http.Handler, middlewares ...Middleware) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

// Default — набор, который нужен каждому HTTP-сервису проекта.
//
//	Recover  → паника в хендлере превращается в 500, а не в остановку процесса
//	otelhttp → спан на каждый запрос, traceparent из заголовков подхватывается
//	Metrics  → RED: Rate, Errors, Duration
//	Logging  → строка на запрос, с trace_id (его подмешивает pkg/otelx)
//
// otelhttp стоит ВТОРЫМ, до метрик и логов: спан должен уже существовать,
// когда логгер полезет за trace_id в контекст.
func Default(serviceName string) []Middleware {
	return []Middleware{
		Recover(),
		func(next http.Handler) http.Handler {
			return otelhttp.NewHandler(next, serviceName)
		},
		Metrics(serviceName),
		Logging(),
	}
}

// Recover ловит панику, пишет стек и отвечает 500.
//
// Без него любой nil-указатель в хендлере убивает весь процесс со всеми
// остальными запросами — net/http по умолчанию действительно так делает
// (точнее, глушит панику, но соединение обрывает без ответа).
func Recover() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					slog.ErrorContext(r.Context(), "паника в хендлере",
						"panic", rec,
						"method", r.Method,
						"path", r.URL.Path,
						"stack", string(debug.Stack()),
					)
					Error(w, "внутренняя ошибка", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// Logging пишет одну строку на запрос.
//
// Именно одну, и после ответа: лог «начал обрабатывать» вдвое увеличивает
// объём и почти никогда не нужен — незавершённый запрос виден по отсутствию
// записи и по трейсу.
func Logging() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			started := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			slog.InfoContext(r.Context(), "http",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration_ms", time.Since(started).Milliseconds(),
				"bytes", rec.written,
			)
		})
	}
}

// Metrics — метрики RED на каждый запрос.
//
//	Rate     — requests_total, скорость берётся из rate() в PromQL
//	Errors   — он же с label status: доля 5xx считается запросом, а не
//	           отдельной метрикой
//	Duration — гистограмма, потому что среднее по времени ответа бесполезно:
//	           оно не показывает хвост, а страдают пользователи именно в хвосте
//
// Label'ы выбраны так, чтобы количество временных рядов оставалось конечным:
// method, route, status. URL целиком в label класть НЕЛЬЗЯ — /media/photos/{id}
// с миллионом id даст миллион рядов и уронит Prometheus. Поэтому route — это
// шаблон из ServeMux, а не r.URL.Path.
func Metrics(serviceName string) Middleware {
	meter := otel.Meter("gosplash/httpx")

	requests, _ := meter.Int64Counter(
		"http_server_requests_total",
		metric.WithDescription("Количество HTTP-запросов"),
	)
	duration, _ := meter.Float64Histogram(
		"http_server_request_duration_seconds",
		metric.WithDescription("Длительность обработки HTTP-запроса"),
		metric.WithUnit("s"),
	)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			started := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			// r.Pattern — шаблон маршрута из ServeMux (Go 1.22+):
			// "GET /media/photos/{id}", а не конкретный путь. Ровно то,
			// что нужно как label.
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}

			attrs := metric.WithAttributes(
				attribute.String("service", serviceName),
				attribute.String("method", r.Method),
				attribute.String("route", route),
				attribute.Int("status", rec.status),
			)
			requests.Add(r.Context(), 1, attrs)
			duration.Record(r.Context(), time.Since(started).Seconds(), attrs)
		})
	}
}

// statusRecorder запоминает код ответа: сам http.ResponseWriter его не отдаёт.
type statusRecorder struct {
	http.ResponseWriter
	status  int
	written int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.written += n
	return n, err
}

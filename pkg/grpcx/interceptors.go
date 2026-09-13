package grpcx

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ServerConfig — параметры серверных интерсепторов.
type ServerConfig struct {
	// ServiceName попадает в лейбл метрик, как service.name в pkg/otelx.
	ServiceName string
	// JWTSecret — секрет проверки подписи HMAC. Публичные (не требующие
	// токена) методы перечисляются в PublicMethods, а не проверкой "секрет
	// пуст — значит auth выключен": пустой секрет по ошибке иначе тихо
	// отключил бы аутентификацию всему сервису.
	JWTSecret []byte
	// PublicMethods — точный список методов без аутентификации, ключ —
	// полное имя вида "/gosplash.catalog.v1.CatalogService/GetListing".
	// Список, а не аннотация в .proto: pkg/grpcx не имеет доступа к
	// protobuf-опциям сервисов, и явный список в main.go проще прочитать
	// целиком, чем искать аннотации по всем .proto файлам.
	PublicMethods map[string]struct{}
	// DefaultTimeout — дедлайн, который сервер проставляет сам, если его
	// не прислал клиент. 0 — не проставлять.
	DefaultTimeout time.Duration
}

// isPublic сообщает, освобождён ли метод от аутентификации.
func (c ServerConfig) isPublic(fullMethod string) bool {
	_, ok := c.PublicMethods[fullMethod]
	return ok
}

// ─────────────────────────────────────────────────────────────────────────────
// МЕТРИКИ RED
// ─────────────────────────────────────────────────────────────────────────────

var grpcMeter = otel.Meter("gosplash/grpcx")

var (
	grpcRequestsTotal   metric.Int64Counter
	grpcRequestDuration metric.Float64Histogram
)

func init() {
	var err error
	grpcRequestsTotal, err = grpcMeter.Int64Counter(
		"grpc_server_requests_total",
		metric.WithDescription("Количество gRPC-запросов по методу и коду"),
	)
	if err != nil {
		slog.Error("grpcx: метрика grpc_server_requests_total", "error", err)
	}
	grpcRequestDuration, err = grpcMeter.Float64Histogram(
		"grpc_server_request_duration_seconds",
		metric.WithDescription("Длительность обработки gRPC-запроса"),
		metric.WithUnit("s"),
	)
	if err != nil {
		slog.Error("grpcx: метрика grpc_server_request_duration_seconds", "error", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// UNARY
// ─────────────────────────────────────────────────────────────────────────────

// UnaryServerInterceptors собирает цепочку в порядке ВЫПОЛНЕНИЯ (как
// httpx.Chain: первый элемент видит запрос первым и ответ последним).
//
//	Recovery  → паника где угодно ниже по цепочке (включая другие
//	            интерсепторы) превращается в codes.Internal, а не в падение
//	            процесса и обрыв ВСЕХ остальных активных RPC.
//	Logging   → одна строка на вызов, читает code уже готового ответа.
//	Metrics   → RED, тот же принцип, что и в pkg/httpx.Metrics.
//	Deadline  → проставляет дефолтный дедлайн, если клиент не прислал свой,
//	            ДО того, как запрос дойдёт до аутентификации и хендлера.
//	Auth      → ближе всего к хендлеру: достаёт JWT из metadata, кладёт
//	            user_id в context.
//
// Auth — последний, а не первый, потому что даже отказ аутентификации
// обязан попасть в логи и метрики (это и есть попытка неавторизованного
// доступа, её нужно видеть), а не проскакивать мимо них.
func UnaryServerInterceptors(cfg ServerConfig) []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{
		recoveryUnaryInterceptor(),
		loggingUnaryInterceptor(),
		metricsUnaryInterceptor(cfg.ServiceName),
		deadlineUnaryInterceptor(cfg.DefaultTimeout),
		authUnaryInterceptor(cfg),
	}
}

func recoveryUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.ErrorContext(ctx, "grpc: паника в обработчике",
					"method", info.FullMethod,
					"panic", rec,
					"stack", string(debug.Stack()),
				)
				err = status.Error(codes.Internal, "внутренняя ошибка")
			}
		}()
		return handler(ctx, req)
	}
}

func loggingUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		resp, err := handler(ctx, req)
		slog.InfoContext(ctx, "grpc",
			"method", info.FullMethod,
			"code", status.Code(err).String(),
			"duration_ms", time.Since(started).Milliseconds(),
		)
		return resp, err
	}
}

func metricsUnaryInterceptor(serviceName string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		resp, err := handler(ctx, req)
		recordGRPCRequest(ctx, serviceName, info.FullMethod, status.Code(err), time.Since(started))
		return resp, err
	}
}

func recordGRPCRequest(ctx context.Context, serviceName, method string, code codes.Code, dur time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("service", serviceName),
		attribute.String("method", method),
		attribute.String("code", code.String()),
	)
	grpcRequestsTotal.Add(ctx, 1, attrs)
	grpcRequestDuration.Record(ctx, dur.Seconds(), attrs)
}

// deadlineUnaryInterceptor проставляет дефолтный дедлайн, ТОЛЬКО если его
// не прислал клиент.
//
// gRPC-транспорт сам переносит клиентский дедлайн (заголовок grpc-timeout)
// в ctx ДО вызова интерсепторов — если он там уже есть, ctx.Deadline()
// вернёт ok=true, и этот интерсептор ничего не делает: клиентский дедлайн
// и так уважается всем, что ниже по цепочке проверяет ctx.Done(). Ставить
// СВОЙ дедлайн ПОВЕРХ клиентского было бы ошибкой — можно случайно обрезать
// более щедрый бюджет времени, который клиент выделил специально (например,
// на длинный батч).
func deadlineUnaryInterceptor(defaultTimeout time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := ctx.Deadline(); !ok && defaultTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
			defer cancel()
		}
		return handler(ctx, req)
	}
}

func authUnaryInterceptor(cfg ServerConfig) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if cfg.isPublic(info.FullMethod) {
			return handler(ctx, req)
		}
		// Пустой секрет — проверка подписи выключена. Это НЕ «пропускаем
		// всё, что не проверилось»: сервис при старте обязан написать об
		// этом предупреждение в лог (см. main.go каждого сервиса), и режим
		// допустим только локально.
		//
		// Раньше здесь этой ветки не было, и получалось молчаливое
		// противоречие: сервис говорил «аутентификация выключена», а
		// интерсептор всё равно требовал заголовок authorization. Поймать
		// это можно было только живым вызовом — сага падала на первом же
		// шаге с Unauthenticated, хотя JWT в проекте пока никто не выдаёт.
		if len(cfg.JWTSecret) == 0 {
			return handler(ctx, req)
		}
		userID, err := authenticate(ctx, cfg.JWTSecret)
		if err != nil {
			return nil, err
		}
		return handler(contextWithUserID(ctx, userID), req)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// STREAM — те же пять интерсепторов, но обёрнутых для стриминга: у стрима
// нет единого resp/err в момент вызова, поэтому recovery/logging/metrics
// меряют весь стрим целиком (от открытия до Close), а auth и deadline
// подменяют ctx, который стрим отдаёт через ss.Context().
// ─────────────────────────────────────────────────────────────────────────────

// StreamServerInterceptors — порядок соответствует UnaryServerInterceptors.
func StreamServerInterceptors(cfg ServerConfig) []grpc.StreamServerInterceptor {
	return []grpc.StreamServerInterceptor{
		recoveryStreamInterceptor(),
		loggingStreamInterceptor(),
		metricsStreamInterceptor(cfg.ServiceName),
		deadlineStreamInterceptor(cfg.DefaultTimeout),
		authStreamInterceptor(cfg),
	}
}

// wrappedStream подменяет Context(), возвращаемый серверным стримом:
// grpc.ServerStream не даёt другого способа изменить context на пути вниз
// по цепочке интерсепторов.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

func recoveryStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.ErrorContext(ss.Context(), "grpc: паника в стриме",
					"method", info.FullMethod,
					"panic", rec,
					"stack", string(debug.Stack()),
				)
				err = status.Error(codes.Internal, "внутренняя ошибка")
			}
		}()
		return handler(srv, ss)
	}
}

func loggingStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		started := time.Now()
		err := handler(srv, ss)
		slog.InfoContext(ss.Context(), "grpc-stream",
			"method", info.FullMethod,
			"code", status.Code(err).String(),
			"duration_ms", time.Since(started).Milliseconds(),
		)
		return err
	}
}

func metricsStreamInterceptor(serviceName string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		started := time.Now()
		err := handler(srv, ss)
		recordGRPCRequest(ss.Context(), serviceName, info.FullMethod, status.Code(err), time.Since(started))
		return err
	}
}

func deadlineStreamInterceptor(defaultTimeout time.Duration) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		if _, ok := ctx.Deadline(); !ok && defaultTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
			defer cancel()
			ss = &wrappedStream{ServerStream: ss, ctx: ctx}
		}
		return handler(srv, ss)
	}
}

func authStreamInterceptor(cfg ServerConfig) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if cfg.isPublic(info.FullMethod) {
			return handler(srv, ss)
		}
		// Симметрично unary: пустой секрет — проверка выключена.
		if len(cfg.JWTSecret) == 0 {
			return handler(srv, ss)
		}
		userID, err := authenticate(ss.Context(), cfg.JWTSecret)
		if err != nil {
			return err
		}
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: contextWithUserID(ss.Context(), userID)})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// JWT
// ─────────────────────────────────────────────────────────────────────────────

type contextKey int

const userIDKey contextKey = iota

func contextWithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
}

// UserID достаёт user_id, положенный auth-интерсептором. ok=false — либо
// метод публичный (интерсептор не запускался), либо вызван вне gRPC-сервера
// grpcx.
func UserID(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(userIDKey).(string)
	return v, ok
}

// authenticate достаёт Bearer-токен из metadata, проверяет подпись HMAC и
// возвращает claim "sub" как user_id.
//
// Секрет передаётся параметром (ServerConfig.JWTSecret), а не читается из
// os.Getenv здесь: по правилу проекта (docs/STYLE.md) os.Getenv вызывается
// только в pkg/config, дальше по коду ходит уже разобранное значение.
func authenticate(ctx context.Context, secret []byte) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "нет metadata запроса")
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return "", status.Error(codes.Unauthenticated, "заголовок authorization отсутствует")
	}
	raw := strings.TrimPrefix(values[0], "Bearer ")

	token, err := jwt.Parse(raw, func(t *jwt.Token) (any, error) {
		// Явная проверка алгоритма — обязательная часть проверки JWT, а не
		// перестраховка: если этого не сделать, токен с alg=none или с
		// asymmetric-алгоритмом, подписанный ЧУЖИМ приватным ключом,
		// пройдёт проверку, потому что jwt-библиотека применит переданный
		// сюда secret как ключ для ЛЮБОГО алгоритма, который заявит сам
		// токен (classic "alg confusion").
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("неожиданный метод подписи: %v", t.Header["alg"])
		}
		return secret, nil
	})
	if err != nil || !token.Valid {
		return "", status.Error(codes.Unauthenticated, "невалидный токен")
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "невалидные claims токена")
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return "", status.Error(codes.Unauthenticated, "в токене нет sub (user_id)")
	}
	return sub, nil
}

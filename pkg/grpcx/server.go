package grpcx

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// ServerParams — то, что нужно каждому gRPC-серверу проекта, аналогично
// httpx.Default для HTTP-серверов.
type ServerParams struct {
	ServiceName    string
	JWTSecret      []byte
	PublicMethods  []string
	DefaultTimeout time.Duration

	// KeepaliveEnforcement — минимальные требования к клиентским пингам.
	// Нулевое значение означает "использовать defaults grpc-go", а не
	// "keepalive выключен": сам grpc-go всегда что-то использует.
	KeepaliveEnforcement keepalive.EnforcementPolicy
	KeepaliveParams      keepalive.ServerParameters

	// ExtraUnary / ExtraStream — дополнительные интерсепторы конкретного
	// сервиса (например, wallet может добавить свою проверку
	// idempotency_key). Добавляются ПОСЛЕ базового набора, то есть ближе
	// к хендлеру: видят уже аутентифицированный ctx с user_id.
	ExtraUnary  []grpc.UnaryServerInterceptor
	ExtraStream []grpc.StreamServerInterceptor
}

// NewServer собирает *grpc.Server с трассировкой (otelgrpc), keepalive и
// полным набором интерсепторов пакета (recovery, logging, metrics, deadline,
// auth) плюс интерсепторами вызывающего сервиса, если они заданы.
func NewServer(p ServerParams) *grpc.Server {
	public := make(map[string]struct{}, len(p.PublicMethods))
	for _, m := range p.PublicMethods {
		public[m] = struct{}{}
	}

	cfg := ServerConfig{
		ServiceName:    p.ServiceName,
		JWTSecret:      p.JWTSecret,
		PublicMethods:  public,
		DefaultTimeout: p.DefaultTimeout,
	}

	unary := append(UnaryServerInterceptors(cfg), p.ExtraUnary...)
	stream := append(StreamServerInterceptors(cfg), p.ExtraStream...)

	opts := []grpc.ServerOption{
		// StatsHandler, а не interceptor: он же используется сегодня в
		// services/media и services/catalog (см. cmd/main.go), поведение
		// не меняется при переходе на пакет.
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
		grpc.KeepaliveEnforcementPolicy(p.KeepaliveEnforcement),
	}
	if p.KeepaliveParams != (keepalive.ServerParameters{}) {
		opts = append(opts, grpc.KeepaliveParams(p.KeepaliveParams))
	}

	return grpc.NewServer(opts...)
}

// Serve запускает сервер на addr и блокирует вызывающую горутину — как
// httpx.Serve, вызывать в go func().
func Serve(server *grpc.Server, addr, name string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("grpcx: listen %s: %w", addr, err)
	}
	slog.Info("grpc: слушаю", "server", name, "addr", addr)
	if err := server.Serve(listener); err != nil {
		return fmt.Errorf("grpcx: serve: %w", err)
	}
	return nil
}

// Shutdown останавливает сервер, давая доработать активным RPC, но не
// дольше timeout.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: в отличие от pkg/httpx.Shutdown, у *grpc.Server нет
// метода Shutdown(ctx) — GracefulStop() не принимает контекст и либо
// дождётся ВСЕХ активных RPC и стримов сама, либо будет ждать бесконечно
// (например, если клиент открыл server-streaming WatchListing и не
// закрывает соединение). Таймаут поэтому реализован снаружи: если
// GracefulStop не уложился, вызывается Stop() — жёсткий обрыв оставшихся
// RPC, тот же компромисс, что и таймаут в httpx.Shutdown, только без
// возможности точно достучаться до net/http.Server.Shutdown семантики.
func Shutdown(server *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		slog.Error("grpc: graceful stop не уложился в таймаут, обрываю активные RPC")
		server.Stop()
		<-done // Stop() заставляет GracefulStop() выше вернуться немедленно
	}
}

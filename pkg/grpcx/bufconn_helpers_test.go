package grpcx

// Общие помощники для интерсепторных тестов: поднимают gRPC-сервер поверх
// bufconn (в памяти, без TCP и без портов, которые могли бы конфликтовать
// между тестами) и дают клиентское соединение к нему.

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func unavailableErr() error {
	return status.Error(codes.Unavailable, "сервис временно недоступен")
}

// newBufconnServer поднимает *grpc.Server с переданными опциями и
// регистрирует testServiceDesc с impl. Возвращает клиентское соединение;
// сервер и соединение останавливаются автоматически по t.Cleanup.
func newBufconnServer(t *testing.T, impl testServiceServer, opts ...grpc.ServerOption) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(opts...)
	srv.RegisterService(&testServiceDesc, impl)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func callEcho(ctx context.Context, conn *grpc.ClientConn, value string) (*wrapperspb.StringValue, error) {
	out := new(wrapperspb.StringValue)
	err := conn.Invoke(ctx, testMethodEcho, wrapperspb.String(value), out)
	return out, err
}

func callPanic(ctx context.Context, conn *grpc.ClientConn) error {
	out := new(wrapperspb.StringValue)
	return conn.Invoke(ctx, testMethodPanic, wrapperspb.String("x"), out)
}

func callEchoStream(ctx context.Context, conn *grpc.ClientConn, value string) (*wrapperspb.StringValue, error) {
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{StreamName: "EchoStream", ServerStreams: true}, testMethodEchoStream)
	if err != nil {
		return nil, err
	}
	if err := stream.SendMsg(wrapperspb.String(value)); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	out := new(wrapperspb.StringValue)
	err = stream.RecvMsg(out)
	return out, err
}

func callPanicStream(ctx context.Context, conn *grpc.ClientConn, value string) error {
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{StreamName: "PanicStream", ServerStreams: true}, testMethodPanicStream)
	if err != nil {
		return err
	}
	if err := stream.SendMsg(wrapperspb.String(value)); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	out := new(wrapperspb.StringValue)
	return stream.RecvMsg(out)
}

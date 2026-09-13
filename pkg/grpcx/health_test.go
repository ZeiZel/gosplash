package grpcx

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

func TestHealth_ServingPoUmolchaniyuIPereklyuchenieVNotServing(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	h := NewHealth()
	h.Register(srv)
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

	client := healthpb.NewHealthClient(conn)

	resp, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus(),
		"свежесозданный Health обязан отвечать SERVING")

	// Первый шаг graceful shutdown: переключаем статус, НЕ останавливая
	// сервер — активные RPC продолжают работать, но readiness-проба уже
	// увидит NOT_SERVING и уберёт под из балансировки.
	h.NotServing()

	resp, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, resp.GetStatus())

	h.Serving()
	resp, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
}

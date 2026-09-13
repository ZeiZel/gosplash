package grpcx

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"gosplash/pkg/resilience"
)

// dialBufconnWithInterceptor — как newBufconnServer, но клиентский
// интерсептор задаётся ПРИ ОТКРЫТИИ соединения (единственное место, где
// grpc-go вообще принимает UnaryClientInterceptor), поэтому сервер и клиент
// здесь собираются отдельно, в отличие от остальных тестов пакета.
func dialBufconnWithInterceptor(t *testing.T, impl testServiceServer, interceptor grpc.UnaryClientInterceptor) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	srv.RegisterService(&testServiceDesc, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(interceptor),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestRetryClientInterceptor_RetraitPokaNeUspeetLibNeKonchatsyaPopytki(t *testing.T) {
	impl := &fakeServer{failTimes: 2} // первые два вызова — Unavailable, третий — успех
	retrier := resilience.NewRetrier(resilience.RetryConfig{
		MaxAttempts: 4,
		BaseDelay:   1 * time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	}, resilience.Idempotent)

	conn := dialBufconnWithInterceptor(t, impl, retryUnaryClientInterceptor(retrier))

	out, err := callEcho(context.Background(), conn, "привет")

	require.NoError(t, err, "после исчерпания failTimes retry обязан довести вызов до успеха")
	assert.Equal(t, "привет", out.GetValue())
	assert.Equal(t, 3, impl.callCount(), "два отказа + один успешный вызов")
}

func TestRetryClientInterceptor_IsсherpavPopytkiVozvrashaetPosledniuyuOshibku(t *testing.T) {
	impl := &fakeServer{failTimes: 100} // никогда не отвечает успехом
	retrier := resilience.NewRetrier(resilience.RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   1 * time.Millisecond,
		MaxDelay:    5 * time.Millisecond,
	}, resilience.Idempotent)

	conn := dialBufconnWithInterceptor(t, impl, retryUnaryClientInterceptor(retrier))

	_, err := callEcho(context.Background(), conn, "привет")

	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, 3, impl.callCount(), "должны быть использованы все MaxAttempts")
}

func TestRetryClientInterceptor_NeIdempotentnyKlientNeRetraitVyzovCherezSet(t *testing.T) {
	// Тот же контракт, что и в pkg/resilience, но проверенный на настоящем
	// сетевом вызове (пусть и через bufconn): даже когда сервер отвечает
	// Unavailable (формально retryable), неидемпотентный ретраер не делает
	// вторую попытку.
	impl := &fakeServer{failTimes: 100}
	retrier := resilience.NewRetrier(resilience.RetryConfig{
		MaxAttempts: 5,
		BaseDelay:   1 * time.Millisecond,
		MaxDelay:    5 * time.Millisecond,
	}, resilience.NotIdempotent)

	conn := dialBufconnWithInterceptor(t, impl, retryUnaryClientInterceptor(retrier))

	_, err := callEcho(context.Background(), conn, "списание")

	require.Error(t, err)
	assert.Equal(t, 1, impl.callCount(), "неидемпотентный вызов не должен повторяться")
}

func TestBreakerClientInterceptor_OtkrytyBreakerOtvechaetMgnovennoBezVyzovaServera(t *testing.T) {
	impl := &fakeServer{failTimes: 1000}
	breaker := resilience.NewBreaker(resilience.BreakerConfig{
		Name:        t.Name(),
		MaxRequests: 1,
		Interval:    time.Minute,
		Timeout:     time.Minute, // не истечёт за время теста
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 1
		},
	})

	conn := dialBufconnWithInterceptor(t, impl, breakerUnaryClientInterceptor(breaker))

	// Первый вызов реально идёт на сервер и проваливается — этим же вызовом
	// breaker открывается (порог ReadyToTrip выставлен в 1).
	_, err := callEcho(context.Background(), conn, "x")
	require.Error(t, err)
	require.Equal(t, gobreaker.StateOpen, breaker.State())
	callsAfterFirst := impl.callCount()

	// Второй вызов обязан провалиться мгновенно, кодом Unavailable, и БЕЗ
	// обращения к серверу — именно это превращает таймаут в "503 за 50мс".
	_, err = callEcho(context.Background(), conn, "y")
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, callsAfterFirst, impl.callCount(),
		"при открытом breaker сервер не должен получать запрос вовсе")
}

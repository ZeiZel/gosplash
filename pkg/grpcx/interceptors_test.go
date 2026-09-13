package grpcx

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func serverOpts(cfg ServerConfig) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(UnaryServerInterceptors(cfg)...),
		grpc.ChainStreamInterceptor(StreamServerInterceptors(cfg)...),
	}
}

func signToken(t *testing.T, secret []byte, sub string) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": sub,
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	signed, err := token.SignedString(secret)
	require.NoError(t, err)
	return signed
}

func withBearer(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func TestRecovery_PanikaVUnaryStanovitsyaInternal(t *testing.T) {
	// Без recovery паника в обработчике оборвала бы соединение (или уронила
	// бы процесс целиком, унеся с собой все остальные активные RPC) —
	// клиент не получил бы вообще никакого статуса.
	impl := &fakeServer{}
	cfg := ServerConfig{ServiceName: "test", PublicMethods: map[string]struct{}{testMethodPanic: {}}}
	conn := newBufconnServer(t, impl, serverOpts(cfg)...)

	err := callPanic(context.Background(), conn)

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

func TestRecovery_PanikaVStrimeStanovitsyaInternal(t *testing.T) {
	impl := &fakeServer{}
	cfg := ServerConfig{ServiceName: "test", PublicMethods: map[string]struct{}{testMethodPanicStream: {}}}
	conn := newBufconnServer(t, impl, serverOpts(cfg)...)

	err := callPanicStream(context.Background(), conn, "x")

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

func TestAuth_ValidnyTokenProhoditIProkidyvaetUserID(t *testing.T) {
	secret := []byte("секрет-теста")
	impl := &fakeServer{}
	cfg := ServerConfig{ServiceName: "test", JWTSecret: secret}
	conn := newBufconnServer(t, impl, serverOpts(cfg)...)

	token := signToken(t, secret, "user-42")
	ctx := withBearer(context.Background(), token)

	out, err := callEcho(ctx, conn, "привет")
	require.NoError(t, err)
	assert.Equal(t, "привет", out.GetValue())

	userID, ok := UserID(impl.contextOfLastCall())
	require.True(t, ok, "auth-интерсептор обязан положить user_id в context обработчика")
	assert.Equal(t, "user-42", userID)
}

func TestAuth_BezTokenaOtvergaetsya(t *testing.T) {
	secret := []byte("секрет-теста")
	impl := &fakeServer{}
	cfg := ServerConfig{ServiceName: "test", JWTSecret: secret}
	conn := newBufconnServer(t, impl, serverOpts(cfg)...)

	_, err := callEcho(context.Background(), conn, "привет")

	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestAuth_BitiyTokenOtvergaetsya(t *testing.T) {
	secret := []byte("секрет-теста")
	impl := &fakeServer{}
	cfg := ServerConfig{ServiceName: "test", JWTSecret: secret}
	conn := newBufconnServer(t, impl, serverOpts(cfg)...)

	// Подписан ДРУГИМ секретом — подпись не сойдётся.
	token := signToken(t, []byte("чужой секрет"), "user-42")
	ctx := withBearer(context.Background(), token)

	_, err := callEcho(ctx, conn, "привет")

	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestAuth_PublichnyMetodProhoditBezTokena(t *testing.T) {
	secret := []byte("секрет-теста")
	impl := &fakeServer{}
	cfg := ServerConfig{
		ServiceName:   "test",
		JWTSecret:     secret,
		PublicMethods: map[string]struct{}{testMethodEcho: {}},
	}
	conn := newBufconnServer(t, impl, serverOpts(cfg)...)

	out, err := callEcho(context.Background(), conn, "без токена")

	require.NoError(t, err, "исключённый из auth метод обязан работать без authorization")
	assert.Equal(t, "без токена", out.GetValue())
}

func TestAuth_StrimTozheTrebuetToken(t *testing.T) {
	secret := []byte("секрет-теста")
	impl := &fakeServer{}
	cfg := ServerConfig{ServiceName: "test", JWTSecret: secret}
	conn := newBufconnServer(t, impl, serverOpts(cfg)...)

	_, err := callEchoStream(context.Background(), conn, "x")
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	token := signToken(t, secret, "user-7")
	ctx := withBearer(context.Background(), token)
	out, err := callEchoStream(ctx, conn, "x")
	require.NoError(t, err)
	assert.Equal(t, "x", out.GetValue())

	userID, ok := UserID(impl.contextOfLastCall())
	require.True(t, ok)
	assert.Equal(t, "user-7", userID)
}

func TestDeadline_StavitsyaTolkoEsliKlientNePrislalSvoy(t *testing.T) {
	impl := &fakeServer{}
	cfg := ServerConfig{
		ServiceName:    "test",
		PublicMethods:  map[string]struct{}{testMethodEcho: {}},
		DefaultTimeout: 50 * time.Millisecond,
	}
	conn := newBufconnServer(t, impl, serverOpts(cfg)...)

	_, err := callEcho(context.Background(), conn, "без дедлайна")
	require.NoError(t, err)

	_, ok := impl.contextOfLastCall().Deadline()
	assert.True(t, ok, "если клиент не прислал дедлайн, сервер обязан поставить свой по умолчанию")
}

func TestDeadline_KlientskyDedlineNePerezapisyvaetsya(t *testing.T) {
	impl := &fakeServer{}
	cfg := ServerConfig{
		ServiceName:    "test",
		PublicMethods:  map[string]struct{}{testMethodEcho: {}},
		DefaultTimeout: 10 * time.Millisecond, // короче клиентского
	}
	conn := newBufconnServer(t, impl, serverOpts(cfg)...)

	clientTimeout := time.Second
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	_, err := callEcho(ctx, conn, "с дедлайном")
	require.NoError(t, err)

	deadline, ok := impl.contextOfLastCall().Deadline()
	require.True(t, ok)
	// Если бы сервер переставил дедлайн на DefaultTimeout, оставшееся время
	// было бы порядка 10мс, а не сотен миллисекунд — клиентский бюджет был
	// бы обрезан без всякой причины.
	assert.Greater(t, time.Until(deadline), 100*time.Millisecond,
		"сервер не должен урезать более щедрый дедлайн, присланный клиентом")
}

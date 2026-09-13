package bootstrap_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"gosplash/services/wallet/internal/bootstrap"
)

// fakeRelay — Relay без сети: Run блокируется до отмены ctx, как настоящий
// outbox.Relay, но не открывает ни одного соединения. Именно это и есть
// находка задания: раз outbox.New не может собраться без живых Kafka
// и Postgres (см. комментарий Deps.Relay), Deps принимает интерфейс,
// а не *outbox.Relay, — и тест подставляет вот эту подделку.
type fakeRelay struct {
	closed bool
}

func (r *fakeRelay) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (r *fakeRelay) Close() { r.closed = true }

func (r *fakeRelay) Ping(context.Context) error { return nil }

// newTestDB — рабочий *gorm.DB без сети: sqlite в памяти вместо реального
// Postgres. NewApp сам в БД не ходит (только оборачивает *gorm.DB в
// репозиторий), поэтому для проверки сборки и остановки App годится любая
// живая GORM-база, а поднимать ради этого контейнер с Postgres незачем.
func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err, "sqlite в памяти обязана открыться без ошибок")
	return db
}

// TestNewApp_TrebuetObyazatelnyeZavisimosti проверяет отказ без похода
// в сеть: раз DB/Relay не заданы, ошибка возвращается сразу.
func TestNewApp_TrebuetObyazatelnyeZavisimosti(t *testing.T) {
	db := newTestDB(t)

	_, err := bootstrap.NewApp(bootstrap.Deps{Relay: &fakeRelay{}})
	assert.Error(t, err, "без DB App собираться не должен")

	_, err = bootstrap.NewApp(bootstrap.Deps{DB: db})
	assert.Error(t, err, "без Relay App собираться не должен")
}

// TestApp_RunOstanavlivaetsyaPoOtmeneKontekstaIСloseIdempotenten — главный
// тест композиционного корня: App собирается без сети (sqlite в памяти +
// fakeRelay), Run быстро возвращается по отменённому контексту (а не
// зависает, как зависал бы kafkax.Consumer.Close без AllowRebalance —
// см. docs задания), и Close можно звать сколько угодно раз подряд.
//
// ServeMetricsAndPprof регистрирует "GET /metrics" в http.DefaultServeMux
// (pkg/httpx.ServeMetricsAndPprof) — повторная регистрация в том же
// процессе паникует, поэтому Run в этом пакете вызывается РОВНО ОДИН РАЗ
// на весь тестовый бинарник, отсюда один тест на все три проверки, а не три.
func TestApp_RunOstanavlivaetsyaPoOtmeneKontekstaIСloseIdempotenten(t *testing.T) {
	relay := &fakeRelay{}
	application, err := bootstrap.NewApp(bootstrap.Deps{
		DB:          newTestDB(t),
		Relay:       relay,
		GRPCAddr:    ":0",
		HTTPAddr:    ":0",
		MetricsAddr: ":0",
	})
	require.NoError(t, err, "App обязан собраться из живых, но локальных зависимостей")

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- application.Run(ctx) }()

	// Даём серверам реально подняться на ":0", потом сразу просим остановиться —
	// иначе можно отменить контекст раньше, чем grpcx.Serve успел вызвать
	// net.Listen, и тест бы проверял не то поведение.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-runErr:
		assert.NoError(t, err, "остановка по отменённому контексту не должна возвращать ошибку")
	case <-time.After(5 * time.Second):
		t.Fatal("Run не вернулся за 5 секунд после отмены контекста — похоже на зависший shutdown")
	}

	assert.False(t, relay.closed, "App.Close не обязан закрывать чужой Relay — им владеет run() в main.go")

	assert.NotPanics(t, func() {
		require.NoError(t, application.Close())
		require.NoError(t, application.Close())
	}, "Close обязан быть идемпотентным и не паниковать при повторном вызове")
}

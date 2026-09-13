package bootstrap_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/analytics/internal/bootstrap"
	"gosplash/services/analytics/internal/domain"
)

// fakeStatsReader — подделка ports.StatsReader (docs/STYLE.md: «прикладной
// слой тестируется подделками портов, без контейнеров»). Методы в этом
// тесте не вызываются: Run поднимает серверы и сразу останавливается по
// отменённому ctx, не обслужив ни одного запроса.
type fakeStatsReader struct{}

func (fakeStatsReader) TopPhotos(context.Context, domain.Period, int32) ([]domain.PhotoRank, error) {
	return nil, nil
}
func (fakeStatsReader) PhotoStats(context.Context, string, domain.Period) (domain.PhotoStats, error) {
	return domain.PhotoStats{}, nil
}

// fakeConsumers — Consumers без Kafka: RunViews/RunPurchases блокируются до
// отмены ctx, как настоящие kafkax.Consumer.Run, но не открывают ни одного
// соединения — находка задания: analyticskafka.New не может собраться без
// живой Kafka (см. её комментарий в internal/adapters/kafka/consumer.go),
// поэтому Deps принимает интерфейс, а не *kafka.Consumers.
type fakeConsumers struct{ closed bool }

func (c *fakeConsumers) RunViews(ctx context.Context) error     { <-ctx.Done(); return nil }
func (c *fakeConsumers) RunPurchases(ctx context.Context) error { <-ctx.Done(); return nil }
func (c *fakeConsumers) PingViews(context.Context) error        { return nil }
func (c *fakeConsumers) PingPurchases(context.Context) error    { return nil }
func (c *fakeConsumers) Close()                                 { c.closed = true }

func validDeps() bootstrap.Deps {
	return bootstrap.Deps{
		StatsReader: fakeStatsReader{},
		Consumers:   &fakeConsumers{},
		GRPCAddr:    ":0",
		HTTPAddr:    ":0",
		MetricsAddr: ":0",
	}
}

// TestNewApp_TrebuetObyazatelnyeZavisimosti проверяет отказ без похода
// в сеть: без StatsReader/Consumers App собираться не должен.
func TestNewApp_TrebuetObyazatelnyeZavisimosti(t *testing.T) {
	deps := validDeps()
	deps.StatsReader = nil
	_, err := bootstrap.NewApp(deps)
	assert.Error(t, err, "без StatsReader App собираться не должен")

	deps = validDeps()
	deps.Consumers = nil
	_, err = bootstrap.NewApp(deps)
	assert.Error(t, err, "без Consumers App собираться не должен")
}

// TestApp_RunOstanavlivaetsyaPoOtmeneKontekstaICloseIdempotenten — главный
// тест композиционного корня: App собирается из подделок (без ClickHouse
// и Kafka), Run быстро возвращается по отменённому контексту (не зависает,
// как зависал бы kafkax.Consumer.Close без AllowRebalance — см. задание),
// а Close можно звать сколько угодно раз подряд.
//
// ServeMetricsAndPprof регистрирует "GET /metrics" в http.DefaultServeMux —
// повторная регистрация в том же процессе паникует, поэтому Run в этом
// пакете вызывается РОВНО ОДИН РАЗ на весь тестовый бинарник.
func TestApp_RunOstanavlivaetsyaPoOtmeneKontekstaICloseIdempotenten(t *testing.T) {
	consumers := &fakeConsumers{}
	deps := validDeps()
	deps.Consumers = consumers

	application, err := bootstrap.NewApp(deps)
	require.NoError(t, err, "App обязан собраться из одних подделок портов")

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- application.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-runErr:
		assert.NoError(t, err, "остановка по отменённому контексту не должна возвращать ошибку")
	case <-time.After(5 * time.Second):
		t.Fatal("Run не вернулся за 5 секунд после отмены контекста — похоже на зависший shutdown")
	}

	assert.False(t, consumers.closed, "App.Close не обязан закрывать чужие Consumers — ими владеет run() в main.go")

	assert.NotPanics(t, func() {
		require.NoError(t, application.Close())
		require.NoError(t, application.Close())
	}, "Close обязан быть идемпотентным и не паниковать при повторном вызове")
}

package kafkax

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// deadBroker — заведомо недоступный адрес (порт 1 — привилегированный,
// на нём никто не слушает). Тесты в этом файле ничего не отправляют по
// сети до того среза, на котором раньше происходило зависание: PollFetches
// проверяет отменённый ctx ДО обращения к брокеру, а Close() виснет на
// внутренней синхронизации kgo (sync.Cond), а не на сетевом таймауте.
// Поэтому кластер/докер для этих тестов не нужен.
var deadBroker = []string{"127.0.0.1:1"}

func noopHandler(context.Context, *eventsv1.Envelope) error { return nil }

// waitOrFail дожидается закрытия ch не дольше timeout, иначе валит тест.
func waitOrFail(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}

// TestConsumerRun_OtmenaKontekstaNeVeshaetClose — регрессия на баг «под не
// уходит из Terminating»: Consumer.Run(ctx) c уже отменённым ctx обязан не
// только сам быстро вернуться, но и не оставить клиент в состоянии, когда
// последующий Close() виснет навсегда.
//
// НАСТОЯЩАЯ ПРИЧИНА (см. также комментарий в kafka.go, Run): PollFetches
// с kgo.BlockRebalanceOnPoll() ставит внутренний счётчик "poller" перед
// КАЖДЫМ возвратом — включая фиктивный fetch с ctx.Err(), которым отвечает
// на уже отменённый контекст (twmb/franz-go/pkg/kgo/consumer.go: "we still
// need to add a poller ... since we are returning a fetch"). Client.Close()
// сначала покидает consumer group (LeaveGroupContext), а тот ждёт, пока
// счётчик poller'ов не станет нулевым — снять его может только
// AllowRebalance(). До фикса Run() возвращался по ctx.Err() БЕЗ вызова
// AllowRebalance(), и Close() блокировался НАВСЕГДА (godoc Client.Close:
// «will hang if you polled, did not allow rebalances, and want to close»).
// В кластере это выглядело как под, который держит партиции в Terminating,
// пока Kubernetes не убьёт его SIGKILL'ом по истечении
// terminationGracePeriodSeconds.
//
// До фикса: падает на шаге Close (висит все 3с). После фикса: оба шага
// укладываются в миллисекунды.
func TestConsumerRun_OtmenaKontekstaNeVeshaetClose(t *testing.T) {
	c, err := NewConsumer(deadBroker, "test-group-consumer", "test-topic")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // уже отменён — как ctx сервиса после SIGTERM к моменту,
	// когда его увидит цикл Run.

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx, noopHandler) }()

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Consumer.Run не вернулся за 2с по отменённому ctx")
	}

	closeDone := make(chan struct{})
	go func() {
		c.Close()
		close(closeDone)
	}()
	waitOrFail(t, closeDone, 3*time.Second,
		"Consumer.Close завис — BlockRebalanceOnPoll не был снят при выходе Run() по отменённому ctx")
}

// TestRetryConsumerRun_OtmenaKontekstaNeVeshaetClose — тот же баг и тот же
// фикс, но в RetryConsumer.Run (retry.go): цикл PollFetches там устроен
// идентично основному консьюмеру и страдал ровно тем же дефектом.
func TestRetryConsumerRun_OtmenaKontekstaNeVeshaetClose(t *testing.T) {
	rc, err := NewRetryConsumer(deadBroker, "test-group-retry", "test-topic")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- rc.Run(ctx, noopHandler) }()

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("RetryConsumer.Run не вернулся за 2с по отменённому ctx")
	}

	closeDone := make(chan struct{})
	go func() {
		rc.Close()
		close(closeDone)
	}()
	waitOrFail(t, closeDone, 3*time.Second,
		"RetryConsumer.Close завис — BlockRebalanceOnPoll не был снят при выходе Run() по отменённому ctx")
}

// Отдельного дедлока в partitionEngine.stopAll() НЕ обнаружено: stop()
// ждёт только те горутины, чьи каналы уже закрыты через quit, а сами
// горутины не блокируются ни на чём, кроме select{quit, recs} или
// синхронного process(handler) — это поведение уже покрыто существующими
// TestPartitionEngine_StopAllOstanavlivaetVseVorkery и
// TestPartitionEngine_RevokedZhdyotZaversheniyaVorkerov в engine_test.go
// (второй явно проверяет, что stop блокируется РОВНО до конца обработки
// занятого воркера и не дольше). Новый тест сюда не нужен — баг был не
// в движке, а в том, что PollFetches добавляет "poller" на КАЖДЫЙ путь
// возврата, включая отменённый ctx, а Run() не звал AllowRebalance() на
// этом пути (см. TestConsumerRun_OtmenaKontekstaNeVeshaetClose выше).

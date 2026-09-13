package kafkax

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func fetchesOf(topic string, partition int32, records ...*kgo.Record) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      topic,
		Partitions: []kgo.FetchPartition{{Partition: partition, Records: records}},
	}}}}
}

func TestPartitionEngine_DispatchObrabatyvaetZapisiPoPartitsiyam(t *testing.T) {
	var mu sync.Mutex
	var processed []int32 // партиция, которой принадлежала запись

	engine := newPartitionEngine(context.Background(), func(_ context.Context, record *kgo.Record) {
		mu.Lock()
		processed = append(processed, record.Partition)
		mu.Unlock()
	})

	engine.assigned(context.Background(), nil, map[string][]int32{"t": {0, 1}})
	defer engine.stopAll()

	engine.dispatch(fetchesOf("t", 0, &kgo.Record{Partition: 0}))
	engine.dispatch(fetchesOf("t", 1, &kgo.Record{Partition: 1}))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(processed) == 2
	}, time.Second, time.Millisecond, "обе записи должны быть обработаны своими горутинами")
}

func TestPartitionEngine_DispatchBezVorkeraNePadaet(t *testing.T) {
	// Партицию отозвали между PollFetches и dispatch — записи для неё просто
	// не находят воркера. Это не ошибка (см. комментарий в dispatch), но
	// и не должно паниковать.
	engine := newPartitionEngine(context.Background(), func(context.Context, *kgo.Record) {
		t.Fatal("process не должен вызываться для неназначенной партиции")
	})

	assert.NotPanics(t, func() {
		engine.dispatch(fetchesOf("t", 5, &kgo.Record{Partition: 5}))
	})
}

func TestPartitionEngine_RevokedZhdyotZaversheniyaVorkerov(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var finished atomicBool

	engine := newPartitionEngine(context.Background(), func(context.Context, *kgo.Record) {
		close(started)
		<-release
		finished.set(true)
	})

	engine.assigned(context.Background(), nil, map[string][]int32{"t": {0}})
	engine.dispatch(fetchesOf("t", 0, &kgo.Record{Partition: 0}))

	<-started // воркер зашёл в обработку и застрял на release

	revokedDone := make(chan struct{})
	go func() {
		// revoked обязан ЗАБЛОКИРОВАТЬСЯ, пока воркер не отпустят —
		// в этом весь смысл BlockRebalanceOnPoll + goroutine-per-partition:
		// без этого ожидания кgo мог бы отдать партицию другому консьюмеру,
		// пока эта горутина ещё дописывает и коммитит текущую запись.
		engine.revoked(context.Background(), nil, map[string][]int32{"t": {0}})
		close(revokedDone)
	}()

	select {
	case <-revokedDone:
		t.Fatal("revoked вернулся до того, как воркер закончил обработку")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-revokedDone:
	case <-time.After(time.Second):
		t.Fatal("revoked не вернулся после освобождения воркера")
	}
	assert.True(t, finished.get())
}

func TestPartitionEngine_StopAllOstanavlivaetVseVorkery(t *testing.T) {
	engine := newPartitionEngine(context.Background(), func(context.Context, *kgo.Record) {})
	engine.assigned(context.Background(), nil, map[string][]int32{"a": {0, 1}, "b": {0}})

	engine.stopAll()

	engine.mu.Lock()
	defer engine.mu.Unlock()
	assert.Empty(t, engine.workers, "после stopAll ни одного воркера остаться не должно")
}

// atomicBool — минимальный потокобезопасный флаг для теста выше, без
// добавления sync/atomic.Bool ради одного места (появился в Go 1.19,
// использовать можно, но здесь достаточно и мьютекса — меньше импортов).
type atomicBool struct {
	mu sync.Mutex
	v  bool
}

func (b *atomicBool) set(v bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.v = v
}

func (b *atomicBool) get() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.v
}

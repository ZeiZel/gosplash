package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// fakePublisher — подделка EventPublisher: считает опубликованные конверты
// потокобезопасно, потому что flush выполняется в фоновой горутине
// батчера, а проверки — в горутине теста.
type fakePublisher struct {
	mu        sync.Mutex
	published []*eventsv1.PhotoViewed
}

func (p *fakePublisher) Publish(_ context.Context, _ string, env *eventsv1.Envelope) error {
	var payload eventsv1.PhotoViewed
	if err := env.GetPayload().UnmarshalTo(&payload); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.published = append(p.published, &payload)
	return nil
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.published)
}

func TestViewBatcher_SbrasyvaetPoTaymeru(t *testing.T) {
	pub := &fakePublisher{}
	// flushSize нарочно большой — батч НЕ должен собраться по размеру,
	// единственная причина флаша здесь — таймер.
	batcher := NewViewBatcher(pub, 1000, 20*time.Millisecond)
	defer batcher.Close()

	batcher.Enqueue("photo-1", 1, 0, "")
	batcher.Enqueue("photo-2", 2, 0, "")

	require.Eventually(t, func() bool { return pub.count() == 2 }, time.Second, 5*time.Millisecond,
		"буфер обязан сброситься по таймеру, не дожидаясь flushSize")
}

func TestViewBatcher_SbrasyvaetPoRazmeru(t *testing.T) {
	pub := &fakePublisher{}
	// Интервал нарочно большой — единственная причина флаша здесь — размер.
	batcher := NewViewBatcher(pub, 3, time.Hour)
	defer batcher.Close()

	batcher.Enqueue("photo-1", 1, 0, "")
	batcher.Enqueue("photo-2", 2, 0, "")
	batcher.Enqueue("photo-3", 3, 0, "")

	require.Eventually(t, func() bool { return pub.count() == 3 }, time.Second, 5*time.Millisecond,
		"буфер обязан сброситься сразу по достижении flushSize")
}

func TestViewBatcher_SbrasyvaetPriZakrytii(t *testing.T) {
	pub := &fakePublisher{}
	// Интервал и размер оба недостижимы за время теста — единственный
	// способ увидеть событие в pub это флаш из Close.
	batcher := NewViewBatcher(pub, 1000, time.Hour)

	batcher.Enqueue("photo-1", 1, 0, "")
	batcher.Enqueue("photo-2", 2, 0, "")

	batcher.Close()

	assert.Equal(t, 2, pub.count(), "graceful shutdown обязан дописать накопленный буфер, а не потерять его")
}

func TestViewBatcher_PerepolnenieBuferaNeBlokiruetEnqueue(t *testing.T) {
	// Конструируем батчер БЕЗ фоновой горутины run() (не через
	// NewViewBatcher): единственное, что проверяет этот тест, — что Enqueue
	// на ПОЛНОСТЬЮ заполненном канале-буфере не блокируется, а молча
	// отбрасывает событие. Живой consumer только помешал бы: канал
	// вычитывался бы быстрее, чем нужно для гарантированного переполнения.
	b := &ViewBatcher{
		producer:      &fakePublisher{},
		flushSize:     defaultFlushSize,
		flushInterval: time.Hour,
		buf:           make(chan *eventsv1.PhotoViewed, 1),
		done:          make(chan struct{}),
	}
	b.buf <- &eventsv1.PhotoViewed{PhotoId: "уже-в-буфере"}

	done := make(chan struct{})
	go func() {
		b.Enqueue("photo-x", 1, 0, "")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Enqueue заблокировался при переполненном буфере — путь чтения не должен зависеть от Kafka")
	}
	assert.Len(t, b.buf, 1, "переполнение отбрасывает НОВОЕ событие, не трогая уже накопленное")
}

package clickhouse

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriter_FlushPoRazmeru(t *testing.T) {
	var calls int32
	var gotRows [][]int
	var mu sync.Mutex

	w := NewWriter(3, time.Hour, func(_ context.Context, rows []int) error {
		atomic.AddInt32(&calls, 1)
		mu.Lock()
		gotRows = append(gotRows, append([]int(nil), rows...))
		mu.Unlock()
		return nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			require.NoError(t, w.Add(context.Background(), v))
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "3 строки при maxBatch=3 обязаны уйти ОДНОЙ пачкой")
	require.Len(t, gotRows, 1)
	assert.ElementsMatch(t, []int{0, 1, 2}, gotRows[0])
}

func TestWriter_FlushPoTaymeru(t *testing.T) {
	var calls int32
	w := NewWriter(1000, 20*time.Millisecond, func(_ context.Context, rows []int) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})

	err := w.Add(context.Background(), 1)

	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls),
		"одна строка при maxBatch=1000 обязана дождаться флаша по таймеру, а не пачки")
}

func TestWriter_OshibkaVstavkiVozvraschaetsyaVsemOzhidayuschim(t *testing.T) {
	insertErr := errors.New("clickhouse недоступен")
	w := NewWriter(2, time.Hour, func(_ context.Context, rows []int) error {
		return insertErr
	})

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = w.Add(context.Background(), idx)
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		assert.ErrorIs(t, err, insertErr)
	}
}

func TestWriter_DvoynoyFlushBezopasen(t *testing.T) {
	// Таймер и достижение maxBatch могут вызвать flush() почти одновременно.
	// Второй вызов обязан застать pending уже пустым и не запустить insert
	// с нулём строк.
	var calls int32
	w := NewWriter(1, 5*time.Millisecond, func(_ context.Context, rows []int) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})

	require.NoError(t, w.Add(context.Background(), 1))
	time.Sleep(30 * time.Millisecond) // даём таймеру первой (уже отправленной) пачки шанс сработать вхолостую

	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "insert не должен вызываться на пустой пачке")
}

func TestWriter_OtmenaKontekstaNeBlokiruetOstalnyeGorutiny(t *testing.T) {
	release := make(chan struct{})
	w := NewWriter(2, time.Hour, func(_ context.Context, rows []int) error {
		<-release
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Add(ctx, 1) }()

	cancel()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Add не вернулся после отмены контекста — значит, ждёт insert вместо ctx.Done()")
	}

	close(release)
}

package resilience

import (
	"errors"
	"testing"
	"time"

	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBreaker_ZakrytPerehoditVOtkrytPoslePorogaOtkazov(t *testing.T) {
	cfg := BreakerConfig{
		Name:        t.Name(),
		MaxRequests: 1,
		Interval:    time.Minute,
		Timeout:     time.Minute,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 3
		},
	}
	b := NewBreaker(cfg)
	require.Equal(t, gobreaker.StateClosed, b.State())

	boom := errors.New("недоступен")
	for i := 0; i < 3; i++ {
		err := b.Execute(func() error { return boom })
		require.ErrorIs(t, err, boom)
	}

	assert.Equal(t, gobreaker.StateOpen, b.State(),
		"после порога подряд идущих отказов breaker обязан открыться")
}

func TestBreaker_VOtkrytomSostoyaniiOtkazMgnovennyIBezVyzovaOperatsii(t *testing.T) {
	cfg := BreakerConfig{
		Name:        t.Name(),
		MaxRequests: 1,
		Interval:    time.Minute,
		Timeout:     time.Minute, // не истечёт за время теста
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 1
		},
	}
	b := NewBreaker(cfg)

	// Один отказ — breaker уже открыт по настройкам этого теста.
	_ = b.Execute(func() error { return errors.New("недоступен") })
	require.Equal(t, gobreaker.StateOpen, b.State())

	var called bool
	err := b.Execute(func() error {
		called = true
		return nil
	})

	assert.ErrorIs(t, err, ErrBreakerOpen)
	assert.False(t, called, "в открытом состоянии операция не должна вызываться вовсе — в этом весь смысл мгновенного отказа")
}

func TestBreaker_PoIstecheniiTaymautaPerehoditVPoluotkrytoe(t *testing.T) {
	const timeout = 20 * time.Millisecond
	cfg := BreakerConfig{
		Name:        t.Name(),
		MaxRequests: 1,
		Interval:    time.Minute,
		Timeout:     timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 1
		},
	}
	b := NewBreaker(cfg)

	_ = b.Execute(func() error { return errors.New("недоступен") })
	require.Equal(t, gobreaker.StateOpen, b.State())

	// gobreaker пересчитывает состояние лениво, по времени, а не по таймеру:
	// State() сама увидит, что Timeout истёк, и вернёт HalfOpen без
	// дополнительного вызова Execute.
	time.Sleep(timeout + 15*time.Millisecond)
	assert.Equal(t, gobreaker.StateHalfOpen, b.State(),
		"по истечении Timeout breaker обязан пропустить пробный запрос, а не оставаться закрытым для трафика вечно")
}

func TestBreaker_UspeshnyProbnyZapposVPoluotkrytomZakryvaetBreaker(t *testing.T) {
	const timeout = 15 * time.Millisecond
	cfg := BreakerConfig{
		Name:        t.Name(),
		MaxRequests: 1,
		Interval:    time.Minute,
		Timeout:     timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 1
		},
	}
	b := NewBreaker(cfg)

	_ = b.Execute(func() error { return errors.New("недоступен") })
	require.Equal(t, gobreaker.StateOpen, b.State())

	time.Sleep(timeout + 10*time.Millisecond)

	err := b.Execute(func() error { return nil })
	require.NoError(t, err)
	assert.Equal(t, gobreaker.StateClosed, b.State(),
		"успешный пробный запрос в half-open обязан закрыть breaker")
}

func TestBreaker_ExecuteVozvraschaetOshibkuOperatsii(t *testing.T) {
	b := NewBreaker(DefaultBreakerConfig(t.Name()))

	want := errors.New("своя ошибка операции")
	err := b.Execute(func() error { return want })

	assert.ErrorIs(t, err, want, "breaker не должен подменять ошибку операции своей")
}

package kafkax

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackoff_VGranicah(t *testing.T) {
	// Свойство, а не конкретное значение (docs/STYLE.md): при полном jitter
	// результат случаен, но обязан укладываться в [0, потолок экспоненты].
	for attempt := 1; attempt <= 10; attempt++ {
		for i := 0; i < 50; i++ {
			d := Backoff(attempt)
			assert.GreaterOrEqual(t, d, time.Duration(0))
			assert.LessOrEqual(t, d, backoffCap)
		}
	}
}

func TestBackoff_AttemptMenshe1TrakuyetsyaKakPervaya(t *testing.T) {
	for i := 0; i < 50; i++ {
		d := Backoff(0)
		assert.LessOrEqual(t, d, backoffBase, "Backoff(0) должен вести себя как Backoff(1)")
		d = Backoff(-5)
		assert.LessOrEqual(t, d, backoffBase)
	}
}

func TestBackoff_RastyotSPopytkami(t *testing.T) {
	// Верхняя граница окна растёт с номером попытки — проверяем через
	// максимум из многих выборок, а не через одно значение, потому что
	// сам Backoff случаен (полный jitter).
	maxOf := func(attempt int, n int) time.Duration {
		var max time.Duration
		for i := 0; i < n; i++ {
			if d := Backoff(attempt); d > max {
				max = d
			}
		}
		return max
	}

	small := maxOf(1, 200)
	large := maxOf(5, 200)
	assert.Greater(t, large, small, "окно ожидания на 5-й попытке должно быть шире, чем на 1-й")
}

func TestBackoff_UpiraetsyaVPotolok(t *testing.T) {
	for i := 0; i < 50; i++ {
		d := Backoff(30) // заведомо за пределами разумной экспоненты
		assert.LessOrEqual(t, d, backoffCap)
	}
}

func TestBackoff_NeNegativen(t *testing.T) {
	// rand.Int64N(0) паникует — если бы граница считалась неверно (например,
	// int64(exp) вместо int64(exp)+1 при exp==0), тест поймал бы панику.
	assert.NotPanics(t, func() {
		for attempt := -3; attempt <= 40; attempt++ {
			_ = Backoff(attempt)
		}
	})
}

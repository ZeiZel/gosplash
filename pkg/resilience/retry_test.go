package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func retryTestConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 4,
		BaseDelay:   2 * time.Millisecond,
		MaxDelay:    50 * time.Millisecond,
	}
}

func TestRetrier_KolichestvoPopytokRavnoMaxAttempts(t *testing.T) {
	// Ошибка всегда retryable (Unavailable) и никогда не заканчивается
	// успехом — значит, Retrier обязан исчерпать ВСЕ попытки, не меньше и
	// не больше.
	r := NewRetrier(retryTestConfig(), Idempotent)

	var attempts int
	err := r.Do(context.Background(), "test.Method", func(context.Context) error {
		attempts++
		return status.Error(codes.Unavailable, "сервис недоступен")
	})

	require.Error(t, err)
	assert.Equal(t, 4, attempts, "должны быть использованы все попытки из MaxAttempts")
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestRetrier_UspehNaVtoroyPopytkeOstanavlivaetRetry(t *testing.T) {
	r := NewRetrier(retryTestConfig(), Idempotent)

	var attempts int
	err := r.Do(context.Background(), "test.Method", func(context.Context) error {
		attempts++
		if attempts < 2 {
			return status.Error(codes.Unavailable, "ещё не готов")
		}
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 2, attempts, "после успеха дальнейшие попытки не нужны")
}

func TestRetrier_NeRetraebeliyKodNeRetraitsya(t *testing.T) {
	// codes.InvalidArgument — осмысленный ответ сервера: клиент прислал
	// плохой запрос, и повтор того же запроса даст тот же результат.
	// Retrier обязан вернуть ошибку с первой же попытки, не тратя бюджет
	// ретраев впустую.
	r := NewRetrier(retryTestConfig(), Idempotent)

	var attempts int
	err := r.Do(context.Background(), "test.Method", func(context.Context) error {
		attempts++
		return status.Error(codes.InvalidArgument, "плохой запрос")
	})

	require.Error(t, err)
	assert.Equal(t, 1, attempts, "неретраебельная ошибка не должна повторяться")
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestRetrier_NeIdempotentnayaOperatsiyaNikogdaNeRetraitsya(t *testing.T) {
	// Это и есть проверка правила безопасности: конструктор с явным
	// NotIdempotent обязан выполнить операцию РОВНО один раз, даже если
	// ошибка — Unavailable, то есть формально retryable. Повторный вызов
	// "списать деньги" на неопределённый исход первой попытки — это риск
	// двойного списания, и Do() не должен давать для этого шанс.
	r := NewRetrier(retryTestConfig(), NotIdempotent)

	var attempts int
	err := r.Do(context.Background(), "wallet.CommitFunds", func(context.Context) error {
		attempts++
		return status.Error(codes.Unavailable, "сервис недоступен")
	})

	require.Error(t, err)
	assert.Equal(t, 1, attempts, "неидемпотентная операция не ретраится, даже при retryable-ошибке")
}

func TestRetrier_IstoshchyonnyDedlineOstanavlivaetPopytki(t *testing.T) {
	// Дедлайн короче, чем пауза перед следующей попыткой (BaseDelay мал,
	// но при экспоненте быстро превышает оставшееся время) — Retrier
	// обязан прекратить попытки, не дожидаясь физического наступления
	// ctx.Done(), и вернуть последнюю РЕАЛЬНУЮ ошибку, а не обёрнутый
	// DeadlineExceeded из контекста.
	cfg := RetryConfig{
		MaxAttempts: 10,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    time.Second,
	}
	r := NewRetrier(cfg, Idempotent)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	var attempts int
	lastErr := status.Error(codes.Unavailable, "недоступен")
	err := r.Do(ctx, "test.Method", func(context.Context) error {
		attempts++
		return lastErr
	})

	require.Error(t, err)
	assert.Less(t, attempts, cfg.MaxAttempts, "дедлайн должен остановить попытки раньше, чем они закончатся сами")
	assert.True(t, errors.Is(err, lastErr) || status.Code(err) == codes.Unavailable,
		"наружу должна уйти последняя ошибка операции, а не искусственный DeadlineExceeded")
}

func TestRetrier_ZaderzhkiRastutIImeyutRazbros(t *testing.T) {
	// Свойства, а не конкретные значения (см. docs/STYLE.md): full jitter
	// даёт СЛУЧАЙНУЮ величину, поэтому проверяем границы и сам факт разброса,
	// а не то, что задержка равна ровно X.
	cfg := RetryConfig{BaseDelay: 10 * time.Millisecond, MaxDelay: time.Second}

	const samples = 200

	// Внутри одного attempt значения не должны все совпадать — иначе это
	// не jitter, а замаскированная константа.
	seen := make(map[time.Duration]bool, samples)
	for i := 0; i < samples; i++ {
		d := fullJitterDelay(cfg, 0)
		assert.GreaterOrEqual(t, d, time.Duration(0))
		assert.Less(t, d, cfg.BaseDelay, "full jitter не должен превышать верхнюю границу backoff")
		seen[d] = true
	}
	assert.Greater(t, len(seen), 1, "full jitter обязан давать разные значения, а не одно и то же")

	// Верхняя граница окна растёт экспоненциально с номером попытки — берём
	// максимум по большой выборке на разных attempt и сравниваем максимумы:
	// они должны расти (пока не упрутся в MaxDelay).
	maxAt := func(attempt int) time.Duration {
		var max time.Duration
		for i := 0; i < samples; i++ {
			if d := fullJitterDelay(cfg, attempt); d > max {
				max = d
			}
		}
		return max
	}

	max0 := maxAt(0)
	max3 := maxAt(3)
	assert.Greater(t, max3, max0, "окно задержки на attempt=3 должно быть шире, чем на attempt=0")

	// MaxDelay — потолок: на большом attempt окно не улетает за него.
	for i := 0; i < samples; i++ {
		d := fullJitterDelay(cfg, 20)
		assert.LessOrEqual(t, d, cfg.MaxDelay)
	}
}

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil ошибка", nil, false},
		{"Unavailable ретраится", status.Error(codes.Unavailable, ""), true},
		{"DeadlineExceeded ретраится", status.Error(codes.DeadlineExceeded, ""), true},
		{"InvalidArgument не ретраится", status.Error(codes.InvalidArgument, ""), false},
		{"NotFound не ретраится", status.Error(codes.NotFound, ""), false},
		{"обычная не-grpc ошибка не ретраится", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsRetryable(tc.err))
		})
	}
}

package kafkax

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryable_ErrorsIs(t *testing.T) {
	cause := errors.New("s3 недоступен")
	err := Retryable(cause)

	assert.ErrorIs(t, err, ErrRetryable)
	assert.NotErrorIs(t, err, ErrPermanent)
	// Исходная ошибка должна остаться доступной дальше по цепочке.
	assert.ErrorIs(t, err, cause)
}

func TestPermanent_ErrorsIs(t *testing.T) {
	cause := errors.New("неизвестный event_type")
	err := Permanent(cause)

	assert.ErrorIs(t, err, ErrPermanent)
	assert.NotErrorIs(t, err, ErrRetryable)
	assert.ErrorIs(t, err, cause)
}

func TestRetryable_Permanent_Nil(t *testing.T) {
	assert.NoError(t, Retryable(nil))
	assert.NoError(t, Permanent(nil))
}

func TestClassifiedError_ErrorTekstSohranyaetsya(t *testing.T) {
	// %w в fmt.Errorf оборачивает classifiedError дальше — Error() обязан
	// прокидывать исходное сообщение, иначе лог получит бесполезное "".
	err := fmt.Errorf("upsert карточки: %w", Retryable(errors.New("connection refused")))
	assert.Contains(t, err.Error(), "connection refused")
	assert.ErrorIs(t, err, ErrRetryable)
}

func TestIsPermanent(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"permanent явно размечена", Permanent(errors.New("x")), true},
		{"retryable явно размечена", Retryable(errors.New("x")), false},
		{"неклассифицированная — retryable по умолчанию", errors.New("x"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isPermanent(tt.err))
		})
	}
}

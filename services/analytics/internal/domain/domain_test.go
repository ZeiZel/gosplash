package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/analytics/internal/domain"
)

func TestParsePeriod_PrinimaetTolkoTriZnacheniya(t *testing.T) {
	cases := []struct {
		raw     string
		want    domain.Period
		wantErr bool
	}{
		{raw: "day", want: domain.PeriodDay},
		{raw: "week", want: domain.PeriodWeek},
		{raw: "month", want: domain.PeriodMonth},
		{raw: "", wantErr: true},
		{raw: "year", wantErr: true},
		{raw: "DAY", wantErr: true}, // регистрозависимо: контракт proto говорит "day | week | month"
	}

	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := domain.ParsePeriod(tc.raw)
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, domain.ErrInvalidPeriod)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestPeriod_Duration_MesyatsBolsheNedeliBolsheDnya(t *testing.T) {
	// Проверяем СВОЙСТВО (упорядоченность), а не конкретные значения —
	// см. docs/STYLE.md: тесты не должны быть завязаны на магические числа
	// сильнее, чем того требует поведение.
	assert.Less(t, domain.PeriodDay.Duration(), domain.PeriodWeek.Duration())
	assert.Less(t, domain.PeriodWeek.Duration(), domain.PeriodMonth.Duration())
}

func TestPeriod_Since_VozvrashaetProshloe(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	since := domain.PeriodDay.Since(now)
	assert.True(t, since.Before(now))
	assert.Equal(t, 24*time.Hour, now.Sub(since))
}

func TestValidateLimit(t *testing.T) {
	cases := []struct {
		name    string
		limit   int32
		wantErr bool
	}{
		{name: "ноль — клиент не указал, это не ошибка", limit: 0, wantErr: false},
		{name: "минимум", limit: domain.MinTopPhotosLimit, wantErr: false},
		{name: "максимум", limit: domain.MaxTopPhotosLimit, wantErr: false},
		{name: "отрицательный", limit: -1, wantErr: true},
		{name: "больше максимума", limit: domain.MaxTopPhotosLimit + 1, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := domain.ValidateLimit(tc.limit)
			if tc.wantErr {
				require.Error(t, err)
				assert.True(t, errors.Is(err, domain.ErrInvalidLimit))
				return
			}
			assert.NoError(t, err)
		})
	}
}

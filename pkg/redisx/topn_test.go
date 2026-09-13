package redisx

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDayKey_FormatIUTC(t *testing.T) {
	c := &Client{service: "analytics"}

	// Специально берём час, который в +14 (Kiritimati) уже следующий день,
	// а в -12 (Baker Island) ещё предыдущий: если бы dayKey считал день
	// в локальной зоне процесса, а не в UTC, тест ловил бы это на CI-раннере
	// с другим TZ, чем у разработчика.
	at := time.Date(2026, time.January, 15, 23, 30, 0, 0, time.FixedZone("UTC+3", 3*60*60))

	got := dayKey(c, at)

	assert.Equal(t, "gosplash:analytics:views:20260115", got,
		"23:30 по UTC+3 — это ещё 20:30 UTC того же дня")
}

func TestDayKey_RazniyeSutkiDayutRazniyeKlyuchi(t *testing.T) {
	c := &Client{service: "analytics"}

	day1 := dayKey(c, time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC))
	day2 := dayKey(c, time.Date(2026, time.March, 2, 12, 0, 0, 0, time.UTC))

	assert.NotEqual(t, day1, day2)
}

func TestTop_NulevoyNOtdaetPustoBezOshibki(t *testing.T) {
	c := &Client{service: "analytics"}

	entries, err := c.Top(context.Background(), time.Now(), 0)
	assert.NoError(t, err)
	assert.Empty(t, entries)
}

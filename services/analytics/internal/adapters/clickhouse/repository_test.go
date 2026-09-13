package clickhouse

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/analytics/internal/domain"
)

// Эти тесты не поднимают ClickHouse (docs/STYLE.md: unit-тесты без Docker) —
// они проверяют СОБРАННЫЙ SQL и ПОРЯДОК аргументов, то есть ровно то место,
// где опечатка в имени таблицы или колонки не поймалась бы компилятором.
// Сам факт, что запрос синтаксически валиден и правда возвращает то, что
// нужно, — проверено ВРУЧНУЮ на живом ClickHouse при разработке (см. отчёт)
// и покрыто интеграционным тестом repository_integration_test.go.

func TestTopPhotosQuery_StroitsyaPoPeriodu(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		period       domain.Period
		wantDuration time.Duration
	}{
		{period: domain.PeriodDay, wantDuration: 24 * time.Hour},
		{period: domain.PeriodWeek, wantDuration: 7 * 24 * time.Hour},
		{period: domain.PeriodMonth, wantDuration: 30 * 24 * time.Hour},
	}

	for _, tc := range cases {
		t.Run(string(tc.period), func(t *testing.T) {
			query, args := topPhotosQuery(tc.period, 50, now)

			// Три обязательных таблицы — если кто-то опечатается в имени,
			// тест упадёт раньше, чем это увидит ClickHouse в проде.
			assert.Contains(t, query, "photo_views_daily")
			assert.Contains(t, query, "FROM photo_views")
			assert.Contains(t, query, "FROM purchases")
			assert.Contains(t, query, "GROUP BY photo_id")
			assert.Contains(t, query, "ORDER BY r.views DESC")

			require.Len(t, args, 3, "day-граница, limit, ts-граница — в этом порядке")
			dayFrom, ok := args[0].(time.Time)
			require.True(t, ok)
			assert.Equal(t, now.Add(-tc.wantDuration), dayFrom)

			assert.Equal(t, int32(50), args[1])

			tsFrom, ok := args[2].(time.Time)
			require.True(t, ok)
			assert.Equal(t, now.Add(-tc.wantDuration), tsFrom)
		})
	}
}

func TestTopPhotosQuery_UzkyCTEPeredDzhoynom(t *testing.T) {
	// PATTERN из repository.go: ranked (LIMIT) должен идти ДО join'ов,
	// иначе оптимизация "сначала сократить, потом join" превращается
	// в декоративный комментарий, а не реальное поведение запроса.
	query, _ := topPhotosQuery(domain.PeriodDay, 10, time.Now())

	rankedIdx := strings.Index(query, "WITH ranked")
	joinIdx := strings.Index(query, "INNER JOIN")
	require.NotEqual(t, -1, rankedIdx)
	require.NotEqual(t, -1, joinIdx)
	assert.Less(t, rankedIdx, joinIdx)
}

func TestPhotoViewStatsQuery(t *testing.T) {
	since := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	query, args := photoViewStatsQuery("photo-1", since)

	assert.Contains(t, query, "FROM photo_views")
	assert.Contains(t, query, "uniqCombined(viewer_id)")
	// coalesce обязателен — без него uniqCombined над периодом без единого
	// НЕанонимного просмотра вернёт NULL, а не 0 (проверено на живом
	// ClickHouse, см. комментарий у функции).
	assert.Contains(t, query, "coalesce(uniqCombined(viewer_id), 0)")
	assert.Equal(t, []any{"photo-1", since}, args)
}

func TestPhotoPurchaseStatsQuery(t *testing.T) {
	since := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	query, args := photoPurchaseStatsQuery("photo-1", since)

	assert.Contains(t, query, "FROM purchases")
	assert.Contains(t, query, "sum(price_cents)")
	assert.Equal(t, []any{"photo-1", since}, args)
}

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

func TestFunnelCountsQuery(t *testing.T) {
	since := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	query, args := funnelCountsQuery(since)

	// Счётчики обязаны идти из витрин, а не из сырых таблиц — иначе вся
	// экономика SummingMergeTree (см. migrations) не работает.
	assert.Contains(t, query, "FROM uploads_daily")
	assert.Contains(t, query, "FROM thumbnails_daily")
	// coalesce — пустой период это нули отчёта, а не NULL Scan'а.
	assert.Contains(t, query, "coalesce(sum(uploads), 0)")
	assert.Contains(t, query, "coalesce(sum(thumbnails), 0)")
	assert.Equal(t, []any{since, since}, args, "граница периода для обеих витрин — одна")
}

func TestFunnelLatencyQuery(t *testing.T) {
	since := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	query, args := funnelLatencyQuery(since)

	assert.Contains(t, query, "FROM uploads AS u")
	assert.Contains(t, query, "INNER JOIN thumbnails AS t ON u.photo_id = t.photo_id")
	assert.Contains(t, query, "dateDiff('millisecond', u.ts, t.ts)")
	assert.Contains(t, query, "quantileExact(0.5)")
	assert.Contains(t, query, "quantileExact(0.95)")
	// ifNaN, а не coalesce: quantileExact над пустым множеством — NaN,
	// не NULL (см. комментарий у запроса).
	assert.Contains(t, query, "ifNaN(quantileExact(0.5)(latency_ms), 0)")
	assert.Equal(t, []any{since}, args)
}

func TestReadyPercent(t *testing.T) {
	cases := []struct {
		name       string
		uploads    uint64
		thumbnails uint64
		want       float64
	}{
		{name: "полная воронка", uploads: 100, thumbnails: 100, want: 100},
		{name: "пять процентов застряли", uploads: 100, thumbnails: 95, want: 95},
		{name: "нет превью", uploads: 10, thumbnails: 0, want: 0},
		// Деление на ноль обязано давать ноль отчёта, а не +Inf: пустой
		// период — это «воронки не было», а не «бесконечная готовность».
		{name: "нет загрузок", uploads: 0, thumbnails: 0, want: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// InDelta, а не Equal: процент — float, и равенство битов —
			// слишком хрупкая проверка для результата деления.
			assert.InDelta(t, tc.want, readyPercent(tc.uploads, tc.thumbnails), 1e-9)
		})
	}
}

func TestAuthorStatsQueries(t *testing.T) {
	since := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	t.Run("views", func(t *testing.T) {
		query, args := authorViewStatsQuery(42, since)
		assert.Contains(t, query, "FROM photo_views")
		assert.Contains(t, query, "WHERE author_id = ? AND ts >= ?")
		assert.Contains(t, query, "coalesce(uniqCombined(viewer_id), 0)")
		assert.Equal(t, []any{int64(42), since}, args)
	})

	t.Run("purchases", func(t *testing.T) {
		query, args := authorPurchaseStatsQuery(42, since)
		assert.Contains(t, query, "FROM purchases")
		assert.Contains(t, query, "coalesce(sum(price_cents), 0)")
		assert.Equal(t, []any{int64(42), since}, args)
	})

	t.Run("uploads", func(t *testing.T) {
		query, args := authorUploadsQuery(42, since)
		assert.Contains(t, query, "FROM uploads")
		assert.Contains(t, query, "WHERE author_id = ? AND ts >= ?")
		assert.Equal(t, []any{int64(42), since}, args)
	})
}

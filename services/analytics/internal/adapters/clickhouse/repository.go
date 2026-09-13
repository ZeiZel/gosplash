package clickhouse

import (
	"context"
	"fmt"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"gosplash/services/analytics/internal/domain"
)

// Repository — реализация ports.StatsReader: читает сводки из ClickHouse.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ — почему уникальные зрители считаются В CLICKHOUSE
// (uniqCombined), а не через pkg/redisx/hll.go (HyperLogLog в Redis), хотя
// обе структуры данных решают одну и ту же задачу тем же самым математическим
// приёмом:
//
//  1. Redis-путь потребовал бы ВТОРОГО писателя на каждый просмотр —
//     PFADD пришлось бы вызывать либо из catalog (сервис вне зоны
//     ответственности этой фазы, его нельзя трогать), либо из
//     internal/adapters/kafka этого сервиса. Второй вариант возможен, но
//     заводит ВТОРОЙ источник правды: Redis знал бы одно число уникальных
//     зрителей, а ClickHouse (через сырые строки photo_views) — своё,
//     и они разошлись бы при любой потере сообщения одним из двух
//     потребителей одного и того же топика.
//  2. AnalyticsService.PhotoStats и так ходит в ClickHouse за views,
//     purchases и revenue_cents — лишний прыжок в Redis ради ОДНОГО поля
//     того же ответа не даёт выигрыша по задержке, который оправдывал бы
//     второй источник правды. Redis для HLL имеет смысл, когда ответ нужен
//     МГНОВЕННО и БЕЗ похода в аналитическую базу вообще (например, счётчик
//     на странице фото, который рисуется при каждом заходе, — как
//     catalog.internal.adapters.redis.ViewCounter делает для топа
//     просмотров). Здесь ответ и так формируется отдельным аналитическим
//     запросом, так что честнее иметь ОДИН источник правды: ClickHouse.
//
// uniqCombined, а не uniqExact: тот держит В ПАМЯТИ множество всех
// увиденных id ради точного ответа — на миллиардах строк это гигабайты
// оперативной памяти сервера ClickHouse ради точности, которая всё равно
// не нужна отчёту (см. комментарий к unique_viewers_approx в
// proto/gosplash/analytics/v1/analytics.proto). uniqCombined — гибрид:
// малое множество — точный подсчёт, большое — переключается на HyperLogLog
// с постоянной памятью и погрешностью около 1%; в отличие от голого
// uniqHLL12 он не платит эту погрешность там, где в ней нет необходимости
// (мало уникальных значений).
type Repository struct {
	conn chdriver.Conn
}

func NewRepository(conn chdriver.Conn) *Repository {
	return &Repository{conn: conn}
}

// ─────────────────────────────────────────────────────────────────────────────
// TopPhotos
// ─────────────────────────────────────────────────────────────────────────────

// topPhotosRow — строка результата запроса ranked+join (см. topPhotosQuery).
// Теги ch:"..." — это то, ЧТО именно clickhouse-go подставит в поле при
// Select (struct_map.go самого драйвера ищет колонку по этому имени, а не
// по имени Go-поля).
type topPhotosRow struct {
	PhotoID   string `ch:"photo_id"`
	AuthorID  int64  `ch:"author_id"`
	Views     uint64 `ch:"views"`
	Purchases uint64 `ch:"purchases"`
}

func (r *Repository) TopPhotos(ctx context.Context, period domain.Period, limit int32) ([]domain.PhotoRank, error) {
	query, args := topPhotosQuery(period, limit, time.Now())

	var rows []topPhotosRow
	if err := r.conn.Select(ctx, &rows, query, args...); err != nil {
		return nil, fmt.Errorf("clickhouse: top photos: %w", err)
	}

	ranks := make([]domain.PhotoRank, 0, len(rows))
	for _, row := range rows {
		ranks = append(ranks, domain.PhotoRank{
			PhotoID:   row.PhotoID,
			AuthorID:  row.AuthorID,
			Views:     int64(row.Views),
			Purchases: int64(row.Purchases),
		})
	}
	return ranks, nil
}

// topPhotosQuery строит SQL для TopPhotos. Вынесена отдельной чистой
// функцией (без conn) специально ради теста без Docker: repository_test.go
// проверяет ТЕКСТ и АРГУМЕНТЫ запроса по периоду, не поднимая ClickHouse.
//
// PATTERN: узкий CTE перед джойном. ranked сначала сокращает миллионы строк
// photo_views_daily до limit фото, и только ПОТОМ джойнит их с сырым
// photo_views (за author_id) и purchases (за числом покупок) — обе правые
// части джойна остаются маленькими независимо от объёма таблиц целиком.
// Обратный порядок (сначала джойн всех строк, потом ORDER BY+LIMIT) заставил
// бы ClickHouse материализовать джойн для КАЖДОЙ строки за период, а не
// только для top-N.
//
// author_id достаётся джойном к сырым photo_views, а не хранится в самой
// photo_views_daily: то материализованное представление специально узкое
// (day, photo_id, views — как и задано в спецификации фазы), потому что
// author_id — атрибут ФОТО, а не факт "просмотрели", и дублировать его
// в каждой строке ежедневного среза значило бы разъезжаться с ним, если
// (гипотетически) авторство когда-нибудь передадут. Цена — лишний JOIN
// здесь; в реальной системе он ушёл бы в отдельное измерение (dimension
// table), синхронизируемое из catalog.listing.published, что для объёма
// этой фазы избыточно.
func topPhotosQuery(period domain.Period, limit int32, now time.Time) (string, []any) {
	dayFrom := period.Since(now)
	tsFrom := period.Since(now)

	const query = `
WITH ranked AS (
    SELECT photo_id, sum(views) AS views
    FROM photo_views_daily
    WHERE day >= ?
    GROUP BY photo_id
    ORDER BY views DESC
    LIMIT ?
)
SELECT
    r.photo_id AS photo_id,
    pv.author_id AS author_id,
    r.views AS views,
    coalesce(pu.purchases, 0) AS purchases
FROM ranked AS r
INNER JOIN (
    SELECT photo_id, any(author_id) AS author_id
    FROM photo_views
    GROUP BY photo_id
) AS pv ON pv.photo_id = r.photo_id
LEFT JOIN (
    SELECT photo_id, count() AS purchases
    FROM purchases
    WHERE ts >= ?
    GROUP BY photo_id
) AS pu ON pu.photo_id = r.photo_id
ORDER BY r.views DESC`

	return query, []any{dayFrom, limit, tsFrom}
}

// ─────────────────────────────────────────────────────────────────────────────
// PhotoStats
// ─────────────────────────────────────────────────────────────────────────────

func (r *Repository) PhotoStats(ctx context.Context, photoID string, period domain.Period) (domain.PhotoStats, error) {
	since := period.Since(time.Now())

	viewsQuery, viewsArgs := photoViewStatsQuery(photoID, since)
	var views uint64
	var uniq uint64
	if err := r.conn.QueryRow(ctx, viewsQuery, viewsArgs...).Scan(&views, &uniq); err != nil {
		return domain.PhotoStats{}, fmt.Errorf("clickhouse: статистика просмотров %s: %w", photoID, err)
	}

	purchasesQuery, purchasesArgs := photoPurchaseStatsQuery(photoID, since)
	var purchases uint64
	var revenue int64
	if err := r.conn.QueryRow(ctx, purchasesQuery, purchasesArgs...).Scan(&purchases, &revenue); err != nil {
		return domain.PhotoStats{}, fmt.Errorf("clickhouse: статистика покупок %s: %w", photoID, err)
	}

	return domain.PhotoStats{
		PhotoID:             photoID,
		Views:               int64(views),
		UniqueViewersApprox: int64(uniq),
		Purchases:           int64(purchases),
		RevenueCents:        revenue,
	}, nil
}

// photoViewStatsQuery — views и uniqCombined(viewer_id) одним проходом по
// photo_views: два числа, которые всё равно нужны из одной и той же таблицы
// за одно и то же условие WHERE, поэтому запрос один, а не два.
//
// coalesce(..., 0): uniqCombined над столбцом, где ВСЕ значения в выборке
// NULL (фото смотрели только анонимно или вообще не смотрели за период),
// возвращает NULL, а не 0 — проверено вручную на живом ClickHouse 25.8.
// Без coalesce Scan(&uniq) в uint64 упал бы с ошибкой на пустом периоде,
// хотя "ноль уникальных зрителей" — ожидаемый, а не ошибочный ответ.
func photoViewStatsQuery(photoID string, since time.Time) (string, []any) {
	const query = `
SELECT count() AS views, coalesce(uniqCombined(viewer_id), 0) AS uniq
FROM photo_views
WHERE photo_id = ? AND ts >= ?`
	return query, []any{photoID, since}
}

func photoPurchaseStatsQuery(photoID string, since time.Time) (string, []any) {
	const query = `
SELECT count() AS purchases, coalesce(sum(price_cents), 0) AS revenue
FROM purchases
WHERE photo_id = ? AND ts >= ?`
	return query, []any{photoID, since}
}

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
	// Одна и та же граница периода обслуживает ОБА сравнения: day у
	// ежедневного среза и ts у сырых покупок отсчитываются от одного и
	// того же period.Since(now), поэтому и переменная одна — расщепление
	// на dayFrom/tsFrom дублировало одно значение без смысла.
	since := period.Since(now)

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

	return query, []any{since, limit, since}
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

// ─────────────────────────────────────────────────────────────────────────────
// FunnelStats
// ─────────────────────────────────────────────────────────────────────────────

// FunnelStats считает воронку «загружено → превью готово» за период.
//
// Числа событий берутся из дневных витрин, а не из сырых uploads/thumbnails:
// это ровно та же экономия, что у TopPhotos с photo_views_daily — sum() по
// granules витрины вместо полного скана сырых таблиц ради двух чисел.
func (r *Repository) FunnelStats(ctx context.Context, period domain.Period) (domain.FunnelStats, error) {
	since := period.Since(time.Now())

	countsQuery, countsArgs := funnelCountsQuery(since)
	var uploads, thumbnails uint64
	if err := r.conn.QueryRow(ctx, countsQuery, countsArgs...).Scan(&uploads, &thumbnails); err != nil {
		return domain.FunnelStats{}, fmt.Errorf("clickhouse: счётчики воронки: %w", err)
	}

	// Лаг считается по сырым таблицам: квантилям нужны timestamp'ы каждого
	// события, а не дневные суммы — витрина здесь не помогла бы ничем.
	latencyQuery, latencyArgs := funnelLatencyQuery(since)
	var p50, p95 float64
	if err := r.conn.QueryRow(ctx, latencyQuery, latencyArgs...).Scan(&p50, &p95); err != nil {
		return domain.FunnelStats{}, fmt.Errorf("clickhouse: лаг воронки: %w", err)
	}

	stats := domain.FunnelStats{
		Uploads:      int64(uploads),
		Thumbnails:   int64(thumbnails),
		ReadyPercent: readyPercent(uploads, thumbnails),
		// dateDiff возвращает целые миллисекунды, так что float64→int64
		// здесь — не округление, а смена типа представления.
		LatencyP50Ms: int64(p50),
		LatencyP95Ms: int64(p95),
	}
	return stats, nil
}

// readyPercent — thumbnails/uploads*100. Отдельная функция ради теста
// граничного случая uploads==0 (деление на ноль) без ClickHouse.
func readyPercent(uploads, thumbnails uint64) float64 {
	if uploads == 0 {
		return 0
	}
	return float64(thumbnails) / float64(uploads) * 100
}

// funnelCountsQuery — обе стороны воронки одним запросом: скалярные
// подзапросы к двум витринам дешевле двух отдельных round-trip'ов, а
// читаются они всё равно всегда вместе.
//
// coalesce — по той же причине, что в photoPurchaseStatsQuery: sum() над
// пустой витриной (период без событий) обязан быть нулём отчёта, а не NULL'ом
// Scan'а.
func funnelCountsQuery(since time.Time) (string, []any) {
	const query = `
SELECT
    (SELECT coalesce(sum(uploads), 0) FROM uploads_daily WHERE day >= ?) AS uploads,
    (SELECT coalesce(sum(thumbnails), 0) FROM thumbnails_daily WHERE day >= ?) AS thumbnails`
	return query, []any{since, since}
}

// funnelLatencyQuery — квантили лага между media.photo.uploaded и
// media.photo.thumbnail-ready.
//
// ЧЕСТНАЯ ОГОВОРКА ПРО JOIN. INNER JOIN по photo_id здесь достаточен,
// потому что у фото ровно ОДИН upload и ОДИН thumbnail-ready (события
// публикуются по факту, а не на каждое изменение) — то есть джойн не
// порождает размножения строк. Точный инструмент ClickHouse для задач
// вида «следующее событие после» — ASOF JOIN, но здесь он избыточен:
// пары всегда фиксированы, искать «ближайший следующий» не нужно.
// Редкие дубли at-least-once (повторная доставка после не закоммиченного
// offset'а) сдвигают квантили на доли процента — это цена доставки,
// разобранная в batch.go; бороться с ней ради перцентилей, которые и так
// приблизительные по своей природе, не стоит.
//
// t.ts >= u.ts, а не просто период по t: пара «upload сегодня,
// thumbnail-ready вчера» невозможна по смыслу события, и условие отсекает
// случайно попавшие в окно хвосты чужих периодов.
//
// ifNaN обязателен: quantileExact над пустым множеством возвращает NaN, а
// не NULL (агрегат без строк не «нулевой», а «неопределённый»), поэтому
// coalesce здесь не спасает. Ноль в ответе — «за период ни одной готовой
// пары», ожидаемое значение отчёта, а не ошибка.
func funnelLatencyQuery(since time.Time) (string, []any) {
	const query = `
SELECT
    ifNaN(quantileExact(0.5)(latency_ms), 0) AS p50_ms,
    ifNaN(quantileExact(0.95)(latency_ms), 0) AS p95_ms
FROM (
    SELECT dateDiff('millisecond', u.ts, t.ts) AS latency_ms
    FROM uploads AS u
    INNER JOIN thumbnails AS t ON u.photo_id = t.photo_id
    WHERE u.ts >= ? AND t.ts >= u.ts
)`
	return query, []any{since}
}

// ─────────────────────────────────────────────────────────────────────────────
// AuthorStats
// ─────────────────────────────────────────────────────────────────────────────

// AuthorStats сводит по автору три таблицы: просмотры (с уникальными
// зрителями), покупки и загрузки. Три запроса, а не один UNION'ом или
// джойном: таблицы не связаны ключом на этом срезе (общий только author_id),
// и шесть чисел из трёх несвязанных источников читаются и дебажатся ровно
// по отдельности.
func (r *Repository) AuthorStats(ctx context.Context, authorID int64, period domain.Period) (domain.AuthorStats, error) {
	since := period.Since(time.Now())

	var views, uniq uint64
	viewsQuery, viewsArgs := authorViewStatsQuery(authorID, since)
	if err := r.conn.QueryRow(ctx, viewsQuery, viewsArgs...).Scan(&views, &uniq); err != nil {
		return domain.AuthorStats{}, fmt.Errorf("clickhouse: просмотры автора %d: %w", authorID, err)
	}

	var purchases uint64
	var revenue int64
	purchasesQuery, purchasesArgs := authorPurchaseStatsQuery(authorID, since)
	if err := r.conn.QueryRow(ctx, purchasesQuery, purchasesArgs...).Scan(&purchases, &revenue); err != nil {
		return domain.AuthorStats{}, fmt.Errorf("clickhouse: покупки автора %d: %w", authorID, err)
	}

	var uploads uint64
	uploadsQuery, uploadsArgs := authorUploadsQuery(authorID, since)
	if err := r.conn.QueryRow(ctx, uploadsQuery, uploadsArgs...).Scan(&uploads); err != nil {
		return domain.AuthorStats{}, fmt.Errorf("clickhouse: загрузки автора %d: %w", authorID, err)
	}

	return domain.AuthorStats{
		Views:               int64(views),
		UniqueViewersApprox: int64(uniq),
		Purchases:           int64(purchases),
		RevenueCents:        revenue,
		Uploads:             int64(uploads),
	}, nil
}

// authorViewStatsQuery — то же, что photoViewStatsQuery, но срез по
// author_id вместо photo_id.
//
// ФИЛЬТР ПО author_id В photo_views НЕ ПО ПОРЯДКУ ORDER BY (photo_id, ts):
// автор не первая колонка ключа, поэтому разреженный индекс не помогает и
// ClickHouse сканирует колонку author_id целиком за период. Для объёмов
// проекта это честно дешевле, чем перекладывать ORDER BY под единственный
// отчёт (все остальные запросы таблицы — по photo_id и сломались бы), —
// когда авторских срезов станет много, правильное решение не новый ORDER BY,
// а витрина/проекция под авторов, как photo_views_daily под топы.
func authorViewStatsQuery(authorID int64, since time.Time) (string, []any) {
	const query = `
SELECT count() AS views, coalesce(uniqCombined(viewer_id), 0) AS uniq
FROM photo_views
WHERE author_id = ? AND ts >= ?`
	return query, []any{authorID, since}
}

// authorPurchaseStatsQuery — покупок на порядки меньше, чем просмотров,
// поэтому тот же скан по author_id (purchases отсортирована по
// (photo_id, ts), не под авторов), что и в photo_views, здесь стоит копейки:
// даже без индекса строк на три порядка меньше.
func authorPurchaseStatsQuery(authorID int64, since time.Time) (string, []any) {
	const query = `
SELECT count() AS purchases, coalesce(sum(price_cents), 0) AS revenue
FROM purchases
WHERE author_id = ? AND ts >= ?`
	return query, []any{authorID, since}
}

// authorUploadsQuery — единственная из трёх таблиц среза, где фильтр по
// автору ИДЁТ по ORDER BY (author_id, ts): таблицу специально клали под
// этот отчёт (см. migrations/auto.go), и разреженный индекс здесь работает
// как задуман.
func authorUploadsQuery(authorID int64, since time.Time) (string, []any) {
	const query = `
SELECT count() AS uploads
FROM uploads
WHERE author_id = ? AND ts >= ?`
	return query, []any{authorID, since}
}

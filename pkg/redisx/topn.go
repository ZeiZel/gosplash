package redisx

import (
	"context"
	"fmt"
	"time"
)

// PATTERN: Sorted Set как готовый топ-N, а не "SELECT ... ORDER BY views
// DESC LIMIT N" по базе на каждый показ.
//
// У Sorted Set нужное свойство встроено в структуру данных: элементы в нём
// ВСЕГДА хранятся в порядке по score (это скип-лист + хэш-таблица внутри
// Redis), поэтому ZREVRANGE — это чтение уже готового порядка за O(log N + M),
// без единого сравнения на запрос. Инкремент счётчика (ZINCRBY) — тоже
// O(log N). У PostgreSQL то же самое стоило бы иначе: ORDER BY views DESC
// LIMIT N без индекса — это сортировка всей таблицы просмотров при каждом
// обращении к топу; с индексом по views — эффективнее, но каждый ZINCRBY
// эквивалентен UPDATE, который двигает строку в B-tree индекса и создаёт
// новую версию строки (MVCC), а таблица просмотров растёт на порядки быстрее
// таблицы фотографий (см. похожий довод в pkg/dbx про COUNT(*) вместо
// отдельного счётчика). Sorted Set хранится в памяти и рассчитан именно на
// частые инкременты с постоянным чтением "верхушки" — это его штатный режим
// работы, а не оптимизация частного случая.
//
// Ключ по дню (views:<yyyymmdd>), а не один вечный счётчик: топ считается
// «за сутки», и дневной ключ даёт это бесплатно — не нужно ни отдельного
// job'а, обнуляющего счётчики в полночь, ни хранения истории для вычитания
// вчерашних просмотров. Просто заводится новый ключ, а старый через TTL
// сам исчезает.

// topTTL — 48 часов, а не 24: если ZINCRBY придёт с опозданием (Kafka-лаг,
// ретрай consumer'а) уже после полуночи по факту события, ключ вчерашнего
// дня должен ещё существовать, чтобы просмотр учёлся в правильных сутках.
const topTTL = 48 * time.Hour

// dayKey строит ключ дня в UTC. UTC, а не локальная зона процесса: иначе
// сервис, запущенный на машине с другим TZ (или после смены летнего
// времени), считал бы день по-другому, чем консьюмер, — и события легли бы
// в разные ключи по чистой случайности часового пояса.
func dayKey(c *Client, at time.Time) string {
	return c.Key("views", at.UTC().Format("20060102"))
}

// IncrView увеличивает счётчик просмотров item в сутках, которым принадлежит
// at (событие приходит из Kafka с собственным временем, поэтому at — не
// time.Now(), а время самого события).
func (c *Client) IncrView(ctx context.Context, item string, at time.Time) error {
	key := dayKey(c, at)
	if err := c.rdb.ZIncrBy(ctx, key, 1, item).Err(); err != nil {
		return fmt.Errorf("redisx: zincrby %q: %w", key, err)
	}
	// EXPIRE обновляется на каждой записи: пока в сутки идут события, TTL
	// постоянно отодвигается вперёд, и ключ живёт всё активное время дня.
	// Как только события заканчиваются (день прошёл), ключ проживёт ещё
	// topTTL от последнего ZINCRBY и сам исчезнет — отдельная job на очистку
	// не нужна.
	if err := c.rdb.Expire(ctx, key, topTTL).Err(); err != nil {
		return fmt.Errorf("redisx: expire %q: %w", key, err)
	}
	return nil
}

// TopEntry — одна строка топа.
type TopEntry struct {
	Item  string
	Score float64
}

// Top возвращает первые n элементов топа за сутки, которым принадлежит at,
// отсортированные по убыванию просмотров.
func (c *Client) Top(ctx context.Context, at time.Time, n int) ([]TopEntry, error) {
	if n <= 0 {
		return nil, nil
	}
	key := dayKey(c, at)

	raw, err := c.rdb.ZRevRangeWithScores(ctx, key, 0, int64(n-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("redisx: zrevrange %q: %w", key, err)
	}

	entries := make([]TopEntry, 0, len(raw))
	for _, z := range raw {
		member, ok := z.Member.(string)
		if !ok {
			return nil, fmt.Errorf("redisx: zrevrange %q: элемент не строка: %v", key, z.Member)
		}
		entries = append(entries, TopEntry{Item: member, Score: z.Score})
	}
	return entries, nil
}

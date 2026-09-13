package redisx

import (
	"context"
	"fmt"
)

// PATTERN: HyperLogLog — приблизительный COUNT(DISTINCT viewer_id) в
// постоянной памяти.
//
// Точный подсчёт уникальных зрителей популярного фото потребовал бы хранить
// множество их идентификаторов целиком (Set из миллиона viewer_id — это
// мегабайты на КАЖДОЕ фото). HyperLogLog хранит не элементы, а вероятностную
// оценку их количества: ровно 12 КБ на структуру НЕЗАВИСИМО от того, десять
// там уникальных зрителей или десять миллионов, с погрешностью around 0.81%.
// Разница на порядки в памяти в обмен на то, что PFCOUNT отвечает "941 203",
// когда на самом деле было 941 950, — для метрики "уникальных зрителей за
// сутки" на дашборде это абсолютно приемлемый размен: с точностью до
// процента никто не принимает решений, а вот держать по Set на каждое из
// миллиона фото — это уже вопрос, влезает ли Redis в память сервера вообще.
//
// Цена, которую здесь осознанно платят: PFCOUNT — это ТОЛЬКО количество,
// самих viewer_id из HLL восстановить нельзя (это не сжатый список, а
// агрегированное состояние счётчика). Если когда-нибудь понадобится не
// "сколько", а "кто именно" — это уже другая структура данных (Set) и другая
// цена памяти.
// TrackUniqueView регистрирует viewer как посмотревшего item. Повторная
// регистрация того же viewer для того же item — не ошибка и не удваивает
// счётчик: в этом и есть смысл PFADD, в отличие от, например, INCR.
func (c *Client) TrackUniqueView(ctx context.Context, item, viewer string) error {
	key := c.Key("uniques", item)
	if err := c.rdb.PFAdd(ctx, key, viewer).Err(); err != nil {
		return fmt.Errorf("redisx: pfadd %q: %w", key, err)
	}
	return nil
}

// UniqueViewCount возвращает приблизительное количество уникальных зрителей
// item, накопленное TrackUniqueView.
func (c *Client) UniqueViewCount(ctx context.Context, item string) (int64, error) {
	key := c.Key("uniques", item)
	n, err := c.rdb.PFCount(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("redisx: pfcount %q: %w", key, err)
	}
	return n, nil
}

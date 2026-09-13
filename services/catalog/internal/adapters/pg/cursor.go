package pg

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PATTERN: keyset-пагинация (курсор), а не OFFSET.
//
// OFFSET N заставляет Postgres прочитать и выбросить N строк на КАЖДЫЙ
// запрос страницы — на первой странице это незаметно, на сотой (OFFSET
// 2000) база уже реально читает и сортирует 2020 строк, чтобы отдать 20.
// Курсор устраняет это: WHERE (published_at, id) < (курсор) с индексом по
// (published_at DESC, id DESC) читает РОВНО те строки, которые нужны отдать,
// независимо от того, какая это по счёту страница. Цена — курсор непрозрачен
// и не позволяет "перепрыгнуть сразу на страницу 50", только идти вперёд
// по одному шагу; для бесконечной ленты (а не постраничного каталога с
// номерами страниц) это ровно то поведение, которое нужно.
//
// Курсор кодирует (published_at, id) последней увиденной карточки — ту же
// пару колонок, по которой идёт ORDER BY, а не просто id: одного id
// недостаточно, потому что сортировка идёт по published_at, и без него
// нельзя было бы восстановить "строго после какой точки продолжать".
type pageCursor struct {
	PublishedAt time.Time
	ID          string
}

// encodeCursor — непрозрачная строка для клиента. base64, а не голый
// "unixnano|id": клиент не должен парсить курсор или полагаться на его
// формат, иначе смена формата курсора станет breaking change публичного API.
func encodeCursor(c pageCursor) string {
	raw := fmt.Sprintf("%d|%s", c.PublishedAt.UnixNano(), c.ID)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(s string) (pageCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return pageCursor{}, fmt.Errorf("битый курсор: %w", err)
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return pageCursor{}, fmt.Errorf("битый курсор: неверный формат")
	}
	ns, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return pageCursor{}, fmt.Errorf("битый курсор: время: %w", err)
	}
	if parts[1] == "" {
		return pageCursor{}, fmt.Errorf("битый курсор: пустой id")
	}
	return pageCursor{PublishedAt: time.Unix(0, ns), ID: parts[1]}, nil
}

// keyedRow — то немногое, что нужно trimPage от строки результата: её
// собственный (published_at, id). Метод cursorKey определён на ListingRow
// (listing_repository.go) и промоутится в listingWithAuthor через
// встраивание — trimPage работает с обоими типами без единой правки.
type keyedRow interface {
	cursorKey() (at time.Time, id string, ok bool)
}

// trimPage — ЧИСТАЯ функция: решает "есть ли следующая страница" и строит
// курсор на неё, не трогая базу. Выделена отдельно от List именно ради
// тестируемости без Postgres (docs/STYLE.md: прикладной и около-прикладной
// код тестируется подделками/фикстурами, без контейнеров).
//
// rows — результат запроса с LIMIT limit+1 (см. List): если приехало limit+1
// строк, последняя лишняя — по НЕЙ строится next_cursor, а сама она
// отрезается от отдаваемой страницы. Если рядов limit или меньше — это
// последняя страница, next_cursor пуст.
func trimPage[T keyedRow](rows []T, limit int) ([]T, string) {
	if len(rows) <= limit {
		return rows, ""
	}
	page := rows[:limit]
	at, id, ok := page[limit-1].cursorKey()
	if !ok {
		// PublishedAt == nil на последней строке страницы (карточка без
		// published_at не должна была вообще пройти фильтр status=published,
		// но лучше отдать страницу без курсора, чем закодировать мусор).
		return page, ""
	}
	return page, encodeCursor(pageCursor{PublishedAt: at, ID: id})
}

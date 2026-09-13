package pg

import (
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Курсор — чистая функция, поэтому её можно и нужно проверить без базы:
// вся суть keyset-пагинации в том, что курсор кодирует и восстанавливает
// (published_at, id) без потерь и без обращения к Postgres.

func TestCursor_KruglyyRoundtrip(t *testing.T) {
	original := pageCursor{
		PublishedAt: time.Date(2026, 9, 12, 10, 30, 0, 123456789, time.UTC),
		ID:          "photo-42",
	}

	encoded := encodeCursor(original)
	decoded, err := decodeCursor(encoded)

	require.NoError(t, err)
	assert.True(t, original.PublishedAt.Equal(decoded.PublishedAt),
		"время должно восстановиться с точностью до наносекунды")
	assert.Equal(t, original.ID, decoded.ID)
}

func TestCursor_NeprozrachnayaStroka(t *testing.T) {
	// Курсор — непрозрачная строка для клиента: он не должен уметь угадать
	// формат по виду значения. Проверяем свойство, а не конкретное значение
	// хэша: результат не совпадает буквально с "unixnano|id".
	c := pageCursor{PublishedAt: time.Unix(0, 1000), ID: "x"}
	encoded := encodeCursor(c)
	assert.NotContains(t, encoded, "|", "курсор не должен быть человекочитаемым разделённым значением")
}

func TestCursor_BitiyKursorDayotOshibku(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"не base64", "не курсор вовсе!!!"},
		{"нет разделителя", base64Encode("12345")},
		{"пустой id", base64Encode("12345|")},
		{"время не число", base64Encode("abc|photo-1")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeCursor(tt.input)
			assert.Error(t, err, "битый курсор обязан диагностироваться, а не паниковать")
		})
	}
}

func base64Encode(s string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(s))
}

// row — минимальная фикстура для trimPage: ничего, кроме (published_at, id),
// без похода в Postgres. ListingRow подошёл бы и сам, но row подчёркивает,
// что trimPage знает только про cursorKey(), а не про всю схему таблицы.
type row struct {
	at time.Time
	id string
}

func (r row) cursorKey() (time.Time, string, bool) { return r.at, r.id, true }

func fixtureRows(n int) []row {
	base := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	rows := make([]row, n)
	for i := 0; i < n; i++ {
		// Убывающее время — тот же порядок, что и ORDER BY published_at DESC
		// в реальном запросе.
		rows[i] = row{at: base.Add(-time.Duration(i) * time.Minute), id: fmt.Sprintf("photo-%d", i)}
	}
	return rows
}

func TestTrimPage_PoslednyayaStranitsa(t *testing.T) {
	// Строк ровно limit (или меньше) — все они попадают в ответ, курсора
	// на следующую страницу нет: это и есть "последняя страница".
	rows := fixtureRows(3)

	page, next := trimPage(rows, 3)

	assert.Len(t, page, 3)
	assert.Empty(t, next, "next_cursor обязан быть пуст, когда страниц больше нет")
}

func TestTrimPage_EstSleduyuschayaStranitsa(t *testing.T) {
	// limit+1 строк — способ узнать о следующей странице без отдельного
	// COUNT(*): лишняя строка отрезается и по НЕЙ строится курсор.
	rows := fixtureRows(4)

	page, next := trimPage(rows, 3)

	require.Len(t, page, 3, "лишняя (4-я) строка не должна попасть в отдаваемую страницу")
	require.NotEmpty(t, next)

	decoded, err := decodeCursor(next)
	require.NoError(t, err)
	assert.Equal(t, rows[2].id, decoded.ID, "курсор строится по ПОСЛЕДНЕЙ строке ОТДАВАЕМОЙ страницы, а не по отрезанной")
	assert.True(t, rows[2].at.Equal(decoded.PublishedAt))
}

func TestTrimPage_PustoyVvod(t *testing.T) {
	page, next := trimPage([]row{}, 20)
	assert.Empty(t, page)
	assert.Empty(t, next)
}

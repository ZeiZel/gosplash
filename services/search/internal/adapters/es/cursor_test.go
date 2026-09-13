package es

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCursor_PustayaStroka_PervayaStranitsa(t *testing.T) {
	values, err := decodeCursor("")
	require.NoError(t, err)
	assert.Nil(t, values, "пустой курсор — первая страница, search_after ещё нет")
}

func TestCursor_EncodeDecode_KrugloeSvoystvo(t *testing.T) {
	tests := []struct {
		name   string
		values []any
	}{
		{"score и id", []any{12.5, "listing-9"}},
		{"нулевой score", []any{float64(0), "listing-1"}},
		{"score с высокой точностью", []any{1.0000001, "listing-2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor, err := encodeCursor(tt.values)
			require.NoError(t, err)
			assert.NotEmpty(t, cursor)

			got, err := decodeCursor(cursor)
			require.NoError(t, err)
			require.Len(t, got, len(tt.values))
			for i := range tt.values {
				assert.EqualValues(t, tt.values[i], got[i])
			}
		})
	}
}

func TestCursor_Bitiy(t *testing.T) {
	tests := []struct {
		name   string
		cursor string
	}{
		{"не base64", "!!!не-base64!!!"},
		{"base64, но не JSON", "bm90LWpzb24"},
		{"пустой массив", mustEncode(t, []any{})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeCursor(tt.cursor)
			assert.Error(t, err)
		})
	}
}

func mustEncode(t *testing.T, values []any) string {
	t.Helper()
	cursor, err := encodeCursor(values)
	require.NoError(t, err)
	return cursor
}

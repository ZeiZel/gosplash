package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestFitLongSide_SohranyaetProporcii — table-driven тест на арифметику
// превью: горизонтальное, вертикальное, квадратное фото и оригинал меньше
// цели (обязан остаться БЕЗ апскейла).
func TestFitLongSide_SohranyaetProporcii(t *testing.T) {
	tests := []struct {
		name         string
		origW, origH int
		target       int
		wantW, wantH int
	}{
		{
			name:  "горизонтальное фото уменьшается по ширине",
			origW: 2000, origH: 1000, target: 800,
			wantW: 800, wantH: 400,
		},
		{
			name:  "вертикальное фото уменьшается по высоте",
			origW: 1000, origH: 2000, target: 800,
			wantW: 400, wantH: 800,
		},
		{
			name:  "квадратное фото — обе стороны равны цели",
			origW: 1000, origH: 1000, target: 800,
			wantW: 800, wantH: 800,
		},
		{
			name:  "оригинал меньше цели — апскейла нет, размер не меняется",
			origW: 300, origH: 200, target: 800,
			wantW: 300, wantH: 200,
		},
		{
			name:  "оригинал ровно равен цели по длинной стороне — без изменений",
			origW: 800, origH: 400, target: 800,
			wantW: 800, wantH: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, h := FitLongSide(tt.origW, tt.origH, tt.target)
			assert.Equal(t, tt.wantW, w, "ширина")
			assert.Equal(t, tt.wantH, h, "высота")
		})
	}
}

func TestFitLongSide_ProporciyaSohranyaetsyaSTochnostyuDoOkrugleniya(t *testing.T) {
	// Не самое "круглое" отношение сторон — проверяем, что соотношение
	// после ресайза остаётся близким к исходному (в пределах округления
	// до целого пикселя), а не просто что числа не нулевые.
	w, h := FitLongSide(1920, 1080, 320)

	assert.Equal(t, 320, w)
	assert.InDelta(t, 1080.0/1920.0, float64(h)/float64(w), 0.01)
}

func TestFitLongSide_VyrozhdennyeSluchai(t *testing.T) {
	// Нулевые/отрицательные размеры не должны паниковать (деление на ноль)
	// — это сигнал, что раньше в пайплайне уже что-то пошло не так,
	// а не повод падать здесь.
	w, h := FitLongSide(0, 0, 800)
	assert.Equal(t, 0, w)
	assert.Equal(t, 0, h)

	w, h = FitLongSide(100, 100, 0)
	assert.Equal(t, 100, w)
	assert.Equal(t, 100, h)
}

func TestSizeLabel_TriShtatnyhRazmera(t *testing.T) {
	assert.Equal(t, "small", SizeLabel(0))
	assert.Equal(t, "medium", SizeLabel(1))
	assert.Equal(t, "large", SizeLabel(2))
}

func TestSizeLabel_NeshtatnoeKolichestvoNePanikuet(t *testing.T) {
	assert.Equal(t, "size4", SizeLabel(3))
	assert.Equal(t, "size0", SizeLabel(-1))
}

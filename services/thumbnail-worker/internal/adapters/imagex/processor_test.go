package imagex

import (
	"bytes"
	"errors"
	"image"
	"image/color"
	"image/png"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/thumbnail-worker/internal/domain"
)

// Настоящие декодер/энкодер, без подделок — эти гарантии (валидация по
// сигнатуре байт, отсутствие апскейла) физически реализованы ЗДЕСЬ,
// а не в internal/app, поэтому проверяются на реальном коде адаптера.

func pngBytes(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, color.RGBA{R: uint8(x % 256), G: uint8(y % 256), B: 200, A: 255})
		}
	}
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, img))
	return buf.Bytes()
}

func TestDecode_OtklonyaetNeIzobrazhenie(t *testing.T) {
	p := New()

	_, err := p.Decode(strings.NewReader("это точно не картинка, просто текст"))

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrNotAnImage,
		"невалидные байты обязаны классифицироваться как domain.ErrNotAnImage")
}

func TestDecode_OpredelyaetFormatPoSignatureANeRoRasshireniyu(t *testing.T) {
	// Файл БЕЗ какого-либо намёка на расширение или Content-Type — единственное,
	// на что может опираться Decode, это сигнатура байт PNG.
	p := New()
	data := pngBytes(t, 100, 50)

	decoded, err := p.Decode(bytes.NewReader(data))

	require.NoError(t, err)
	assert.Equal(t, 100, decoded.Width)
	assert.Equal(t, 50, decoded.Height)
}

func TestThumbnail_UmenshaetSSohraneniemProporcii(t *testing.T) {
	p := New()
	decoded, err := p.Decode(bytes.NewReader(pngBytes(t, 2000, 1000)))
	require.NoError(t, err)

	thumb, err := p.Thumbnail(decoded, 800)

	require.NoError(t, err)
	assert.Equal(t, 800, thumb.Width)
	assert.Equal(t, 400, thumb.Height)
	assert.Equal(t, "jpeg", thumb.Format)
	assert.Equal(t, "jpg", thumb.Ext)
	assert.NotEmpty(t, thumb.Data)

	// Результат обязан сам быть валидным JPEG нужного размера — это то,
	// что реально уедет в S3 и будет отдано браузеру.
	decodedBack, format, err := image.Decode(bytes.NewReader(thumb.Data))
	require.NoError(t, err)
	assert.Equal(t, "jpeg", format)
	assert.Equal(t, 800, decodedBack.Bounds().Dx())
	assert.Equal(t, 400, decodedBack.Bounds().Dy())
}

func TestThumbnail_NeUvelichivaetMalenkiyOriginal(t *testing.T) {
	p := New()
	decoded, err := p.Decode(bytes.NewReader(pngBytes(t, 100, 60)))
	require.NoError(t, err)

	thumb, err := p.Thumbnail(decoded, 1600)

	require.NoError(t, err)
	assert.Equal(t, 100, thumb.Width, "апскейла быть не должно")
	assert.Equal(t, 60, thumb.Height)
}

func TestThumbnail_ChestnoNazyvaetFormatJPEGNeWebP(t *testing.T) {
	// Явная проверка требования задания: раз кодируем в JPEG — ключевые поля
	// обязаны говорить "jpeg"/"jpg", а не выдавать себя за webp.
	p := New()
	decoded, err := p.Decode(bytes.NewReader(pngBytes(t, 500, 500)))
	require.NoError(t, err)

	thumb, err := p.Thumbnail(decoded, 320)

	require.NoError(t, err)
	assert.NotEqual(t, "webp", thumb.Format)
	assert.NotEqual(t, "webp", thumb.Ext)
	assert.Equal(t, "jpeg", thumb.Format)
}

func TestDecode_PustyeBaytyOshibka(t *testing.T) {
	p := New()

	_, err := p.Decode(bytes.NewReader(nil))

	require.Error(t, err)
	target := domain.ErrNotAnImage
	assert.True(t, errors.Is(err, target))
}

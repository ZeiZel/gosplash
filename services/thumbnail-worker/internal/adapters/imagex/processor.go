// Package imagex — адаптер декодирования оригиналов и генерации превью.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ (и самое спорное во всём сервисе): формат превью —
// JPEG с качеством 85, а не WebP, хотя docs/PLAN.md (шаг 1.6) и называет
// именно WebP.
//
// В задании было три пути:
//
//  1. golang.org/x/image. У него есть ТОЛЬКО webp-ДЕКОДЕР
//     (golang.org/x/image/webp) — то есть эта библиотека позволяет ПРОЧИТАТЬ
//     чужой webp-файл, но не умеет ЗАПИСАТЬ ни одного байта в этом формате.
//     Кодера для WebP в чистом Go в этом пакете нет.
//  2. github.com/chai2010/webp — единственный реальный источник WebP-
//     энкодера в Go-экосистеме. Но это cgo-обёртка над системной libwebp:
//     сборка требует C-тулчейна и заголовков libwebp НА КАЖДОЙ машине,
//     которая собирает сервис, — включая CI и будущий Docker-образ, для
//     которого в проекте пока нет ни одного Dockerfile с шагом
//     "apt-get install libwebp-dev". Локально на машине разработчика
//     (macOS + Homebrew) libwebp есть, и `go build` с этой библиотекой
//     проходит, — но переносить в учебный репозиторий зависимость, которая
//     молча ломает `go build ./...` на любой машине без предустановленной
//     системной библиотеки, значит платить хрупкостью сборки ради формата,
//     который для превью каталога (маленькая картинка в ленте) визуально
//     неотличим от JPEG q85 при экономии размера в единицы процентов.
//  3. Честный откат на JPEG (quality 85) — то, что выбрано здесь.
//
// Расплата явная и она НЕ скрыта: EncodedThumbnail.Format="jpeg",
// EncodedThumbnail.Ext="jpg", ключ объекта в S3 заканчивается на ".jpg",
// Content-Type — "image/jpeg" (internal/app.generateAll). Нигде в данных,
// в имени объекта или в событии media.photo.thumbnail-ready это не
// выдаётся за webp. День, когда в проекте появится чистый Go WebP-энкодер
// (или Dockerfile с libwebp-dev и осознанным решением тянуть cgo),
// достаточно переписать ЭТОТ файл — internal/app и контракт события
// (proto Thumbnail не хранит формат вообще, только size/storage_key/
// width/height) не заметят разницы. Это и есть цена, ради которой кодек
// вынесен за порт ports.ImageProcessor, а не зашит в internal/app.
//
// Ресайз — golang.org/x/image/draw (CatmullRom, бикубическая интерполяция):
// чистый Go, без cgo, тот же официальный репозиторий golang.org/x/image,
// откуда и webp-декодер, так что новой внешней зависимости для одной этой
// операции это не добавляет.
package imagex

import (
	"bytes"
	"fmt"
	"image"
	_ "image/gif" // регистрирует декодер "gif" в image.RegisterFormat (проверка "это изображение" и Decode)
	"image/jpeg"
	_ "image/png" // регистрирует декодер "png" в image.RegisterFormat
	"io"

	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp" // регистрирует декодер "webp" в image.RegisterFormat — нужен только для чтения чужих webp-оригиналов

	"gosplash/services/thumbnail-worker/internal/domain"
	"gosplash/services/thumbnail-worker/internal/ports"
)

// jpegQuality — см. package doc: 85 задано прямо в задании как разумный
// компромисс между размером файла и заметностью артефактов сжатия для
// превью, которые показываются мелкими в ленте каталога.
const jpegQuality = 85

// Processor — реализация ports.ImageProcessor.
//
// Без состояния и без полей: декодирование и кодирование не хранят ничего
// между вызовами, поэтому один общий экземпляр безопасно используется
// параллельно из нескольких горутин пула (internal/app.Service.sem).
type Processor struct{}

func New() *Processor { return &Processor{} }

// Decode проверяет, что r — валидное изображение, и возвращает его размеры.
//
// image.Decode определяет формат ПО СИГНАТУРЕ БАЙТ среди зарегистрированных
// декодеров (jpeg/png/gif импортированы для побочного эффекта регистрации,
// webp — отдельным блочным импортом выше) — а не по расширению файла и не
// по Content-Type, оба из которых прислал клиент media и оба можно
// подделать или просто перепутать (см. задание и docs/STYLE.md).
//
// ЛЮБАЯ ошибка декодирования (неизвестный формат, битые байты, оборванный
// поток) трактуется как domain.ErrNotAnImage. Это огрубление: обрыв сети
// при чтении из S3 в середине декодирования технически ОТЛИЧАЕТСЯ от
// "внутри лежит не картинка" и в принципе мог бы быть retryable. Отличить
// их без разбора конкретного типа ошибки декодера (а стандартные image/*
// его не дают — там просто errors.New с текстом) требует лишней сложности,
// а задание прямо требует: "битый/не-изображение... → kafkax.Permanent(err)".
// Проще и честнее принять этот компромисс, чем гадать по тексту ошибки.
func (p *Processor) Decode(r io.Reader) (ports.DecodedImage, error) {
	img, _, err := image.Decode(r)
	if err != nil {
		return ports.DecodedImage{}, fmt.Errorf("декодирование: %w: %w", domain.ErrNotAnImage, err)
	}

	b := img.Bounds()
	return ports.DecodedImage{Img: img, Width: b.Dx(), Height: b.Dy()}, nil
}

// Thumbnail строит превью по длинной стороне targetLongSide без апскейла
// (domain.FitLongSide) и кодирует его в JPEG.
func (p *Processor) Thumbnail(in ports.DecodedImage, targetLongSide int) (ports.EncodedThumbnail, error) {
	w, h := domain.FitLongSide(in.Width, in.Height, targetLongSide)

	resized := in.Img
	if w != in.Width || h != in.Height {
		dst := image.NewRGBA(image.Rect(0, 0, w, h))
		// CatmullRom — бикубический фильтр: заметно чище ApproxBiLinear при
		// сильном уменьшении (а превью почти всегда сильное уменьшение),
		// и это разовая CPU-bound операция на фоновом воркере, а не путь
		// горячего HTTP-запроса — разница в стоимости фильтра здесь не
		// имеет значения по сравнению с качеством результата.
		draw.CatmullRom.Scale(dst, dst.Bounds(), in.Img, in.Img.Bounds(), draw.Over, nil)
		resized = dst
	}
	// w == in.Width && h == in.Height (оригинал не больше цели, апскейла
	// нет, domain.FitLongSide вернул исходные размеры без изменений) —
	// resized остаётся исходным decoded-изображением, лишний RGBA-буфер
	// того же размера никого не спасает и только тратит память.

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, resized, &jpeg.Options{Quality: jpegQuality}); err != nil {
		return ports.EncodedThumbnail{}, fmt.Errorf("кодирование превью %dx%d: %w", w, h, err)
	}

	return ports.EncodedThumbnail{
		Data:   buf.Bytes(),
		Width:  w,
		Height: h,
		Format: "jpeg",
		Ext:    "jpg",
	}, nil
}

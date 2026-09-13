// Package domain — предметная область thumbnail-worker'а: что такое превью
// и как считать его размеры, независимо от того, где лежат байты и как они
// закодированы.
//
// Здесь НЕТ ни тегов gorm/json/protobuf, ни импортов инфраструктуры, ни даже
// stdlib-пакета "image": домен не обязан знать, ЧЕМ декодируется картинка —
// он знает только арифметику (как посчитать размер превью) и то, какие факты
// о фото и его превью вообще существуют. Декодирование и кодирование —
// забота internal/adapters/imagex, работа с файлом — internal/adapters/s3.
package domain

import (
	"errors"
	"math"
)

// Ошибки домена. internal/adapters переводят их в свой язык: adapters/kafka
// решает, что делать с ними в kafkax (Permanent/Retryable, см. пакет
// adapters/kafka), adapters/pg и adapters/s3 — оборачивают ими более
// конкретные технические ошибки, чтобы вызывающий код мог сравнить их через
// errors.Is, не зная про GORM или minio.
var (
	// ErrPhotoNotFound — в шарде media нет строки с таким (user_id, photo_id).
	// Событие media.photo.uploaded ссылается на несуществующее фото — то есть
	// либо гонка (событие приехало раньше вставки, что at-least-once и retry
	// на месте уже покрывают), либо испорченные данные. Повторная попытка
	// СРАЗУ после появления события — retryable (это делает kafkax сам, пока
	// адаптер не классифицировал ошибку явно); если фото не появляется и после
	// исчерпания retry-раундов — DLQ, дальше руками.
	ErrPhotoNotFound = errors.New("фото не найдено в шарде media")

	// ErrOriginalMissing — объекта с этим ключом нет в бакете оригиналов.
	// Постоянная ошибка: повторное чтение того же ключа не заставит объект
	// появиться. Если оригинал ещё не успел долететь до S3 (гонка с media),
	// это баг в другом месте, а не повод превращать permanent в retryable.
	ErrOriginalMissing = errors.New("оригинал отсутствует в хранилище")

	// ErrNotAnImage — байты не разбираются ни одним зарегистрированным
	// декодером изображений. Формат определяется ПО СИГНАТУРЕ (magic bytes),
	// а не по расширению файла или Content-Type — оба присылает клиент
	// media, и оба можно подделать или просто перепутать.
	ErrNotAnImage = errors.New("файл не является изображением")
)

// StatusReady — статус фото после того, как все превью сгенерированы и
// загружены. Единственный статус, которым распоряжается thumbnail-worker:
// "uploaded" и "processing" выставляет media (её таблицу мы не создаём,
// только обновляем уже существующую строку — см. internal/adapters/pg).
const StatusReady = "ready"

// SizeLabels — соответствие позиции в списке размеров конфигурации
// человекочитаемой метке контракта события (proto Thumbnail.size:
// "small|medium|large", см. proto/gosplash/events/v1/events.proto).
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: контракт события хранит МЕТКУ, а не число пикселей —
// потребитель (catalog) хочет спросить "маленькое превью", а не помнить,
// что мелкое сейчас 320px, а если THUMBNAIL_SIZES когда-нибудь поменяют на
// 300/750/1500 — вчерашние события в Kafka не должны стать нечитаемыми.
// Порядок важен: конфигурация обязана перечислять размеры от меньшего
// к большему (как и в THUMBNAIL_SIZES по умолчанию: 320,800,1600) — только
// тогда позиция 0/1/2 соответствует small/medium/large.
var SizeLabels = []string{"small", "medium", "large"}

// SizeLabel возвращает метку для позиции index в списке размеров.
//
// Контракт события предполагает РОВНО три превью. Если конфигурация задаёт
// другое количество размеров, дальше идут "size4", "size5"... — это
// не паникует, но и не является поддерживаемым сценарием: catalog ожидает
// конкретно small/medium/large.
func SizeLabel(index int) string {
	if index >= 0 && index < len(SizeLabels) {
		return SizeLabels[index]
	}
	return "size" + itoa(index+1)
}

// itoa — не strconv.Itoa ради экономии импорта: это единственное место
// в пакете, где вообще нужно превратить число в строку, и то только
// в ветке, которая при штатной конфигурации (3 размера) не выполняется.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// Thumbnail — одно готовое превью фото.
type Thumbnail struct {
	// Size — целевая длинная сторона из конфигурации (320/800/1600).
	// Не путать с Width/Height: Size — это ЗАПРОШЕННЫЙ размер, Width/Height —
	// фактический результат, который может быть меньше при отсутствии
	// апскейла (см. FitLongSide).
	Size int
	// Label — "small"/"medium"/"large", см. SizeLabel.
	Label string

	Width  int
	Height int

	StorageKey string
	// Format — реальный формат закодированных байт ("jpeg"). Обязан
	// СОВПАДАТЬ с расширением в StorageKey и Content-Type в S3 — см.
	// комментарий в internal/adapters/imagex про выбор JPEG вместо WebP.
	Format string
}

// PhotoOriginal — минимальные данные оригинала, нужные, чтобы прочитать файл
// и сгенерировать превью. Не domain.Photo целиком (как в services/media):
// thumbnail-worker не владеет всей моделью фото, ему нужен только ключ
// в хранилище.
type PhotoOriginal struct {
	ID         string
	UserID     int64
	StorageKey string
}

// FitLongSide считает размеры превью по длинной стороне targetLongSide
// с сохранением пропорций и БЕЗ АПСКЕЙЛА.
//
// Если оригинал уже не больше цели (targetLongSide >= длинной стороны
// оригинала), результат — исходные размеры без изменений: увеличивать
// маленькую картинку до "превью 1600px" бессмысленно (пустая работа) и
// вредно (апскейл только портит чёткость, никакой новой информации в кадре
// не появляется).
//
// origW/origH/target <= 0 — вырожденный случай (например, декодер отдал
// нулевые размеры); возвращаем исходные значения как есть, ничего не считая:
// это сигнал вызывающему коду, что дальше могло пойти не так, но не повод
// делить на ноль здесь.
func FitLongSide(origW, origH, target int) (width, height int) {
	if origW <= 0 || origH <= 0 || target <= 0 {
		return origW, origH
	}

	longSide := origW
	if origH > longSide {
		longSide = origH
	}
	if target >= longSide {
		return origW, origH
	}

	scale := float64(target) / float64(longSide)
	width = int(math.Round(float64(origW) * scale))
	height = int(math.Round(float64(origH) * scale))
	if width < 1 {
		width = 1
	}
	if height < 1 {
		height = 1
	}
	return width, height
}

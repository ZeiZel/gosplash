// Package ports — интерфейсы, через которые прикладной слой (internal/app)
// разговаривает с внешним миром: хранилищем метаданных, объектным
// хранилищем, распределённым локом и кодеком изображений.
//
// PATTERN: hexagonal (ports & adapters), см. подробный разбор в
// services/media/internal/ports/ports.go — здесь та же идея. Интерфейсы
// маленькие и по одному на роль: один большой Repository со всеми методами
// сразу удобно писать и мучительно мокать в тестах internal/app.
//
// ImageProcessor заслуживает отдельного слова: декодирование и кодирование
// изображения — не поход во внешнюю систему (ни сети, ни диска, ни базы),
// и на первый взгляд могло бы жить прямо в internal/app как обычная функция.
// Порт здесь ровно по той причине, ради которой порты вообще существуют:
// выбор кодека (сейчас — JPEG q85, см. internal/adapters/imagex) — это
// ИМЕННО то решение, которое должно быть заменяемым без изменения сценария.
// Появится нормальный чистый Go WebP-энкодер — меняется только адаптер
// imagex, ни строчки в internal/app.
package ports

import (
	"context"
	"image"
	"io"
	"time"

	"gosplash/services/thumbnail-worker/internal/domain"
)

// ObjectStorage — хранилище байтов (локально MinIO).
//
// В отличие от services/media, здесь нужен Get, а не только Put: media
// отдаёт оригиналы клиенту по presigned URL и сама байты не читает,
// а thumbnail-worker обязан прочитать оригинал сам, чтобы его пересжать.
type ObjectStorage interface {
	Get(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	Put(ctx context.Context, bucket, key string, reader io.Reader, size int64, contentType string) error
}

// DecodedImage — результат разбора байт оригинала.
//
// Несёт готовые Width/Height, чтобы internal/app никогда не импортировал
// stdlib "image" и не занимался арифметикой над image.Image сам — это
// работа адаптера (imagex) и домена (domain.FitLongSide).
type DecodedImage struct {
	Img    image.Image
	Width  int
	Height int
}

// EncodedThumbnail — закодированные байты одного превью.
type EncodedThumbnail struct {
	Data   []byte
	Width  int
	Height int
	// Format/Ext ОБЯЗАНЫ быть согласованы и честны: если внутри JPEG,
	// Format="jpeg" и Ext="jpg" — никогда "webp", см. internal/adapters/imagex.
	Format string
	Ext    string
}

// ImageProcessor — декодирование оригинала и генерация превью.
type ImageProcessor interface {
	// Decode проверяет, что r — валидное изображение (по сигнатуре байт,
	// НЕ по расширению файла и не по Content-Type), и возвращает его
	// исходные размеры. Ошибка декодирования — domain.ErrNotAnImage.
	Decode(r io.Reader) (DecodedImage, error)

	// Thumbnail строит превью по длинной стороне targetLongSide без
	// апскейла (domain.FitLongSide) и кодирует его.
	Thumbnail(img DecodedImage, targetLongSide int) (EncodedThumbnail, error)
}

// Lock — активный лок на фото. Единственный метод — как у *redisx.Lock,
// поэтому адаптер internal/adapters/redis возвращает его без прослойки.
type Lock interface {
	Unlock(ctx context.Context) error
}

// Locker — распределённый лок на photo_id.
//
// ОПТИМИЗАЦИЯ, а не гарантия взаимного исключения — подробный разбор почему
// см. в pkg/redisx/lock.go (doc-комментарий Lock) и в internal/app/service.go
// у места вызова. Метод назван по смыслу (AcquirePhotoLock), а не просто
// Acquire(key) — internal/app не обязан знать, как строится namespace ключа
// (client.Key(...) в internal/adapters/redis), только про то, что лочит.
type Locker interface {
	AcquirePhotoLock(ctx context.Context, photoID string, ttl time.Duration) (Lock, error)
}

// ReadyCommit — всё, что нужно, чтобы атомарно (в одной транзакции)
// зафиксировать готовность превью: отметить событие обработанным
// (idempotency.Claim), обновить статус и размеры фото и записать
// outbox-событие media.photo.thumbnail-ready.
type ReadyCommit struct {
	// EventID/EventType/Topic — конверт события media.photo.uploaded,
	// КОТОРОЕ СЕЙЧАС ОБРАБАТЫВАЕТСЯ. Это ключ idempotency.Claim — то, что
	// не даёt применить один и тот же факт дважды при повторной доставке.
	EventID   string
	EventType string
	Topic     string

	UserID  int64
	PhotoID string

	Width  int
	Height int

	Thumbnails []domain.Thumbnail
}

// PhotoRepository — доступ к таблице photos (владеет ею media, мы только
// читаем ключ оригинала и обновляем статус/размеры) и к атомарной фиксации
// готовности превью.
type PhotoRepository interface {
	// GetOriginal — ключ оригинала в S3 для (userID, photoID).
	// Фото не найдено → domain.ErrPhotoNotFound.
	GetOriginal(ctx context.Context, userID int64, photoID string) (domain.PhotoOriginal, error)

	// CommitReady — idempotency.Claim + UPDATE photos + outbox.Write
	// ОДНОЙ транзакцией на шарде пользователя (см. internal/adapters/pg).
	//
	// claimed=false означает, что событие EventID уже было обработано
	// раньше: вызывающий обязан ПРОПУСТИТЬ публикацию (её уже сделала та,
	// первая, обработка) и не считать это ошибкой.
	CommitReady(ctx context.Context, in ReadyCommit) (claimed bool, err error)
}

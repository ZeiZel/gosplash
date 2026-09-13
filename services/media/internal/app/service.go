// Package app — прикладной слой: сценарии использования media-сервиса.
//
// Знает только про domain и ports. Ни одного импорта gorm, minio, kgo, grpc
// или net/http — если такой появится, значит, сценарий пророс в инфраструктуру
// и его больше нельзя протестировать без докера.
package app

import (
	"bytes"
	"context"
	"fmt"
	"image"
	_ "image/gif"  // регистрирует декодер в image.DecodeConfig — сам пакет не используется напрямую
	_ "image/jpeg" // то же самое для JPEG
	_ "image/png"  // то же самое для PNG
	"io"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"

	"gosplash/services/media/internal/domain"
	"gosplash/services/media/internal/ports"
)

type PhotoService struct {
	repo    ports.PhotoRepository
	storage ports.ObjectStorage
	bucket  string
}

func NewPhotoService(
	repo ports.PhotoRepository,
	storage ports.ObjectStorage,
	bucket string,
) *PhotoService {
	return &PhotoService{repo: repo, storage: storage, bucket: bucket}
}

// UploadInput — вход сценария загрузки.
//
// Здесь сознательно НЕТ поля Mime. Раньше Content-Type от клиента доезжал
// досюда и использовался как есть; теперь это было бы ложью в комментарии
// "проверяем по сигнатуре, а не по Content-Type" — реальный mime целиком
// выводится из байтов файла внутри Upload (sniffImage), и хранить рядом ещё
// и непроверенное значение от клиента значило бы держать в структуре два
// источника правды об одном и том же поле, один из которых заведомо ложный.
type UploadInput struct {
	UserID   int64
	Title    string
	Filename string
	Size     int64
	Content  io.Reader
}

// Upload — вся фича целиком: файл в хранилище, метаданные и факт загрузки —
// в базу.
//
// Порядок шагов не случаен и не бесплатен:
//
//  1. Сигнатура файла. Прежде чем тратить место в S3 и строку в базе,
//     убеждаемся, что это ДЕЙСТВИТЕЛЬНО изображение — по байтам (sniffImage),
//     а не по Content-Type, который прислал клиент и которому нельзя
//     доверять: это просто текстовый заголовок формы, его может прислать
//     кто угодно с любым значением. Заодно достаём настоящие width/height —
//     клиент их не присылает и не может быть источником правды о них.
//  2. Хранилище. Если упадёт — в базе ничего нет, пользователь получил ошибку,
//     состояние согласовано.
//  3. Метаданные и факт «фото загружено» — ОДНОЙ транзакцией Postgres внутри
//     репозитория (adapters/pg.PhotoRepository.Create, docs/adr/0008-*):
//     либо в базе появляется и строка photos, и строка outbox, либо не
//     появляется ни та, ни другая. Раньше (фаза 0) здесь был третий,
//     отдельный шаг — публикация в Kafka ПОСЛЕ коммита метаданных через
//     ports.EventPublisher, — и его сбой просто логировался: фото считалось
//     загруженным, а событие терялось молча. Порт EventPublisher в этой фазе
//     убран целиком, а не переведён на outbox.Write, ровно по этой причине:
//     сохранить его означало бы оставить сценарию возможность (пусть даже
//     "исправленную" outbox'ом) вызвать Create и EventPublisher раздельно, то
//     есть оставить лазейку для ИМЕННО ТОЙ рассинхронизации, которую фаза 1
//     обязана закрыть. Убрав порт, мы убрали саму возможность собрать этот
//     баг заново — прикладной слой физически не может опубликовать событие
//     не там же, где записал фото, потому что у него больше нет для этого
//     отдельного метода.
//
// Если упадёт хранилище (шаг 2) — в S3 объекта нет, ошибка вернётся сразу.
// Если упадёт шаг 3 — в S3 останется объект, на который никто не ссылается:
// это «мусор», а не «потеря данных», его вычищает lifecycle-политика бакета.
func (s *PhotoService) Upload(ctx context.Context, in UploadInput) (*domain.Photo, error) {
	if in.UserID <= 0 {
		return nil, domain.ErrNoUserID
	}
	if in.Filename == "" {
		return nil, domain.ErrEmptyFileName
	}

	content, mime, width, height, err := sniffImage(in.Content)
	if err != nil {
		return nil, err
	}

	id := uuid.NewString()

	// Ключ детерминирован и начинается с user_id. Это не украшение:
	// по префиксу видно, чьи файлы, их удобно чистить целиком, а в реальном S3
	// префикс ещё и влияет на то, как хранилище распределяет нагрузку.
	key := fmt.Sprintf("originals/%d/%s%s", in.UserID, id, strings.ToLower(path.Ext(in.Filename)))

	// 2. Байты. content — поток (io.MultiReader из sniffImage), а не []byte:
	// файл на 50 МБ проходит через сервис кусками и не оседает в памяти целиком.
	if err := s.storage.Put(ctx, s.bucket, key, content, in.Size, mime); err != nil {
		return nil, err
	}

	// 3. Метаданные + факт. Шард выберется внутри репозитория по UserID.
	photo := &domain.Photo{
		ID:         id,
		UserID:     in.UserID,
		Title:      in.Title,
		StorageKey: key,
		Mime:       mime,
		SizeBytes:  in.Size,
		Width:      width,
		Height:     height,
		Status:     domain.StatusUploaded,
	}
	if err := s.repo.Create(ctx, photo); err != nil {
		return nil, err
	}

	return photo, nil
}

// sniffImage определяет реальный формат файла по СИГНАТУРЕ байтов и достаёт
// его width/height, при этом не читая файл в память целиком: image.DecodeConfig
// разбирает только заголовок контейнера (для JPEG/PNG/GIF это десятки байт),
// а не декодирует кадр целиком ради пары чисел — decode всего изображения
// на 50 МБ только затем, чтобы узнать его размеры, был бы расточительством.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: DecodeConfig потребляет часть исходного потока
// безвозвратно, а тот же поток нужен ЕЩЁ РАЗ, целиком, для заливки в S3
// несколькими строками ниже в Upload. io.TeeReader копирует ровно те байты,
// что прочитал DecodeConfig, в буфер; io.MultiReader склеивает "буфер +
// то, что осталось непрочитанным в исходном потоке" обратно в один Reader
// без потери и без дублирования байтов — вызывающий получает поток, из
// которого ничего не пропало, хотя часть уже была прочитана заранее.
//
// Поддерживаются jpeg/png/gif — форматы, декодеры которых есть в стандартной
// библиотеке (см. blank-импорты вверху файла). webp/heic/avif сознательно не
// добавлены: их декодеров нет в stdlib, а тянуть стороннюю зависимость ради
// сниффинга формата — решение с отдельной ценой (зависимость, её уязвимости,
// её обновления), не оправданное объёмом учебного проекта.
func sniffImage(r io.Reader) (content io.Reader, mime string, width, height int, err error) {
	var header bytes.Buffer
	cfg, format, err := image.DecodeConfig(io.TeeReader(r, &header))
	if err != nil {
		// Не оборачиваем исходную ошибку decode: пользователю нужно "это не
		// изображение", а не "unexpected EOF" или "invalid PNG header" —
		// подробности формата ему ни о чём не говорят и не про что он может
		// исправить, кроме как прислать другой файл.
		return nil, "", 0, 0, domain.ErrNotAnImage
	}
	return io.MultiReader(&header, r), "image/" + format, cfg.Width, cfg.Height, nil
}

func (s *PhotoService) Get(ctx context.Context, userID int64, id string) (*domain.Photo, error) {
	return s.repo.GetByID(ctx, userID, id)
}

func (s *PhotoService) ListByUser(ctx context.Context, userID int64, limit int) ([]domain.Photo, error) {
	return s.repo.ListByUser(ctx, userID, limit)
}

// Delete — мягкое удаление: статус меняется на domain.StatusDeleted и в той
// же транзакции пишется факт "фото удалено" (adapters/pg.PhotoRepository.
// Delete, тот же приём атомарности, что и в Upload).
//
// Файлы из S3 здесь и вообще на этой фазе НЕ удаляются. Причина не в лени:
// media.photo.deleted ещё не обработан подписчиками (catalog должен убрать
// карточку из ленты, thumbnail-worker — не начинать генерировать превью для
// уже удалённого фото), и стереть оригинал раньше значило бы, что кто-то
// из них получит событие про объект, которого физически уже нет. Настоящее
// удаление байтов — работа саги удаления (фаза 3): она подписывается на то
// же событие и подчищает S3 ПОСЛЕ того, как остальные участники поезда
// подтвердили, что им оригинал больше не нужен.
func (s *PhotoService) Delete(ctx context.Context, userID int64, id string) error {
	return s.repo.Delete(ctx, userID, id)
}

// DownloadURL — временная подписанная ссылка на оригинал. Файл клиент качает
// прямо из хранилища, сервис в передаче байтов не участвует: не тратит ни
// трафик, ни горутины.
func (s *PhotoService) DownloadURL(ctx context.Context, photo *domain.Photo, ttl time.Duration) (string, error) {
	return s.storage.PresignedURL(ctx, s.bucket, photo.StorageKey, ttl)
}

func (s *PhotoService) ShardOf(userID int64) int { return s.repo.ShardOf(userID) }

func (s *PhotoService) CountByShard(ctx context.Context) ([]int64, error) {
	return s.repo.CountByShard(ctx)
}

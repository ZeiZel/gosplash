// Package app — прикладной слой: единственный сценарий thumbnail-worker'а —
// "обработать одно событие media.photo.uploaded".
//
// Знает только про domain и ports (плюс стандартную библиотеку и
// golang.org/x/sync/errgroup — обычную библиотеку синхронизации, а не
// инфраструктурный клиент). Ни одного импорта gorm, kgo, redis, minio,
// grpc или net/http — если такой появится, значит, сценарий пророс
// в инфраструктуру и его больше нельзя протестировать без докера.
package app

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"gosplash/services/thumbnail-worker/internal/domain"
	"gosplash/services/thumbnail-worker/internal/ports"
)

// Service — сценарий генерации превью.
type Service struct {
	repo    ports.PhotoRepository
	storage ports.ObjectStorage
	locker  ports.Locker
	imgs    ports.ImageProcessor

	bucketOriginals  string
	bucketThumbnails string
	sizes            []int
	lockTTL          time.Duration

	// sem ограничивает число ОДНОВРЕМЕННЫХ CPU-bound ресайзов ВО ВСЁМ
	// ПРОЦЕССЕ, а не в рамках одного сообщения.
	//
	// PATTERN: bounded worker pool через буферизованный канал-семафор.
	// kafkax уже даёт горутину на партицию (kafka.go), но партиций может
	// быть больше, чем ядер, а на каждое сообщение этот сценарий запускает
	// ЕЩЁ по одной горутине на каждый из трёх размеров превью. Без общего
	// ограничителя число одновременных ресайзов растёт вместе с числом
	// параллельно обрабатываемых сообщений: с шестью партициями и тремя
	// размерами это уже 18 конкурентных декодирований полноразмерных
	// фотографий в памяти — и это именно тот сценарий "горутина на
	// сообщение → тысяча параллельных ресайзов и OOM", от которого
	// предостерегает pkg/config (ThumbnailConfig.Workers). Семафор — общий
	// для всех сообщений и партиций сразу, потому что процесс один, а не
	// один на партицию: ограничивать нужно суммарную нагрузку на CPU/память
	// этого конкретного инстанса.
	sem chan struct{}
}

// NewService собирает сценарий. workers обязан быть положительным — подстановку
// runtime.NumCPU() при workers<=0 делает main.go (композиция), а не
// конструктор: это забота о значении по умолчанию, а не о поведении сценария.
func NewService(
	repo ports.PhotoRepository,
	storage ports.ObjectStorage,
	locker ports.Locker,
	imgs ports.ImageProcessor,
	bucketOriginals, bucketThumbnails string,
	sizes []int,
	lockTTL time.Duration,
	workers int,
) *Service {
	if workers < 1 {
		workers = 1
	}
	return &Service{
		repo:             repo,
		storage:          storage,
		locker:           locker,
		imgs:             imgs,
		bucketOriginals:  bucketOriginals,
		bucketThumbnails: bucketThumbnails,
		sizes:            sizes,
		lockTTL:          lockTTL,
		sem:              make(chan struct{}, workers),
	}
}

// ProcessInput — вход сценария. Плоские типы, а не *eventsv1.Envelope:
// разбор конверта и классификация ошибки для kafkax — забота
// internal/adapters/kafka, а не этого пакета (см. package doc).
type ProcessInput struct {
	EventID   string
	EventType string
	Topic     string

	PhotoID string
	UserID  int64
}

// Process — сценарий целиком: лок → чтение оригинала → валидация и генерация
// превью → атомарная фиксация готовности.
//
// Порядок шагов не случаен:
//
//  1. Лок (ОПТИМИЗАЦИЯ, см. package doc ports.Locker) — не гарантия, но
//     дешёвый способ не делать одну и ту же дорогую работу дважды при
//     ребалансе. Лок не удался → пропускаем работу без ошибки: кто-то
//     другой уже её делает.
//  2. Чтение и декодирование оригинала — если это не изображение, дальше
//     идти незачем, и ошибка ДОЛЖНА дойти до adapters/kafka классифицированной
//     как постоянная (через errors.Is(domain.ErrNotAnImage) там).
//  3. Генерация всех превью.
//  4. CommitReady — ЕДИНСТВЕННОЕ место, где идемпотентность (processed_events)
//     является ГАРАНТИЕЙ, а не оптимизацией (см. package doc pkg/idempotency).
//     Если Claim решит, что событие уже применено, работа (превью уже
//     сгенерированы и загружены к этому моменту) не пропадает даром: ключи
//     объектов детерминированы (userID/photoID/size), повторная загрузка —
//     это идемпотентный PUT поверх того же самого содержимого, а не мусор.
//     Разносить Claim на отдельную транзакцию ДО генерации превью означало
//     бы либо повторную транзакцию в конце (тогда что мешало бы гонке между
//     ними?), либо потерю гарантии "Claim и бизнес-изменение — одна
//     транзакция" (docs/STYLE.md, pkg/idempotency). Дешевле смириться
//     с редкой избыточной перегенерацией, чем разменивать на неё гарантию.
func (s *Service) Process(ctx context.Context, in ProcessInput) error {
	lock, err := s.locker.AcquirePhotoLock(ctx, in.PhotoID, s.lockTTL)
	if err != nil {
		return fmt.Errorf("лок фото %s: %w", in.PhotoID, err)
	}
	if lock == nil {
		slog.InfoContext(ctx, "thumbnail-worker: лок занят, пропускаю — другой инстанс уже обрабатывает",
			"photo_id", in.PhotoID)
		return nil
	}
	defer func() {
		if uerr := lock.Unlock(ctx); uerr != nil {
			// Не гарантия — см. package doc. Логируем и идём дальше:
			// корректность держится на CommitReady, а не на владении локом.
			slog.WarnContext(ctx, "thumbnail-worker: не смог снять лок (не критично)",
				"photo_id", in.PhotoID, "error", uerr)
		}
	}()

	original, err := s.repo.GetOriginal(ctx, in.UserID, in.PhotoID)
	if err != nil {
		return fmt.Errorf("оригинал фото %s: %w", in.PhotoID, err)
	}

	reader, err := s.storage.Get(ctx, s.bucketOriginals, original.StorageKey)
	if err != nil {
		return fmt.Errorf("чтение оригинала %s: %w", original.StorageKey, err)
	}
	defer reader.Close()

	decoded, err := s.imgs.Decode(reader)
	if err != nil {
		return fmt.Errorf("разбор изображения %s: %w", original.StorageKey, err)
	}

	thumbs, err := s.generateAll(ctx, in, decoded)
	if err != nil {
		return fmt.Errorf("генерация превью фото %s: %w", in.PhotoID, err)
	}

	claimed, err := s.repo.CommitReady(ctx, ports.ReadyCommit{
		EventID:    in.EventID,
		EventType:  in.EventType,
		Topic:      in.Topic,
		UserID:     in.UserID,
		PhotoID:    in.PhotoID,
		Width:      decoded.Width,
		Height:     decoded.Height,
		Thumbnails: thumbs,
	})
	if err != nil {
		return fmt.Errorf("фиксация готовности фото %s: %w", in.PhotoID, err)
	}
	if !claimed {
		slog.InfoContext(ctx, "thumbnail-worker: событие уже обработано, пропускаю (идемпотентность)",
			"event_id", in.EventID, "photo_id", in.PhotoID)
		return nil
	}

	slog.InfoContext(ctx, "thumbnail-worker: превью готовы",
		"photo_id", in.PhotoID, "user_id", in.UserID, "count", len(thumbs))
	return nil
}

// generateAll строит и загружает превью для всех сконфигурированных
// размеров, бегая errgroup'ом, но ограниченным общим семафором s.sem.
func (s *Service) generateAll(ctx context.Context, in ProcessInput, decoded ports.DecodedImage) ([]domain.Thumbnail, error) {
	results := make([]domain.Thumbnail, len(s.sizes))

	g, gctx := errgroup.WithContext(ctx)
	for i, size := range s.sizes {
		g.Go(func() error {
			select {
			case s.sem <- struct{}{}:
			case <-gctx.Done():
				return gctx.Err()
			}
			defer func() { <-s.sem }()

			thumb, err := s.imgs.Thumbnail(decoded, size)
			if err != nil {
				return fmt.Errorf("превью %dpx: %w", size, err)
			}

			key := thumbnailKey(in.UserID, in.PhotoID, size, thumb.Ext)
			contentType := "image/" + thumb.Format
			if err := s.storage.Put(gctx, s.bucketThumbnails, key,
				bytes.NewReader(thumb.Data), int64(len(thumb.Data)), contentType); err != nil {
				return fmt.Errorf("загрузка превью %dpx: %w", size, err)
			}

			results[i] = domain.Thumbnail{
				Size:       size,
				Label:      domain.SizeLabel(i),
				Width:      thumb.Width,
				Height:     thumb.Height,
				StorageKey: key,
				Format:     thumb.Format,
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

// thumbnailKey — thumbnails/{user_id}/{photo_id}_{size}.{ext}.
//
// Детерминирован (без случайных суффиксов): повторная генерация того же
// превью перезаписывает тот же объект, а не плодит мусор — это и есть то,
// что делает безопасной идемпотентность CommitReady (см. комментарий Process).
func thumbnailKey(userID int64, photoID string, size int, ext string) string {
	return fmt.Sprintf("thumbnails/%d/%s_%d.%s", userID, photoID, size, ext)
}

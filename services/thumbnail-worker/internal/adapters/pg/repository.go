// Package pg — адаптер к PostgreSQL. Единственное место в thumbnail-worker,
// которое знает про GORM, про строение таблицы photos и про то, что баз
// несколько (шарды media).
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: своя база для processed_events/outbox тут не заведена
// — вместо этого thumbnail-worker пишет их на ТЕ ЖЕ шарды, что и media
// (conf.Media.ShardDSNs, выбор шарда по user_id тем же hash(user_id), что
// и у media — см. pkg/dbx.Shards). Три причины:
//
//  1. Это ПРАВИЛЬНО с точки зрения шардирования, а не просто удобно.
//     processed_events и обновление photos.status ДОЛЖНЫ жить в одной
//     транзакции (см. pkg/idempotency, doc-комментарий Claim) — а транзакция
//     в PostgreSQL не бывает between двумя разными серверами. Если бы
//     processed_events лежала в отдельной базе thumbnail-worker'а, атомарность
//     "отметил событие обработанным + обновил статус фото" была бы физически
//     невозможна, и пришлось бы городить saga/2PC ради задачи, которая
//     тривиально решается тем, что данные и их отметка об обработке лежат
//     на одном сервере.
//  2. Правило проекта "os.Getenv только в pkg/config" не оставляет способа
//     завести новую переменную THUMBNAIL_DSN, не тронув pkg/config — а pkg/**
//     вне зоны ответственности этой задачи. conf.Media.ShardDSNs уже есть
//     и уже задаёт ровно то разбиение данных, которое нужно.
//  3. Честная цена этого решения: таблицы processed_events ("processed_events",
//     см. pkg/idempotency) и outbox ("outbox", см. pkg/outbox) на шардах media
//     физически СЕЙЧАС используются только thumbnail-worker'ом — сама media
//     (services/media/migrations/auto.go) их не создаёт и не пишет в них,
//     она публикует media.photo.uploaded напрямую. Если однажды media тоже
//     перейдёт на outbox на ЭТИХ ЖЕ шардах, придётся озаботиться тем, что
//     pkg/idempotency.ProcessedEvent.TableName() и pkg/outbox.OutboxRow.TableName()
//     фиксированы и общие на любого писателя одной базы — событие-id (UUID v7)
//     глобально уникален, так что коллизии PRIMARY KEY не будет, но таблица
//     перестанет быть "одна отметка — один консьюмер", как задумано в doc-
//     комментарии pkg/idempotency. Для текущей фазы проекта это не проблема
//     (media в неё не пишет), но это тот компромисс, о котором нужно знать
//     заранее, а не переоткрывать через полгода дебага.
package pg

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"gosplash/pkg/dbx"
	"gosplash/pkg/idempotency"
	"gosplash/pkg/kafkax"
	"gosplash/pkg/outbox"
	"gosplash/services/thumbnail-worker/internal/domain"
	"gosplash/services/thumbnail-worker/internal/ports"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// serviceName — владелец очереди outbox. Воркер делит шарды с media, поэтому
// таблица у него своя (outbox_thumbnail-worker), и заголовок producer событий
// media.photo.thumbnail-ready честно говорит, кто их породил
// (см. pkg/outbox.TableFor).
const serviceName = "thumbnail-worker"

// PhotoRow — МИНИМАЛЬНАЯ модель таблицы photos: только то, что
// thumbnail-worker читает (storage_key) и обновляет (status, width, height).
//
// Таблицу и её полную схему создаёт и владеет ею media
// (services/media/internal/adapters/pg.PhotoRow) — thumbnail-worker её
// НЕ мигрирует (см. migrations/auto.go: там AutoMigrate только для
// processed_events и outbox). Собственный узкий тип вместо импорта чужого
// internal-пакета: internal одного сервиса недоступен другому по правилам
// самого Go, да и не должен быть — схема таблицы это деталь media, а не
// публичный контракт. GORM пишет UPDATE по именам колонок, а не по всем
// полям сразу, поэтому "неполный" тип здесь абсолютно безопасен: Updates()
// с map ниже трогает только явно перечисленные колонки.
type PhotoRow struct {
	ID         string `gorm:"type:uuid;primaryKey"`
	UserID     int64  `gorm:"not null"`
	StorageKey string `gorm:"not null"`
	Status     string `gorm:"size:20;not null;default:uploaded"`
	Width      int
	Height     int
}

func (PhotoRow) TableName() string { return "photos" }

// PhotoRepository — реализация ports.PhotoRepository.
type PhotoRepository struct {
	shards *dbx.Shards
}

func NewPhotoRepository(shards *dbx.Shards) *PhotoRepository {
	return &PhotoRepository{shards: shards}
}

// Ping — для /readyz, тем же приёмом, что и у media (pkg/dbx.Shards.Ping).
func (r *PhotoRepository) Ping(ctx context.Context) error { return r.shards.Ping(ctx) }

func (r *PhotoRepository) GetOriginal(ctx context.Context, userID int64, photoID string) (domain.PhotoOriginal, error) {
	var row PhotoRow
	err := r.shards.For(userID).WithContext(ctx).
		Where("id = ? AND user_id = ?", photoID, userID).
		First(&row).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.PhotoOriginal{}, fmt.Errorf("фото %s: %w", photoID, domain.ErrPhotoNotFound)
	}
	if err != nil {
		return domain.PhotoOriginal{}, fmt.Errorf("чтение фото %s: %w", photoID, err)
	}
	return domain.PhotoOriginal{ID: row.ID, UserID: row.UserID, StorageKey: row.StorageKey}, nil
}

// CommitReady — единственное место, где idempotency.Claim, UPDATE photos
// и outbox.Write выполняются ОДНОЙ транзакцией. Порядок операций внутри неё
// важен по той же причине, что и в pkg/idempotency doc-комментарии Claim:
// Claim — первым действием, чтобы повтор события не тратил ни одного лишнего
// запроса сверх самого Claim.
func (r *PhotoRepository) CommitReady(ctx context.Context, in ports.ReadyCommit) (bool, error) {
	var claimed bool

	err := r.shards.For(in.UserID).Transaction(func(tx *gorm.DB) error {
		tx = tx.WithContext(ctx)

		ok, err := idempotency.Claim(tx, in.EventID, in.EventType, in.Topic)
		if err != nil {
			return fmt.Errorf("claim события %s: %w", in.EventID, err)
		}
		claimed = ok
		if !ok {
			// Событие уже применено раньше. Транзакция коммитится пустой
			// (Claim уже откатил бы её сам при ошибке выше) — это не rollback,
			// а осознанный no-op: откатывать нечего, мы ничего не поменяли.
			return nil
		}

		res := tx.Model(&PhotoRow{}).
			Where("id = ? AND user_id = ?", in.PhotoID, in.UserID).
			Updates(map[string]any{
				"status": domain.StatusReady,
				"width":  in.Width,
				"height": in.Height,
			})
		if res.Error != nil {
			return fmt.Errorf("обновление статуса фото %s: %w", in.PhotoID, res.Error)
		}
		if res.RowsAffected == 0 {
			return fmt.Errorf("фото %s: %w", in.PhotoID, domain.ErrPhotoNotFound)
		}

		env, err := buildThumbnailReadyEnvelope(in)
		if err != nil {
			return fmt.Errorf("сборка события thumbnail-ready: %w", err)
		}
		if err := outbox.Write(ctx, tx, serviceName, env, kafkax.TopicPhotoThumbnailReady); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return claimed, nil
}

// buildThumbnailReadyEnvelope собирает конверт PhotoThumbnailReady.
//
// Живёт в pg-адаптере, а не в отдельном kafka-адаптере, хотя формально это
// "сборка Kafka-конверта": outbox.Write требует tx этой самой транзакции,
// и разносить эти две строки по разным пакетам не убрало бы зависимость
// от kafkax/eventsv1 отсюда — просто спрятало бы её на один файл дальше.
// Ровно так же устроен пример в doc-комментарии pkg/outbox.Write.
func buildThumbnailReadyEnvelope(in ports.ReadyCommit) (*eventsv1.Envelope, error) {
	payload := &eventsv1.PhotoThumbnailReady{
		PhotoId:    in.PhotoID,
		UserId:     in.UserID,
		Thumbnails: make([]*eventsv1.Thumbnail, 0, len(in.Thumbnails)),
	}
	for _, t := range in.Thumbnails {
		payload.Thumbnails = append(payload.Thumbnails, &eventsv1.Thumbnail{
			Size:       t.Label,
			StorageKey: t.StorageKey,
			Width:      int32(t.Width),
			Height:     int32(t.Height),
		})
	}

	return kafkax.NewEnvelope(kafkax.EventPhotoThumbnailReady, in.PhotoID, payload)
}

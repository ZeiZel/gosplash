// Package pg — адаптер к PostgreSQL. Единственное место в media, которое
// знает про GORM, про таблицы и про то, что баз несколько.
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"gosplash/pkg/dbx"
	"gosplash/pkg/kafkax"
	"gosplash/pkg/outbox"
	"gosplash/services/media/internal/domain"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// serviceName — владелец очереди outbox. Определяет и таблицу (outbox_media),
// и заголовок producer публикуемых событий: media и thumbnail-worker делят
// шарды, поэтому очередь у каждого своя (см. pkg/outbox.TableFor).
const serviceName = "media"

// PhotoRow — строка таблицы photos. Лежит на ОДНОМ из шардов media,
// на каком именно — решает hash(user_id), см. pkg/dbx.
//
// Отдельный тип, а не доменный domain.Photo с тегами: схема таблицы меняется
// чаще модели предметной области, и связывать их значит платить правкой домена
// за каждый новый индекс. Цена отдельного типа — две функции отображения ниже;
// цена связанных типов — доменная модель, которая знает про B-tree.
//
// Обрати внимание, чего здесь нет: gorm.Model. Он даёт автоинкрементный
// uint-ID, а в шардированной таблице это ловушка — последовательности
// на разных серверах независимы, и шард 0 с шардом 1 быстро выдадут
// одинаковый id=1. Поэтому ключ — UUID, сгенерированный приложением.
type PhotoRow struct {
	ID     string `gorm:"type:uuid;primaryKey"`
	UserID int64  `gorm:"not null;index:idx_photos_user_created,priority:1"`

	Title      string `gorm:"size:200"`
	StorageKey string `gorm:"not null"`
	Mime       string `gorm:"size:100"`
	SizeBytes  int64
	Width      int
	Height     int

	Status string `gorm:"size:20;not null;default:uploaded"`

	// Индекс составной: (user_id, created_at DESC) — ровно то, что нужно
	// для «покажи мои фото, новые сверху». Один запрос, один шард.
	CreatedAt time.Time `gorm:"index:idx_photos_user_created,priority:2,sort:desc"`
	UpdatedAt time.Time
}

func (PhotoRow) TableName() string { return "photos" }

func toDomain(r *PhotoRow) *domain.Photo {
	return &domain.Photo{
		ID:         r.ID,
		UserID:     r.UserID,
		Title:      r.Title,
		StorageKey: r.StorageKey,
		Mime:       r.Mime,
		SizeBytes:  r.SizeBytes,
		Width:      r.Width,
		Height:     r.Height,
		Status:     r.Status,
		CreatedAt:  r.CreatedAt,
		UpdatedAt:  r.UpdatedAt,
	}
}

func toRow(p *domain.Photo) *PhotoRow {
	return &PhotoRow{
		ID:         p.ID,
		UserID:     p.UserID,
		Title:      p.Title,
		StorageKey: p.StorageKey,
		Mime:       p.Mime,
		SizeBytes:  p.SizeBytes,
		Width:      p.Width,
		Height:     p.Height,
		Status:     p.Status,
	}
}

// PhotoRepository — доступ к таблице photos. Единственное отличие от обычного
// репозитория: внутри не одно соединение, а набор шардов, и первым делом
// каждый метод выбирает нужный.
type PhotoRepository struct {
	shards *dbx.Shards
}

func NewPhotoRepository(shards *dbx.Shards) *PhotoRepository {
	return &PhotoRepository{shards: shards}
}

func (r *PhotoRepository) ShardOf(userID int64) int { return r.shards.Index(userID) }

// Create записывает строку photos и строку outbox с фактом "фото загружено"
// ОДНОЙ транзакцией Postgres.
//
// PATTERN: transactional outbox (pkg/outbox, docs/adr/0008-*). Атомарность
// не изобретается заново — её даёт сам Postgres для двух insert'ов одной
// транзакции; цена — Relay, который вычитывает outbox отдельным процессом
// (main.go), и небольшая задержка публикации (PollInterval).
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: это вообще работает только потому, что media
// шардирован ПО user_id (docs/adr/0002-*), и строка photos, и связанная
// с ней строка outbox для одного и того же фото ВСЕГДА вычисляют один и тот
// же индекс шарда — у них один и тот же ключ шардирования. Соответственно,
// обе строки физически попадают в одну и ту же базу, то есть в обычную
// не-распределённую транзакцию одного Postgres-сервера. Если бы ключ
// шардирования события отличался от ключа шардирования фото, транзакцию
// пришлось бы либо разносить по базам (а Postgres не умеет 2PC между
// независимыми серверами без XA, которого здесь нет), либо смириться
// с тем же разъездом данных, который outbox и должен был устранить.
func (r *PhotoRepository) Create(ctx context.Context, photo *domain.Photo) error {
	row := toRow(photo)

	// .For(userID) — вся суть роутинга: выбираем ОДИН шард один раз, и Create,
	// и Write внутри транзакции ниже идут строго в него.
	err := r.shards.For(photo.UserID).WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(row).Error; err != nil {
			return fmt.Errorf("создание фото: %w", err)
		}

		env, err := kafkax.NewEnvelope(kafkax.EventPhotoUploaded, photo.ID, &eventsv1.PhotoUploaded{
			PhotoId: photo.ID,
			UserId:  photo.UserID,
		})
		if err != nil {
			return fmt.Errorf("конверт события %s: %w", kafkax.EventPhotoUploaded, err)
		}
		if err := outbox.Write(ctx, tx, serviceName, env, kafkax.TopicPhotoUploaded); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	// CreatedAt проставил GORM — возвращаем его вызывающему, иначе домен
	// уедет с нулевым временем.
	photo.CreatedAt = row.CreatedAt
	photo.UpdatedAt = row.UpdatedAt
	return nil
}

// Delete — мягкое удаление и факт "фото удалено" в ТОЙ ЖЕ транзакции, тем
// же приёмом, что и Create. Файл в S3 не трогаем: это решение прикладного
// слоя и HTTP-адаптера, репозиторий про S3 вообще ничего не знает.
//
// UPDATE ... AND status != 'deleted' — а не просто по id/user_id — делает
// повторный вызов идемпотентным на уровне СЧИТАННЫХ строк: если фото уже
// удалено, RowsAffected будет 0, и мы вернём ErrNotFound, не публикуя
// событие повторно. Без этого условия второй DELETE того же фото каждый раз
// заново эмитировал бы media.photo.deleted — событие о факте, который уже
// случился раньше, и подписчики (сага удаления, фаза 3) получили бы его
// на каждый повторный клик, а не один раз.
func (r *PhotoRepository) Delete(ctx context.Context, userID int64, id string) error {
	return r.shards.For(userID).WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&PhotoRow{}).
			Where("id = ? AND user_id = ? AND status != ?", id, userID, domain.StatusDeleted).
			Updates(map[string]any{"status": domain.StatusDeleted, "updated_at": time.Now()})
		if result.Error != nil {
			return fmt.Errorf("удаление фото: %w", result.Error)
		}
		if result.RowsAffected == 0 {
			return domain.ErrNotFound
		}

		env, err := kafkax.NewEnvelope(kafkax.EventPhotoDeleted, id, &eventsv1.PhotoDeleted{
			PhotoId: id,
			UserId:  userID,
		})
		if err != nil {
			return fmt.Errorf("конверт события %s: %w", kafkax.EventPhotoDeleted, err)
		}
		return outbox.Write(ctx, tx, serviceName, env, kafkax.TopicPhotoDeleted)
	})
}

func (r *PhotoRepository) GetByID(ctx context.Context, userID int64, id string) (*domain.Photo, error) {
	var row PhotoRow
	err := r.shards.For(userID).WithContext(ctx).
		Where("id = ? AND user_id = ?", id, userID).
		First(&row).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("чтение фото: %w", err)
	}
	return toDomain(&row), nil
}

// ListByUser — «мои фото». Ключ шардирования известен, поэтому это обычный
// запрос к одной базе по составному индексу.
//
// status != deleted — намеренно только здесь, не в GetByID: в списке «мои
// фото» удалённому нечего делать, а по прямой ссылке на конкретное фото
// (GetByID) владелец имеет право увидеть, что оно в статусе "deleted" —
// строка ведь физически ещё в базе (см. Delete).
func (r *PhotoRepository) ListByUser(ctx context.Context, userID int64, limit int) ([]domain.Photo, error) {
	var rows []PhotoRow
	err := r.shards.For(userID).WithContext(ctx).
		Where("user_id = ? AND status != ?", userID, domain.StatusDeleted).
		Order("created_at DESC").
		Limit(limit).
		Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("список фото: %w", err)
	}

	photos := make([]domain.Photo, 0, len(rows))
	for i := range rows {
		photos = append(photos, *toDomain(&rows[i]))
	}
	return photos, nil
}

// CountByShard — сколько строк на каждом шарде. Диагностическая ручка,
// чтобы увидеть распределение своими глазами (make shards).
//
// Заодно это пример scatter-gather: ключа шардирования нет, поэтому
// приходится опрашивать ВСЕ шарды. С двумя это незаметно, с двадцатью
// такой запрос будет ждать самый медленный сервер и съест по соединению
// на каждом. Публичные списки так строить нельзя — для них есть catalog.
func (r *PhotoRepository) CountByShard(ctx context.Context) ([]int64, error) {
	counts := make([]int64, 0, r.shards.Count())
	for i, conn := range r.shards.All() {
		var n int64
		if err := conn.WithContext(ctx).Model(&PhotoRow{}).Count(&n).Error; err != nil {
			return nil, fmt.Errorf("шард %d: %w", i, err)
		}
		counts = append(counts, n)
	}
	return counts, nil
}

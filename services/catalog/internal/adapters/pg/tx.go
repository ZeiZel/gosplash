package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"gosplash/pkg/idempotency"
	"gosplash/pkg/outbox"
	"gosplash/services/catalog/internal/domain"
	"gosplash/services/catalog/internal/ports"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// serviceName — владелец очереди outbox (таблица outbox_catalog) и значение
// заголовка producer у событий catalog.listing.published.
const serviceName = "catalog"

// UnitOfWork — граница идемпотентного консьюмера каталога: одна транзакция
// на событие, Claim (pkg/idempotency) первым действием, дальше — работа
// вызывающего через ports.IndexTx, коммит/откат целиком.
type UnitOfWork struct {
	db *gorm.DB
}

func NewUnitOfWork(db *gorm.DB) *UnitOfWork {
	return &UnitOfWork{db: db}
}

// WithClaim — см. подробный разбор компромисса (транзакция держится открытой
// на время работы fn, включая внешние вызовы) в ports.UnitOfWork.
func (u *UnitOfWork) WithClaim(ctx context.Context, eventID, eventType, topic string, fn func(ports.IndexTx) error) (bool, error) {
	applied := false

	err := u.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		ok, err := idempotency.Claim(tx, eventID, eventType, topic)
		if err != nil {
			return err
		}
		if !ok {
			// Событие уже применено раньше. Транзакция коммитится пустой
			// (Claim ничего не менял, потому что вставка не прошла из-за
			// конфликта) — fn НЕ вызывается, ни одного лишнего запроса,
			// включая сетевые (media по gRPC внутри fn).
			return nil
		}
		applied = true
		return fn(&indexTx{tx: tx})
	})
	if err != nil {
		return false, err
	}
	return applied, nil
}

// indexTx — рабочая транзакция одного события. Живёт ровно на время одного
// вызова WithClaim, отдельного экспортированного конструктора не имеет.
type indexTx struct {
	tx *gorm.DB
}

// UpsertListing создаёт черновик карточки.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: ON CONFLICT DO NOTHING, а НЕ UpdateAll, как было в
// фазе 0. Тогда идемпотентность держалась целиком на upsert'е — "записать то
// же состояние второй раз ничего не меняет" — и это было безопасно, пока
// карточку писал ОДИН обработчик. Теперь есть ВТОРОЙ писатель того же
// photo_id (HandlePhotoThumbnailReady, ApplyThumbnails ниже), и порядок
// применения двух топиков не гарантирован. UpdateAll здесь означал бы: если
// TransactionThumbnailReady почему-то применился РАНЬШЕ (не должен, но Claim
// защищает только от ПОВТОРА одного и того же события, а не от гонки разных
// событий) и перевёл карточку в published, а следом всё же "долетел" повторно
// обрабатываемый PhotoUploaded — UpdateAll тихо откатил бы published обратно
// в draft и стёр бы уже записанные превью. DO NOTHING делает вставку строго
// однократной: если строка уже есть (в любом статусе), Uploaded её не трогает.
func (t *indexTx) UpsertListing(ctx context.Context, listing *domain.Listing) error {
	thumbnails, err := marshalThumbnails(listing.Thumbnails)
	if err != nil {
		return err
	}
	row := &ListingRow{
		ID:         listing.ID,
		AuthorID:   listing.AuthorID,
		Title:      listing.Title,
		Tags:       listing.Tags,
		StorageKey: listing.StorageKey,
		Thumbnails: thumbnails,
		PriceCents: listing.PriceCents,
		Currency:   listing.Currency,
		Status:     domain.StatusDraft,
		Width:      listing.Width,
		Height:     listing.Height,
	}

	err = t.tx.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoNothing: true,
	}).Create(row).Error
	if err != nil {
		return fmt.Errorf("создание карточки: %w", err)
	}
	return nil
}

// ApplyThumbnails дописывает превью и переводит карточку в published.
func (t *indexTx) ApplyThumbnails(ctx context.Context, photoID string, thumbnails map[string]string, publishedAt time.Time) (*domain.Listing, error) {
	var row ListingRow
	// FOR UPDATE: строка блокируется на время транзакции. Без этого два
	// одновременных применения (redelivery одного события в двух местах,
	// хотя Claim такого не допустит, но защита от гонки должна быть на
	// уровне данных, а не только на уровне протокола консьюмера) могли бы
	// прочитать одно и то же старое состояние thumbnails и потерять
	// обновление друг друга при записи (lost update).
	err := t.tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ?", photoID).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("применение превью: %w", err)
	}

	existing, err := unmarshalThumbnails(row.Thumbnails)
	if err != nil {
		return nil, err
	}
	for size, key := range thumbnails {
		existing[size] = key
	}
	merged, err := marshalThumbnails(existing)
	if err != nil {
		return nil, err
	}

	at := publishedAt
	updates := map[string]any{
		"thumbnails":   merged,
		"status":       domain.StatusPublished,
		"published_at": at,
	}
	if err := t.tx.WithContext(ctx).Model(&ListingRow{}).Where("id = ?", photoID).Updates(updates).Error; err != nil {
		return nil, fmt.Errorf("публикация карточки: %w", err)
	}

	row.Thumbnails = merged
	row.Status = domain.StatusPublished
	row.PublishedAt = &at

	var author AuthorSnapshotRow
	authorName := ""
	if err := t.tx.WithContext(ctx).Where("id = ?", row.AuthorID).First(&author).Error; err == nil {
		authorName = author.Name
	}
	return toDomain(&row, authorName)
}

// UpsertAuthorSnapshot — DO NOTHING по той же причине, что описана в
// listing_repository.go у AuthorSnapshotRow: имя — заглушка на момент
// первого появления автора (см. internal/app/indexer.go), и если позже
// появится настоящее событие с именем, DO NOTHING не должно быть помехой —
// обновление настоящим именем идёт ОТДЕЛЬНЫМ путём (пока не реализован,
// см. отчёт о работе), а не через этот метод.
func (t *indexTx) UpsertAuthorSnapshot(ctx context.Context, authorID int64, name string) error {
	err := t.tx.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoNothing: true,
	}).Create(&AuthorSnapshotRow{ID: authorID, Name: name, UpdatedAt: time.Now()}).Error
	if err != nil {
		return fmt.Errorf("слепок автора: %w", err)
	}
	return nil
}

func (t *indexTx) Outbox(ctx context.Context, env *eventsv1.Envelope, topic string) error {
	return outbox.Write(ctx, t.tx, serviceName, env, topic)
}

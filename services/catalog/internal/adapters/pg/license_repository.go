package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"gosplash/services/catalog/internal/domain"
)

// LicenseRow — строка таблицы licenses.
//
// OrderID уникален — это и есть механизм идемпотентности: повтор саги с тем
// же order_id либо находит уже созданную строку (Grant), либо не находит
// ничего для отзыва (Revoke), но никогда не создаёт вторую лицензию и не
// путает две разные покупки.
type LicenseRow struct {
	ID        string `gorm:"type:uuid;primaryKey"`
	ListingID string `gorm:"column:listing_id;not null;index"`
	BuyerID   int64  `gorm:"column:buyer_id;not null;index"`
	OrderID   string `gorm:"column:order_id;not null;uniqueIndex"`

	Status string `gorm:"size:20;not null"`
	Reason string `gorm:"size:200"`

	GrantedAt time.Time
	RevokedAt *time.Time
}

func (LicenseRow) TableName() string { return "licenses" }

func licenseFromRow(r *LicenseRow) *domain.License {
	l := &domain.License{
		ID:        r.ID,
		ListingID: r.ListingID,
		BuyerID:   r.BuyerID,
		OrderID:   r.OrderID,
		Status:    r.Status,
		Reason:    r.Reason,
		GrantedAt: r.GrantedAt,
	}
	if r.RevokedAt != nil {
		l.RevokedAt = *r.RevokedAt
	}
	return l
}

// LicenseRepository — выдача и отзыв лицензий, идемпотентные по order_id.
type LicenseRepository struct {
	db *gorm.DB
}

func NewLicenseRepository(db *gorm.DB) *LicenseRepository {
	return &LicenseRepository{db: db}
}

// Grant выдаёт лицензию.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: идемпотентность здесь НЕ через pkg/idempotency.
// ProcessedEvent дедуплицирует Kafka-СОБЫТИЯ по event_id внутри ОДНОЙ
// транзакции с бизнес-изменением консьюмера; order_id — это бизнес-ключ
// шага САГИ, вызываемого по gRPC (а не событием из Kafka), и смешивать два
// разных понятия "уже сделано" в одной таблице значило бы, что откат чужой
// саги (Forget) мог бы случайно отменить и вот эту отметку. Поэтому здесь —
// отдельный, прямой приём: сначала SELECT по order_id (быстрый путь без
// повторной работы на повторе саги), а если не нашли — INSERT с
// ON CONFLICT DO NOTHING на order_id, который закрывает гонку между
// "проверили и не нашли" и "вставили" (два одновременных вызова с одним
// order_id — повтор саги, случившийся в двух горутинах, или ретрай на фоне
// живого запроса).
func (r *LicenseRepository) Grant(ctx context.Context, listingID string, buyerID int64, orderID string) (*domain.License, error) {
	var existing LicenseRow
	err := r.db.WithContext(ctx).Where("order_id = ?", orderID).First(&existing).Error
	if err == nil {
		return licenseFromRow(&existing), nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("license: проверка order_id: %w", err)
	}

	row := &LicenseRow{
		ID:        newLicenseID(),
		ListingID: listingID,
		BuyerID:   buyerID,
		OrderID:   orderID,
		Status:    domain.LicenseGranted,
		GrantedAt: time.Now(),
	}
	result := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "order_id"}},
		DoNothing: true,
	}).Create(row)
	if result.Error != nil {
		return nil, fmt.Errorf("license: выдача: %w", result.Error)
	}
	if result.RowsAffected > 0 {
		return licenseFromRow(row), nil
	}

	// RowsAffected == 0: кто-то обогнал нас между SELECT и INSERT. Читаем
	// то, что записал победитель гонки, — именно ЭТО и обязан вернуть
	// идемпотентный Grant, а не ошибку.
	var winner LicenseRow
	if err := r.db.WithContext(ctx).Where("order_id = ?", orderID).First(&winner).Error; err != nil {
		return nil, fmt.Errorf("license: чтение после гонки: %w", err)
	}
	return licenseFromRow(&winner), nil
}

// Revoke отзывает лицензию по order_id.
//
// UPDATE ... WHERE status = granted — один запрос закрывает ОБА идемпотентных
// случая сразу, без отдельного SELECT: лицензии с таким order_id не было
// (WHERE ничего не находит) и лицензия уже отозвана раньше (WHERE тоже не
// находит, потому что status уже не granted) неотличимы по внешнему эффекту
// — и не должны отличаться: RowsAffected == 0 в обоих означает "отзывать
// нечего", revoked=false, без ошибки.
func (r *LicenseRepository) Revoke(ctx context.Context, orderID, reason string) (bool, error) {
	now := time.Now()
	result := r.db.WithContext(ctx).Model(&LicenseRow{}).
		Where("order_id = ? AND status = ?", orderID, domain.LicenseGranted).
		Updates(map[string]any{
			"status":     domain.LicenseRevoked,
			"reason":     reason,
			"revoked_at": now,
		})
	if result.Error != nil {
		return false, fmt.Errorf("license: отзыв: %w", result.Error)
	}
	return result.RowsAffected > 0, nil
}

func newLicenseID() string {
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	return uuid.NewString()
}

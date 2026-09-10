// Package migrations — схема media-service. Применяется к КАЖДОМУ шарду.
package migrations

import (
	"fmt"
	"time"

	"gorm.io/gorm"

	"photostock/services/media/migrations/models"
)

func Migrate(db *gorm.DB) error {
	// Обычные таблицы — GORM справляется сам.
	if err := db.AutoMigrate(&models.Image{}, &models.Thumbnail{}); err != nil {
		return fmt.Errorf("automigrate: %w", err)
	}

	// Outbox партиционирован по дню — AutoMigrate такое не умеет, raw SQL.
	// Ключ партиционирования (created_at) обязан входить в PK.
	if err := db.Exec(`
		CREATE TABLE IF NOT EXISTS outbox (
			id           BIGSERIAL,
			aggregate_id UUID        NOT NULL,
			topic        TEXT        NOT NULL,
			payload      BYTEA       NOT NULL,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
			published_at TIMESTAMPTZ,
			PRIMARY KEY (id, created_at)
		) PARTITION BY RANGE (created_at);

		CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
			ON outbox (created_at) WHERE published_at IS NULL;
	`).Error; err != nil {
		return fmt.Errorf("outbox: %w", err)
	}

	// Партиции на сегодня и неделю вперёд. Relay в media-service вызывает
	// EnsureDailyPartitions ежедневно, чтобы они не кончались.
	return EnsureDailyPartitions(db, time.Now(), 7)
}

// EnsureDailyPartitions создаёт партиции outbox от from на days дней вперёд.
// Идемпотентна: IF NOT EXISTS.
func EnsureDailyPartitions(db *gorm.DB, from time.Time, days int) error {
	day := from.UTC().Truncate(24 * time.Hour)
	for i := 0; i < days; i++ {
		start, end := day.AddDate(0, 0, i), day.AddDate(0, 0, i+1)
		q := fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS outbox_%s PARTITION OF outbox FOR VALUES FROM ('%s') TO ('%s')`,
			start.Format("2006_01_02"), start.Format("2006-01-02"), end.Format("2006-01-02"),
		)
		if err := db.Exec(q).Error; err != nil {
			return err
		}
	}
	return nil
}

// Package migrations — схема, которой владеет thumbnail-worker.
//
// НЕ в internal/: сюда ходит tools/automigrate, а internal одного сервиса
// снаружи не импортируется (см. тот же приём и то же объяснение в
// services/media/migrations/auto.go).
//
// Мигрирует ОДИН шард. thumbnail-worker использует ТЕ ЖЕ шарды, что и media
// (conf.Media.ShardDSNs) — подробное обоснование этого решения см. в doc-
// комментарии internal/adapters/pg/repository.go. Отсюда следствие для
// схемы: таблицу photos эта функция НЕ трогает (ею владеет и мигрирует
// media), а мигрирует только то, чем thumbnail-worker реально владеет —
// processed_events (pkg/idempotency) и outbox (pkg/outbox), — на тех же
// физических базах.
package migrations

import (
	"fmt"

	"gorm.io/gorm"

	"gosplash/pkg/idempotency"
	"gosplash/pkg/outbox"
)

// Migrate приводит схему шарда к текущим моделям processed_events и outbox.
//
// AutoMigrate идемпотентен: повторный запуск ничего не сломает. Таблицу
// photos здесь намеренно нет — её создаёт и мигрирует media, а этот пакет
// не должен знать о ней больше, чем требуется для чтения/обновления
// нескольких колонок (см. internal/adapters/pg.PhotoRow).
func Migrate(db *gorm.DB) error {
	if err := db.AutoMigrate(&idempotency.ProcessedEvent{}); err != nil {
		return fmt.Errorf("processed_events: %w", err)
	}
	if err := outbox.Migrate(db, "thumbnail-worker"); err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	return nil
}

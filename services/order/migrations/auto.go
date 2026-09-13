// Package migrations — схема order-service. Как и в catalog
// (services/catalog/migrations/auto.go), пакет называется migrations,
// а не internal/migrations: единственный вызывающий, которому позволено
// его импортировать, — tools/automigrate, а из internal этого делать
// нельзя (см. комментарий там же).
//
// Здесь НЕТ таблицы saga_instances — см. docs/adr/0004-saga-srazu-na-temporal.md:
// состояние саги хранит Temporal, а не order-service.
package migrations

import (
	"fmt"

	"gorm.io/gorm"

	"gosplash/pkg/outbox"
	"gosplash/services/order/internal/adapters/pg"
)

// Migrate создаёт таблицы order-service: orders, idempotency_keys
// (ADR 0018) и очередь outbox (order.order.placed / order.order.paid).
func Migrate(db *gorm.DB) error {
	if err := db.AutoMigrate(
		&pg.OrderRow{},
		&pg.IdempotencyKeyRow{},
	); err != nil {
		return fmt.Errorf("схема order: %w", err)
	}
	if err := outbox.Migrate(db, "order"); err != nil {
		return err
	}
	return nil
}

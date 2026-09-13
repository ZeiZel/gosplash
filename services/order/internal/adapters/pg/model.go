// Package pg — хранилище order-service в PostgreSQL через GORM (как везде
// в проекте, docs/STYLE.md).
//
// Здесь сознательно НЕТ таблицы saga_instances: состояние САГИ (какой шаг
// сейчас выполняется, что уже нужно откатывать) хранит Temporal — см.
// docs/adr/0004-saga-srazu-na-temporal.md. Таблица orders хранит только
// то, что нужно для быстрого GetOrder и для витрины: высокоуровневый
// статус, а не пошаговый прогресс саги.
package pg

import "time"

// OrderRow — строка таблицы orders.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: в отличие от domain.Order, здесь нет поля AuthorID.
// Счёт автора (payee для CommitFunds) нужен ИСКЛЮЧИТЕЛЬНО саге, а сага
// получает его ОДИН РАЗ как вход PlaceOrderWorkflow и дальше хранит в
// истории воркфлоу Temporal — по тому же принципу, по которому в orders
// нет и reservation_id/ledger_entry_ids: это данные саги, а не заказа.
// Дублировать их здесь значило бы завести второй источник правды для того,
// что уже надёжно хранит Temporal.
type OrderRow struct {
	ID            string `gorm:"column:id;primaryKey"`
	BuyerID       int64  `gorm:"column:buyer_id;not null;index"`
	ListingID     string `gorm:"column:listing_id;not null;index"`
	PriceCents    int64  `gorm:"column:price_cents;not null"`
	Currency      string `gorm:"column:currency;not null"`
	Status        string `gorm:"column:status;not null;index"`
	FailureReason string `gorm:"column:failure_reason"`
	LicenseID     string `gorm:"column:license_id"`

	CreatedAt time.Time `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (OrderRow) TableName() string { return "orders" }

// IdempotencyKeyRow — fallback-таблица HTTP idempotency-key. См.
// internal/adapters/idempotency и docs/adr/0018-idempotency-http.md:
// Redis здесь быстрый путь, эта таблица — источник правды.
type IdempotencyKeyRow struct {
	Key      string `gorm:"column:key;primaryKey"`
	Status   string `gorm:"column:status;not null"`
	Response []byte `gorm:"column:response"`

	CreatedAt time.Time `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (IdempotencyKeyRow) TableName() string { return "idempotency_keys" }

// Значения IdempotencyKeyRow.Status. Те же два состояния, что во внутреннем
// envelope pkg/redisx (in_progress/done) — «свободно» здесь, как и там,
// это отсутствие строки, а не значение в ней.
const (
	IdempotencyStatusInProgress = "in_progress"
	IdempotencyStatusDone       = "done"
)

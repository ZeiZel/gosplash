// Package pg — реализация ports.WalletStore/ports.TxStore на GORM
// поверх PostgreSQL.
//
// Соответствие таблиц строкам ниже — то же самое, что «строка таблицы и
// доменная модель — разные типы» из docs/STYLE.md: сюда, и только сюда,
// попадают теги gorm, и только отсюда есть отображение в domain.* и обратно
// (rows.go). Сама денежная логика (проверки, идемпотентность, переходы
// статуса) в пакете НЕ живёт — она в internal/app/service.go, а здесь —
// механический перевод примитивов ports.TxStore в SQL.
package pg

import "time"

// AccountRow — строка таблицы accounts.
//
// ID — BIGINT, а не UUID: proto/gosplash/wallet/v1 объявляет account_id как
// int64 (см. GetBalanceRequest), и заводить дополнительный UUID поверх уже
// заданного контрактом числового id только ради «единообразия с остальными
// таблицами» было бы бессмысленным слоем непрямоты.
type AccountRow struct {
	ID        int64     `gorm:"primaryKey"`
	OwnerID   int64     `gorm:"column:owner_id;not null;index"`
	Currency  string    `gorm:"size:3;not null"`
	CreatedAt time.Time `gorm:"not null;default:now()"`
}

func (AccountRow) TableName() string { return "accounts" }

// LedgerEntryRow — строка таблицы ledger_entries. APPEND-ONLY: см.
// заголовок internal/domain/domain.go про то, почему UPDATE/DELETE по этой
// таблице запрещены на уровне логики (в PostgreSQL это дополнительно можно
// закрепить REVOKE UPDATE, DELETE — миграция делает это явно, см.
// migrations/auto.go).
type LedgerEntryRow struct {
	ID          string    `gorm:"type:uuid;primaryKey"`
	AccountID   int64     `gorm:"column:account_id;not null;index"`
	AmountCents int64     `gorm:"column:amount_cents;not null"`
	Direction   string    `gorm:"size:6;not null"`
	OperationID string    `gorm:"column:operation_id;type:uuid;not null;index"`
	Reason      string    `gorm:"size:64;not null"`
	ReferenceID string    `gorm:"column:reference_id;size:64;not null;index"`
	CreatedAt   time.Time `gorm:"not null;default:now();index"`
}

func (LedgerEntryRow) TableName() string { return "ledger_entries" }

// ReservationRow — строка таблицы reservations.
//
// IdempotencyKey уникален — это и есть механизм идемпотентности
// ReserveFunds (docs/STYLE.md: unique-констрейнт, а не «сначала SELECT,
// потом INSERT»).
type ReservationRow struct {
	ID                  string    `gorm:"type:uuid;primaryKey"`
	AccountID           int64     `gorm:"column:account_id;not null;index"`
	AmountCents         int64     `gorm:"column:amount_cents;not null"`
	Currency            string    `gorm:"size:3;not null"`
	Status              string    `gorm:"size:16;not null;index"`
	IdempotencyKey      string    `gorm:"column:idempotency_key;uniqueIndex;size:255;not null"`
	AvailableAfterCents int64     `gorm:"column:available_after_cents;not null"`
	CreatedAt           time.Time `gorm:"not null;default:now()"`
}

func (ReservationRow) TableName() string { return "reservations" }

// OperationRow — журнал уже выполненных CommitFunds/ReleaseFunds
// (ports.TxStore.FindOperation/RecordOperation).
//
// Отдельная таблица, а не переиспользование pkg/idempotency.ProcessedEvent:
// ProcessedEvent дедуплицирует Kafka-СОБЫТИЯ по event_id внутри обработки
// консьюмера, а здесь — идемпотентность шагов САГИ, вызываемых по gRPC, с
// идемпотентным ключом, заданным ВЫЗЫВАЮЩИМ (order_id), а не событием
// брокера. Смешивать два разных понятия "уже сделано" в одной таблице
// значило бы, что pkg/idempotency.Forget (предназначенный для переигровки
// сломанной обработки Kafka-события) мог бы случайно стереть отметку о
// реальной денежной операции — ровно тот приём, которым в
// services/catalog/internal/adapters/pg/license_repository.go объясняется
// точно такое же решение для лицензий.
type OperationRow struct {
	IdempotencyKey string    `gorm:"column:idempotency_key;primaryKey;size:255"`
	Kind           string    `gorm:"size:16;primaryKey"`
	ReservationID  string    `gorm:"column:reservation_id;not null;index"`
	ResultJSON     []byte    `gorm:"column:result_json;type:jsonb;not null"`
	CreatedAt      time.Time `gorm:"not null;default:now()"`
}

func (OperationRow) TableName() string { return "wallet_operations" }

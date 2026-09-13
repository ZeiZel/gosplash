package pg

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"gosplash/pkg/kafkax"
	"gosplash/pkg/outbox"
	"gosplash/services/wallet/internal/domain"
	"gosplash/services/wallet/internal/ports"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// serviceName — владелец очереди outbox (outbox_wallet) и заголовок
// producer у wallet.account.debited.
const serviceName = "wallet"

// Store — ports.WalletStore на GORM.
type Store struct {
	db *gorm.DB
}

func NewStore(db *gorm.DB) *Store {
	return &Store{db: db}
}

func (s *Store) WithinTx(ctx context.Context, fn func(ports.TxStore) error) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(&txStore{tx: tx})
	})
}

// GetBalance читает баланс БЕЗ транзакции и без блокировок — см.
// обоснование в internal/ports/ports.go у WalletStore.GetBalance.
func (s *Store) GetBalance(ctx context.Context, accountID int64) (domain.Balance, error) {
	var account AccountRow
	err := s.db.WithContext(ctx).Where("id = ?", accountID).First(&account).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.Balance{}, domain.ErrAccountNotFound
	}
	if err != nil {
		return domain.Balance{}, fmt.Errorf("wallet: чтение счёта: %w", err)
	}

	balance, err := ledgerBalance(s.db.WithContext(ctx), accountID)
	if err != nil {
		return domain.Balance{}, err
	}
	reserved, err := activeReservedSum(s.db.WithContext(ctx), accountID)
	if err != nil {
		return domain.Balance{}, err
	}

	return domain.Balance{
		AccountID: accountID,
		Currency:  account.Currency,
		Available: balance - reserved,
		Reserved:  reserved,
	}, nil
}

// txStore — ports.TxStore на одной транзакции GORM.
type txStore struct {
	tx *gorm.DB
}

func (t *txStore) LockAccount(ctx context.Context, accountID int64) (*domain.Account, error) {
	var row AccountRow
	err := t.tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ?", accountID).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrAccountNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("wallet: блокировка счёта: %w", err)
	}
	return &domain.Account{ID: row.ID, OwnerID: row.OwnerID, Currency: row.Currency, CreatedAt: row.CreatedAt}, nil
}

// ledgerBalance — сумма проводок счёта: кредит минус дебет.
//
// NB: НЕ материализованный агрегат — SUM по индексу account_id на каждый
// вызов. Это осознанный выбор фазы 3, не оптимизация "потом забыли": цена и
// альтернативы разобраны в docs/adr/0014-*, раздел «Производительность».
func ledgerBalance(db *gorm.DB, accountID int64) (int64, error) {
	var sum int64
	err := db.Raw(`
		SELECT COALESCE(SUM(CASE WHEN direction = ? THEN amount_cents ELSE -amount_cents END), 0)
		FROM ledger_entries WHERE account_id = ?`,
		domain.DirectionCredit, accountID,
	).Scan(&sum).Error
	if err != nil {
		return 0, fmt.Errorf("wallet: сумма проводок: %w", err)
	}
	return sum, nil
}

func activeReservedSum(db *gorm.DB, accountID int64) (int64, error) {
	var sum int64
	err := db.Raw(`
		SELECT COALESCE(SUM(amount_cents), 0)
		FROM reservations WHERE account_id = ? AND status = ?`,
		accountID, domain.ReservationActive,
	).Scan(&sum).Error
	if err != nil {
		return 0, fmt.Errorf("wallet: сумма активных резервов: %w", err)
	}
	return sum, nil
}

func (t *txStore) LedgerBalance(ctx context.Context, accountID int64) (int64, error) {
	return ledgerBalance(t.tx.WithContext(ctx), accountID)
}

func (t *txStore) ActiveReservedSum(ctx context.Context, accountID int64) (int64, error) {
	return activeReservedSum(t.tx.WithContext(ctx), accountID)
}

func reservationFromRow(r *ReservationRow) *domain.Reservation {
	return &domain.Reservation{
		ID:                  r.ID,
		AccountID:           r.AccountID,
		AmountCents:         r.AmountCents,
		Currency:            r.Currency,
		Status:              r.Status,
		IdempotencyKey:      r.IdempotencyKey,
		AvailableAfterCents: r.AvailableAfterCents,
		CreatedAt:           r.CreatedAt,
	}
}

func (t *txStore) FindReservationByIdempotencyKey(ctx context.Context, key string) (*domain.Reservation, error) {
	var row ReservationRow
	err := t.tx.WithContext(ctx).Where("idempotency_key = ?", key).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("wallet: поиск резерва по idempotency_key: %w", err)
	}
	return reservationFromRow(&row), nil
}

func (t *txStore) LockReservation(ctx context.Context, reservationID string) (*domain.Reservation, error) {
	var row ReservationRow
	err := t.tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ?", reservationID).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrReservationNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("wallet: блокировка резерва: %w", err)
	}
	return reservationFromRow(&row), nil
}

func (t *txStore) InsertReservation(ctx context.Context, r *domain.Reservation) (bool, error) {
	row := &ReservationRow{
		ID:                  r.ID,
		AccountID:           r.AccountID,
		AmountCents:         r.AmountCents,
		Currency:            r.Currency,
		Status:              r.Status,
		IdempotencyKey:      r.IdempotencyKey,
		AvailableAfterCents: r.AvailableAfterCents,
	}
	// ON CONFLICT DO NOTHING на idempotency_key — атомарная защита от гонки
	// вместо «сначала проверили, потом вставили» (docs/STYLE.md,
	// pkg/idempotency.Claim — тот же приём).
	result := t.tx.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "idempotency_key"}},
		DoNothing: true,
	}).Create(row)
	if result.Error != nil {
		return false, fmt.Errorf("wallet: вставка резерва: %w", result.Error)
	}
	return result.RowsAffected > 0, nil
}

func (t *txStore) SetReservationStatus(ctx context.Context, reservationID, status string) error {
	result := t.tx.WithContext(ctx).Model(&ReservationRow{}).
		Where("id = ?", reservationID).Update("status", status)
	if result.Error != nil {
		return fmt.Errorf("wallet: смена статуса резерва: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return domain.ErrReservationNotFound
	}
	return nil
}

func (t *txStore) InsertLedgerEntries(ctx context.Context, entries []*domain.LedgerEntry) error {
	rows := make([]*LedgerEntryRow, 0, len(entries))
	for _, e := range entries {
		rows = append(rows, &LedgerEntryRow{
			ID: e.ID, AccountID: e.AccountID, AmountCents: e.AmountCents,
			Direction: e.Direction, OperationID: e.OperationID,
			Reason: e.Reason, ReferenceID: e.ReferenceID,
		})
	}
	// Вставка одним запросом (Create со срезом) — обе проводки операции
	// попадают в таблицу одним оператором INSERT, без промежуточного
	// состояния "дебет уже есть, кредита ещё нет" даже внутри самой БД.
	if err := t.tx.WithContext(ctx).Create(&rows).Error; err != nil {
		return fmt.Errorf("wallet: запись проводок: %w", err)
	}
	return nil
}

func (t *txStore) LedgerEntryIDsByReference(ctx context.Context, referenceID string) ([]string, error) {
	var ids []string
	err := t.tx.WithContext(ctx).Model(&LedgerEntryRow{}).
		Where("reference_id = ?", referenceID).Order("id").Pluck("id", &ids).Error
	if err != nil {
		return nil, fmt.Errorf("wallet: чтение проводок по reference_id: %w", err)
	}
	return ids, nil
}

func (t *txStore) FindOperation(ctx context.Context, key, kind string) ([]byte, bool, error) {
	var row OperationRow
	err := t.tx.WithContext(ctx).Where("idempotency_key = ? AND kind = ?", key, kind).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("wallet: поиск выполненной операции: %w", err)
	}
	return row.ResultJSON, true, nil
}

func (t *txStore) RecordOperation(ctx context.Context, key, kind, reservationID string, resultJSON []byte) (bool, error) {
	result := t.tx.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "idempotency_key"}, {Name: "kind"}},
		DoNothing: true,
	}).Create(&OperationRow{
		IdempotencyKey: key,
		Kind:           kind,
		ReservationID:  reservationID,
		ResultJSON:     resultJSON,
	})
	if result.Error != nil {
		return false, fmt.Errorf("wallet: запись выполненной операции: %w", result.Error)
	}
	return result.RowsAffected > 0, nil
}

func (t *txStore) WriteAccountDebited(ctx context.Context, event domain.AccountDebitedEvent) error {
	env, err := kafkax.NewEnvelope(kafkax.EventAccountDebited, strconv.FormatInt(event.AccountID, 10), &eventsv1.AccountDebited{
		AccountId:   event.AccountID,
		AmountCents: event.AmountCents,
		Currency:    event.Currency,
		Reason:      event.Reason,
		ReferenceId: event.ReferenceID,
	})
	if err != nil {
		return fmt.Errorf("wallet: сборка события account.debited: %w", err)
	}
	// PATTERN: transactional outbox (pkg/outbox, docs/adr/0008-*) — запись
	// в outbox_wallet В ТОЙ ЖЕ транзакции, что и проводки CommitFunds.
	if err := outbox.Write(ctx, t.tx, serviceName, env, kafkax.TopicAccountDebited); err != nil {
		return err
	}
	return nil
}

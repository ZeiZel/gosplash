package pg

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"gosplash/services/wallet/internal/domain"
)

// newTestStore — Store над sqlite в памяти: SQL GetBalance сознательно
// написан без PG-специфики (скалярные подзапросы и ?-плейсхолдеры одинаково
// работают в обеих БД), поэтому форму запроса можно проверять без
// контейнера с Postgres.
//
// Таблицы создаются ГОТОВЫМ DDL, а не AutoMigrate по строкам из rows.go:
// теги `default:now()` превращаются в DEFAULT now(), которого в sqlite нет,
// а тесту достаточно тех колонок, которых касается сам SQL.
//
// Сборка без CGO (CGO_ENABLED=0) получает от mattn/go-sqlite3 заглушку
// вместо драйвера — в таком окружении тест пропускается: он не про
// окружение, а про форму SQL, и запускается там, где sqlite доступен.
func newTestStore(t *testing.T) *Store {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil && strings.Contains(err.Error(), "requires cgo") {
		t.Skip("sqlite недоступен: сборка без CGO (mattn/go-sqlite3 требует cgo)")
	}
	require.NoError(t, err, "sqlite в памяти обязана открыться без ошибок")

	require.NoError(t, db.Exec(`
		CREATE TABLE accounts (
			id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL,
			currency TEXT NOT NULL, created_at DATETIME);
		CREATE TABLE ledger_entries (
			id TEXT PRIMARY KEY, account_id INTEGER NOT NULL,
			amount_cents INTEGER NOT NULL, direction TEXT NOT NULL,
			operation_id TEXT NOT NULL, reason TEXT NOT NULL,
			reference_id TEXT NOT NULL, created_at DATETIME);
		CREATE TABLE reservations (
			id TEXT PRIMARY KEY, account_id INTEGER NOT NULL,
			amount_cents INTEGER NOT NULL, currency TEXT NOT NULL,
			status TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE,
			available_after_cents INTEGER NOT NULL, created_at DATETIME);
	`).Error)

	return NewStore(db)
}

// seedBalance заносит счёту приход/расход ПАРНЫМИ проводками от счёта 0
// (условный внешний источник, тот же приём, что в тестах app) и добавляет
// резервы с указанным статусом.
func seedBalance(t *testing.T, s *Store, accountID int64, credit, debit int64, reservations map[string]int64) {
	t.Helper()

	insertPair := func(amount int64, direction string) {
		require.NoError(t, s.db.Exec(
			`INSERT INTO ledger_entries (id, account_id, amount_cents, direction, operation_id, reason, reference_id)
			 VALUES (?, 0, ?, ?, 'seed-op', 'seed', 'seed'), (?, ?, ?, ?, 'seed-op', 'seed', 'seed')`,
			"seed-"+direction+"-0", amount, opposite(direction), "seed-"+direction+"-1", accountID, amount, direction,
		).Error)
	}
	if credit > 0 {
		insertPair(credit, domain.DirectionCredit)
	}
	if debit > 0 {
		insertPair(debit, domain.DirectionDebit)
	}
	for status, amount := range reservations {
		require.NoError(t, s.db.Exec(
			`INSERT INTO reservations (id, account_id, amount_cents, currency, status, idempotency_key, available_after_cents)
			 VALUES (?, ?, ?, 'RUB', ?, ?, 0)`,
			"res-"+status, accountID, amount, status, "key-"+status,
		).Error)
	}
}

func opposite(direction string) string {
	if direction == domain.DirectionCredit {
		return domain.DirectionDebit
	}
	return domain.DirectionCredit
}

// TestGetBalance_VseChislaIzOdnogoOperatora — валюта, баланс и резервы
// согласованы между собой: available = balance − reserved обязано сходиться
// ровно на тех данных, что видны ОДНОМУ оператору (см. комментарий у
// Store.GetBalance про то, почему запрос один, а не три).
func TestGetBalance_VseChislaIzOdnogoOperatora(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.db.Exec(
		`INSERT INTO accounts (id, owner_id, currency) VALUES (1, 100, 'RUB')`).Error)
	seedBalance(t, store, 1, 10_000, 1_000, map[string]int64{
		domain.ReservationActive: 4_000,
	})

	balance, err := store.GetBalance(context.Background(), 1)

	require.NoError(t, err)
	assert.Equal(t, int64(1), balance.AccountID)
	assert.Equal(t, "RUB", balance.Currency)
	assert.Equal(t, int64(9_000), balance.Available, "available = 10000 приход − 1000 расход − 4000 активный резерв")
	assert.Equal(t, int64(4_000), balance.Reserved)
}

// TestGetBalance_SchitaetTolkoAktivnyeRezervy — released/committed резервы
// в available не удерживаются.
func TestGetBalance_SchitaetTolkoAktivnyeRezervy(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.db.Exec(
		`INSERT INTO accounts (id, owner_id, currency) VALUES (1, 100, 'RUB')`).Error)
	seedBalance(t, store, 1, 10_000, 0, map[string]int64{
		domain.ReservationActive:   2_000,
		domain.ReservationReleased: 3_000,
	})

	balance, err := store.GetBalance(context.Background(), 1)

	require.NoError(t, err)
	assert.Equal(t, int64(2_000), balance.Reserved, "учитывается только активный резерв")
	assert.Equal(t, int64(8_000), balance.Available)
}

func TestGetBalance_NeizvestnyySchyot(t *testing.T) {
	store := newTestStore(t)

	_, err := store.GetBalance(context.Background(), 999)

	require.ErrorIs(t, err, domain.ErrAccountNotFound)
}

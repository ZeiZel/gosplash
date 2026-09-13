package app

import (
	"context"
	"sync"

	"gosplash/services/wallet/internal/domain"
	"gosplash/services/wallet/internal/ports"
)

// systemAccountID — счёт внешнего источника средств: тестам нужно
// "занести" деньги покупателю ПЕРЕД тем, как их резервировать, а
// единственный законный способ создать проводку в double-entry — это
// парная запись, а не односторонний плюс на баланс. Внешний счёт и есть
// вторая нога такой проводки (в реальном банке это был бы корреспондентский
// счёт эмитента), и он же превращает seedFunds в такую же операцию, как и
// любая другая: инвариант "сумма всех проводок равна нулю" остаётся верным
// ДАЖЕ включая тестовые данные, а не только "настоящие" операции.
const systemAccountID int64 = 0

// fakeStore — подделка ports.WalletStore/ports.TxStore для unit-тестов
// прикладного слоя: НИ ОДНОГО контейнера, НИ ОДНОЙ настоящей БД, но с тем
// же контрактом транзакции — WithinTx откатывает ВСЕ изменения, если fn
// вернул ошибку, ровно как настоящий db.Transaction(...).
//
// mu — ОДНА блокировка на всё время работы fn внутри WithinTx: это сильнее,
// чем построчные FOR UPDATE настоящей реализации (adapters/pg), зато
// достаточно, чтобы проверить ПРАВИЛА (идемпотентность, переходы статуса,
// парность проводок), а не поведение под конкурентной нагрузкой реальной
// БД — для этого в проекте есть другой инструмент (docs/STYLE.md:
// интеграционные тесты с testcontainers), и здесь его сознательно нет
// (см. docs/adr/0014-*, «чего не делаем»).
type fakeStore struct {
	mu sync.Mutex

	accounts         map[int64]*domain.Account
	ledger           []*domain.LedgerEntry
	reservations     map[string]*domain.Reservation
	reservationByKey map[string]string
	operations       map[string]*fakeOperation
	debitedEvents    []domain.AccountDebitedEvent
}

type fakeOperation struct {
	reservationID string
	resultJSON    []byte
}

func newFakeStore(accounts ...*domain.Account) *fakeStore {
	m := make(map[int64]*domain.Account, len(accounts))
	for _, a := range accounts {
		m[a.ID] = a
	}
	return &fakeStore{
		accounts:         m,
		reservations:     make(map[string]*domain.Reservation),
		reservationByKey: make(map[string]string),
		operations:       make(map[string]*fakeOperation),
	}
}

// seedFunds заносит деньги на счёт ПАРНОЙ проводкой от systemAccountID —
// см. комментарий у константы выше.
func (f *fakeStore) seedFunds(accountID, amountCents int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	opID := newID()
	f.ledger = append(f.ledger,
		&domain.LedgerEntry{ID: newID(), AccountID: systemAccountID, AmountCents: amountCents, Direction: domain.DirectionDebit, OperationID: opID, Reason: "seed", ReferenceID: "seed"},
		&domain.LedgerEntry{ID: newID(), AccountID: accountID, AmountCents: amountCents, Direction: domain.DirectionCredit, OperationID: opID, Reason: "seed", ReferenceID: "seed"},
	)
}

// ledgerSum — сумма amount_cents по ВСЕМ проводкам ВСЕХ счетов, с учётом
// знака направления. Ровно то, что в mk/wallet.mk (wallet-check) проверяет
// один SQL-запрос по настоящей таблице; здесь — то же самое по срезу.
func (f *fakeStore) ledgerSum() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var sum int64
	for _, e := range f.ledger {
		if e.Direction == domain.DirectionCredit {
			sum += e.AmountCents
		} else {
			sum -= e.AmountCents
		}
	}
	return sum
}

func (f *fakeStore) ledgerEntryCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ledger)
}

func (f *fakeStore) reservationCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reservations)
}

func (f *fakeStore) debitedEventCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.debitedEvents)
}

// ─────────────────────────────────────────────────────────────────────────────
// ports.WalletStore
// ─────────────────────────────────────────────────────────────────────────────

type fakeSnapshot struct {
	ledgerLen        int
	debitedLen       int
	reservations     map[string]*domain.Reservation
	reservationByKey map[string]string
	operations       map[string]*fakeOperation
}

func (f *fakeStore) snapshot() fakeSnapshot {
	reservations := make(map[string]*domain.Reservation, len(f.reservations))
	for k, v := range f.reservations {
		reservations[k] = v
	}
	byKey := make(map[string]string, len(f.reservationByKey))
	for k, v := range f.reservationByKey {
		byKey[k] = v
	}
	ops := make(map[string]*fakeOperation, len(f.operations))
	for k, v := range f.operations {
		ops[k] = v
	}
	return fakeSnapshot{
		ledgerLen:        len(f.ledger),
		debitedLen:       len(f.debitedEvents),
		reservations:     reservations,
		reservationByKey: byKey,
		operations:       ops,
	}
}

// restore — откат. Ledger и debitedEvents — append-only срезы: откат просто
// обрезает их до длины на момент начала транзакции (элементы после этого
// никогда не мутируются на месте, только добавляются — см. методы ниже).
// Карты (reservations, operations) — тем же приёмом, каким SetReservation
// Status и RecordOperation НИКОГДА не мутируют существующий *domain.Reservation
// на месте, а заменяют указатель в карте: старый указатель, сохранённый
// в снимке, остаётся валидным и неизменным.
func (f *fakeStore) restore(s fakeSnapshot) {
	f.ledger = f.ledger[:s.ledgerLen]
	f.debitedEvents = f.debitedEvents[:s.debitedLen]
	f.reservations = s.reservations
	f.reservationByKey = s.reservationByKey
	f.operations = s.operations
}

func (f *fakeStore) WithinTx(_ context.Context, fn func(ports.TxStore) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	snap := f.snapshot()
	if err := fn(&fakeTx{store: f}); err != nil {
		f.restore(snap)
		return err
	}
	return nil
}

func (f *fakeStore) GetBalance(_ context.Context, accountID int64) (domain.Balance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	account, ok := f.accounts[accountID]
	if !ok {
		return domain.Balance{}, domain.ErrAccountNotFound
	}

	var balance, reserved int64
	for _, e := range f.ledger {
		if e.AccountID != accountID {
			continue
		}
		if e.Direction == domain.DirectionCredit {
			balance += e.AmountCents
		} else {
			balance -= e.AmountCents
		}
	}
	for _, r := range f.reservations {
		if r.AccountID == accountID && r.Status == domain.ReservationActive {
			reserved += r.AmountCents
		}
	}

	return domain.Balance{
		AccountID: accountID,
		Currency:  account.Currency,
		Available: balance - reserved,
		Reserved:  reserved,
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// ports.TxStore
// ─────────────────────────────────────────────────────────────────────────────

// fakeTx работает БЕЗ собственной блокировки: она уже держится снаружи,
// в WithinTx, на всё время работы fn — как и должно быть внутри одной
// транзакции.
type fakeTx struct {
	store *fakeStore
}

func (t *fakeTx) LockAccount(_ context.Context, accountID int64) (*domain.Account, error) {
	a, ok := t.store.accounts[accountID]
	if !ok {
		return nil, domain.ErrAccountNotFound
	}
	cp := *a
	return &cp, nil
}

func (t *fakeTx) LedgerBalance(_ context.Context, accountID int64) (int64, error) {
	var sum int64
	for _, e := range t.store.ledger {
		if e.AccountID != accountID {
			continue
		}
		if e.Direction == domain.DirectionCredit {
			sum += e.AmountCents
		} else {
			sum -= e.AmountCents
		}
	}
	return sum, nil
}

func (t *fakeTx) ActiveReservedSum(_ context.Context, accountID int64) (int64, error) {
	var sum int64
	for _, r := range t.store.reservations {
		if r.AccountID == accountID && r.Status == domain.ReservationActive {
			sum += r.AmountCents
		}
	}
	return sum, nil
}

func (t *fakeTx) FindReservationByIdempotencyKey(_ context.Context, key string) (*domain.Reservation, error) {
	id, ok := t.store.reservationByKey[key]
	if !ok {
		return nil, nil
	}
	cp := *t.store.reservations[id]
	return &cp, nil
}

func (t *fakeTx) LockReservation(_ context.Context, reservationID string) (*domain.Reservation, error) {
	r, ok := t.store.reservations[reservationID]
	if !ok {
		return nil, domain.ErrReservationNotFound
	}
	cp := *r
	return &cp, nil
}

func (t *fakeTx) InsertReservation(_ context.Context, r *domain.Reservation) (bool, error) {
	// ON CONFLICT DO NOTHING на idempotency_key — тот же атомарный приём,
	// что и в adapters/pg (см. комментарий в internal/ports/ports.go).
	if _, exists := t.store.reservationByKey[r.IdempotencyKey]; exists {
		return false, nil
	}
	cp := *r
	t.store.reservations[r.ID] = &cp
	t.store.reservationByKey[r.IdempotencyKey] = r.ID
	return true, nil
}

func (t *fakeTx) SetReservationStatus(_ context.Context, reservationID, status string) error {
	r, ok := t.store.reservations[reservationID]
	if !ok {
		return domain.ErrReservationNotFound
	}
	cp := *r
	cp.Status = status
	t.store.reservations[reservationID] = &cp
	return nil
}

func (t *fakeTx) InsertLedgerEntries(_ context.Context, entries []*domain.LedgerEntry) error {
	for _, e := range entries {
		cp := *e
		t.store.ledger = append(t.store.ledger, &cp)
	}
	return nil
}

func (t *fakeTx) LedgerEntryIDsByReference(_ context.Context, referenceID string) ([]string, error) {
	var ids []string
	for _, e := range t.store.ledger {
		if e.ReferenceID == referenceID {
			ids = append(ids, e.ID)
		}
	}
	return ids, nil
}

func operationKey(key, kind string) string { return kind + "|" + key }

func (t *fakeTx) FindOperation(_ context.Context, key, kind string) ([]byte, bool, error) {
	op, ok := t.store.operations[operationKey(key, kind)]
	if !ok {
		return nil, false, nil
	}
	return op.resultJSON, true, nil
}

func (t *fakeTx) RecordOperation(_ context.Context, key, kind, reservationID string, resultJSON []byte) (bool, error) {
	k := operationKey(key, kind)
	if _, exists := t.store.operations[k]; exists {
		return false, nil
	}
	t.store.operations[k] = &fakeOperation{reservationID: reservationID, resultJSON: resultJSON}
	return true, nil
}

func (t *fakeTx) WriteAccountDebited(_ context.Context, event domain.AccountDebitedEvent) error {
	t.store.debitedEvents = append(t.store.debitedEvents, event)
	return nil
}

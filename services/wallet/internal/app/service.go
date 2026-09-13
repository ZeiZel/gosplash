// Package app — прикладной слой wallet: четыре сценария контракта
// wallet.v1 (GetBalance, ReserveFunds, CommitFunds, ReleaseFunds).
//
// Вся денежная логика — проверки, переходы статуса резерва, идемпотентность,
// парность проводок — живёт ЗДЕСЬ, а не в internal/adapters/pg (см.
// обоснование в internal/ports/ports.go). internal/app импортирует только
// domain и ports — ни gorm, ни gRPC, ни protobuf здесь не появляется, и
// именно поэтому файл service_test.go проверяет ВСЕ требуемые свойства
// (идемпотентность, инвариант двойной записи, отказ при нехватке средств,
// поведение Commit/Release друг после друга, отказ на разных валютах) без
// единого контейнера — подделкой ports.WalletStore.
package app

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"gosplash/services/wallet/internal/domain"
	"gosplash/services/wallet/internal/ports"
)

// Service — реализация сценариев wallet.v1 поверх ports.WalletStore.
type Service struct {
	store ports.WalletStore
}

func NewService(store ports.WalletStore) *Service {
	return &Service{store: store}
}

// commitResult / releaseResult — то, что кладётся в JSON-журнал операций
// (ports.TxStore.RecordOperation) и достаётся обратно при повторе с тем же
// idempotency_key. Отдельные типы, а не переиспользование ответа gRPC:
// internal/app не имеет права знать про protobuf (см. заголовок пакета
// и docs/STYLE.md), а хранить нужно ровно тот минимум данных, которого
// достаточно, чтобы собрать ответ заново.
type commitResult struct {
	LedgerEntryIDs []string `json:"ledger_entry_ids"`
}

type releaseResult struct {
	AvailableAfterCents int64  `json:"available_after_cents"`
	Currency            string `json:"currency"`
}

func newID() string {
	// uuid.NewV7, а не v4: первые 48 бит — unix-время, вставка в PRIMARY
	// KEY (reservations.id, ledger_entries.id) идёт в «правый край»
	// B-tree, как и везде в проекте (см. pkg/outbox/model.go, тот же
	// довод). Если по какой-то причине V7 недоступен — тот же безопасный
	// фолбэк, что и в остальном коде.
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	return uuid.NewString()
}

// ─────────────────────────────────────────────────────────────────────────────
// GetBalance
// ─────────────────────────────────────────────────────────────────────────────

func (s *Service) GetBalance(ctx context.Context, accountID int64) (domain.Balance, error) {
	return s.store.GetBalance(ctx, accountID)
}

// ─────────────────────────────────────────────────────────────────────────────
// ReserveFunds
// ─────────────────────────────────────────────────────────────────────────────

// Reserve замораживает amount на счёте accountID. Идемпотентен по key:
// повтор с тем же ключом возвращает ТОТ ЖЕ резерв и ТОТ ЖЕ available_after,
// не создавая второй резерв и не трогая баланс повторно.
func (s *Service) Reserve(ctx context.Context, accountID int64, amount domain.Money, key string) (*domain.Reservation, int64, error) {
	if key == "" {
		return nil, 0, domain.ErrEmptyIdempotencyKey
	}
	if amount.AmountCents <= 0 {
		return nil, 0, domain.ErrInvalidAmount
	}

	var reservation *domain.Reservation
	var availableAfter int64

	err := s.store.WithinTx(ctx, func(tx ports.TxStore) error {
		// Идемпотентность: быстрый путь. Если резерв с этим ключом уже
		// существует (созданный этим же вызовом раньше, либо конкурентным
		// повтором, который нас обогнал), возвращаем СОХРАНЁННЫЙ снимок
		// available_after, а не пересчитываем его заново — к моменту
		// повтора баланс мог измениться другими операциями, а правило
		// идемпотентности требует вернуть тот же ответ, что и в первый раз.
		if existing, err := tx.FindReservationByIdempotencyKey(ctx, key); err != nil {
			return err
		} else if existing != nil {
			reservation = existing
			availableAfter = existing.AvailableAfterCents
			return nil
		}

		account, err := tx.LockAccount(ctx, accountID)
		if err != nil {
			return err
		}
		if account.Currency != amount.Currency {
			return domain.ErrCurrencyMismatch
		}

		balance, err := tx.LedgerBalance(ctx, accountID)
		if err != nil {
			return err
		}
		reserved, err := tx.ActiveReservedSum(ctx, accountID)
		if err != nil {
			return err
		}
		available := balance - reserved
		if available < amount.AmountCents {
			// Резерв НЕ создаётся, баланс не меняется — отказ дешёвый
			// и не оставляет следов в истории (в отличие от «создать
			// резерв и сразу его release», что раздуло бы таблицу резервов
			// отказами, которые и так видны по коду ошибки вызывающему).
			return domain.ErrInsufficientFunds
		}

		row := &domain.Reservation{
			ID:                  newID(),
			AccountID:           accountID,
			AmountCents:         amount.AmountCents,
			Currency:            amount.Currency,
			Status:              domain.ReservationActive,
			IdempotencyKey:      key,
			AvailableAfterCents: available - amount.AmountCents,
		}

		created, err := tx.InsertReservation(ctx, row)
		if err != nil {
			return err
		}
		if !created {
			// Гонка: конкурентный вызов с тем же idempotency_key вставил
			// строку между нашим FindReservationByIdempotencyKey и
			// InsertReservation (в реальной БД FOR UPDATE на account уже
			// исключает это для ОДНОГО account_id — см. LockAccount выше —
			// но ключ идемпотентности в принципе не привязан к account_id
			// схемой, поэтому проверяем и здесь, а не полагаемся только
			// на блокировку). Побеждает тот, кто вставил первым.
			winner, err := tx.FindReservationByIdempotencyKey(ctx, key)
			if err != nil {
				return err
			}
			if winner == nil {
				return fmt.Errorf("wallet: резерв не создан, но конфликт вставки сообщён")
			}
			reservation = winner
			availableAfter = winner.AvailableAfterCents
			return nil
		}

		reservation = row
		availableAfter = row.AvailableAfterCents
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return reservation, availableAfter, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// CommitFunds
// ─────────────────────────────────────────────────────────────────────────────

// Commit подтверждает резерв: списывает у покупателя (счёт резерва),
// зачисляет автору (payeeAccountID), парой проводок в одной транзакции.
func (s *Service) Commit(ctx context.Context, reservationID string, payeeAccountID int64, key string) ([]string, error) {
	if key == "" {
		return nil, domain.ErrEmptyIdempotencyKey
	}

	var ledgerEntryIDs []string

	err := s.store.WithinTx(ctx, func(tx ports.TxStore) error {
		if raw, found, err := tx.FindOperation(ctx, key, ports.OperationCommit); err != nil {
			return err
		} else if found {
			var res commitResult
			if err := json.Unmarshal(raw, &res); err != nil {
				return fmt.Errorf("wallet: разбор сохранённого результата commit: %w", err)
			}
			ledgerEntryIDs = res.LedgerEntryIDs
			return nil
		}

		reservation, err := tx.LockReservation(ctx, reservationID)
		if err != nil {
			return err
		}

		switch reservation.Status {
		case domain.ReservationCommitted:
			// Резерв уже подтверждён — но НЕ этим idempotency_key (иначе
			// сработал бы FindOperation выше). Такое возможно, если сага
			// ретраит Commit с новым ключом (например, потеряла старый
			// после падения). Возвращаем УЖЕ созданные проводки, а не
			// создаём вторую пару — иначе баланс задвоился бы, а «сумма
			// проводок равна нулю» перестала бы означать «всё сошлось»:
			// она держится только пока каждая ПАРА создаётся РОВНО один раз.
			ids, err := tx.LedgerEntryIDsByReference(ctx, reservationID)
			if err != nil {
				return err
			}
			if _, err := recordJSON(ctx, tx, key, ports.OperationCommit, reservationID, commitResult{LedgerEntryIDs: ids}); err != nil {
				return err
			}
			ledgerEntryIDs = ids
			return nil
		case domain.ReservationReleased:
			// Резерв уже освобождён компенсацией — подтверждать нечего:
			// деньги, которые он держал, уже вернулись в available другим
			// путём. Это настоящая ошибка, а не штатный повтор: она
			// сигнализирует рассинхронизацию саги (Commit пришёл ПОСЛЕ
			// Release того же резерва), и молчаливый успех замаскировал бы
			// баг вызывающего кода вместо того, чтобы его показать.
			return domain.ErrReservationAlreadyReleased
		}

		// active — собственно коммит.
		buyerID := reservation.AccountID

		buyer, payee, err := lockAccountsSorted(ctx, tx, buyerID, payeeAccountID)
		if err != nil {
			return err
		}
		if buyer.Currency != reservation.Currency || payee.Currency != reservation.Currency {
			// Конвертация в проекте не делается: перевод между разными
			// валютами отвергается целиком, а не пересчитывается по
			// какому-либо курсу, который здесь неоткуда взять и
			// невозможно было бы обосновать на момент исполнения.
			return domain.ErrCurrencyMismatch
		}

		operationID := newID()
		debit := &domain.LedgerEntry{
			ID: newID(), AccountID: buyer.ID, AmountCents: reservation.AmountCents,
			Direction: domain.DirectionDebit, OperationID: operationID,
			Reason: "order_payment", ReferenceID: reservationID,
		}
		credit := &domain.LedgerEntry{
			ID: newID(), AccountID: payee.ID, AmountCents: reservation.AmountCents,
			Direction: domain.DirectionCredit, OperationID: operationID,
			Reason: "order_payment", ReferenceID: reservationID,
		}
		// Обе проводки — ОДНИМ вызовом, чтобы между ними не было
		// промежуточного состояния, в котором дебет уже виден, а кредита
		// ещё нет: см. заголовок пакета domain про инвариант «сумма равна
		// нулю» и цену append-only книги.
		if err := tx.InsertLedgerEntries(ctx, []*domain.LedgerEntry{debit, credit}); err != nil {
			return err
		}
		if err := tx.SetReservationStatus(ctx, reservationID, domain.ReservationCommitted); err != nil {
			return err
		}

		// PATTERN: transactional outbox — событие в той же транзакции,
		// что и проводки (pkg/outbox, docs/adr/0008-*).
		if err := tx.WriteAccountDebited(ctx, domain.AccountDebitedEvent{
			AccountID:   buyer.ID,
			AmountCents: reservation.AmountCents,
			Currency:    reservation.Currency,
			Reason:      "order_payment",
			ReferenceID: reservationID,
		}); err != nil {
			return err
		}

		ids := []string{debit.ID, credit.ID}
		if _, err := recordJSON(ctx, tx, key, ports.OperationCommit, reservationID, commitResult{LedgerEntryIDs: ids}); err != nil {
			return err
		}
		ledgerEntryIDs = ids
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ledgerEntryIDs, nil
}

// lockAccountsSorted блокирует ДВА счёта строго в порядке возрастания id.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: без фиксированного порядка два конкурентных Commit
// с обратными парами счетов (A→B и B→A) могут заблокировать по одному счёту
// каждый и встать в клинч навсегда (classic deadlock). Единый порядок
// (меньший id — первым) гарантирует, что ЛЮБЫЕ два конкурентных вызова
// берут блокировки в ОДНОМ и том же относительном порядке — тогда один
// из них всегда успевает захватить оба счёта первым, а второй просто ждёт.
func lockAccountsSorted(ctx context.Context, tx ports.TxStore, idA, idB int64) (a, b *domain.Account, err error) {
	first, second := idA, idB
	swapped := false
	if second < first {
		first, second = second, first
		swapped = true
	}

	lockedFirst, err := tx.LockAccount(ctx, first)
	if err != nil {
		return nil, nil, err
	}

	var lockedSecond *domain.Account
	if first == second {
		lockedSecond = lockedFirst
	} else {
		lockedSecond, err = tx.LockAccount(ctx, second)
		if err != nil {
			return nil, nil, err
		}
	}

	if swapped {
		return lockedSecond, lockedFirst, nil
	}
	return lockedFirst, lockedSecond, nil
}

func recordJSON(ctx context.Context, tx ports.TxStore, key, kind, reservationID string, v any) (bool, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return false, fmt.Errorf("wallet: сериализация результата операции: %w", err)
	}
	return tx.RecordOperation(ctx, key, kind, reservationID, payload)
}

// ─────────────────────────────────────────────────────────────────────────────
// ReleaseFunds
// ─────────────────────────────────────────────────────────────────────────────

// Release — КОМПЕНСАЦИЯ, а не откат. Она не «отменяет резерв, как будто
// его не было» — она отдельная операция, снимающая резерв, и обе (сам
// резерв и его освобождение) остаются в истории. Разница важна: откат
// (rollback) предполагает, что промежуточного состояния как бы не
// существовало, и годится только внутри ОДНОЙ незакоммиченной транзакции.
// Резерв же мог быть виден снаружи (например, в GetBalance) уже после
// своего создания — компенсация признаёт, что состояние БЫЛО, и явно
// говорит, что теперь оно отменено, с указанием причины (reason) и
// временем — то есть добавляет новый факт в историю, а не стирает старый.
func (s *Service) Release(ctx context.Context, reservationID, reason, key string) (domain.Money, error) {
	if key == "" {
		return domain.Money{}, domain.ErrEmptyIdempotencyKey
	}

	var result domain.Money

	err := s.store.WithinTx(ctx, func(tx ports.TxStore) error {
		if raw, found, err := tx.FindOperation(ctx, key, ports.OperationRelease); err != nil {
			return err
		} else if found {
			var res releaseResult
			if err := json.Unmarshal(raw, &res); err != nil {
				return fmt.Errorf("wallet: разбор сохранённого результата release: %w", err)
			}
			result = domain.Money{AmountCents: res.AvailableAfterCents, Currency: res.Currency}
			return nil
		}

		reservation, err := tx.LockReservation(ctx, reservationID)
		if err != nil {
			return err
		}

		if reservation.Status == domain.ReservationCommitted {
			// Резерв уже подтверждён: деньги списаны и зачислены реальными
			// проводками. «Освободить» их сейчас означало бы либо соврать
			// (ничего не изменится, но ответ скажет «доступно больше») либо
			// пришлось бы писать компенсирующую проводку — а это уже другая
			// операция (refund), с другим reason и другим инициатором, а не
			// побочный эффект вызова Release. Возвращаем ошибку: это сигнал
			// рассогласования саги, а не штатный повтор.
			return domain.ErrReservationAlreadyCommitted
		}

		if reservation.Status == domain.ReservationReleased {
			// Повторный Release уже освобождённого резерва — НЕ ошибка:
			// компенсация обязана быть безопасной при повторе (proto:
			// «повтор с тем же ключом возвращает тот же результат»), и это
			// верно даже если ключ идемпотентности другой — с точки зрения
			// денег «резерв уже снят» неотличимо от «мы только что его
			// сняли». Баланс этим вызовом не меняется, available
			// пересчитывается заново (это чтение, не запись).
			available, currency, err := currentAvailable(ctx, tx, reservation)
			if err != nil {
				return err
			}
			if _, err := recordJSON(ctx, tx, key, ports.OperationRelease, reservationID,
				releaseResult{AvailableAfterCents: available, Currency: currency}); err != nil {
				return err
			}
			result = domain.Money{AmountCents: available, Currency: currency}
			return nil
		}

		// active — освобождаем.
		if err := tx.SetReservationStatus(ctx, reservationID, domain.ReservationReleased); err != nil {
			return err
		}
		available, currency, err := currentAvailable(ctx, tx, reservation)
		if err != nil {
			return err
		}
		if _, err := recordJSON(ctx, tx, key, ports.OperationRelease, reservationID,
			releaseResult{AvailableAfterCents: available, Currency: currency}); err != nil {
			return err
		}
		result = domain.Money{AmountCents: available, Currency: currency}
		return nil
	})
	if err != nil {
		return domain.Money{}, err
	}
	return result, nil
}

func currentAvailable(ctx context.Context, tx ports.TxStore, r *domain.Reservation) (int64, string, error) {
	balance, err := tx.LedgerBalance(ctx, r.AccountID)
	if err != nil {
		return 0, "", err
	}
	reserved, err := tx.ActiveReservedSum(ctx, r.AccountID)
	if err != nil {
		return 0, "", err
	}
	return balance - reserved, r.Currency, nil
}

package app

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/wallet/internal/domain"
)

const (
	buyerID  = 1
	authorID = 2
	rub      = "RUB"
	usd      = "USD"
)

func newBuyerAuthor(t *testing.T, buyerBalance int64) (*fakeStore, *Service) {
	t.Helper()
	store := newFakeStore(
		&domain.Account{ID: buyerID, OwnerID: 100, Currency: rub},
		&domain.Account{ID: authorID, OwnerID: 200, Currency: rub},
	)
	store.seedFunds(buyerID, buyerBalance)
	return store, NewService(store)
}

// ─────────────────────────────────────────────────────────────────────────────
// Идемпотентность
// ─────────────────────────────────────────────────────────────────────────────

func TestReserve_PovtorSTemZheKlyuchomNichegoNeMenyaet(t *testing.T) {
	store, svc := newBuyerAuthor(t, 10_000)
	ctx := context.Background()

	r1, avail1, err := svc.Reserve(ctx, buyerID, domain.Money{AmountCents: 3_000, Currency: rub}, "order-1")
	require.NoError(t, err)

	r2, avail2, err := svc.Reserve(ctx, buyerID, domain.Money{AmountCents: 3_000, Currency: rub}, "order-1")
	require.NoError(t, err)

	assert.Equal(t, r1.ID, r2.ID, "повтор обязан вернуть ТОТ ЖЕ резерв")
	assert.Equal(t, avail1, avail2, "повтор обязан вернуть ТОТ ЖЕ available_after")
	assert.Equal(t, 1, store.reservationCount(), "повтор не должен создавать второй резерв")

	balance, err := svc.GetBalance(ctx, buyerID)
	require.NoError(t, err)
	assert.Equal(t, int64(7_000), balance.Available, "баланс не должен уменьшиться дважды")
}

func TestCommit_PovtorSTemZheKlyuchomVozvrashaetTeZheProvodkiINeSozdaetNovye(t *testing.T) {
	store, svc := newBuyerAuthor(t, 10_000)
	ctx := context.Background()

	// seedFunds в newBuyerAuthor уже написал пару "посевных" проводок —
	// считаем ПРИРОСТ, а не абсолютное число строк.
	countBeforeCommit := store.ledgerEntryCount()

	reservation, _, err := svc.Reserve(ctx, buyerID, domain.Money{AmountCents: 3_000, Currency: rub}, "order-2")
	require.NoError(t, err)

	ids1, err := svc.Commit(ctx, reservation.ID, authorID, "commit-order-2")
	require.NoError(t, err)
	require.Len(t, ids1, 2, "коммит обязан создать РОВНО пару проводок")
	assert.Equal(t, countBeforeCommit+2, store.ledgerEntryCount())

	ids2, err := svc.Commit(ctx, reservation.ID, authorID, "commit-order-2")
	require.NoError(t, err)

	assert.Equal(t, ids1, ids2, "повтор обязан вернуть ТЕ ЖЕ id проводок")
	assert.Equal(t, countBeforeCommit+2, store.ledgerEntryCount(), "повтор не должен создавать вторую пару проводок")
	assert.Equal(t, 1, store.debitedEventCount(), "повтор не должен публиковать событие дважды")
}

func TestRelease_PovtorSTemZheKlyuchomNeMenyaetSostoyanie(t *testing.T) {
	store, svc := newBuyerAuthor(t, 10_000)
	ctx := context.Background()

	countBefore := store.ledgerEntryCount()

	reservation, _, err := svc.Reserve(ctx, buyerID, domain.Money{AmountCents: 3_000, Currency: rub}, "order-3")
	require.NoError(t, err)

	m1, err := svc.Release(ctx, reservation.ID, "покупатель передумал", "release-order-3")
	require.NoError(t, err)

	m2, err := svc.Release(ctx, reservation.ID, "покупатель передумал", "release-order-3")
	require.NoError(t, err)

	assert.Equal(t, m1, m2, "повтор обязан вернуть ТОТ ЖЕ available_after")
	assert.Equal(t, countBefore, store.ledgerEntryCount(), "release никогда не пишет проводок")

	balance, err := svc.GetBalance(ctx, buyerID)
	require.NoError(t, err)
	assert.Equal(t, int64(10_000), balance.Available, "после release деньги обязаны вернуться в available")
}

// ─────────────────────────────────────────────────────────────────────────────
// Инвариант двойной записи: сумма ВСЕХ проводок ВСЕХ счетов равна нулю
// после ЛЮБОЙ последовательности операций (property-style).
// ─────────────────────────────────────────────────────────────────────────────

func TestInvariant_SummaProvodokVsegdaRavnaNulyu(t *testing.T) {
	const accountsCount = 4
	accounts := make([]*domain.Account, 0, accountsCount)
	for i := int64(1); i <= accountsCount; i++ {
		accounts = append(accounts, &domain.Account{ID: i, OwnerID: i * 10, Currency: rub})
	}
	store := newFakeStore(accounts...)
	for _, a := range accounts {
		store.seedFunds(a.ID, 100_000)
	}
	svc := NewService(store)
	ctx := context.Background()

	rng := rand.New(rand.NewSource(42))
	var activeReservations []string

	for i := 0; i < 500; i++ {
		switch rng.Intn(3) {
		case 0: // ReserveFunds на случайную сумму со случайного счёта
			acc := accounts[rng.Intn(len(accounts))].ID
			amount := int64(1 + rng.Intn(5_000))
			key := "reserve-" + strconv.Itoa(i)
			r, _, err := svc.Reserve(ctx, acc, domain.Money{AmountCents: amount, Currency: rub}, key)
			if err == nil {
				activeReservations = append(activeReservations, r.ID)
			} else {
				require.ErrorIs(t, err, domain.ErrInsufficientFunds, "единственная ожидаемая ошибка Reserve в этом сценарии")
			}
		case 1: // CommitFunds случайного активного резерва случайному автору
			if len(activeReservations) == 0 {
				continue
			}
			idx := rng.Intn(len(activeReservations))
			reservationID := activeReservations[idx]
			payee := accounts[rng.Intn(len(accounts))].ID
			_, err := svc.Commit(ctx, reservationID, payee, "commit-"+strconv.Itoa(i))
			require.NoError(t, err)
			activeReservations = append(activeReservations[:idx], activeReservations[idx+1:]...)
		case 2: // ReleaseFunds случайного активного резерва
			if len(activeReservations) == 0 {
				continue
			}
			idx := rng.Intn(len(activeReservations))
			reservationID := activeReservations[idx]
			_, err := svc.Release(ctx, reservationID, "случайная компенсация", "release-"+strconv.Itoa(i))
			require.NoError(t, err)
			activeReservations = append(activeReservations[:idx], activeReservations[idx+1:]...)
		}

		// Инвариант проверяется НА КАЖДОМ шаге, а не только в конце: если
		// он когда-нибудь нарушится, тест укажет точный шаг, а не просто
		// "где-то в 500 операциях есть баг".
		require.Zero(t, store.ledgerSum(), "сумма всех проводок обязана быть равна нулю после шага %d", i)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Недостаточно средств
// ─────────────────────────────────────────────────────────────────────────────

func TestReserve_NedostatochnoSredstv_ReservNeSozdayotsyaBalansNeMenyaetsya(t *testing.T) {
	store, svc := newBuyerAuthor(t, 1_000)
	ctx := context.Background()

	_, _, err := svc.Reserve(ctx, buyerID, domain.Money{AmountCents: 5_000, Currency: rub}, "order-4")

	require.ErrorIs(t, err, domain.ErrInsufficientFunds)
	assert.Equal(t, 0, store.reservationCount(), "резерв не должен появиться в хранилище")

	balance, err := svc.GetBalance(ctx, buyerID)
	require.NoError(t, err)
	assert.Equal(t, int64(1_000), balance.Available, "баланс обязан остаться прежним")
	assert.Zero(t, balance.Reserved)
}

// ─────────────────────────────────────────────────────────────────────────────
// Резерв уменьшает available, но не меняет сумму проводок
// ─────────────────────────────────────────────────────────────────────────────

func TestReserve_UmenshaetAvailableNoNeMenyaetProvodki(t *testing.T) {
	store, svc := newBuyerAuthor(t, 10_000)
	ctx := context.Background()

	sumBefore := store.ledgerSum()
	countBefore := store.ledgerEntryCount()

	_, availableAfter, err := svc.Reserve(ctx, buyerID, domain.Money{AmountCents: 4_000, Currency: rub}, "order-5")
	require.NoError(t, err)

	assert.Equal(t, int64(6_000), availableAfter)
	assert.Equal(t, sumBefore, store.ledgerSum(), "резерв — НЕ проводка, сумма проводок не меняется")
	assert.Equal(t, countBefore, store.ledgerEntryCount(), "резерв не добавляет ни одной строки в ledger_entries")

	balance, err := svc.GetBalance(ctx, buyerID)
	require.NoError(t, err)
	assert.Equal(t, int64(6_000), balance.Available)
	assert.Equal(t, int64(4_000), balance.Reserved)
}

// ─────────────────────────────────────────────────────────────────────────────
// Commit после Release и Release после Commit
// ─────────────────────────────────────────────────────────────────────────────

func TestCommit_PosleReleaseVozvrashaetOshibku(t *testing.T) {
	_, svc := newBuyerAuthor(t, 10_000)
	ctx := context.Background()

	reservation, _, err := svc.Reserve(ctx, buyerID, domain.Money{AmountCents: 2_000, Currency: rub}, "order-6")
	require.NoError(t, err)

	_, err = svc.Release(ctx, reservation.ID, "передумал", "release-order-6")
	require.NoError(t, err)

	_, err = svc.Commit(ctx, reservation.ID, authorID, "commit-order-6")

	require.ErrorIs(t, err, domain.ErrReservationAlreadyReleased,
		"подтвердить резерв, который уже освобождён компенсацией, нельзя — "+
			"это сигнал рассинхронизации саги, а не штатная ситуация")
}

func TestRelease_PosleCommitVozvrashaetOshibku(t *testing.T) {
	_, svc := newBuyerAuthor(t, 10_000)
	ctx := context.Background()

	reservation, _, err := svc.Reserve(ctx, buyerID, domain.Money{AmountCents: 2_000, Currency: rub}, "order-7")
	require.NoError(t, err)

	_, err = svc.Commit(ctx, reservation.ID, authorID, "commit-order-7")
	require.NoError(t, err)

	_, err = svc.Release(ctx, reservation.ID, "передумал", "release-order-7")

	require.ErrorIs(t, err, domain.ErrReservationAlreadyCommitted,
		"освободить резерв, деньги которого уже списаны и зачислены, нельзя — "+
			"откатывать нечего, а тихий успех замаскировал бы баг саги")
}

// ─────────────────────────────────────────────────────────────────────────────
// Разные валюты
// ─────────────────────────────────────────────────────────────────────────────

func TestReserve_RaznyeValyutyOtvergaetsya(t *testing.T) {
	store := newFakeStore(&domain.Account{ID: buyerID, Currency: rub})
	store.seedFunds(buyerID, 10_000)
	svc := NewService(store)

	_, _, err := svc.Reserve(context.Background(), buyerID, domain.Money{AmountCents: 1_000, Currency: usd}, "order-8")

	require.ErrorIs(t, err, domain.ErrCurrencyMismatch)
	assert.Equal(t, 0, store.reservationCount())
}

func TestCommit_RaznyeValyutyMezhduPokupatelemIAvtoromOtvergaetsya(t *testing.T) {
	store := newFakeStore(
		&domain.Account{ID: buyerID, Currency: rub},
		&domain.Account{ID: authorID, Currency: usd}, // автор — в другой валюте
	)
	store.seedFunds(buyerID, 10_000)
	svc := NewService(store)
	ctx := context.Background()

	reservation, _, err := svc.Reserve(ctx, buyerID, domain.Money{AmountCents: 2_000, Currency: rub}, "order-9")
	require.NoError(t, err)

	countBeforeCommit := store.ledgerEntryCount()

	_, err = svc.Commit(ctx, reservation.ID, authorID, "commit-order-9")

	require.ErrorIs(t, err, domain.ErrCurrencyMismatch, "конвертация не делается — перевод между разными валютами отвергается")
	assert.Equal(t, countBeforeCommit, store.ledgerEntryCount(), "проводки не должны появиться при отказе")
}

// ─────────────────────────────────────────────────────────────────────────────
// Валидация входа
// ─────────────────────────────────────────────────────────────────────────────

func TestMutatingMetody_TrebuyutIdempotencyKey(t *testing.T) {
	store, svc := newBuyerAuthor(t, 10_000)
	ctx := context.Background()

	_, _, err := svc.Reserve(ctx, buyerID, domain.Money{AmountCents: 1_000, Currency: rub}, "")
	require.ErrorIs(t, err, domain.ErrEmptyIdempotencyKey)

	_, err = svc.Commit(ctx, "любой-id", authorID, "")
	require.ErrorIs(t, err, domain.ErrEmptyIdempotencyKey)

	_, err = svc.Release(ctx, "любой-id", "причина", "")
	require.ErrorIs(t, err, domain.ErrEmptyIdempotencyKey)

	assert.Equal(t, 0, store.reservationCount())
}

func TestReserve_NepolozhitelnayaSummaOtvergaetsya(t *testing.T) {
	_, svc := newBuyerAuthor(t, 10_000)
	ctx := context.Background()

	_, _, err := svc.Reserve(ctx, buyerID, domain.Money{AmountCents: 0, Currency: rub}, "order-10")
	require.ErrorIs(t, err, domain.ErrInvalidAmount)

	_, _, err = svc.Reserve(ctx, buyerID, domain.Money{AmountCents: -1, Currency: rub}, "order-11")
	require.ErrorIs(t, err, domain.ErrInvalidAmount)
}

// ─────────────────────────────────────────────────────────────────────────────
// Прочее
// ─────────────────────────────────────────────────────────────────────────────

func TestGetBalance_NeizvestnyySchyotVozvrashaetOshibku(t *testing.T) {
	store := newFakeStore()
	svc := NewService(store)

	_, err := svc.GetBalance(context.Background(), 999)

	require.True(t, errors.Is(err, domain.ErrAccountNotFound))
}

func TestReserve_NeizvestnyySchyotVozvrashaetOshibku(t *testing.T) {
	store := newFakeStore()
	svc := NewService(store)

	_, _, err := svc.Reserve(context.Background(), 999, domain.Money{AmountCents: 100, Currency: rub}, "order-12")

	require.True(t, errors.Is(err, domain.ErrAccountNotFound))
}

func TestCommit_NeizvestnyRezervVozvrashaetOshibku(t *testing.T) {
	_, svc := newBuyerAuthor(t, 10_000)

	_, err := svc.Commit(context.Background(), "no-such-reservation", authorID, "commit-unknown")

	require.ErrorIs(t, err, domain.ErrReservationNotFound)
}

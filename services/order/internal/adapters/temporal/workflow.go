package temporal

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/log"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"gosplash/services/order/internal/domain"
)

// a — держатель ТИПА и ИМЁН методов Activities для workflow.ExecuteActivity.
// Никогда не разыменовывается (ни в workflow.go, ни в runtime): Temporal
// резолвит activity по имени метода через reflection на этом значении,
// а реальный вызов уходит воркеру, который зарегистрировал НАСТОЯЩИЙ
// экземпляр *Activities с живыми клиентами (см. cmd/worker/main.go).
// new(Activities), а не nil-указатель — так рекомендует сама документация
// go.temporal.io/sdk/workflow (OnActivity/ExecuteActivity: "используй
// экземпляр, как если бы регистрировал", nil-приёмник исторически работал,
// но не гарантирован).
var a = new(Activities)

// forwardActivityOptions — шаги, которые ЕЩЁ можно отменить компенсацией.
// Ограниченное число попыток: если ReserveFunds/GrantLicense/CommitFunds
// не удаются за разумное время, сага обязана СДАТЬСЯ и откатиться, а не
// пытаться бесконечно — у пользователя, ожидающего ответ на «купить»,
// нет бесконечного терпения.
var forwardActivityOptions = workflow.ActivityOptions{
	StartToCloseTimeout:    15 * time.Second,
	ScheduleToCloseTimeout: 2 * time.Minute,
	RetryPolicy: &sdktemporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2,
		MaximumInterval:    30 * time.Second,
		MaximumAttempts:    5,
	},
}

// criticalActivityOptions — шаги, которые ОБЯЗАНЫ рано или поздно
// завершиться успехом, потому что откатывать их либо нечем (ConfirmOrder
// после CommitFunds — деньги уже переведены), либо они и есть сама
// компенсация (RevokeLicense/ReleaseFunds), либо это просто бухгалтерия
// заказа (UpdateStatus/FailOrder), которая не должна остановиться на
// полпути. MaximumAttempts не задан — 0 означает «неограниченно», и
// ScheduleToCloseTimeout тоже не задан по той же причине: единственный
// предел здесь — MaximumInterval, ограничивающий паузу между попытками,
// а не их число.
//
// Это ОДИН из немногих мест в проекте, где отсутствие таймаута — осознанное
// решение, а не забытая настройка (см. docs/STYLE.md про то, что таймаут
// обычно обязателен): цена — воркфлоу, который завис на недоступном
// wallet/catalog, будет ретраить activity вечно, и это ВИДНО в Temporal UI
// (make wf-show), а не тихо потеряно, как было бы при panic/exit
// самописного воркера.
var criticalActivityOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 15 * time.Second,
	RetryPolicy: &sdktemporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2,
		MaximumInterval:    time.Minute,
	},
}

// PlaceOrderWorkflow — сага покупки лицензии.
//
//	ReserveFunds(wallet) → GrantLicense(catalog) → CommitFunds(wallet) → ConfirmOrder
//
// с компенсациями в ОБРАТНОМ порядке при ошибке любого шага:
// RevokeLicense(catalog), ReleaseFunds(wallet).
//
// ВАЖНОЕ ОГРАНИЧЕНИЕ (docs/adr/0004, раздел «риск, за которым надо
// следить»): эта функция переигрывается из истории при каждом восстановлении
// воркера, и поэтому ей запрещены time.Now(), rand, любые прямые походы
// в сеть или в базу — все они здесь ОТСУТСТВУЮТ НАМЕРЕННО, единственный
// ввод-вывод идёт через workflow.ExecuteActivity в activities.go. Если
// когда-нибудь понадобится текущее время внутри воркфлоу — только
// workflow.Now(), не time.Now(); случайность — только workflow.SideEffect,
// не math/rand напрямую. Их здесь пока нет, потому что сага не нуждается
// ни в том, ни в другом.
func PlaceOrderWorkflow(ctx workflow.Context, in PlaceOrderInput) error {
	logger := workflow.GetLogger(ctx)

	// compensation — накопленный список действий на случай отката,
	// PATTERN: saga (compensating transactions). Компенсация — это НЕ
	// rollback: RevokeLicense и ReleaseFunds — самостоятельные операции
	// со своим idempotency_key, они не «отменяют» запись в истории
	// шага-предшественника, а добавляют туда НОВУЮ запись. Оба факта —
	// «резерв поставили» и «резерв сняли» — остаются в ledger_entries
	// и в истории воркфлоу навсегда, и именно поэтому разбор инцидента
	// (make wf-show) показывает ПОЛНУЮ картину, а не только конечный итог.
	var compensations []func(workflow.Context) error

	forwardCtx := workflow.WithActivityOptions(ctx, forwardActivityOptions)
	criticalCtx := workflow.WithActivityOptions(ctx, criticalActivityOptions)

	// ── 1. ReserveFunds ──────────────────────────────────────────────────
	var reserveOut reserveFundsOutput
	err := workflow.ExecuteActivity(forwardCtx, a.ReserveFunds, reserveFundsInput{
		OrderID:    in.OrderID,
		BuyerID:    in.BuyerID,
		PriceCents: in.PriceCents,
		Currency:   in.Currency,
	}).Get(ctx, &reserveOut)
	if err != nil {
		return failOrder(ctx, logger, in.OrderID, err, compensations)
	}

	// ReleaseFunds кладём в стек ТОЛЬКО СЕЙЧАС, а не заранее: ему нужен
	// reservation_id, которого не существует до успешного ReserveFunds,
	// и proto ReleaseFundsRequest не принимает ничего другого как ключ
	// (см. wallet.proto). Если сам ReserveFunds не удался, компенсировать
	// нечего — деньги не были тронуты.
	reservationID := reserveOut.ReservationID
	compensations = append(compensations, func(cctx workflow.Context) error {
		return workflow.ExecuteActivity(workflow.WithActivityOptions(cctx, criticalActivityOptions), a.ReleaseFunds, releaseFundsInput{
			OrderID:       in.OrderID,
			ReservationID: reservationID,
			Reason:        "саговая компенсация: сага заказа не завершилась успехом",
		}).Get(cctx, nil)
	})

	if err := workflow.ExecuteActivity(criticalCtx, a.UpdateStatus, updateStatusInput{
		OrderID: in.OrderID, Status: domain.StatusFundsReserved,
	}).Get(ctx, nil); err != nil {
		return failOrder(ctx, logger, in.OrderID, err, compensations)
	}

	// ── 2. GrantLicense ──────────────────────────────────────────────────
	//
	// НЕОЧЕВИДНОЕ РЕШЕНИЕ: RevokeLicense кладём в стек компенсаций ДО
	// вызова GrantLicense, а не после его успеха. Причина — в самом
	// контракте catalog: RevokeLicenseRequest ключуется ТОЛЬКО order_id
	// (proto/gosplash/catalog/v1/catalog.proto), и revoked=false — это не
	// ошибка, а штатный ответ «отзывать было нечего». Раз компенсация
	// безопасна даже когда GrantLicense не выполнился (сетевой сбой мог
	// произойти уже ПОСЛЕ того, как catalog создал лицензию, но ДО того,
	// как ответ дошёл до этой activity), она обязана быть в списке ещё
	// до попытки — иначе именно этот пограничный случай («GrantLicense
	// технически выполнился, но activity вернула ошибку») остался бы
	// без отката. ReleaseFunds выше НЕ может себе такого позволить —
	// ему буквально нечем компенсировать до тех пор, пока не появится id.
	compensations = append(compensations, func(cctx workflow.Context) error {
		return workflow.ExecuteActivity(workflow.WithActivityOptions(cctx, criticalActivityOptions), a.RevokeLicense, revokeLicenseInput{
			OrderID: in.OrderID,
			Reason:  "саговая компенсация: сага заказа не завершилась успехом",
		}).Get(cctx, nil)
	})

	var grantOut grantLicenseOutput
	if err := workflow.ExecuteActivity(forwardCtx, a.GrantLicense, grantLicenseInput{
		OrderID: in.OrderID, ListingID: in.ListingID, BuyerID: in.BuyerID,
	}).Get(ctx, &grantOut); err != nil {
		return failOrder(ctx, logger, in.OrderID, err, compensations)
	}

	if err := workflow.ExecuteActivity(criticalCtx, a.UpdateStatus, updateStatusInput{
		OrderID: in.OrderID, Status: domain.StatusLicenseGranted,
	}).Get(ctx, nil); err != nil {
		return failOrder(ctx, logger, in.OrderID, err, compensations)
	}

	// ── 3. CommitFunds — точка невозврата ────────────────────────────────
	//
	// У CommitFunds НЕТ собственной компенсации в списке (proto описывает
	// только ReleaseFunds и RevokeLicense — см. package doc). Если сам
	// вызов не удался, деньги не списаны, и накопленных компенсаций
	// (ReleaseFunds, RevokeLicense) достаточно, чтобы откатить всё. Если
	// же CommitFunds УСПЕШЕН, а падает уже следующий шаг (ConfirmOrder) —
	// откатывать поздно: деньги реально переведены автору. Поэтому
	// ConfirmOrder ниже уходит в criticalActivityOptions с неограниченными
	// попытками, а не в ветку failOrder.
	var commitOut commitFundsOutput
	if err := workflow.ExecuteActivity(forwardCtx, a.CommitFunds, commitFundsInput{
		OrderID: in.OrderID, ReservationID: reservationID, PayeeAccountID: in.PayeeAccountID,
	}).Get(ctx, &commitOut); err != nil {
		return failOrder(ctx, logger, in.OrderID, err, compensations)
	}

	// ── 4. ConfirmOrder ──────────────────────────────────────────────────
	if err := workflow.ExecuteActivity(criticalCtx, a.ConfirmOrder, confirmOrderInput{
		OrderID:    in.OrderID,
		LicenseID:  grantOut.LicenseID,
		BuyerID:    in.BuyerID,
		AuthorID:   in.PayeeAccountID,
		ListingID:  in.ListingID,
		PriceCents: in.PriceCents,
		Currency:   in.Currency,
	}).Get(ctx, nil); err != nil {
		// Сюда попадаем только если criticalActivityOptions (по сути
		// неограниченные попытки) всё же исчерпались — то есть
		// ScheduleToCloseTimeout здесь не задан, и достичь этой ветки
		// в проде практически невозможно. Она остаётся на случай явной
		// отмены воркфлоу (make wf-terminate): тогда деньги уже списаны,
		// а заказ не отмечен завершённым — это состояние, требующее
		// ручного разбора, и именно поэтому мы НЕ запускаем компенсации
		// отсюда (RevokeLicense/ReleaseFunds после реального списания
		// денег вернули бы клиенту деньги, но НЕ отозвали бы лицензию
		// автоматически, оставив систему в другом несогласованном
		// состоянии — хуже исходного).
		return fmt.Errorf("не удалось подтвердить оплаченный заказ %s: %w", in.OrderID, err)
	}

	return nil
}

// failOrder — общая ветка отказа: отметить "compensating", прогнать
// накопленные компенсации в ОБРАТНОМ порядке (последний успешный шаг
// откатывается первым), затем отметить "failed" с причиной.
func failOrder(ctx workflow.Context, logger log.Logger, orderID string, cause error, compensations []func(workflow.Context) error) error {
	logger.Warn("сага прервана, запускаю компенсации", "order_id", orderID, "reason", cause.Error())

	criticalCtx := workflow.WithActivityOptions(ctx, criticalActivityOptions)

	if len(compensations) > 0 {
		if err := workflow.ExecuteActivity(criticalCtx, a.UpdateStatus, updateStatusInput{
			OrderID: orderID, Status: domain.StatusCompensating,
		}).Get(ctx, nil); err != nil {
			logger.Error("не удалось отметить заказ как compensating", "order_id", orderID, "error", err.Error())
		}

		for i := len(compensations) - 1; i >= 0; i-- {
			if err := compensations[i](ctx); err != nil {
				// criticalActivityOptions даёт неограниченные попытки без
				// ScheduleToCloseTimeout — на практике эта ветка достижима
				// только через отмену воркфлоу целиком. Раз сама
				// компенсация не выполнена, отмечать заказ "failed" было
				// бы неправдой (часть эффектов не откачена) — воркфлоу
				// завершается ошибкой, и незавершённая сага остаётся
				// видна в Temporal UI как повод для ручного вмешательства.
				logger.Error("компенсация не выполнена", "order_id", orderID, "error", err.Error())
				return fmt.Errorf("компенсация заказа %s не выполнена: %w (исходная причина: %w)", orderID, err, cause)
			}
		}
	}

	if err := workflow.ExecuteActivity(criticalCtx, a.FailOrder, failOrderInput{
		OrderID: orderID, Reason: cause.Error(),
	}).Get(ctx, nil); err != nil {
		return fmt.Errorf("не удалось пометить заказ %s проваленным: %w", orderID, err)
	}

	return cause
}

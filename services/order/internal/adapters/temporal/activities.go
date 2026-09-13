package temporal

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sdktemporal "go.temporal.io/sdk/temporal"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
	"gosplash/pkg/kafkax"
	"gosplash/services/order/internal/ports"
)

// Activities — все шаги саги, которые реально ходят в сеть или в базу.
// Регистрируется в cmd/worker целиком (worker.RegisterActivity(activities) —
// Temporal SDK сам находит и регистрирует каждый экспортированный метод
// как отдельную activity по имени метода, поэтому здесь нет ручного списка
// имён).
type Activities struct {
	wallet  ports.WalletSaga
	catalog ports.CatalogSaga
	orders  ports.SagaOrderRepository
}

func NewActivities(wallet ports.WalletSaga, catalog ports.CatalogSaga, orders ports.SagaOrderRepository) *Activities {
	return &Activities{wallet: wallet, catalog: catalog, orders: orders}
}

// classifyGRPCError решает, стоит ли Temporal-у ретраить ошибку.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: без этой классификации ЛЮБАЯ ошибка activity
// (включая «недостаточно средств» или «карточка не в продаже») ретраилась
// бы по RetryPolicy до MaximumAttempts — RetryPolicy решает механику
// повтора, но не решает, ИМЕЕТ ли повтор смысл. Постоянные бизнес-ошибки
// (InvalidArgument, FailedPrecondition, NotFound, PermissionDenied,
// AlreadyExists, Unauthenticated) оборачиваются в
// NewNonRetryableApplicationError и останавливают ретраи немедленно —
// сага быстрее переходит к компенсациям вместо того, чтобы несколько
// минут добросовестно повторять заведомо obречённый вызов. Транзиентные
// (Unavailable, DeadlineExceeded, ResourceExhausted, Aborted и то, что
// вообще не пришло как gRPC-статус) остаются как есть и ретраятся по
// общему правилу.
func classifyGRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound,
		codes.PermissionDenied, codes.AlreadyExists, codes.Unauthenticated:
		return sdktemporal.NewNonRetryableApplicationError(err.Error(), "PERMANENT_GRPC_ERROR", err)
	default:
		return err
	}
}

// ReserveFunds — шаг 1 саги. IDEMPOTENCY: order_id передан wallet как
// idempotency_key (см. ports.WalletSaga) — повторный вызов activity (тот
// же order_id) обязан вернуть тот же reservation_id, а не создать второй
// резерв. Клиент к wallet использует resilience.NotIdempotent (см.
// internal/adapters/grpc/wallet_saga.go): ретраит только Temporal.
func (a *Activities) ReserveFunds(ctx context.Context, in reserveFundsInput) (reserveFundsOutput, error) {
	reservationID, err := a.wallet.ReserveFunds(ctx, in.OrderID, in.BuyerID, in.PriceCents, in.Currency)
	if err != nil {
		return reserveFundsOutput{}, classifyGRPCError(err)
	}
	return reserveFundsOutput{ReservationID: reservationID}, nil
}

// ReleaseFunds — компенсация ReserveFunds.
func (a *Activities) ReleaseFunds(ctx context.Context, in releaseFundsInput) error {
	if err := a.wallet.ReleaseFunds(ctx, in.OrderID, in.ReservationID, in.Reason); err != nil {
		return classifyGRPCError(err)
	}
	return nil
}

// GrantLicense — шаг 2. IDEMPOTENCY: order_id — ключ на стороне catalog
// (см. proto/gosplash/catalog/v1/catalog.proto).
func (a *Activities) GrantLicense(ctx context.Context, in grantLicenseInput) (grantLicenseOutput, error) {
	licenseID, err := a.catalog.GrantLicense(ctx, in.OrderID, in.ListingID, in.BuyerID)
	if err != nil {
		return grantLicenseOutput{}, classifyGRPCError(err)
	}
	return grantLicenseOutput{LicenseID: licenseID}, nil
}

// RevokeLicense — компенсация GrantLicense. Безопасна даже тогда, когда
// GrantLicense не выполнялся успешно (см. ports.CatalogSaga и
// workflow.go — она кладётся в стек компенсаций ДО вызова GrantLicense).
func (a *Activities) RevokeLicense(ctx context.Context, in revokeLicenseInput) error {
	if _, err := a.catalog.RevokeLicense(ctx, in.OrderID, in.Reason); err != nil {
		return classifyGRPCError(err)
	}
	return nil
}

// CommitFunds — шаг 3, точка невозврата: после успеха деньги действительно
// переведены, и автоматической компенсации у CommitFunds нет (см.
// workflow.go, комментарий у вызова).
func (a *Activities) CommitFunds(ctx context.Context, in commitFundsInput) (commitFundsOutput, error) {
	ids, err := a.wallet.CommitFunds(ctx, in.OrderID, in.ReservationID, in.PayeeAccountID)
	if err != nil {
		return commitFundsOutput{}, classifyGRPCError(err)
	}
	return commitFundsOutput{LedgerEntryIDs: ids}, nil
}

// ConfirmOrder — шаг 4, финал: локальная запись, без вызовов другим
// сервисам. Публикует order.order.paid через outbox в ТОЙ ЖЕ транзакции,
// что и смену статуса на completed (см. ports.SagaOrderRepository.Complete).
func (a *Activities) ConfirmOrder(ctx context.Context, in confirmOrderInput) error {
	event, err := kafkax.NewEnvelope(kafkax.EventOrderPaid, in.OrderID, &eventsv1.OrderPaid{
		OrderId:    in.OrderID,
		BuyerId:    in.BuyerID,
		AuthorId:   in.AuthorID,
		ListingId:  in.ListingID,
		LicenseId:  in.LicenseID,
		PriceCents: in.PriceCents,
		Currency:   in.Currency,
	})
	if err != nil {
		return fmt.Errorf("activities: конверт order.order.paid: %w", err)
	}
	if err := a.orders.Complete(ctx, in.OrderID, in.LicenseID, event); err != nil {
		return fmt.Errorf("activities: завершение заказа: %w", err)
	}
	return nil
}

// UpdateStatus — вспомогательный шаг бухгалтерии саги (funds_reserved,
// license_granted, compensating). Идемпотентен тривиально — см.
// ports.SagaOrderRepository.UpdateStatus.
func (a *Activities) UpdateStatus(ctx context.Context, in updateStatusInput) error {
	if err := a.orders.UpdateStatus(ctx, in.OrderID, in.Status); err != nil {
		return fmt.Errorf("activities: обновление статуса: %w", err)
	}
	return nil
}

// FailOrder — финал неуспешной саги: статус failed + причина, ПОСЛЕ того
// как компенсации отработали (см. workflow.go).
func (a *Activities) FailOrder(ctx context.Context, in failOrderInput) error {
	if err := a.orders.Fail(ctx, in.OrderID, in.Reason); err != nil {
		return fmt.Errorf("activities: пометка заказа проваленным: %w", err)
	}
	return nil
}

package temporal

import (
	"context"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"gosplash/services/order/internal/domain"
)

// Starter — ports.WorkflowStarter, живёт в cmd/order (клиентская сторона
// Temporal — cmd/order лишь ЗАПУСКАЕТ воркфлоу, исполняет его cmd/worker,
// см. package doc cmd/order/main.go про то, почему это два процесса).
type Starter struct {
	client    client.Client
	taskQueue string
}

func NewStarter(c client.Client, taskQueue string) *Starter {
	return &Starter{client: c, taskQueue: taskQueue}
}

// StartPlaceOrder запускает PlaceOrderWorkflow с workflow_id = order.ID.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: workflow_id = order_id, а не сгенерированный отдельно
// run-идентификатор. Это даёт дедупликацию запуска саги БЕСПЛАТНО: два
// запроса StartWorkflow с одним и тем же ID Temporal сам не даёт выполнить
// дважды — WorkflowIDReusePolicy_REJECT_DUPLICATE запрещает повторный запуск
// даже ПОСЛЕ того, как первый воркфлоу успешно завершился (обычный
// AllowDuplicate по умолчанию разрешил бы новый запуск после завершения
// старого — нам это не нужно: заказ покупается ровно один раз, и вторая
// сага для того же order_id — всегда ошибка, а не легитимный кейс).
//
// Это ВТОРОЙ, независимый слой защиты от двойного выполнения саги —
// первый уже стоит в internal/app.OrderService.PlaceOrder (Idempotency-Key
// не пускает вызвать StartPlaceOrder дважды для одного и того же
// пользовательского запроса). Если оба слоя почему-то совпали бы
// (что практически невозможно, см. комментарий в order_service.go), эта
// функция вернула бы ошибку — и это ПРАВИЛЬНОЕ поведение: тихо
// проигнорировать WorkflowExecutionAlreadyStarted означало бы скрыть
// потенциальный баг вместо того, чтобы дать ему всплыть в логе.
func (s *Starter) StartPlaceOrder(ctx context.Context, order *domain.Order) error {
	_, err := s.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    order.ID,
		TaskQueue:             s.taskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}, PlaceOrderWorkflow, PlaceOrderInput{
		OrderID:        order.ID,
		BuyerID:        order.BuyerID,
		ListingID:      order.ListingID,
		PriceCents:     order.PriceCents,
		Currency:       order.Currency,
		PayeeAccountID: order.AuthorID,
	})
	if err != nil {
		return fmt.Errorf("temporal: запуск PlaceOrderWorkflow для заказа %s: %w", order.ID, err)
	}
	return nil
}

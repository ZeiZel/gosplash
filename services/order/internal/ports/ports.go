// Package ports — интерфейсы, через которые order-service разговаривает
// с внешним миром. Как и в catalog (см. его internal/ports/ports.go),
// интерфейс объявлен рядом с тем, кто им пользуется, а не рядом
// с реализацией.
//
// Порты сознательно РАЗДЕЛЕНЫ по процессу-потребителю, а не собраны в один
// «интерфейс сервиса». app.OrderService (живёт в cmd/order) и
// internal/adapters/temporal.Activities (живёт в cmd/worker) — это два
// РАЗНЫХ процесса, они никогда не делят один и тот же экземпляр реализации,
// и не должны делить один и тот же тип интерфейса просто потому что где-то
// внутри у них общий *gorm.DB: у app.OrderService нет причин знать про
// UpdateStatus/Complete/Fail, а у Activities — про Create. Поэтому вместо
// одного OrderRepository здесь два маленьких: OrderRepository (для app)
// и SagaOrderRepository (для activities), даже когда за обоими в итоге
// стоит один internal/adapters/pg.OrderRepository.
package ports

import (
	"context"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
	"gosplash/services/order/internal/domain"
)

// ── Используются app.OrderService (cmd/order) ────────────────────────────

// OrderRepository — чтение и создание заказа со стороны публичного API.
type OrderRepository interface {
	// Create вставляет строку заказа и событие order.order.placed В ОДНОЙ
	// транзакции (PATTERN: transactional outbox, pkg/outbox) — событие
	// обязано появиться тогда и только тогда, когда заказ действительно
	// создан.
	Create(ctx context.Context, order *domain.Order, placedEvent *eventsv1.Envelope) error
	GetByID(ctx context.Context, id string) (*domain.Order, error)
}

// IdempotencyStatus — три исхода Begin, см. pkg/redisx.IdempotencyStatus.
// Не переиспользуем тип pkg/redisx напрямую: app не должен знать, что за
// идемпотентностью стоит Redis (см. internal/adapters/idempotency — там
// есть ещё и PostgreSQL-fallback, о котором app тоже знать не обязан).
type IdempotencyStatus int

const (
	IdempotencyFree IdempotencyStatus = iota
	IdempotencyInProgress
	IdempotencyDone
)

// IdempotencyResult — исход Begin.
type IdempotencyResult struct {
	Status IdempotencyStatus
	// Response — сохранённый ответ. Валиден только при Status == IdempotencyDone.
	Response []byte
}

// IdempotencyStore — HTTP idempotency-key (см. ADR 0018). Begin/Complete —
// та же пара методов и та же семантика трёх исходов, что и у
// pkg/redisx.Client.Begin/Complete: интерфейс НАМЕРЕННО скопирован
// один в один, а не расширен, потому что двухуровневая реализация
// (Redis + fallback в PostgreSQL, см. internal/adapters/idempotency)
// снаружи обязана выглядеть неотличимо от одного Redis — это и есть весь
// смысл fallback'а.
type IdempotencyStore interface {
	Begin(ctx context.Context, key string) (IdempotencyResult, error)
	Complete(ctx context.Context, key string, response []byte) error
}

// Listing — минимальный снимок карточки catalog, нужный для создания
// заказа: цена и счёт автора (payee для будущего CommitFunds). Не domain.Listing
// каталога — это чужая модель, order копирует из неё только то, что нужно
// именно ему, ручным отображением (см. internal/adapters/grpc/catalog_reader.go).
type Listing struct {
	ID         string
	AuthorID   int64
	PriceCents int64
	Currency   string
	// Status — "draft" | "published" | "removed". Покупать можно только
	// published, см. domain.ErrListingNotAvailable.
	Status string
}

// ListingReader — единственное, что app.OrderService знает о catalog: цена
// и продавец карточки на момент создания заказа. Это ЧТЕНИЕ (GetListing),
// поэтому клиент, реализующий порт, вправе использовать транспорт
// с resilience.Idempotent (см. pkg/grpcx.WithRetry) — блок 5 задания.
type ListingReader interface {
	GetListing(ctx context.Context, listingID string) (Listing, error)
}

// WorkflowStarter — запуск саги. workflow_id = order.ID выбирается
// РЕАЛИЗАЦИЕЙ (internal/adapters/temporal.Starter), а не вызывающим кодом:
// это внутренняя договорённость между order-service и Temporal, и app не
// обязан знать, как формируется workflow_id, только то, что повторный
// запуск с тем же Order безопасен (см. комментарий у Starter).
type WorkflowStarter interface {
	StartPlaceOrder(ctx context.Context, order *domain.Order) error
}

// ── Используются internal/adapters/temporal.Activities (cmd/worker) ─────

// SagaOrderRepository — то, что нужно шагам саги от собственной базы
// order-service: обновить статус, завершить (с outbox order.order.paid)
// или провалить заказ. Никогда не создаёт заказ — это исключительно
// обязанность OrderRepository.Create со стороны публичного API.
type SagaOrderRepository interface {
	// UpdateStatus — простая смена статуса без outbox (funds_reserved,
	// license_granted, compensating). Идемпотентна тривиально: SET status=X
	// одинаков при любом числе повторов.
	UpdateStatus(ctx context.Context, id, status string) error
	// Complete — успешное завершение: status=completed, license_id и
	// событие order.order.paid В ОДНОЙ транзакции.
	Complete(ctx context.Context, id, licenseID string, paidEvent *eventsv1.Envelope) error
	// Fail — status=failed с причиной. Событий не публикует: неуспех заказа
	// не входит в контракт order.order.* (см. proto/gosplash/events/v1).
	Fail(ctx context.Context, id, reason string) error
}

// WalletSaga — шаги саги, ходящие в wallet. Обе мутирующие операции
// (двигают деньги) принимают orderID и используют его КАК idempotency_key —
// см. proto/gosplash/wallet/v1/wallet.proto.
type WalletSaga interface {
	ReserveFunds(ctx context.Context, orderID string, buyerID, amountCents int64, currency string) (reservationID string, err error)
	CommitFunds(ctx context.Context, orderID, reservationID string, payeeAccountID int64) (ledgerEntryIDs []string, err error)
	// ReleaseFunds — компенсация ReserveFunds. Нуждается в reservationID,
	// поэтому её можно вызвать только ПОСЛЕ успешного ReserveFunds — в
	// отличие от RevokeLicense ниже, у которой ключ — orderID и она
	// безопасна даже тогда, когда GrantLicense не выполнялся.
	ReleaseFunds(ctx context.Context, orderID, reservationID, reason string) error
}

// CatalogSaga — шаги саги, ходящие в catalog за лицензией.
type CatalogSaga interface {
	GrantLicense(ctx context.Context, orderID, listingID string, buyerID int64) (licenseID string, err error)
	// RevokeLicense — компенсация. Ключ — ТОЛЬКО orderID (см.
	// proto/gosplash/catalog/v1/catalog.proto: RevokeLicenseRequest не
	// содержит license_id) — это и делает её безопасной для вызова даже
	// когда соответствующий GrantLicense никогда не завершался успешно:
	// catalog просто ответит revoked=false, «отзывать было нечего».
	RevokeLicense(ctx context.Context, orderID, reason string) (revoked bool, err error)
}

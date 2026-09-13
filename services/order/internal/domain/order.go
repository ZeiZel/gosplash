// Package domain — предметная область заказа.
//
// Здесь сознательно НЕТ ни одной строчки, знающей про HTTP, gRPC, Redis,
// PostgreSQL или Temporal. Order — это то, что происходит с покупкой лицензии
// с точки зрения бизнеса, а не то, как это записано в таблице orders или
// в истории воркфлоу. См. docs/STYLE.md: доменная модель и строка таблицы —
// разные типы с ручным отображением (см. internal/adapters/pg).
//
// Состояние самой САГИ (какой шаг сейчас выполняется, что уже откатывать)
// здесь тоже не хранится — это то, что ADR 0004 отдаёт Temporal. Order несёт
// только ВЫСОКОУРОВНЕВЫЙ статус для быстрого чтения (GetOrder), а не историю
// шагов саги.
package domain

import (
	"errors"
	"time"
)

// Статусы жизненного цикла заказа. Прошедшее время — по той же причине, что
// и в proto: во время ретрая шага заказ остаётся в ПРЕДЫДУЩЕМ статусе, и это
// честнее, чем «в процессе».
//
//	pending → funds_reserved → license_granted → completed
//	        ↘ compensating → failed
const (
	StatusPending        = "pending"
	StatusFundsReserved  = "funds_reserved"
	StatusLicenseGranted = "license_granted"
	StatusCompensating   = "compensating"
	StatusCompleted      = "completed"
	StatusFailed         = "failed"
)

var (
	// ErrNotFound — заказа с таким id нет.
	ErrNotFound = errors.New("заказ не найден")

	// ErrIdempotencyKeyRequired — POST /orders обязан прислать заголовок
	// Idempotency-Key. Без него один и тот же клик «купить» превратился бы
	// в два независимых заказа при любом клиентском ретрае.
	ErrIdempotencyKeyRequired = errors.New("idempotency key обязателен")

	// ErrIdempotencyInProgress — тот же ключ уже обрабатывается ПАРАЛЛЕЛЬНО
	// другим запросом. Вызывающий обязан ответить 409, а не выполнять
	// операцию второй раз (см. pkg/redisx.IdempotencyInProgress).
	ErrIdempotencyInProgress = errors.New("запрос с этим ключом идемпотентности уже обрабатывается")

	// ErrListingNotFound — карточка не найдена в catalog. Постоянная ошибка:
	// повторять запрос с тем же listing_id бессмысленно.
	ErrListingNotFound = errors.New("карточка не найдена")

	// ErrListingNotAvailable — карточка существует, но не в статусе
	// published (черновик или снята с продажи). Покупка недоступна не
	// потому, что что-то сломалось, а потому что покупать нечего — это
	// FailedPrecondition, а не 500.
	ErrListingNotAvailable = errors.New("карточка недоступна для покупки")
)

// Order — заказ на покупку лицензии.
type Order struct {
	ID         string
	BuyerID    int64
	ListingID  string
	PriceCents int64
	Currency   string

	// AuthorID — счёт автора, куда уйдут деньги при CommitFunds (payee).
	// НЕ хранится в таблице orders (см. internal/adapters/pg/model.go) —
	// это единственное, зачем он нужен, а нужен он только саге. Сага же
	// хранит СВОЙ вход в истории воркфлоу Temporal, а не в базе order-
	// сервиса (см. package doc и README про то, что Temporal делает за нас).
	// Здесь, в domain.Order, поле остаётся исключительно для передачи
	// значения из app.OrderService (который узнаёт его от catalog) в
	// ports.WorkflowStarter при запуске саги — за пределами этого пути
	// оно не читается.
	AuthorID int64

	Status        string
	FailureReason string
	LicenseID     string

	CreatedAt time.Time
	UpdatedAt time.Time
}

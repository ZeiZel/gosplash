// Package ports — интерфейсы, которыми internal/app пользуется, чтобы
// говорить со storage, ничего не зная про GORM, SQL или Postgres
// (docs/STYLE.md, «Раскладка сервиса»).
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: порт — не «WalletRepository с методами Reserve/
// Commit/Release», а низкоуровневый TxStore с примитивами (заблокировать
// счёт, прочитать сумму проводок, вставить проводки, отметить операцию
// выполненной). Вся денежная логика фазы 3 — это ровно то место, где
// «самая важная часть» задачи — тесты: проверки достаточности средств,
// идемпотентность, переходы статуса резерва, парность проводок. Если бы
// эта логика жила в adapters/pg (как, например, лицензии в catalog, где
// решение и есть реализация), проверить её без Postgres было бы нельзя.
// Здесь она — в internal/app/service.go, а TxStore — единственная граница,
// которую в тестах подделывает in-memory реализация с обычным mutex'ом:
// она не хуже настоящей транзакции для проверки ПРАВИЛ, хотя и не проверяет
// поведение под реальной конкурентной нагрузкой БД (для этого в проекте есть
// другой инструмент — интеграционные тесты с testcontainers, docs/STYLE.md;
// здесь их сознательно нет — см. ADR 0014, «чего не делаем»).
package ports

import (
	"context"

	"gosplash/services/wallet/internal/domain"
)

// Виды идемпотентных операций для FindOperation/RecordOperation. Строкой,
// а не typed enum: значение уходит "как есть" в WHERE kind = ? и в колонку
// БД, а typed-обёртка здесь не даёт ничего сверх документации в комментарии.
const (
	OperationCommit  = "commit"
	OperationRelease = "release"
)

// WalletStore — точка входа в хранилище: открыть транзакцию и (отдельно)
// прочитать баланс без неё.
type WalletStore interface {
	// WithinTx выполняет fn в ОДНОЙ транзакции БД. Ошибка fn откатывает
	// транзакцию целиком — то же соглашение, что у gorm.DB.Transaction
	// и у catalog's UnitOfWork.WithClaim.
	WithinTx(ctx context.Context, fn func(TxStore) error) error

	// GetBalance — баланс БЕЗ блокировки строк: GetBalance не участвует
	// в денежной гонке (он ничего не меняет), и открывать ради него
	// пишущую транзакцию с FOR UPDATE было бы чистым замедлением каждого
	// параллельного Reserve/Commit/Release этого же счёта без всякой пользы.
	GetBalance(ctx context.Context, accountID int64) (domain.Balance, error)
}

// TxStore — операции над данными кошелька в рамках одной транзакции.
// Реализация (adapters/pg) обязана выполнять КАЖДЫЙ метод на одном и том же
// *gorm.DB-tx, переданном в fn у WithinTx — иначе гарантия атомарности,
// на которой держится вся идемпотентность и парность проводок, превращается
// в фикцию.
type TxStore interface {
	// LockAccount читает счёт с блокировкой строки (SELECT ... FOR UPDATE).
	//
	// Без блокировки два конкурентных ReserveFunds на один и тот же счёт
	// оба читают одно и то же "старое" available, оба видят "средств
	// хватает" и оба создают резерв — суммарно они зарезервируют больше,
	// чем реально доступно. FOR UPDATE сериализует такие вызовы: второй
	// дождётся коммита первого и увидит уже актуальный available.
	LockAccount(ctx context.Context, accountID int64) (*domain.Account, error)

	// LedgerBalance — сумма проводок по счёту (кредит минус дебет).
	LedgerBalance(ctx context.Context, accountID int64) (int64, error)

	// ActiveReservedSum — сумма активных резервов по счёту.
	ActiveReservedSum(ctx context.Context, accountID int64) (int64, error)

	// FindReservationByIdempotencyKey — идемпотентность ReserveFunds:
	// если резерв с таким ключом уже существует, ReserveFunds обязан
	// вернуть ЕГО, не создавая новый и не трогая баланс повторно.
	FindReservationByIdempotencyKey(ctx context.Context, key string) (*domain.Reservation, error)

	// LockReservation читает резерв с блокировкой строки — тем же приёмом
	// и по той же причине, что LockAccount: CommitFunds и ReleaseFunds
	// меняют его статус, и это обязано быть сериализовано.
	LockReservation(ctx context.Context, reservationID string) (*domain.Reservation, error)

	// InsertReservation вставляет резерв. created=false означает, что
	// idempotency_key уже занят — ЧУЖАЯ конкурентная вставка выиграла
	// гонку между "проверили — не нашли" (FindReservationByIdempotencyKey)
	// и вставкой. Это НЕ ошибка: вызывающий обязан перечитать существующую
	// строку сам, а не считать конфликт отказом (docs/STYLE.md, pkg/
	// idempotency.Claim — тот же приём: один атомарный INSERT ... ON
	// CONFLICT DO NOTHING вместо гонки «SELECT, потом INSERT»).
	InsertReservation(ctx context.Context, r *domain.Reservation) (created bool, err error)

	// SetReservationStatus переводит резерв в committed/released.
	SetReservationStatus(ctx context.Context, reservationID, status string) error

	// InsertLedgerEntries вставляет ОБЕ проводки операции одним вызовом —
	// оператор обязан передать их парой (или большим согласованным набором,
	// но не единственной проводкой без пары), чтобы инвариант «сумма равна
	// нулю» никогда не нарушался даже посередине выполнения (см. заголовок
	// пакета domain).
	InsertLedgerEntries(ctx context.Context, entries []*domain.LedgerEntry) error

	// LedgerEntryIDsByReference — id проводок, созданных операцией с данным
	// ReferenceID (обычно reservation_id). Нужен для идемпотентного повтора
	// CommitFunds С ДРУГИМ idempotency_key на уже committed резерв (см.
	// internal/app/service.go): вернуть надо именно те проводки, что были
	// созданы раньше, а не создавать новые.
	LedgerEntryIDsByReference(ctx context.Context, referenceID string) ([]string, error)

	// FindOperation / RecordOperation — идемпотентность CommitFunds и
	// ReleaseFunds. У них нет своей строки с unique-ключом, как у резерва
	// (они МЕНЯЮТ уже существующую сущность, а не создают новую), поэтому
	// нужен отдельный журнал уже выполненных операций: атомарная вставка
	// ON CONFLICT DO NOTHING под уникальный idempotency_key + kind, а не
	// «сначала проверили, потом сделали» — по той же причине, что и у
	// InsertReservation.
	FindOperation(ctx context.Context, key, kind string) (resultJSON []byte, found bool, err error)
	RecordOperation(ctx context.Context, key, kind, reservationID string, resultJSON []byte) (recorded bool, err error)

	// WriteAccountDebited — PATTERN: transactional outbox (pkg/outbox).
	// Событие пишется в ТОЙ ЖЕ транзакции, что и проводки: иначе окно между
	// коммитом проводок и публикацией события — то самое место, где процесс
	// может упасть, деньги уже списаны, а событие никто никогда не увидит.
	WriteAccountDebited(ctx context.Context, event domain.AccountDebitedEvent) error
}

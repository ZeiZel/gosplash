package grpc

import (
	"errors"

	"google.golang.org/grpc/codes"

	"gosplash/pkg/grpcx"
	"gosplash/services/wallet/internal/domain"
)

// codedErr делает err пригодным для grpcx.ToStatus (docs/STYLE.md: домен
// не знает про gRPC, перевод "domain.ErrX → codes.Y" — работа адаптера,
// тот же приём, что в services/catalog/internal/adapters/grpc/errors.go).
type codedErr struct {
	error
	code codes.Code
}

func (e codedErr) GRPCCode() codes.Code { return e.code }

// toDomainStatus переводит доменную ошибку wallet в статус с нужным кодом
// и понятной клиенту причиной (errdetails.ErrorInfo.Reason).
//
// Порядок веток важен: сначала конкретные доменные ошибки — они несут код,
// который вызывающему НУЖЕН для принятия решения (повторять ли операцию,
// сообщать ли покупателю «не хватает денег»), и лишь в конце — Internal
// по умолчанию для всего, что не распознано (grpcx.ToStatus делает это
// сам, если codedErr не подобрался).
func toDomainStatus(err error, reason string) error {
	switch {
	case errors.Is(err, domain.ErrAccountNotFound):
		return grpcx.ToStatus(codedErr{err, codes.NotFound}, reason)
	case errors.Is(err, domain.ErrReservationNotFound):
		return grpcx.ToStatus(codedErr{err, codes.NotFound}, reason)
	case errors.Is(err, domain.ErrInsufficientFunds):
		// FailedPrecondition, а не Internal: запрос корректен по форме,
		// проблема — в ТЕКУЩЕМ состоянии счёта. Клиент отличает это от
		// "сервер сломан" и может, например, показать покупателю
		// осмысленное "пополните счёт" вместо общего "попробуйте позже".
		return grpcx.ToStatus(codedErr{err, codes.FailedPrecondition}, reason)
	case errors.Is(err, domain.ErrReservationAlreadyCommitted),
		errors.Is(err, domain.ErrReservationAlreadyReleased):
		// Тоже FailedPrecondition: состояние резерва не позволяет
		// выполнить операцию, но сам вызов оформлен верно — вызывающая
		// сага должна разобрать эту ситуацию (несогласованность её
		// собственного состояния), а не слепо ретраить.
		return grpcx.ToStatus(codedErr{err, codes.FailedPrecondition}, reason)
	case errors.Is(err, domain.ErrCurrencyMismatch),
		errors.Is(err, domain.ErrInvalidAmount),
		errors.Is(err, domain.ErrEmptyIdempotencyKey):
		// InvalidArgument: сам запрос сформирован некорректно и НИКАКОЙ
		// повтор с теми же параметрами не поможет — в отличие от
		// FailedPrecondition, где смена состояния (например, пополнение
		// счёта) сделала бы повтор успешным.
		return grpcx.ToStatus(codedErr{err, codes.InvalidArgument}, reason)
	default:
		return grpcx.ToStatus(err, reason)
	}
}

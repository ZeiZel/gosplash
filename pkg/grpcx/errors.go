package grpcx

import (
	"errors"
	"log/slog"
	"net/http"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DomainError — то, чему должна соответствовать доменная ошибка сервиса,
// если она хочет уйти клиенту конкретным gRPC-кодом, а не общим
// codes.Internal. По правилу docs/STYLE.md доменные ошибки — это переменные
// в internal/domain, сравниваемые через errors.Is; ToStatus ожидает, что
// хотя бы часть из них реализует этот интерфейс явно (обычно — обёрткой
// рядом с объявлением ошибки в самом domain-пакете сервиса).
type DomainError interface {
	error
	GRPCCode() codes.Code
}

type grpcStatusError interface {
	GRPCStatus() *status.Status
}

// ToStatus переводит произвольную ошибку в status.Error с деталями
// errdetails.ErrorInfo.
//
// reason — машиночитаемая причина по конвенции errdetails (например,
// "LISTING_NOT_FOUND", "INSUFFICIENT_FUNDS"): в отличие от текста ошибки,
// она не должна меняться при правках формулировок и по ней клиент может
// ветвить логику, не разбирая человеческий текст.
//
// Три исхода:
//  1. err уже gRPC-статус (прилетел из вызова другого сервиса через
//     pkg/grpcx.Dial) — передаётся КАК ЕСТЬ, повторное оборачивание
//     потеряло бы исходный код (например, подменило бы Unavailable на
//     Internal) и исказило бы решение вызывающего о ретрае.
//  2. err реализует DomainError — код берётся из него.
//  3. иначе — codes.Internal со СТАТИЧЕСКИМ текстом "внутренняя ошибка
//     сервера": доменная ошибка без объявленного маппинга это баг (её
//     забыли завести как DomainError), и текст неопознанной ошибки может
//     содержать детали инфраструктуры (CWE-209), поэтому клиент его не
//     видит — оригинал уходит в slog.Error вместе с reason. Internal, а не
//     Unknown: он явно говорит "здесь баг сервера", а не "проверьте свой
//     запрос".
func ToStatus(err error, reason string) error {
	if err == nil {
		return nil
	}

	var gs grpcStatusError
	if errors.As(err, &gs) {
		return gs.GRPCStatus().Err()
	}

	code := codes.Internal
	msg := err.Error()
	if de, ok := errors.AsType[DomainError](err); ok {
		code = de.GRPCCode()
	} else {
		// Ошибка без маппинга — это баг (см. исход 3 выше), а текст бага
		// может содержать что угодно: DSN, кусок SQL, путь на диске.
		// Отдавать его клиенту — CWE-209 (раскрытие информации через
		// сообщение об ошибке), поэтому наружу уходит статический текст,
		// а оригинал — в лог вместе с reason, по которому запись можно
		// связать с конкретным вызовом. Логируем здесь, а не в вызывающих
		// адаптерах: ToStatus — единственная точка, через которую проходят
		// ВСЕ неопознанные ошибки всех сервисов, и второго такого места,
		// куда можно было бы поставить один лог, в проекте нет.
		slog.Error("grpcx: ToStatus: ошибка без доменного маппинга, клиенту отдан статический текст",
			"reason", reason, "error", err)
		msg = "внутренняя ошибка сервера"
	}

	st := status.New(code, msg)
	withDetails, detailErr := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: "gosplash",
	})
	if detailErr != nil {
		// WithDetails падает практически никогда (обычный источник —
		// исчерпание лимита размера сообщения на гигантских деталях).
		// Деградация до статуса без деталей лучше, чем ошибка обработки
		// ошибки: клиент как минимум получит код и текст.
		return st.Err()
	}
	return withDetails.Err()
}

// CodeToHTTPStatus — обратный маппинг gRPC-кода в HTTP-статус для edge-слоя
// (ручные REST-обёртки поверх gRPC-клиентов; grpc-gateway, если появится,
// использует свой маппинг, но правила те же).
func CodeToHTTPStatus(code codes.Code) int {
	switch code {
	case codes.OK:
		return http.StatusOK
	case codes.Canceled:
		// 499 — конвенция nginx для "клиент оборвал соединение", в
		// стандарте HTTP такого кода нет, но это самый частый выбор именно
		// для этого случая, включая сам grpc-gateway.
		return 499
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DataLoss, codes.Internal, codes.Unknown:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

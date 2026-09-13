package grpc

import "google.golang.org/grpc/codes"

// codedErr — минимальная обёртка, делающая err пригодным для
// grpcx.ToStatus: она реализует grpcx.DomainError (error + GRPCCode()).
//
// Домен (internal/domain) НАМЕРЕННО не знает про коды gRPC — см. docs/STYLE.md
// и комментарий там же: доменные ошибки сравниваются через errors.Is,
// а перевод в конкретный протокол — работа адаптера. Поэтому маппинг
// "domain.ErrX → codes.Y" живёт здесь, а не в пакете domain.
type codedErr struct {
	error
	code codes.Code
}

func (e codedErr) GRPCCode() codes.Code { return e.code }

// withCode оборачивает err, чтобы grpcx.ToStatus вернул клиенту именно code,
// а не codes.Internal по умолчанию.
func withCode(err error, code codes.Code) error {
	if err == nil {
		return nil
	}
	return codedErr{error: err, code: code}
}

package grpc

import "google.golang.org/grpc/codes"

// codedErr — минимальная обёртка, делающая err пригодным для
// grpcx.ToStatus (см. тот же приём в services/catalog/internal/adapters/grpc/errors.go):
// она реализует grpcx.DomainError (error + GRPCCode()).
//
// internal/domain намеренно не знает про коды gRPC (docs/STYLE.md):
// доменные ошибки сравниваются через errors.Is, а перевод в конкретный
// протокол — работа адаптера.
type codedErr struct {
	error
	code codes.Code
}

func (e codedErr) GRPCCode() codes.Code { return e.code }

func withCode(err error, code codes.Code) error {
	if err == nil {
		return nil
	}
	return codedErr{error: err, code: code}
}

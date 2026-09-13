package grpc

import "google.golang.org/grpc/codes"

// codedErr — минимальная обёртка, делающая err пригодным для grpcx.ToStatus
// (error + GRPCCode()). internal/domain намеренно не знает про коды gRPC
// (docs/STYLE.md): маппинг "domain.ErrX → codes.Y" — работа адаптера
// транспорта, а не предметной области. Тот же приём, что и в
// services/catalog/internal/adapters/grpc/errors.go.
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

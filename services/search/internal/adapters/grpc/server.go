package grpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"

	searchv1 "gosplash/gen/go/gosplash/search/v1"
	"gosplash/pkg/grpcx"
	"gosplash/services/search/internal/app"
	"gosplash/services/search/internal/domain"
)

// Server — реализация contract'а из proto/gosplash/search/v1/search.proto.
//
// UnimplementedSearchServiceServer встроен по совету самого grpc-go: когда
// в .proto появится новый метод, сервис продолжит компилироваться и
// ответит Unimplemented, а не сломает сборку.
type Server struct {
	searchv1.UnimplementedSearchServiceServer

	search *app.SearchService
}

func NewServer(search *app.SearchService) *Server {
	return &Server{search: search}
}

func (s *Server) Search(ctx context.Context, req *searchv1.SearchRequest) (*searchv1.SearchResponse, error) {
	result, err := s.search.Search(ctx, toDomainQuery(req))
	if err != nil {
		return nil, grpcx.ToStatus(classifyErr(err), "SEARCH_FAILED")
	}
	return toProtoResponse(result), nil
}

// classifyErr — то же деление, что и в app.classifyESErr, только в терминах
// gRPC-кодов, а не kafkax.Retryable/Permanent: у Search нет ретраев внутри
// consumer'а, вместо этого клиент сам решает, что делать с кодом ответа
// (Unavailable — можно повторить; InvalidArgument — нет, запрос сломан).
func classifyErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrIndexUnavailable):
		return withCode(err, codes.Unavailable)
	case errors.Is(err, domain.ErrInvalidDocument):
		// Здесь ErrInvalidDocument означает "запрос не удалось собрать"
		// (например, битый курсор) — это ошибка КЛИЕНТА, а не сервера.
		return withCode(err, codes.InvalidArgument)
	default:
		return err
	}
}

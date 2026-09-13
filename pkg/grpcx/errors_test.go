package grpcx

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// domainErr — минимальная доменная ошибка проекта: переменная-ошибка,
// реализующая DomainError, как описано в docs/STYLE.md ("доменные ошибки —
// переменные в internal/domain, сравниваются через errors.Is").
type domainErr struct {
	msg  string
	code codes.Code
}

func (e *domainErr) Error() string        { return e.msg }
func (e *domainErr) GRPCCode() codes.Code { return e.code }

func TestToStatus(t *testing.T) {
	notFound := &domainErr{msg: "карточка не найдена", code: codes.NotFound}
	wrappedNotFound := errors.Join(errors.New("контекст вызова"), notFound)
	plain := errors.New("обычная ошибка без маппинга")
	alreadyStatus := status.Error(codes.Unavailable, "уже статус из нижестоящего сервиса")

	cases := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{"nil остаётся nil", nil, codes.OK},
		{"DomainError даёт свой код", notFound, codes.NotFound},
		{"DomainError виден через errors.Is/As даже под обёрткой", wrappedNotFound, codes.NotFound},
		{"ошибка без маппинга — Internal, а не Unknown", plain, codes.Internal},
		{"готовый gRPC-статус передаётся как есть", alreadyStatus, codes.Unavailable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ToStatus(tc.err, "TEST_REASON")
			if tc.err == nil {
				assert.NoError(t, got)
				return
			}
			require.Error(t, got)
			assert.Equal(t, tc.wantCode, status.Code(got))
		})
	}
}

func TestToStatus_DobavlyaetErrorInfoDetail(t *testing.T) {
	err := ToStatus(&domainErr{msg: "нет денег", code: codes.FailedPrecondition}, "INSUFFICIENT_FUNDS")

	st, ok := status.FromError(err)
	require.True(t, ok)

	var found bool
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			found = true
			assert.Equal(t, "INSUFFICIENT_FUNDS", info.GetReason())
			assert.Equal(t, "gosplash", info.GetDomain())
		}
	}
	assert.True(t, found, "ToStatus обязан приложить errdetails.ErrorInfo с переданной причиной")
}

func TestToStatus_NePereobyorachivaetUzheGotovyGRPCStatus(t *testing.T) {
	original := status.Error(codes.PermissionDenied, "нет доступа")

	got := ToStatus(original, "ЛЮБАЯ_ПРИЧИНА")

	// Код обязан остаться исходным (PermissionDenied), а не превратиться в
	// Internal — иначе вызывающий код (например, resilience.IsRetryable)
	// принял бы неверное решение о ретрае на основании перезаписанного кода.
	assert.Equal(t, codes.PermissionDenied, status.Code(got))
}

func TestCodeToHTTPStatus(t *testing.T) {
	cases := []struct {
		code codes.Code
		want int
	}{
		{codes.OK, http.StatusOK},
		{codes.InvalidArgument, http.StatusBadRequest},
		{codes.FailedPrecondition, http.StatusBadRequest},
		{codes.NotFound, http.StatusNotFound},
		{codes.AlreadyExists, http.StatusConflict},
		{codes.Aborted, http.StatusConflict},
		{codes.PermissionDenied, http.StatusForbidden},
		{codes.Unauthenticated, http.StatusUnauthorized},
		{codes.ResourceExhausted, http.StatusTooManyRequests},
		{codes.Unimplemented, http.StatusNotImplemented},
		{codes.Unavailable, http.StatusServiceUnavailable},
		{codes.DeadlineExceeded, http.StatusGatewayTimeout},
		{codes.Internal, http.StatusInternalServerError},
		{codes.Unknown, http.StatusInternalServerError},
		{codes.DataLoss, http.StatusInternalServerError},
	}

	for _, tc := range cases {
		t.Run(tc.code.String(), func(t *testing.T) {
			assert.Equal(t, tc.want, CodeToHTTPStatus(tc.code))
		})
	}
}

package grpcx

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gosplash/pkg/resilience"
)

// retryUnaryClientInterceptor оборачивает вызов в resilience.Retrier.Do.
// method приходит от grpc как полное имя ("/gosplash.wallet.v1.WalletService/GetBalance")
// и становится лейблом метрики retry_attempts_total — конечный набор значений,
// кардинальность не растёт с числом пользователей или заказов.
func retryUnaryClientInterceptor(r *resilience.Retrier) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		return r.Do(ctx, method, func(ctx context.Context) error {
			return invoker(ctx, method, req, reply, cc, opts...)
		})
	}
}

// breakerUnaryClientInterceptor оборачивает вызов в resilience.Breaker.Execute.
//
// Ошибки самого breaker'а (ErrBreakerOpen / ErrTooManyProbes) — это не
// gRPC-статусы, а сентинелы пакета resilience, который ничего не знает про
// gRPC (см. комментарий к Breaker в pkg/resilience). Здесь, на границе с
// транспортом, они переводятся в codes.Unavailable: с точки зрения
// вызывающего кода "breaker открыт" и "сервис реально недоступен" — одно
// и то же наблюдаемое поведение, и статус должен быть таким же, каким был
// бы при настоящем сетевом отказе.
func breakerUnaryClientInterceptor(b *resilience.Breaker) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		err := b.Execute(func() error {
			return invoker(ctx, method, req, reply, cc, opts...)
		})
		if errors.Is(err, resilience.ErrBreakerOpen) || errors.Is(err, resilience.ErrTooManyProbes) {
			return status.Error(codes.Unavailable, "circuit breaker: "+b.Name()+" временно недоступен")
		}
		return err
	}
}

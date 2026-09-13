package grpcx

// Ручной аналог сгенерированного protoc-gen-go-grpc кода — без .proto и без
// buf, потому что зона ответственности этого пакета не включает proto/**.
// Ровно то же самое, что делает генератор: grpc.ServiceDesc с методами
// и обёртки над Invoke/NewStream. Сообщения — готовые типы из
// google.golang.org/protobuf/types/known/wrapperspb, чтобы не заводить свой
// .proto ради теста.

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	testServiceName       = "grpcx.test.TestService"
	testMethodEcho        = "/" + testServiceName + "/Echo"
	testMethodPanic       = "/" + testServiceName + "/Panic"
	testMethodEchoStream  = "/" + testServiceName + "/EchoStream"
	testMethodPanicStream = "/" + testServiceName + "/PanicStream"
)

// testServiceEchoStreamServerIface — то, что генератор назвал бы
// TestService_EchoStreamServer.
type testServiceEchoStreamServerIface interface {
	grpc.ServerStream
	Send(*wrapperspb.StringValue) error
}

type testServiceEchoStreamServer struct {
	grpc.ServerStream
}

func (x *testServiceEchoStreamServer) Send(m *wrapperspb.StringValue) error {
	return x.ServerStream.SendMsg(m)
}

// testServiceServer — то, что генератор назвал бы TestServiceServer.
type testServiceServer interface {
	Echo(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
	Panic(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
	EchoStream(in *wrapperspb.StringValue, stream testServiceEchoStreamServerIface) error
	PanicStream(in *wrapperspb.StringValue, stream testServiceEchoStreamServerIface) error
}

func testServiceEchoHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(wrapperspb.StringValue)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(testServiceServer).Echo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: testMethodEcho}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(testServiceServer).Echo(ctx, req.(*wrapperspb.StringValue))
	}
	return interceptor(ctx, in, info, handler)
}

func testServicePanicHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(wrapperspb.StringValue)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(testServiceServer).Panic(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: testMethodPanic}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(testServiceServer).Panic(ctx, req.(*wrapperspb.StringValue))
	}
	return interceptor(ctx, in, info, handler)
}

func testServiceEchoStreamHandler(srv any, stream grpc.ServerStream) error {
	m := new(wrapperspb.StringValue)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(testServiceServer).EchoStream(m, &testServiceEchoStreamServer{stream})
}

func testServicePanicStreamHandler(srv any, stream grpc.ServerStream) error {
	m := new(wrapperspb.StringValue)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(testServiceServer).PanicStream(m, &testServiceEchoStreamServer{stream})
}

var testServiceDesc = grpc.ServiceDesc{
	ServiceName: testServiceName,
	HandlerType: (*testServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Echo", Handler: testServiceEchoHandler},
		{MethodName: "Panic", Handler: testServicePanicHandler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "EchoStream", Handler: testServiceEchoStreamHandler, ServerStreams: true},
		{StreamName: "PanicStream", Handler: testServicePanicStreamHandler, ServerStreams: true},
	},
	Metadata: "grpcx_test.proto",
}

// fakeServer — реализация testServiceServer, управляемая тестом.
type fakeServer struct {
	mu sync.Mutex

	// failTimes — сколько раз подряд Echo должен вернуть codes.Unavailable,
	// прежде чем ответить успехом. Используется тестами retry/breaker.
	failTimes int
	calls     int
	lastCtx   context.Context
}

func (f *fakeServer) Echo(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	f.mu.Lock()
	f.calls++
	f.lastCtx = ctx
	shouldFail := f.calls <= f.failTimes
	f.mu.Unlock()

	if shouldFail {
		return nil, unavailableErr()
	}
	return in, nil
}

func (f *fakeServer) Panic(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	panic("бабах: тестовая паника в обработчике")
}

func (f *fakeServer) EchoStream(in *wrapperspb.StringValue, stream testServiceEchoStreamServerIface) error {
	f.mu.Lock()
	f.lastCtx = stream.Context()
	f.mu.Unlock()
	return stream.Send(in)
}

func (f *fakeServer) PanicStream(*wrapperspb.StringValue, testServiceEchoStreamServerIface) error {
	panic("бабах: тестовая паника в стриме")
}

func (f *fakeServer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeServer) contextOfLastCall() context.Context {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastCtx
}

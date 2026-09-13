package grpcx

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Health — обёртка над стандартным grpc.health.v1: то, что kubelet и
// grpcurl ожидают увидеть у любого gRPC-сервиса, и то же самое разделение
// на liveness/readiness, что и в pkg/httpx.Health, только под протокол
// grpc.health.v1.Health/Check вместо /healthz и /readyz.
//
// "" (пустая строка сервиса, см. RegisterHealthServer) — это статус
// сервера ЦЕЛИКОМ, ровно то, что проверяет readinessProbe в Kubernetes.
// Протокол допускает статус ПО ОТДЕЛЬНОМУ сервису (например,
// "gosplash.wallet.v1.WalletService"), но в проекте один процесс — один
// gRPC-сервис, и разделять их незачем.
type Health struct {
	server *health.Server
}

// NewHealth создаёт health-сервер в состоянии SERVING.
func NewHealth() *Health {
	h := &Health{server: health.NewServer()}
	h.server.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	return h
}

// Register регистрирует grpc.health.v1 в *grpc.Server.
func (h *Health) Register(server *grpc.Server) {
	healthpb.RegisterHealthServer(server, h.server)
}

// NotServing переводит сервис в NOT_SERVING, не останавливая его.
//
// Первый шаг graceful shutdown, симметрично pkg/httpx.Health.NotReady:
// балансировщик (или k8s readinessProbe поверх grpc.health.v1) обязан
// перестать слать НОВЫЙ трафик до того, как сервер начнёт закрывать
// соединения — иначе часть запросов прилетает в уже останавливающийся
// процесс и получает обрыв соединения вместо ответа.
func (h *Health) NotServing() {
	h.server.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
}

// Serving возвращает сервис в состояние SERVING (используется в тестах и
// при отмене плановой остановки).
func (h *Health) Serving() {
	h.server.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
}

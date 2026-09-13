// Package grpcx — то, что нужно каждому gRPC-сервису и gRPC-клиенту проекта,
// по аналогии с pkg/httpx для HTTP:
//
//	client.go       — фабрика клиентских соединений: балансировка,
//	                  keepalive, трассировка, устойчивость (pkg/resilience).
//	interceptors.go — серверные unary/stream интерсепторы: recovery,
//	                  логирование, метрики RED, JWT-аутентификация,
//	                  проставление дедлайна по умолчанию.
//	health.go       — grpc.health.v1 с ручным переключением статуса при
//	                  graceful shutdown.
//	errors.go       — перевод доменных ошибок в status.Error с деталями и
//	                  обратный маппинг gRPC-кода в HTTP-статус для edge-слоя.
//	server.go       — сборка *grpc.Server с интерсепторами и запуск с
//	                  graceful shutdown, в стиле pkg/httpx.Serve/Shutdown.
//
// Чего в пакете сознательно нет: TLS. Внутренняя сеть проекта — локальный
// docker-compose и учебный Kubernetes-namespace, а не открытый интернет;
// добавление mTLS — отдельная задача (service mesh или ручные сертификаты),
// вне объёма фазы 3 и специально не имитируется, чтобы не создавать
// ложного ощущения защищённости.
package grpcx

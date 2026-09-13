# ─────────────────────────────────────────────────────────────────────────────
# gRPC и устойчивость — фаза 3 (docs/PLAN.md, pkg/grpcx, pkg/resilience).
#
# Цели этого файла проверяют то, что даёт pkg/grpcx поверх голого grpcurl:
# grpc.health.v1 у каждого сервиса и мгновенный отказ вместо таймаута, когда
# зависимость недоступна (circuit breaker, ADR 0015 и docs/adr/0015-*.md).
#
# DC, API определены в корневом Makefile и уже доступны здесь — этот файл
# подключается через `-include mk/*.mk` в его конце.
# ─────────────────────────────────────────────────────────────────────────────

## gRPC (фаза 3)

# P — порт сервиса. Портов, а не имён, потому что один и тот же приём
# (grpcurl localhost:PORT) работает одинаково для любого сервиса, а порты
# уже зафиксированы в .env.example: media 9101, catalog 9102, wallet 9103,
# order 9104.
GRPC_PORT := $(or $(P),9101)

.PHONY: grpc-health
grpc-health: ## grpc.health.v1 у сервиса (make grpc-health P=9103 — wallet)
	@grpcurl -plaintext -connect-timeout 2 localhost:$(GRPC_PORT) grpc.health.v1.Health/Check

.PHONY: grpc-reflect
grpc-reflect: ## список gRPC-методов сервиса через reflection (make grpc-reflect P=9102 — catalog)
	@grpcurl -plaintext -connect-timeout 2 localhost:$(GRPC_PORT) list

.PHONY: breaker-demo
breaker-demo: ## circuit breaker: зависимость "зависает" → таймауты → мгновенный 503 (см. ADR 0015)
	@if [ -z "$(URL)" ] || [ -z "$(SVC)" ]; then \
		echo "Использование: make breaker-demo SVC=wallet URL=$(API)/orders/1"; \
		echo; \
		echo "  SVC — имя процесса зависимости, которую 'подвешиваем' (см. make run-<SVC>);"; \
		echo "  URL — HTTP-ручка ДРУГОГО сервиса, которая по цепочке ходит в SVC через"; \
		echo "        pkg/grpcx.Dial с circuit breaker — появится, когда order/catalog"; \
		echo "        переведут на pkg/grpcx (см. docs/PLAN.md, фаза 3.4-3.5)."; \
		echo; \
		echo "Готово, когда (docs/PLAN.md): 'убитый wallet даёт 503 за 50 мс, а не за 5 с'."; \
		exit 1; \
	fi
	@PID=$$(pgrep -f "/exe/$(SVC)$$" | head -1); \
	if [ -z "$$PID" ]; then \
		echo "не нашёл процесс $(SVC) (искал 'go run' по маске /exe/$(SVC)) — сначала 'make run-$(SVC)'"; \
		exit 1; \
	fi; \
	echo "1) приостанавливаю $(SVC) (pid $$PID) сигналом STOP."; \
	echo "   Специально STOP, а не KILL: убитый процесс отвечает мгновенным"; \
	echo "   connection refused, а нам нужен именно ЗАВИСШИЙ сервис — то, от чего"; \
	echo "   на самом деле лечит circuit breaker (см. pkg/resilience/breaker.go)."; \
	kill -STOP $$PID; \
	echo; \
	echo "2) вызовы идут в $(URL) — первые платят полным retry+deadline,"; \
	echo "   дальнейшие обязаны провалиться мгновенно после открытия breaker:"; \
	for i in 1 2 3 4 5 6; do \
		START=$$(date +%s%3N); \
		CODE=$$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$(URL)"); \
		END=$$(date +%s%3N); \
		printf "   попытка %d: %5d мс, код %s\n" "$$i" "$$((END-START))" "$$CODE"; \
	done; \
	echo; \
	echo "3) возвращаю $(SVC) в работу сигналом CONT."; \
	kill -CONT $$PID

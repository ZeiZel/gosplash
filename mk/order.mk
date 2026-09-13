# ─────────────────────────────────────────────────────────────────────────────
## Фаза 3 order: приём заказов с Idempotency-Key и сага PlaceOrderWorkflow
## на Temporal (docs/PLAN.md, docs/adr/0004-*, docs/adr/0018-*)
#
# Цели этого файла предполагают уже поднятую инфраструктуру и запущенные
# сервисы (`make up`, `make migrate`, `make run-catalog`, `make run-wallet`,
# `make run-order`, `make run-worker`) — как и остальные demo-* цели
# в проекте. DC, PSQL, API, TCTL определены в главном Makefile.
# ─────────────────────────────────────────────────────────────────────────────

ORDER_GRPC_PORT   := $(or $(P),9104)
WALLET_GRPC_PORT  := $(or $(P),9103)
CATALOG_HTTP_PORT := $(or $(P),8102)

# BUYER — покупатель (не B: этот идентификатор уже занят в главном
# Makefile под ANSI bold для `make help`), L — карточка, K — свой ключ
# идемпотентности (по умолчанию генерируется, чтобы повторный запуск make
# не наткнулся на 409 "уже обрабатывается" от предыдущего прогона).
BUYER ?= 1
L ?=
K ?=

.PHONY: order-place
order-place: ## создать заказ (make order-place L=<listing_id> [BUYER=<buyer_id>] [K=<idempotency-key>])
	@if [ -z "$(L)" ]; then \
		echo "Использование: make order-place L=<listing_id> [BUYER=<buyer_id>] [K=<idempotency-key>]"; \
		echo "listing_id можно взять из make demo-catalog или GET $(API)/catalog/listings"; \
		exit 1; \
	fi
	@key="$(K)"; \
	if [ -z "$$key" ]; then key="order-place-$$(date +%s%N)"; fi; \
	echo "Idempotency-Key: $$key"; \
	curl -sS -X POST "$(API)/orders" \
		-H "Idempotency-Key: $$key" \
		-H "Content-Type: application/json" \
		-d "{\"buyer_id\":$(BUYER),\"listing_id\":\"$(L)\"}" | jq .

.PHONY: order-show
order-show: ## показать заказ (make order-show ID=<order_id>)
	@if [ -z "$(ID)" ]; then \
		echo "Использование: make order-show ID=<order_id>"; \
		exit 1; \
	fi
	@curl -sS "$(API)/orders/$(ID)" | jq .

.PHONY: wf-describe
wf-describe: ## текущее состояние воркфлоу саги: шаг, попытки, pending activities (make wf-describe W=<order_id>)
	@if [ -z "$(W)" ]; then \
		echo "Использование: make wf-describe W=<order_id>  (workflow_id саги = order_id)"; \
		exit 1; \
	fi
	@$(TCTL) workflow describe --workflow-id $(W)

.PHONY: demo-saga-ok
demo-saga-ok: ## успешная покупка целиком: ReserveFunds → GrantLicense → CommitFunds → ConfirmOrder, баланс уменьшился, лицензия выдана
	@printf "\033[1mDemo saga: успешная покупка лицензии через Temporal\033[0m\n\n"
	@listing=$$(curl -sS "$(API)/catalog/listings?limit=1" | jq -r '.listings[0].id // empty'); \
	if [ -z "$$listing" ]; then \
		echo "Нет опубликованных карточек — сначала make demo-media && make demo-catalog."; \
		exit 1; \
	fi; \
	echo "Карточка: $$listing"; \
	echo; \
	echo "1. Баланс покупателя $(BUYER) ДО покупки:"; \
	grpcurl -plaintext -connect-timeout 2 -d "{\"account_id\":$(BUYER)}" \
		localhost:$(WALLET_GRPC_PORT) gosplash.wallet.v1.WalletService/GetBalance; \
	echo; \
	key="demo-saga-ok-$$(date +%s%N)"; \
	echo "2. POST /orders (Idempotency-Key: $$key):"; \
	order=$$(curl -sS -X POST "$(API)/orders" \
		-H "Idempotency-Key: $$key" -H "Content-Type: application/json" \
		-d "{\"buyer_id\":$(BUYER),\"listing_id\":\"$$listing\"}"); \
	echo "$$order" | jq .; \
	id=$$(echo "$$order" | jq -r '.id'); \
	if [ -z "$$id" ] || [ "$$id" = "null" ]; then echo "Не удалось создать заказ, смотри ответ выше."; exit 1; fi; \
	echo; \
	echo "3. Сага асинхронна (см. package doc cmd/order/main.go) — опрашиваю GetOrder:"; \
	status=pending; \
	for i in $$(seq 1 15); do \
		sleep 1; \
		status=$$(curl -sS "$(API)/orders/$$id" | jq -r '.status'); \
		echo "   [$$i] status=$$status"; \
		if [ "$$status" = "completed" ] || [ "$$status" = "failed" ]; then break; fi; \
	done; \
	echo; \
	echo "4. Итоговый заказ:"; \
	curl -sS "$(API)/orders/$$id" | jq .; \
	echo; \
	echo "5. Баланс покупателя ПОСЛЕ покупки (ожидается меньше на цену карточки):"; \
	grpcurl -plaintext -connect-timeout 2 -d "{\"account_id\":$(BUYER)}" \
		localhost:$(WALLET_GRPC_PORT) gosplash.wallet.v1.WalletService/GetBalance; \
	echo; \
	echo "6. История саги в Temporal (make wf-describe W=$$id — текущее состояние;"; \
	echo "   make wf-show W=$$id — полная история; UI — http://localhost:58233):"; \
	$(TCTL) workflow describe --workflow-id $$id

.PHONY: demo-saga-fail
demo-saga-fail: ## сага при недоступном catalog: GrantLicense проваливается, RevokeLicense/ReleaseFunds откатывают (сначала останови make run-catalog!)
	@if [ -z "$(L)" ]; then \
		echo "Использование: make demo-saga-fail L=<listing_id> [BUYER=<buyer_id>]"; \
		echo "Возьми listing_id ДО остановки catalog: make demo-catalog или GET $(API)/catalog/listings"; \
		exit 1; \
	fi
	@if curl -sS -o /dev/null --max-time 1 "http://localhost:$(CATALOG_HTTP_PORT)/catalog/healthz"; then \
		echo "catalog всё ещё отвечает на :$(CATALOG_HTTP_PORT) — останови его (Ctrl+C в терминале make run-catalog) и повтори"; \
		exit 1; \
	fi
	@printf "\033[1mDemo saga: catalog недоступен, сага откатывается\033[0m\n\n"
	@echo "1. Баланс покупателя $(BUYER) ДО попытки покупки:"; \
	grpcurl -plaintext -connect-timeout 2 -d "{\"account_id\":$(BUYER)}" \
		localhost:$(WALLET_GRPC_PORT) gosplash.wallet.v1.WalletService/GetBalance; \
	echo; \
	key="demo-saga-fail-$$(date +%s%N)"; \
	echo "2. POST /orders (Idempotency-Key: $$key) — ReserveFunds ещё пройдёт, GrantLicense — нет:"; \
	order=$$(curl -sS -X POST "$(API)/orders" \
		-H "Idempotency-Key: $$key" -H "Content-Type: application/json" \
		-d "{\"buyer_id\":$(BUYER),\"listing_id\":\"$(L)\"}"); \
	echo "$$order" | jq .; \
	id=$$(echo "$$order" | jq -r '.id'); \
	if [ -z "$$id" ] || [ "$$id" = "null" ]; then echo "Не удалось создать заказ, смотри ответ выше."; exit 1; fi; \
	echo; \
	echo "3. Жду, пока RetryPolicy на GrantLicense исчерпает попытки и сага"; \
	echo "   скомпенсирует резерв (forwardActivityOptions — до 5 попыток, до ~60с):"; \
	status=pending; \
	for i in $$(seq 1 30); do \
		sleep 2; \
		status=$$(curl -sS "$(API)/orders/$$id" | jq -r '.status'); \
		echo "   [$$i] status=$$status"; \
		if [ "$$status" = "failed" ]; then break; fi; \
	done; \
	echo; \
	echo "4. Итоговый заказ (ожидается status=failed, failure_reason про catalog):"; \
	curl -sS "$(API)/orders/$$id" | jq .; \
	echo; \
	echo "5. Баланс покупателя ПОСЛЕ отката (ожидается КАК ДО покупки — ReleaseFunds сработал):"; \
	grpcurl -plaintext -connect-timeout 2 -d "{\"account_id\":$(BUYER)}" \
		localhost:$(WALLET_GRPC_PORT) gosplash.wallet.v1.WalletService/GetBalance; \
	echo; \
	echo "6. История саги — видны RevokeLicense (безопасный no-op, лицензию не выдавали)"; \
	echo "   и ReleaseFunds как компенсации, в обратном порядке (make wf-show W=$$id):"; \
	$(TCTL) workflow describe --workflow-id $$id; \
	echo; \
	echo "Не забудь снова поднять catalog: make run-catalog"

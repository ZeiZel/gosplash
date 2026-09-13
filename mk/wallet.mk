# ─────────────────────────────────────────────────────────────────────────────
## Фаза 3 wallet: двойная запись, резервирование, идемпотентные gRPC-операции
## (docs/PLAN.md, docs/adr/0014-*, proto/gosplash/wallet/v1/wallet.proto)
#
# Цели этого файла предполагают уже поднятую инфраструктуру и запущенный
# wallet (`make up-pg`, `make run-wallet` — схема применяется самим
# сервисом при старте, см. services/wallet/migrations/auto.go). DC, PSQL
# определены в главном Makefile.
#
# У wallet нет публичного REST и нет CreateAccount в контракте (wallet.v1
# заморожен) — счета этой демонстрации заводятся и пополняются напрямую
# через psql, ПАРНОЙ проводкой от счёта 0 (условный "внешний источник
# денег"), а не UPDATE — иначе демонстрационные данные сами нарушали бы
# инвариант, который эти же цели проверяют (см. wallet-check).
# ─────────────────────────────────────────────────────────────────────────────

WALLET_GRPC_PORT := $(or $(P),9103)

.PHONY: wallet-balance
wallet-balance: ## баланс счёта через grpcurl (make wallet-balance ID=1)
	@if [ -z "$(ID)" ]; then \
		echo "Использование: make wallet-balance ID=<account_id>"; \
		exit 1; \
	fi
	@grpcurl -plaintext -connect-timeout 2 \
		-d '{"account_id": $(ID)}' \
		localhost:$(WALLET_GRPC_PORT) gosplash.wallet.v1.WalletService/GetBalance

.PHONY: wallet-ledger
wallet-ledger: ## последние проводки из базы wallet (psql, append-only ledger_entries)
	@$(DC) exec -T pg-wallet $(PSQL) -c \
		"SELECT id, account_id, amount_cents, direction, operation_id, reason, reference_id, created_at FROM ledger_entries ORDER BY created_at DESC, id DESC LIMIT 20" \
		2>/dev/null || echo "таблицы ledger_entries ещё нет — сначала make run-wallet (схема применяется при старте)"

.PHONY: wallet-check
wallet-check: ## проверка инварианта двойной записи: SUM(проводок) обязана быть 0 (docs/adr/0014-*)
	@raw=$$($(DC) exec -T pg-wallet $(PSQL) -t -c \
		"SELECT COALESCE(SUM(CASE WHEN direction = 'credit' THEN amount_cents ELSE -amount_cents END), 0) FROM ledger_entries" \
		2>/dev/null); \
	sum=$$(echo "$$raw" | tr -d '[:space:]'); \
	if [ -z "$$sum" ]; then \
		echo "не удалось получить сумму — wallet и Postgres подняты? (make up-pg, make run-wallet)"; \
		exit 1; \
	fi; \
	echo "SUM(ledger_entries) = $$sum"; \
	if [ "$$sum" = "0" ]; then \
		echo "инвариант двойной записи выполняется: деньги не появились и не исчезли."; \
	else \
		echo "ИНВАРИАНТ НАРУШЕН: сумма всех проводок должна быть 0, а не $$sum — см. docs/adr/0014-*."; \
		exit 1; \
	fi

.PHONY: demo-wallet
demo-wallet: ## фаза 3 целиком: пополнение → ReserveFunds → CommitFunds → проводки → инвариант
	@printf "\033[1mDemo wallet: резерв, коммит, двойная запись (docs/adr/0014-*)\033[0m\n\n"
	@printf "0. Заводим демонстрационные счета (CreateAccount нет в contract'е — заводим напрямую):\n"; \
	$(DC) exec -T pg-wallet $(PSQL) -c \
		"INSERT INTO accounts (id, owner_id, currency, created_at) VALUES (1, 101, 'RUB', now()), (2, 202, 'RUB', now()) ON CONFLICT (id) DO NOTHING" >/dev/null 2>&1; \
	echo "   счёт 1 (покупатель) и счёт 2 (автор), оба в RUB."
	@printf "\n1. Заносим покупателю 10000 копеек ПАРНОЙ проводкой от счёта 0\n"; \
	printf "   (условный внешний источник денег — не UPDATE balance, его тут нет):\n"; \
	OPID=$$(uuidgen | tr 'A-Z' 'a-z'); \
	DID=$$(uuidgen | tr 'A-Z' 'a-z'); \
	CID=$$(uuidgen | tr 'A-Z' 'a-z'); \
	$(DC) exec -T pg-wallet $(PSQL) -c \
		"INSERT INTO ledger_entries (id, account_id, amount_cents, direction, operation_id, reason, reference_id, created_at) VALUES ('$$DID', 0, 10000, 'debit', '$$OPID', 'demo_topup', 'demo', now()), ('$$CID', 1, 10000, 'credit', '$$OPID', 'demo_topup', 'demo', now())" >/dev/null; \
	echo "   готово."
	@printf "\n2. Баланс покупателя ПОСЛЕ пополнения:\n"; \
	$(MAKE) -s wallet-balance ID=1
	@printf "\n3. ReserveFunds на 3000 копеек (idempotency_key=demo-order-1):\n"; \
	RESP=$$(grpcurl -plaintext -connect-timeout 2 \
		-d '{"account_id":1,"amount":{"amount_cents":3000,"currency":"RUB"},"idempotency_key":"demo-order-1"}' \
		localhost:$(WALLET_GRPC_PORT) gosplash.wallet.v1.WalletService/ReserveFunds); \
	echo "$$RESP"; \
	RESID=$$(echo "$$RESP" | jq -r .reservationId); \
	if [ -z "$$RESID" ] || [ "$$RESID" = "null" ]; then \
		echo; echo "ReserveFunds не вернул reservation_id — wallet запущен? (make run-wallet)"; \
		exit 1; \
	fi; \
	printf "\n4. Баланс покупателя ПОСЛЕ резерва (available уменьшился, reserved вырос):\n"; \
	$(MAKE) -s wallet-balance ID=1; \
	printf "\n5. CommitFunds — списываем покупателю, зачисляем автору (счёт 2):\n"; \
	grpcurl -plaintext -connect-timeout 2 \
		-d "{\"reservation_id\":\"$$RESID\",\"payee_account_id\":2,\"idempotency_key\":\"demo-commit-1\"}" \
		localhost:$(WALLET_GRPC_PORT) gosplash.wallet.v1.WalletService/CommitFunds; \
	printf "\n6. Проводки обеих сторон операции (ledger_entries):\n"; \
	$(MAKE) -s wallet-ledger; \
	printf "\n7. Инвариант двойной записи по ВСЕЙ истории:\n"; \
	$(MAKE) -s wallet-check

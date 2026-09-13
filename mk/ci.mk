# ─────────────────────────────────────────────────────────────────────────────
## CI (фаза 6)
#
# Цели здесь НЕ дублируют то, что уже есть в корневом Makefile (tidy, fmt,
# vet, build, test, test-cover, lint, proto-lint, proto-breaking, tools) —
# ci-local их ВЫЗЫВАЕТ по очереди, повторяя ровно тот набор проверок, что
# гоняет .github/workflows/ci.yml, а не переопределяет их заново здесь.
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: archcheck
archcheck: ## архитектурные правила: outbox-only Kafka, ports & adapters, изоляция сервисов (tools/archcheck)
	go run ./tools/archcheck/cmd/archcheck -root . -v

.PHONY: lint-fix
lint-fix: ## golangci-lint --fix там, где линтер это умеет (staticcheck, revive, gocritic, errorlint, copyloopvar — см. .golangci.yml)
	@for m in $(MODULES); do echo "→ $$m"; (cd $$m && golangci-lint run --fix ./...) || exit 1; done

# coverage — НЕ то же самое, что test-cover из корневого Makefile:
# test-cover печатает построчный вывод `go test -cover` (по пакету на
# строку, разного формата в разных модулях), а coverage сводит его к ОДНОЙ
# итоговой цифре на модуль через `go tool cover -func` — то, что удобно
# смотреть глазами разом по всем одиннадцати модулям, не то, что удобно
# для отладки конкретного пакета (для неё как раз test-cover).
.PHONY: coverage
coverage: ## суммарное покрытие по модулям, одна строка на модуль (go tool cover -func)
	@for m in $(MODULES); do \
		printf "%-30s" "$$m"; \
		tmp=$$(mktemp); \
		if ! (cd $$m && go test ./... -count=1 -coverprofile="$$tmp" >/dev/null 2>&1); then \
			echo "ошибка тестов"; \
		elif [ -s "$$tmp" ]; then \
			go tool cover -func="$$tmp" 2>/dev/null | tail -1 | awk '{print $$3}'; \
		else \
			echo "нет тестов"; \
		fi; \
		rm -f "$$tmp"; \
	done

# ci-local сознательно НЕ включает integration-тесты (go test -tags=integration):
# им нужен Docker (testcontainers-go поднимает Postgres/Kafka/Redis/CH/ES) и
# минуты, а не секунды, на прогон — и .github/workflows/ci.yml сам держит их
# в отдельном job'е с continue-on-error именно по этой причине (см. комментарий
# там). ci-local — это то, что гоняешь перед КАЖДЫМ пушем; для контейнерных
# тестов есть отдельная команда, см. вывод в конце этой цели.
.PHONY: ci-local
ci-local: ## прогнать локально весь обязательный набор CI: fmt, vet, lint, archcheck, proto-lint, test, build
	$(MAKE) --no-print-directory fmt
	$(MAKE) --no-print-directory vet
	$(MAKE) --no-print-directory lint
	$(MAKE) --no-print-directory archcheck
	$(MAKE) --no-print-directory proto-lint
	$(MAKE) --no-print-directory test
	$(MAKE) --no-print-directory build
	@printf "\n\033[1mci-local: обязательный набор зелёный.\033[0m\n"
	@printf "Не включены (см. комментарий в mk/ci.mk):\n"
	@printf "  proto-breaking     — make proto-breaking (нужна история git с main)\n"
	@printf "  integration-тесты  — go test -tags=integration ./... в нужном модуле (нужен Docker)\n"

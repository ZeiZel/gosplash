# ─────────────────────────────────────────────────────────────────────────────
## Фаза 6: нагрузка и хаос (deploy/k6/**, deploy/chaos/**, docs/perf/**)
#
# DC и API уже определены в главном Makefile. k6 запускается через
# `docker run grafana/k6` (см. deploy/k6/README.md) — локальный k6-бинарь
# не нужен. host.docker.internal — тот же приём, каким NGINX
# (deploy/compose/docker-compose.gateway.yml) достаёт до media/catalog/wallet/order,
# запущенных на хосте через `go run`, а не в контейнерах; --add-host нужен,
# чтобы это работало не только на Docker Desktop, но и на Linux.
#
# chaos-* цели — тонкие обёртки над deploy/chaos/*.sh: сама логика (что
# ломаем, что проверять) — в шапке каждого скрипта, здесь только точка
# входа с параметрами по умолчанию.
# ─────────────────────────────────────────────────────────────────────────────

K6_IMAGE    ?= grafana/k6:latest
K6_BASE_URL ?= http://host.docker.internal:58080
K6_RUN       = docker run --rm -i --add-host=host.docker.internal:host-gateway \
                 -v "$(CURDIR)/deploy/k6:/scripts" -e BASE_URL=$(K6_BASE_URL) $(K6_IMAGE) run

.PHONY: k6-upload
k6-upload: ## k6: всплеск загрузок media — rate limiter отдаёт 429 (это успех, не сбой)
	$(K6_RUN) /scripts/upload_burst.js

.PHONY: k6-catalog
k6-catalog: ## k6: чтение каталога, тёплый/холодный кэш (make k6-catalog MODE=warm|cold)
	$(K6_RUN) -e CACHE_MODE=$(or $(MODE),warm) /scripts/catalog_read.js

.PHONY: k6-order
k6-order: ## k6: покупки через сагу + идемпотентность под нагрузкой (make k6-order REPEAT=0.2)
	$(K6_RUN) -e REPEAT_RATE=$(or $(REPEAT),0.2) -e CONCURRENT_REPEAT_RATE=$(or $(CONCURRENT),0.1) /scripts/place_order.js

.PHONY: chaos-kafka
chaos-kafka: ## chaos: уронить Kafka на N секунд, проверить outbox (make chaos-kafka N=30)
	./deploy/chaos/kill-kafka.sh $(or $(N),30)

.PHONY: chaos-wallet
chaos-wallet: ## chaos: убить wallet посреди саги — breaker и компенсация (make chaos-wallet N=15)
	./deploy/chaos/kill-wallet.sh $(or $(N),15)

.PHONY: chaos-catalog
chaos-catalog: ## chaos: убить catalog перед GrantLicense, показать компенсацию саги (фаза 3)
	./deploy/chaos/kill-catalog.sh

.PHONY: chaos-slow-pg
chaos-slow-pg: ## chaos: сетевая задержка на PostgreSQL через tc netem (make chaos-slow-pg TARGET=pg-catalog)
	./deploy/chaos/slow-pg.sh $(or $(TARGET),pg-catalog)

.PHONY: perf-report
perf-report: ## сводка прогонов в docs/perf: дата, что изменено, подтверждён ли критерий
	@files=$$(find docs/perf -maxdepth 1 -name '*.md' ! -name 'README.md' ! -name 'TEMPLATE.md' 2>/dev/null | sort); \
	if [ -z "$$files" ]; then \
		echo "docs/perf пуст — ни одного реального прогона ещё не записано."; \
		echo "Формат и как снять числа: docs/perf/README.md; шаблон: docs/perf/TEMPLATE.md."; \
		exit 0; \
	fi; \
	for f in $$files; do \
		printf "\033[1m%s\033[0m\n" "$$f"; \
		grep -E '^\- \*\*(Дата и время прогона|Что изменено|Подтверждён)' "$$f" | sed 's/^/  /' || echo "  (не заполнен по шаблону — см. docs/perf/TEMPLATE.md)"; \
		echo; \
	done

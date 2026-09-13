# ─────────────────────────────────────────────────────────────────────────────
## Фаза 5 search: явный маппинг, идемпотентный консьюмер через версионирование,
## поиск с fuzziness/фасетами/search_after, переиндексация через alias
## (docs/PLAN.md, docs/adr/0017-elasticsearch-protiv-tsvector.md).
#
# Цели этого файла предполагают уже поднятую инфраструктуру (`make up` —
# среди прочего поднимает elasticsearch, deploy/compose/elasticsearch.yml)
# и уже запущенный search-сервис. Цели `run-search` в главном Makefile пока
# нет (заведение — работа интегратора при вливании фазы, см.
# services/catalog/../mk/catalog.mk с тем же примечанием) — локально сервис
# поднимается вручную: `cd services/search && go run ./cmd/search`.
#
# DC определён в главном Makefile и уже доступен здесь — этот файл
# подключается через `-include mk/*.mk` в его конце.
# ─────────────────────────────────────────────────────────────────────────────

## Elasticsearch и поиск (фаза 5)

# ES — адрес кластера с хоста. 59200, а не 9200: тот же перенос портов в
# диапазон 5xxxx, что и у остальной инфраструктуры проекта (см. комментарий
# в главном Makefile про порт 5xxxx), значение — из .env.example
# (ELASTICSEARCH_ADDRS).
ES := $(or $(ES_ADDR),http://localhost:59200)

# ALIAS — имя alias'а поиска (SEARCH_INDEX_ALIAS в .env.example). Индекс
# никогда не адресуется напрямую — см. services/search/README.md и
# internal/adapters/es/client.go.
ALIAS := $(or $(ALIAS),listings)

# SEARCH_GRPC_PORT — порт gRPC search-сервиса (SEARCH_GRPC_ADDR в
# .env.example). P, а не собственное имя переменной — тот же приём, что и
# GRPC_PORT в mk/grpc.mk: один и тот же способ (grpcurl localhost:PORT)
# работает для любого сервиса проекта.
SEARCH_GRPC_PORT := $(or $(P),9107)

.PHONY: es-health
es-health: ## здоровье кластера Elasticsearch
	@curl -sS "$(ES)/_cluster/health?pretty" || echo "elasticsearch недоступен на $(ES) — сервис запущен? (make up)"

.PHONY: es-mapping
es-mapping: ## явный маппинг индекса поиска (make es-mapping ALIAS=listings)
	@curl -sS "$(ES)/$(ALIAS)/_mapping?pretty" || echo "не смог получить маппинг $(ALIAS) на $(ES)"

.PHONY: es-count
es-count: ## сколько документов в индексе поиска прямо сейчас
	@curl -sS "$(ES)/$(ALIAS)/_count" | jq -c '{count, alias: "$(ALIAS)"}' \
		2>/dev/null || echo "не смог посчитать документы в $(ALIAS) на $(ES)"

.PHONY: search-q
search-q: ## поиск из командной строки (make search-q Q=закат)
	@if [ -z "$(Q)" ]; then \
		echo "Использование: make search-q Q=<текст запроса> [TAGS=закат,море] [AUTHOR=1] [LIMIT=10]"; \
		exit 1; \
	fi
	@tags_json=$$(echo "$(TAGS)" | awk -F, '{ \
		if (NF == 0 || $$0 == "") { print "[]"; next } \
		printf "["; for (i=1;i<=NF;i++) { printf "%s\"%s\"", (i>1?",":""), $$i }; print "]" \
	}'); \
	req=$$(jq -n --arg q "$(Q)" --argjson tags "$$tags_json" --argjson author "$(or $(AUTHOR),0)" --argjson limit "$(or $(LIMIT),10)" \
		'{query: $$q, limit: $$limit, filters: {tags: $$tags, author_id: $$author}}'); \
	grpcurl -plaintext -connect-timeout 2 -d "$$req" \
		localhost:$(SEARCH_GRPC_PORT) gosplash.search.v1.SearchService/Search

.PHONY: search-reindex
search-reindex: ## переиндексация каталога в новый индекс без даунтайма (через alias)
	@cd tools/search-reindex && go run ./cmd/search-reindex \
		-catalog="$(or $(CATALOG_ADDR),localhost:9102)" \
		-alias="$(ALIAS)" \
		-addrs="$(ES)" \
		$(if $(DRY_RUN),-dry-run,) $(if $(KEEP_OLD),-keep-old,)

.PHONY: demo-search
demo-search: ## поиск с опечаткой и фасетами целиком (docs/adr/0017)
	@printf "\033[1mDemo search: fuzziness, filter-контекст, фасеты\033[0m\n\n"
	@printf "1. Здоровье кластера:\n"; \
	curl -sS "$(ES)/_cluster/health" | jq -c '{status, number_of_nodes}' 2>/dev/null || echo "elasticsearch недоступен"; \
	printf "\n2. Сколько карточек в индексе '$(ALIAS)':\n"; \
	curl -sS "$(ES)/$(ALIAS)/_count" | jq -c '.count' 2>/dev/null; \
	printf "\n3. Точный запрос 'закат':\n"; \
	grpcurl -plaintext -connect-timeout 2 -d '{"query":"закат","limit":5}' \
		localhost:$(SEARCH_GRPC_PORT) gosplash.search.v1.SearchService/Search 2>/dev/null | \
		jq -c '{total, hits: [.hits[]? | {listingId, title, score}]}' 2>/dev/null; \
	printf "\n4. Тот же запрос с опечаткой 'заакат' — fuzziness AUTO обязан найти то же самое:\n"; \
	grpcurl -plaintext -connect-timeout 2 -d '{"query":"заакат","limit":5}' \
		localhost:$(SEARCH_GRPC_PORT) gosplash.search.v1.SearchService/Search 2>/dev/null | \
		jq -c '{total, hits: [.hits[]? | {listingId, title, score}]}' 2>/dev/null; \
	printf "\n5. Фасеты по тегам на пустом запросе (filter-контекст не влияет на _score):\n"; \
	grpcurl -plaintext -connect-timeout 2 -d '{"limit":1}' \
		localhost:$(SEARCH_GRPC_PORT) gosplash.search.v1.SearchService/Search 2>/dev/null | \
		jq -c '.tagFacets // []' 2>/dev/null; \
	printf "\nЕсли всё пусто — индекс ещё не наполнен: make demo-catalog, дождись\n"; \
	printf "публикации catalog.listing.published, и повтори.\n"

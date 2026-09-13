# ─────────────────────────────────────────────────────────────────────────────
## Фаза 1-3 catalog: идемпотентный консьюмер, cache-aside, gRPC-контракт,
## лицензии (docs/PLAN.md, pkg/idempotency, pkg/redisx, pkg/grpcx)
#
# Цели этого файла предполагают уже поднятую инфраструктуру и запущенный
# catalog (`make up`, `make migrate`, `make run-catalog`) — как и остальные
# demo-* цели в проекте. DC, PSQL, API определены в главном Makefile.
# ─────────────────────────────────────────────────────────────────────────────

CATALOG_METRICS_PORT := $(or $(P),8202)
CATALOG_GRPC_PORT    := $(or $(P),9102)

.PHONY: catalog-cache
catalog-cache: ## hit/miss кэша карточек каталога из /metrics (redis_cache_hits/misses_total)
	@raw=$$(curl -sS "http://localhost:$(CATALOG_METRICS_PORT)/metrics" 2>/dev/null); \
	if [ -z "$$raw" ]; then \
		echo "catalog недоступен на :$(CATALOG_METRICS_PORT)/metrics — сервис запущен? (make run-catalog)"; \
		exit 1; \
	fi; \
	found=$$(echo "$$raw" | grep -E '^redis_cache_(hits|misses)_total\{[^}]*service="catalog"'); \
	if [ -z "$$found" ]; then \
		echo "у catalog ещё нет ни одного обращения к кэшу — сначала GET /catalog/listings/{id}"; \
		exit 0; \
	fi; \
	echo "$$found"; \
	hits=$$(echo "$$found" | awk '/hits_total/{sum+=$$NF} END{print sum+0}'); \
	misses=$$(echo "$$found" | awk '/misses_total/{sum+=$$NF} END{print sum+0}'); \
	echo; \
	awk -v h=$$hits -v m=$$misses 'BEGIN{ \
		t=h+m; \
		if (t==0) { print "hit ratio: n/a (0 обращений)"; exit } \
		printf "hit ratio: %d/%d = %.1f%%\n", h, t, (h/t)*100 \
	}'

.PHONY: catalog-top
catalog-top: ## топ-100 просмотренных карточек за сутки (Redis Sorted Set, pkg/redisx/topn.go)
	@curl -sS "$(API)/catalog/top" | jq .

.PHONY: catalog-licenses
catalog-licenses: ## выданные и отозванные лицензии из базы catalog (primary)
	@$(DC) exec -T pg-catalog $(PSQL) -c \
		"SELECT id, listing_id, buyer_id, order_id, status, granted_at, revoked_at FROM licenses ORDER BY granted_at DESC LIMIT 20" \
		2>/dev/null || echo "таблицы licenses ещё нет (make migrate-catalog)"

.PHONY: grpc-catalog
grpc-catalog: ## GetListing через grpcurl (make grpc-catalog ID=photo-1)
	@if [ -z "$(ID)" ]; then \
		echo "Использование: make grpc-catalog ID=<listing_id>"; \
		exit 1; \
	fi
	@grpcurl -plaintext -connect-timeout 2 \
		-d '{"id":"$(ID)"}' \
		localhost:$(CATALOG_GRPC_PORT) gosplash.catalog.v1.CatalogService/GetListing

.PHONY: demo-catalog
demo-catalog: ## фаза 1-3 целиком: uploaded → draft → thumbnail-ready → published → кэш → просмотр
	@printf "\033[1mDemo catalog: идемпотентный индексатор + cache-aside + gRPC\033[0m\n\n"
	@printf "1. Лента (только published, keyset-курсор):\n"; \
	first=$$(curl -sS "$(API)/catalog/listings?limit=5"); \
	echo "$$first" | jq -c '{count, next_cursor, listings: [.listings[] | {id, status, author_name}]}'; \
	id=$$(echo "$$first" | jq -r '.listings[0].id // empty'); \
	if [ -z "$$id" ]; then \
		echo; \
		echo "Лента пуста — сначала make demo-media (создаёт фото и триггерит индексатор)."; \
		exit 0; \
	fi; \
	echo "$$id" > /tmp/gosplash-demo-catalog-id; \
	printf "\n2. Карточка $$id — первый GET (промах кэша, идёт в базу):\n"; \
	curl -sS "$(API)/catalog/listings/$$id" | jq -c '{id, status, author_name, thumbnails}'; \
	printf "\n3. Тот же GET ещё раз (обязан быть hit — см. make catalog-cache):\n"; \
	curl -sS "$(API)/catalog/listings/$$id" | jq -c '{id, status}'; \
	printf "\n4. Счётчик просмотров вырос — топ-100 за сутки:\n"; \
	curl -sS "$(API)/catalog/top" | jq -c '[.top[] | select(.ListingID == "'"$$id"'")]'; \
	printf "\nЕсли шаг 4 пуст — событие просмотра ещё в буфере батчера (флаш по\n"; \
	printf "таймеру/размеру, см. internal/adapters/kafka/view_batcher.go), подожди пару секунд.\n"

.PHONY: demo-catalog-cursor
demo-catalog-cursor: ## демонстрация keyset-пагинации: вторая страница по next_cursor
	@page1=$$(curl -sS "$(API)/catalog/listings?limit=2"); \
	cursor=$$(echo "$$page1" | jq -r '.next_cursor'); \
	echo "страница 1:"; echo "$$page1" | jq -c '.listings[] | {id, published_at}'; \
	if [ -z "$$cursor" ] || [ "$$cursor" = "null" ]; then \
		echo; echo "next_cursor пуст — это последняя страница (или карточек меньше limit)."; \
		exit 0; \
	fi; \
	echo; echo "next_cursor: $$cursor"; \
	page2=$$(curl -sS "$(API)/catalog/listings?limit=2&cursor=$$cursor"); \
	echo "страница 2:"; echo "$$page2" | jq -c '.listings[] | {id, published_at}'

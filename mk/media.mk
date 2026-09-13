# ─────────────────────────────────────────────────────────────────────────────
## Фаза 1-2 media: transactional outbox, rate limiting (docs/adr/0008-*,
## pkg/outbox, pkg/redisx)
#
# Цели этого файла предполагают уже поднятую инфраструктуру и запущенный
# media (`make up`, `make migrate`, `make run-media`) — как и остальные
# demo-* цели в проекте. DC, PSQL, API определены в главном Makefile.
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: media-outbox
media-outbox: ## сколько записей в outbox на каждом шарде media и сколько из них не отправлено
	@for s in pg-shard-0 pg-shard-1; do \
		printf "%-12s " $$s; \
		$(DC) exec -T $$s $(PSQL) -tAc \
			"SELECT count(*) || ' всего, ' || count(*) FILTER (WHERE published_at IS NULL) || ' не отправлено' FROM outbox" \
			2>/dev/null || echo "таблицы ещё нет (make migrate-media)"; \
	done
	@printf "\nЕсли 'не отправлено' растёт и не убывает — Relay либо не запущен\n"
	@printf "(он часть процесса media, см. run-media), либо не может достучаться\n"
	@printf "до Kafka; см. метрику outbox_pending на :8201/metrics.\n"

# Сколько подряд идущих запросов делает demo-media-ratelimit. Больше, чем
# MEDIA_UPLOAD_RATE_CAPACITY из .env (по умолчанию 10) — иначе продемонстрировать
# 429 нечем: первые CAPACITY запросов бакет пропустит, даже если пополнение
# нулевое.
RATELIMIT_N ?= 15

.PHONY: media-ratelimit
media-ratelimit: ## продемонстрировать 429: N запросов подряд от одного пользователя (make media-ratelimit N=20)
	@n=$(or $(N),$(RATELIMIT_N)); \
	printf "\033[1mUpload rate limit: %s запросов подряд от user_id=%s\033[0m\n" "$$n" "$(U)"; \
	printf "Лимит берётся из MEDIA_UPLOAD_RATE_CAPACITY/MEDIA_UPLOAD_RATE_REFILL_PER_SEC\n"; \
	printf "(.env, по умолчанию 10 токенов, пополнение 0.5/с) — token bucket в Redis,\n"; \
	printf "pkg/redisx.Allow. Первые CAPACITY запросов должны пройти (201), дальше — 429\n"; \
	printf "с заголовком Retry-After.\n\n"; \
	for i in $$(seq 1 $$n); do \
		code=$$(curl -sS -D /tmp/gosplash-ratelimit-headers -o /tmp/gosplash-ratelimit-body -w '%{http_code}' -X POST $(API)/media/upload \
			-H "X-User-Id: $(U)" \
			-F "file=@$(F)" \
			-F "title=rate-limit демо $$i"); \
		retry=$$(grep -i '^Retry-After:' /tmp/gosplash-ratelimit-headers 2>/dev/null | tr -d '\r'); \
		printf "  запрос %2d: код %s" "$$i" "$$code"; \
		if [ "$$code" = "429" ]; then printf "  %s" "$$retry"; fi; \
		printf "\n"; \
	done; \
	rm -f /tmp/gosplash-ratelimit-headers /tmp/gosplash-ratelimit-body; \
	printf "\nЕсли ВСЕ запросы вернули 201 — либо F=$(F) не создан (make demo-file),\n"; \
	printf "либо Redis недоступен: тогда лимитер работает fail-open (см. комментарий\n"; \
	printf "к allowUpload в internal/adapters/http/handler.go) и пропускает всё —\n"; \
	printf "это ОСОЗНАННОЕ поведение, а не баг демонстрации.\n"

.PHONY: demo-media
demo-media: .env demo-file ## фаза 1-2 media целиком: загрузка → outbox → Kafka → появление в каталоге
	@printf "\033[1mDemo media: transactional outbox от загрузки до каталога\033[0m\n\n"
	@printf "1. Загружаю фото от user_id=$(U)\n"
	@resp=$$(curl -sS -X POST $(API)/media/upload -H "X-User-Id: $(U)" -F "file=@$(F)" -F "title=demo-media"); \
	echo "$$resp" | jq -c '{id, shard, mime, width, height, status}'; \
	id=$$(echo "$$resp" | jq -r '.id'); \
	echo "$$id" > /tmp/gosplash-demo-media-id; \
	printf "\n2. Строка сразу в базе (metadata) и в outbox (событие ЕЩЁ не в Kafka,\n"; \
	printf "   Relay заберёт её в течение OUTBOX_POLL_INTERVAL):\n\n"; \
	shard=$$(curl -sS "$(API)/media/photos/$$id?user_id=$(U)" | jq -r '.shard'); \
	printf "   шард: %s\n" "$$shard"; \
	$(DC) exec -T pg-shard-$$shard $(PSQL) -tAc \
		"SELECT 'outbox: '||count(*)||' строк, published_at IS NULL у '||count(*) FILTER (WHERE published_at IS NULL) FROM outbox WHERE aggregate_id = '$$id'"; \
	printf "\n3. Жду, пока Relay опубликует (до 5с)...\n"; \
	sleep 5; \
	$(DC) exec -T pg-shard-$$shard $(PSQL) -tAc \
		"SELECT 'outbox теперь: published_at IS NULL у '||count(*) FILTER (WHERE published_at IS NULL)||' из '||count(*) FROM outbox WHERE aggregate_id = '$$id'"; \
	printf "\n4. Каталог получил событие через Kafka + подтянул подробности по gRPC:\n\n"; \
	curl -sS "$(API)/catalog/listings?limit=5" | jq -c '[.listings[] | select(.id == "'"$$id"'") // empty]'; \
	printf "\nЕсли на шаге 4 пусто — дай catalog ещё пару секунд и разница увидится\n"; \
	printf "в demo-media-status (полит на PollInterval + доставку Kafka + консьюмер).\n"

.PHONY: demo-media-status
demo-media-status: ## текущий статус последнего фото из demo-media (make demo-media сначала)
	@if [ ! -f /tmp/gosplash-demo-media-id ]; then echo "сначала make demo-media"; exit 1; fi; \
	id=$$(cat /tmp/gosplash-demo-media-id); \
	curl -sS "$(API)/media/photos/$$id?user_id=$(U)" | jq -c '{id, status, mime, width, height}'

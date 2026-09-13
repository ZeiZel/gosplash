# ─────────────────────────────────────────────────────────────────────────────
## thumbnail-worker (фаза 1, docs/PLAN.md — шаг 1.6)
#
# DC, PSQL, API, U, F уже определены в главном Makefile; run-thumbs — там же.
# Здесь — то, что появилось вместе с самим сервисом: что лежит в бакете
# превью, в каком статусе фото на шардах media и сквозной сценарий
# "загрузил → дождался трёх превью".
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: thumbs-tree
thumbs-tree: ## содержимое бакета превью (make thumbs-tree U=1 — только этого пользователя)
	@if [ -n "$(U)" ]; then \
		$(DC) run --rm --entrypoint sh minio-init -c \
			"mc alias set local http://minio:9000 gosplash gosplash >/dev/null && mc ls --recursive local/gosplash-thumbnails/$(U) 2>/dev/null || echo 'пусто — превью для пользователя $(U) ещё нет'"; \
	else \
		$(DC) run --rm --entrypoint sh minio-init -c \
			"mc alias set local http://minio:9000 gosplash gosplash >/dev/null && mc ls --recursive local/gosplash-thumbnails"; \
	fi

.PHONY: thumbs-status
thumbs-status: ## сколько фото в каждом статусе (uploaded/processing/ready) по шардам media
	@for s in pg-shard-0 pg-shard-1; do \
		printf "\n\033[1m%s\033[0m\n" "$$s"; \
		$(DC) exec -T $$s $(PSQL) -tAc \
			"SELECT status || ': ' || count(*) FROM photos GROUP BY status ORDER BY status" 2>/dev/null \
			|| echo "  таблицы ещё нет — прогони make migrate-media"; \
	done

.PHONY: demo-thumbs
demo-thumbs: .env ## загрузить фото и дождаться появления трёх превью (make demo-thumbs U=1)
	@$(MAKE) --no-print-directory demo-file
	@u="$(or $(U),1)"; \
	printf "\033[1m1. Загружаю фото от пользователя %s\033[0m\n" "$$u"; \
	resp=$$(curl -sS -X POST $(API)/media/upload \
		-H "X-User-Id: $$u" \
		-F "file=@$(F)" \
		-F "title=demo-thumbs"); \
	id=$$(echo "$$resp" | jq -r '.id // empty'); \
	if [ -z "$$id" ]; then \
		echo "загрузка не удалась: $$resp"; \
		exit 1; \
	fi; \
	printf "   photo_id=%s, статус сразу после загрузки: %s\n" "$$id" "$$(echo "$$resp" | jq -r '.status')"; \
	printf "\n\033[1m2. Жду media.photo.uploaded → thumbnail-worker → media.photo.thumbnail-ready\033[0m\n"; \
	printf "   (нужен запущенный make run-thumbs; таймаут — 30с)\n\n"; \
	ok=0; \
	for i in $$(seq 1 30); do \
		found=$$($(DC) run --rm --entrypoint sh minio-init -c \
			"mc alias set local http://minio:9000 gosplash gosplash >/dev/null && mc ls --recursive local/gosplash-thumbnails/$$u 2>/dev/null | grep -c $$id || true" 2>/dev/null); \
		found=$$(echo "$$found" | tail -1); \
		found=$${found:-0}; \
		printf "   попытка %2d/30: превью найдено %s из 3\r" "$$i" "$$found"; \
		if [ "$$found" -ge 3 ] 2>/dev/null; then ok=1; break; fi; \
		sleep 1; \
	done; \
	echo; \
	if [ "$$ok" = "1" ]; then \
		echo "\033[1mготово\033[0m — три превью на месте:"; \
		$(DC) run --rm --entrypoint sh minio-init -c \
			"mc alias set local http://minio:9000 gosplash gosplash >/dev/null && mc ls --recursive local/gosplash-thumbnails/$$u" 2>/dev/null | grep "$$id"; \
		printf "\n   статус фото после обработки: "; \
		curl -sS "$(API)/media/photos?user_id=$$u&limit=50" | jq -r --arg id "$$id" '.photos[] | select(.id==$$id) | .status'; \
	else \
		echo "не дождался трёх превью за 30с — проверь, что run-thumbs и run-media запущены (make lag G=thumbnail-worker)"; \
		exit 1; \
	fi

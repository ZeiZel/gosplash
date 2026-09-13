# ─────────────────────────────────────────────────────────────────────────────
## Стенды: вся инфраструктура одной командой
# ─────────────────────────────────────────────────────────────────────────────
#
# У проекта ДВА способа поднять одну и ту же инфраструктуру, и выбор между
# ними — это не вкусовщина, у них разные задачи:
#
#   docker compose  — быстрый стенд для РАЗРАБОТКИ. Инфраструктура в
#                     контейнерах, сервисы запускаются с хоста через
#                     go run (make run-all). Правка кода видна за секунды,
#                     отладчик цепляется к обычному процессу.
#                     Поднимается за минуту.
#
#   kind            — стенд, приближенный к ПРОДУ: настоящий Kubernetes,
#                     сервисы в контейнерах, Helm-чарты, probes, HPA,
#                     PersistentVolumeClaim. Ловит то, чего compose не
#                     покажет в принципе: неверный readiness, потерю данных
#                     при вытеснении пода, зависший graceful shutdown,
#                     балансировку gRPC через headless Service.
#                     Первый подъём долгий — собираются девять образов.
#
# Держать оба одновременно можно: порты у них не пересекаются (compose
# публикует 5xxxx на хост, в kind ходят через kubectl port-forward).
#
# ЕСЛИ В ОКРУЖЕНИИ НАСТРОЕН HTTP-ПРОКСИ. kubectl уважает HTTP_PROXY/
# HTTPS_PROXY и попытается достучаться до API-сервера kind через него —
# а kind живёт на localhost, до которого прокси обычно не имеет отношения.
# Симптом: kubectl висит или отвечает connection refused, хотя кластер
# поднят. Лечится исключением локальных адресов:
#
#   export NO_PROXY=localhost,127.0.0.1,::1
#
# либо запуском конкретной команды с очищенными переменными:
#
#   env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY make infra-k8s
#
# Целям docker compose это не нужно — они ходят через сокет Docker.
#
# Ниже — по две команды на стенд: поднять и остановить. Всё остальное
# (частичный подъём, логи, статус) — в разделах «Инфраструктура» главного
# Makefile и «Фаза 6: Kubernetes/kind» в mk/k8s.mk.

# ─────────────────────────────────────────────────────────────────────────────
# DOCKER COMPOSE
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: infra-compose
infra-compose: .env ## СТЕНД 1: поднять всю инфраструктуру в docker compose и проверить её
	@printf "$(B)Поднимаю инфраструктуру в docker compose$(R)\n"
	@printf "  PostgreSQL ×7, Kafka + UI, Redis + UI, MinIO, Temporal + UI,\n"
	@printf "  ClickHouse, Elasticsearch, наблюдаемость, NGINX\n\n"
	$(DC) up -d
	@printf "\n$(B)Жду, пока компоненты станут готовы$(R)\n"
	@# Ожидание в ДВА шага, и второй шаг обязателен.
	@#
	@# Шаг 1 — docker healthcheck. Работает только для контейнеров, у которых
	@# healthcheck описан: PostgreSQL, Kafka, Redis, MinIO, Temporal.
	@for i in $$(seq 1 60); do \
		not_ready=$$($(DC) ps --format '{{.Service}} {{.Status}}' 2>/dev/null | grep -c 'starting' || true); \
		[ "$$not_ready" = "0" ] && break; \
		printf "  ещё стартуют: %s\n" "$$not_ready"; sleep 5; \
	done
	@# Шаг 2 — опрос тех, у кого healthcheck НЕТ. Tempo и Loki поднимаются
	@# заметно дольше остальных: их ingester после старта выжидает интервал
	@# стабилизации и до его конца честно отвечает 503 «waiting 15s after
	@# being ready». Без этого шага doctor запускался бы слишком рано и
	@# печатал FAIL на полностью исправном стенде — то есть врал бы.
	@printf "  жду tempo и loki (у них нет healthcheck, прогрев ~40 с)\n"
	@for i in $$(seq 1 40); do \
		ok=0; \
		curl -fsS http://localhost:53200/ready >/dev/null 2>&1 && ok=$$((ok+1)); \
		curl -fsS http://localhost:53100/ready >/dev/null 2>&1 && ok=$$((ok+1)); \
		[ "$$ok" = "2" ] && break; \
		sleep 3; \
	done
	@printf "\n"
	@$(MAKE) --no-print-directory doctor
	@printf "\n$(B)Готово.$(R) Дальше:\n"
	@printf "  make migrate     накатить схемы во все базы\n"
	@printf "  make run-all     запустить сервисы с хоста\n"
	@printf "  make demo        сквозной сценарий\n"
	@printf "  make infra-compose-down   остановить\n\n"

.PHONY: infra-compose-down
infra-compose-down: ## СТЕНД 1: остановить docker compose, ДАННЫЕ СОХРАНИТЬ
	@printf "$(B)Останавливаю docker compose (тома остаются)$(R)\n"
	$(DC) down
	@printf "\nДанные в томах сохранены: следующий make infra-compose поднимет\n"
	@printf "стенд с теми же базами. Чтобы стереть всё — make infra-compose-purge\n\n"

.PHONY: infra-compose-purge
infra-compose-purge: ## СТЕНД 1: остановить И СТЕРЕТЬ данные (чистый старт)
	@printf "$(B)Останавливаю docker compose и удаляю тома$(R)\n"
	$(DC) down -v --remove-orphans
	@printf "\nТома удалены. Следующий подъём даст пустые базы —\n"
	@printf "не забудь make migrate.\n\n"

# ─────────────────────────────────────────────────────────────────────────────
# KUBERNETES (kind — Kubernetes IN Docker)
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: infra-k8s
infra-k8s: ## СТЕНД 2: поднять kind-кластер + всю инфраструктуру + миграции (БЕЗ сервисов)
	@printf "$(B)1/4 Кластер kind ($(KIND_CLUSTER))$(R)\n"
	@kind get clusters 2>/dev/null | grep -qx "$(KIND_CLUSTER)" \
		&& echo "  уже существует, пропускаю create" \
		|| kind create cluster --name $(KIND_CLUSTER) --config deploy/kind/cluster.yaml
	@printf "\n$(B)2/4 Образы инфраструктуры$(R)\n"
	@$(MAKE) --no-print-directory k8s-infra-images
	@printf "\n$(B)3/4 Инфраструктура$(R)\n"
	@$(MAKE) --no-print-directory k8s-infra-up
	@printf "\n$(B)4/4 Миграции$(R)\n"
	@$(MAKE) --no-print-directory k8s-migrate
	@printf "\n$(B)Готово.$(R) Инфраструктура в Kubernetes поднята, схемы накачены.\n"
	@printf "  make infra-k8s-check   проверить, что всё отвечает\n"
	@printf "  make kind-up           доложить сверху сами сервисы (соберёт образы)\n"
	@printf "  make infra-k8s-down    убрать инфраструктуру, кластер оставить\n\n"

.PHONY: infra-k8s-down
infra-k8s-down: ## СТЕНД 2: убрать сервисы и инфраструктуру, КЛАСТЕР ОСТАВИТЬ
	@printf "$(B)Убираю сервисы (helm) и инфраструктуру, кластер оставляю$(R)\n"
	@for c in $(K8S_CHARTS); do \
		helm uninstall $$c -n $(K8S_NAMESPACE) >/dev/null 2>&1 && echo "  удалён релиз $$c" || true; \
	done
	@# Удаление namespace уносит с собой и поды, и Service, и ConfigMap,
	@# и PersistentVolumeClaim. Именно PVC здесь и есть «данные»: без этого
	@# шага они переживут удаление namespace только если лежат вне него,
	@# а у нас все тома namespace-scoped.
	kubectl delete namespace $(K8S_NAMESPACE) --ignore-not-found --wait=true
	@printf "\nКластер kind остался — следующий make infra-k8s поднимет\n"
	@printf "инфраструктуру заново за минуту, без пересоздания узлов.\n"
	@printf "Чтобы снести и кластер — make infra-k8s-purge\n\n"

.PHONY: infra-k8s-purge
infra-k8s-purge: ## СТЕНД 2: снести kind-кластер целиком (узлы, образы, тома)
	@printf "$(B)Сношу кластер kind целиком$(R)\n"
	kind delete cluster --name $(KIND_CLUSTER)
	@printf "\nУзлы удалены вместе с containerd-хранилищем: пропали и загруженные\n"
	@printf "образы, и данные PVC. Следующий подъём будет долгим — образы\n"
	@printf "придётся собирать и загружать заново.\n\n"

.PHONY: infra-k8s-check
infra-k8s-check: ## СТЕНД 2: проверить, что инфраструктура в кластере отвечает
	@printf "$(B)Поды инфраструктуры$(R)\n"
	@kubectl -n $(K8S_NAMESPACE) get pods \
		-l 'app in (pg-shard-0,pg-shard-1,pg-catalog,pg-catalog-replica,pg-wallet,pg-order,pg-temporal,kafka,redis,minio,clickhouse,elasticsearch,temporal)' \
		--no-headers 2>/dev/null | awk '{printf "  %-28s %-6s %s\n",$$1,$$2,$$3}' || echo "  namespace пуст"
	@printf "\n$(B)Тома (данные переживают перезапуск пода)$(R)\n"
	@kubectl -n $(K8S_NAMESPACE) get pvc --no-headers 2>/dev/null \
		| awk '{printf "  %-26s %-8s %s\n",$$1,$$2,$$4}' || echo "  PVC нет"
	@printf "\n$(B)Проверка ответов$(R)\n"
	@printf "  postgres   "; kubectl -n $(K8S_NAMESPACE) exec deploy/pg-catalog -- pg_isready -U gosplash >/dev/null 2>&1 && echo OK || echo FAIL
	@printf "  kafka      "; kubectl -n $(K8S_NAMESPACE) exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && echo OK || echo FAIL
	@printf "  redis      "; kubectl -n $(K8S_NAMESPACE) exec deploy/redis -- redis-cli ping >/dev/null 2>&1 && echo OK || echo FAIL
	@# Проверяем health-эндпоинт, а НЕ "mc ls local": алиас local настраивает
	@# init-джоб (minio-bucket-init), в поде самого сервера его нет, и
	@# проверка через mc давала бы FAIL на полностью здоровом MinIO.
	@printf "  minio      "; kubectl -n $(K8S_NAMESPACE) exec deploy/minio -- curl -fsS http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1 && echo OK || echo FAIL
	@printf "  clickhouse "; kubectl -n $(K8S_NAMESPACE) exec deploy/clickhouse -- clickhouse-client -u gosplash --password gosplash -q "SELECT 1" >/dev/null 2>&1 && echo OK || echo FAIL
	@printf "  elastic    "; kubectl -n $(K8S_NAMESPACE) exec deploy/elasticsearch -- curl -fsS localhost:9200/_cluster/health >/dev/null 2>&1 && echo OK || echo FAIL
	@printf "  temporal   "; kubectl -n $(K8S_NAMESPACE) exec deploy/temporal -- temporal operator cluster health --address 127.0.0.1:7233 >/dev/null 2>&1 && echo OK || echo FAIL
	@printf "\n"

# ─────────────────────────────────────────────────────────────────────────────
# ОБА СТЕНДА
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: stands
stands: ## какой стенд сейчас поднят (оба, один, ни одного)
	@printf "$(B)docker compose$(R)\n"
	@n=$$($(DC) ps --format '{{.Service}}' 2>/dev/null | grep -c . || true); \
		if [ "$$n" -gt 0 ]; then printf "  поднят, контейнеров: %s\n" "$$n"; \
		else printf "  не поднят (make infra-compose)\n"; fi
	@printf "\n$(B)kubernetes (kind)$(R)\n"
	@if kind get clusters 2>/dev/null | grep -qx "$(KIND_CLUSTER)"; then \
		pods=$$(kubectl -n $(K8S_NAMESPACE) get pods --no-headers 2>/dev/null | grep -c . || true); \
		printf "  кластер %s есть, подов в namespace %s: %s\n" "$(KIND_CLUSTER)" "$(K8S_NAMESPACE)" "$$pods"; \
	else printf "  кластер не создан (make infra-k8s)\n"; fi
	@printf "\n"

.PHONY: stands-down
stands-down: ## остановить ОБА стенда (данные сохранить: тома compose и кластер kind остаются)
	@printf "$(B)Останавливаю оба стенда$(R)\n\n"
	-@$(MAKE) --no-print-directory infra-compose-down
	-@$(MAKE) --no-print-directory infra-k8s-down
	@printf "$(B)Оба стенда остановлены.$(R)\n"
	@printf "Тома docker compose и кластер kind сохранены. Полная очистка:\n"
	@printf "  make infra-compose-purge   стереть тома compose\n"
	@printf "  make infra-k8s-purge       снести кластер kind\n\n"

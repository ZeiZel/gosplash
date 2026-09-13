# ═════════════════════════════════════════════════════════════════════════════
# gosplash — команды на каждый день.
#
#   make            — список всех команд с описанием
#   make up         — поднять всю инфраструктуру
#   make migrate    — накатить схемы во все базы
#   make run-media  — запустить сервис
#
# Правило: всё, что делается руками чаще одного раза, попадает сюда.
# Через месяц ты не вспомнишь флаги kafka-console-consumer.sh — и не надо.
# ═════════════════════════════════════════════════════════════════════════════

.DEFAULT_GOAL := help
SHELL := /bin/bash

# Инструменты, поставленные через `go install`, лежат в GOPATH/bin.
# Добавляем его в PATH здесь, чтобы `make proto` работал независимо от того,
# прописан ли этот путь в твоём .zshrc.
export PATH := $(PATH):$(shell go env GOPATH)/bin

# Все compose-файлы лежат в deploy/compose, поэтому -f указывается явно:
# без него docker compose искал бы docker-compose.yml в текущей директории.
DC       := docker compose -f deploy/compose/docker-compose.yml
PSQL     := psql -U gosplash -d gosplash
KAFKA    := /opt/kafka/bin
BOOTSTRAP:= localhost:9092   # внутри контейнера kafka; с хоста — localhost:59094

# Цвета для help. Если терминал их не понимает — просто мусор, поэтому
# отключается через NO_COLOR=1.
ifndef NO_COLOR
  C := \033[36m
  B := \033[1m
  R := \033[0m
endif

# ─────────────────────────────────────────────────────────────────────────────
## Справка
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## показать этот список
	@printf "$(B)gosplash — доступные команды$(R)\n\n"
	@awk 'BEGIN {FS = ":.*?## "} \
		/^## / { printf "\n$(B)%s$(R)\n", substr($$0, 4); next } \
		/^[a-zA-Z0-9_.-]+:.*?## / { printf "  $(C)%-22s$(R) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@printf "\n$(B)Точки входа$(R)\n"
	@printf "  API (NGINX)      http://localhost:58080\n"
	@printf "  Kafka UI         http://localhost:58090\n"
	@printf "  RedisInsight     http://localhost:58091\n"
	@printf "  MinIO console    http://localhost:59001  (gosplash / gosplash)\n"
	@printf "  Temporal UI      http://localhost:58233\n"
	@printf "  Grafana          http://localhost:53000\n"
	@printf "  Prometheus       http://localhost:59090\n"
	@printf "\n  Порты вынесены в диапазон 5xxxx, чтобы не пересекаться\n"
	@printf "  с рабочими контейнерами на 5432/9000/9094.\n\n"

# ─────────────────────────────────────────────────────────────────────────────
## Окружение
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: env
env: .env ## создать .env из .env.example, если его ещё нет

# Правило с зависимостью, а не `cp` в лоб: make не тронет уже существующий
# .env и не затрёт твои правки. Все цели, которым нужны переменные,
# зависят от этого файла.
.env: .env.example
	@if [ -f .env ]; then \
		echo ".env уже есть — не трогаю (шаблон новее, сверься с .env.example)"; \
		touch .env; \
	else \
		cp .env.example .env && echo "создан .env из .env.example"; \
	fi

# ─────────────────────────────────────────────────────────────────────────────
## Инфраструктура
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: up
up: .env ## поднять всё (PG ×6, Kafka, Redis, MinIO, Temporal, наблюдаемость, NGINX)
	$(DC) up -d
	@$(MAKE) --no-print-directory status

.PHONY: down
down: ## остановить контейнеры, данные сохранить
	$(DC) down

.PHONY: nuke
nuke: ## остановить и УДАЛИТЬ все данные (volumes) — чистый старт
	$(DC) down -v --remove-orphans

.PHONY: restart
restart: down up ## перезапустить всё

.PHONY: up-pg up-kafka up-redis up-minio up-temporal up-obs up-gateway
up-pg: ## поднять только PostgreSQL (шарды media, catalog + реплика, wallet, order)
	$(DC) up -d pg-shard-0 pg-shard-1 pg-catalog pg-catalog-replica pg-wallet pg-order
up-kafka: ## поднять только Kafka + UI
	$(DC) up -d kafka kafka-init kafka-ui
up-redis: ## поднять только Redis + RedisInsight
	$(DC) up -d redis redis-ui
up-minio: ## поднять только MinIO
	$(DC) up -d minio minio-init
up-temporal: ## поднять только Temporal + UI
	$(DC) up -d pg-temporal temporal temporal-ui
up-obs: ## поднять только наблюдаемость (collector, tempo, loki, prometheus, grafana)
	$(DC) up -d otel-collector tempo loki promtail prometheus grafana
up-gateway: ## поднять/перечитать NGINX
	$(DC) up -d gateway

.PHONY: reload-gateway
reload-gateway: ## перечитать deploy/nginx/gateway.conf без рестарта
	$(DC) exec gateway nginx -t && $(DC) exec gateway nginx -s reload

.PHONY: ps status
ps: ## список контейнеров
	$(DC) ps
status: ## краткий статус: кто поднялся и на каком порту
	@$(DC) ps --format 'table {{.Service}}\t{{.Status}}\t{{.Ports}}'

.PHONY: logs
logs: ## логи всех контейнеров (make logs S=kafka — одного)
	$(DC) logs -f --tail=100 $(S)

.PHONY: doctor
doctor: ## проверить, что каждый компонент отвечает
	@printf "gateway   "; curl -fsS http://localhost:58080/healthz >/dev/null 2>&1 && echo OK || echo FAIL
	@printf "kafka     "; $(DC) exec -T kafka $(KAFKA)/kafka-broker-api-versions.sh --bootstrap-server $(BOOTSTRAP) >/dev/null 2>&1 && echo OK || echo FAIL
	@printf "redis     "; $(DC) exec -T redis redis-cli ping >/dev/null 2>&1 && echo OK || echo FAIL
	@printf "minio     "; curl -fsS http://localhost:59000/minio/health/live >/dev/null 2>&1 && echo OK || echo FAIL
	@printf "temporal  "; $(DC) exec -T temporal temporal operator cluster health --address temporal:7233 >/dev/null 2>&1 && echo OK || echo FAIL
	@printf "grafana   "; curl -fsS http://localhost:53000/api/health >/dev/null 2>&1 && echo OK || echo FAIL
	@printf "prometheus"; curl -fsS http://localhost:59090/-/healthy >/dev/null 2>&1 && echo " OK" || echo " FAIL"
	@printf "tempo     "; curl -fsS http://localhost:53200/ready >/dev/null 2>&1 && echo OK || echo FAIL
	@printf "loki      "; curl -fsS http://localhost:53100/ready >/dev/null 2>&1 && echo OK || echo FAIL
	@for s in pg-shard-0 pg-shard-1 pg-catalog pg-catalog-replica pg-wallet pg-order; do \
		printf "%-19s" $$s; \
		$(DC) exec -T $$s pg_isready -U gosplash >/dev/null 2>&1 && echo OK || echo FAIL; \
	done

# ─────────────────────────────────────────────────────────────────────────────
## PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: migrate
migrate: .env ## накатить схемы во все базы + настроить репликацию (make migrate ONLY=media)
	go run ./tools/automigrate/cmd/automigrate $(if $(ONLY),-only $(ONLY))

.PHONY: migrate-media migrate-catalog
migrate-media: .env ## миграции только media (оба шарда)
	go run ./tools/automigrate/cmd/automigrate -only media
migrate-catalog: .env ## миграции только catalog (primary, реплика, репликация)
	go run ./tools/automigrate/cmd/automigrate -only catalog

.PHONY: psql-shard0 psql-shard1 psql-catalog psql-catalog-replica psql-wallet psql-order
psql-shard0: ## psql в media shard 0
	$(DC) exec -it pg-shard-0 $(PSQL)
psql-shard1: ## psql в media shard 1
	$(DC) exec -it pg-shard-1 $(PSQL)
psql-catalog: ## psql в catalog (primary)
	$(DC) exec -it pg-catalog $(PSQL)
psql-catalog-replica: ## psql в реплику catalog
	$(DC) exec -it pg-catalog-replica $(PSQL)
psql-wallet: ## psql в wallet
	$(DC) exec -it pg-wallet $(PSQL)
psql-order: ## psql в order
	$(DC) exec -it pg-order $(PSQL)

.PHONY: pg-tables
pg-tables: ## показать таблицы во всех базах разом
	@for s in pg-shard-0 pg-shard-1 pg-catalog pg-catalog-replica pg-wallet pg-order; do \
		printf "\n=== $$s ===\n"; \
		$(DC) exec -T $$s $(PSQL) -c '\dt' 2>/dev/null || echo "недоступна"; \
	done

.PHONY: pg-partitions
pg-partitions: ## показать партиции (появятся в фазах 1 и 4)
	@printf "\n=== outbox на шардах media ===\n"
	@for s in pg-shard-0 pg-shard-1; do \
		$(DC) exec -T $$s $(PSQL) -tAc \
		"SELECT '$$s: '||inhrelid::regclass FROM pg_inherits WHERE inhparent='outbox'::regclass ORDER BY 1" 2>/dev/null; \
	done
	@printf "\n=== photo_views в catalog ===\n"
	@$(DC) exec -T pg-catalog $(PSQL) -tAc \
		"SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='photo_views'::regclass ORDER BY 1" 2>/dev/null || true

.PHONY: pg-shard-balance
pg-shard-balance: ## сколько строк photos легло на каждый шард
	@for s in pg-shard-0 pg-shard-1; do \
		printf "%-12s " $$s; \
		$(DC) exec -T $$s $(PSQL) -tAc \
		"SELECT count(*) || ' photos, ' || count(DISTINCT user_id) || ' users' FROM photos" \
		2>/dev/null || echo "таблицы ещё нет"; \
	done

# ─────────────────────────────────────────────────────────────────────────────
## Kafka
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: topics
topics: ## список топиков с описанием партиций
	$(DC) exec -T kafka $(KAFKA)/kafka-topics.sh --bootstrap-server $(BOOTSTRAP) --describe

.PHONY: topic-tail
topic-tail: ## читать топик с начала (make topic-tail T=media.photo.uploaded)
	$(DC) exec -it kafka $(KAFKA)/kafka-console-consumer.sh \
		--bootstrap-server $(BOOTSTRAP) --topic $(T) --from-beginning \
		--property print.key=true --property print.partition=true

.PHONY: topic-send
topic-send: ## писать в топик руками (make topic-send T=media.photo.uploaded)
	$(DC) exec -it kafka $(KAFKA)/kafka-console-producer.sh \
		--bootstrap-server $(BOOTSTRAP) --topic $(T) --property parse.key=true --property key.separator=:

.PHONY: groups lag
groups: ## список consumer group
	$(DC) exec -T kafka $(KAFKA)/kafka-consumer-groups.sh --bootstrap-server $(BOOTSTRAP) --list
lag: ## отставание консьюмеров (make lag G=thumbnail-worker)
	$(DC) exec -T kafka $(KAFKA)/kafka-consumer-groups.sh --bootstrap-server $(BOOTSTRAP) --describe --group $(G)

.PHONY: topics-recreate
topics-recreate: ## пересоздать топики (сбросить все сообщения и offset'ы)
	$(DC) rm -sf kafka-init >/dev/null 2>&1 || true
	@for t in media.photo.uploaded media.photo.uploaded.retry media.photo.uploaded.dlq \
	          media.photo.thumbnail-ready media.photo.thumbnail-ready.retry media.photo.thumbnail-ready.dlq \
	          media.photo.deleted catalog.listing.published \
	          order.order.placed order.order.paid wallet.account.debited analytics.photo.viewed; do \
		$(DC) exec -T kafka $(KAFKA)/kafka-topics.sh --bootstrap-server $(BOOTSTRAP) --delete --topic $$t 2>/dev/null || true; \
	done
	$(DC) up -d kafka-init

# ─────────────────────────────────────────────────────────────────────────────
## Redis
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: redis-cli redis-keys redis-flush redis-monitor
redis-cli: ## интерактивный redis-cli
	$(DC) exec -it redis redis-cli
redis-keys: ## показать ключи с их TTL
	@$(DC) exec -T redis redis-cli --scan --count 100 | sort | while read k; do \
		printf "%-45s ttl=%s\n" "$$k" "$$($(DC) exec -T redis redis-cli ttl "$$k" | tr -d '\r')"; \
	done
redis-flush: ## очистить кэш целиком (проверить холодный старт)
	$(DC) exec -T redis redis-cli flushall
redis-monitor: ## смотреть команды в реальном времени — видно попадания и промахи
	$(DC) exec -it redis redis-cli monitor

# ─────────────────────────────────────────────────────────────────────────────
## MinIO
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: s3-ls s3-tree
s3-ls: ## список бакетов
	$(DC) run --rm --entrypoint sh minio-init -c \
		"mc alias set local http://minio:9000 gosplash gosplash >/dev/null && mc ls local"
s3-tree: ## содержимое бакетов (make s3-tree B=gosplash-thumbnails)
	$(DC) run --rm --entrypoint sh minio-init -c \
		"mc alias set local http://minio:9000 gosplash gosplash >/dev/null && mc ls --recursive local/$(or $(B),gosplash-originals)"

# ─────────────────────────────────────────────────────────────────────────────
## Temporal
# ─────────────────────────────────────────────────────────────────────────────

TCTL := $(DC) exec -T temporal temporal --address temporal:7233 --namespace gosplash

.PHONY: wf-list wf-show wf-terminate temporal-shell
wf-list: ## список воркфлоу (открытых и закрытых)
	$(TCTL) workflow list
wf-show: ## история воркфлоу по id (make wf-show W=order-123)
	$(TCTL) workflow show --workflow-id $(W)
wf-terminate: ## прибить зависший воркфлоу (make wf-terminate W=order-123)
	$(TCTL) workflow terminate --workflow-id $(W) --reason "manual"
temporal-shell: ## shell внутри контейнера temporal (полный CLI)
	$(DC) exec -it temporal sh

# ─────────────────────────────────────────────────────────────────────────────
## Protobuf
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: proto
proto: ## сгенерировать Go-код из proto/ в gen/go
	cd proto && buf generate

.PHONY: proto-lint proto-format proto-breaking proto-clean
proto-lint: ## линтер контрактов
	cd proto && buf lint
proto-format: ## отформатировать .proto
	cd proto && buf format -w
proto-breaking: ## не сломал ли я контракт относительно последнего коммита
	# Команда запускается из proto/, поэтому git-репозиторий на уровень выше,
	# а subdir указывает, где внутри него лежит модуль контрактов.
	cd proto && buf breaking --against '../.git#branch=HEAD,subdir=proto'
proto-clean: ## удалить сгенерированный код
	rm -rf gen/go

# ─────────────────────────────────────────────────────────────────────────────
## Go
# ─────────────────────────────────────────────────────────────────────────────

# В режиме workspace `go build ./...` из корня не видит пакеты подмодулей:
# паттерн ./... раскрывается только внутри одного модуля. Поэтому все
# go-цели обходят модули по списку.
MODULES := . \
           services/media services/catalog services/wallet services/order \
           services/thumbnail-worker services/analytics services/search \
           tools/automigrate tools/mediactl tools/kafka-reprocess

# Что собирать в ./bin — только то, у чего есть main.
BINARIES := services/media/cmd/media \
            services/analytics/cmd/analytics \
            services/search/cmd/search \
            services/catalog/cmd/catalog \
            services/wallet/cmd/wallet \
            services/order/cmd/order \
            services/order/cmd/worker \
            services/thumbnail-worker/cmd/thumbnail-worker \
            tools/automigrate/cmd/automigrate \
            tools/mediactl/cmd/mediactl \
            tools/kafka-reprocess/cmd/kafka-reprocess

.PHONY: tidy
tidy: ## go mod tidy во всех модулях + go work sync
	@for m in $(MODULES); do echo "→ $$m"; (cd $$m && go mod tidy) || exit 1; done
	go work sync

.PHONY: fmt vet build test test-cover lint
fmt: ## gofmt всего дерева
	gofmt -l -w .
vet: ## go vet по всем модулям
	@for m in $(MODULES); do (cd $$m && go vet ./...) || exit 1; done
	@echo "vet: чисто"
build: ## собрать все бинарники в ./bin
	@mkdir -p bin
	@for b in $(BINARIES); do \
		name=$$(basename $$b); \
		go build -o bin/$$name ./$$b || exit 1; \
		echo "  bin/$$name"; \
	done
test: ## прогнать тесты во всех модулях
	@for m in $(MODULES); do (cd $$m && go test ./... -count=1) || exit 1; done
test-cover: ## тесты с покрытием (по модулям)
	@for m in $(MODULES); do \
		printf "%-30s" $$m; \
		(cd $$m && go test ./... -count=1 -cover 2>&1 | grep -E 'coverage|no test files' | head -1) || true; \
	done
lint: ## golangci-lint (нужен make tools)
	@for m in $(MODULES); do (cd $$m && golangci-lint run ./...) || exit 1; done

.PHONY: tools
tools: ## поставить buf, плагины protoc и линтер
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/cmd/protoc-gen-grpc-gateway@latest
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	@echo "Убедись, что $$(go env GOPATH)/bin есть в PATH"

# ─────────────────────────────────────────────────────────────────────────────
## Запуск сервисов
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: run-media run-catalog run-wallet run-order run-worker run-thumbs
run-media: .env ## media-service: загрузка, шарды (HTTP :8101, gRPC :9101, метрики :8201)
	go run ./services/media/cmd/media
run-catalog: .env ## catalog-service: лента, реплика, Kafka-консьюмер (HTTP :8102, метрики :8202)
	go run ./services/catalog/cmd/catalog
run-wallet: .env ## wallet-service — фаза 3
	go run ./services/wallet/cmd/wallet
run-order: .env ## order-service, API — фаза 3
	go run ./services/order/cmd/order
run-worker: .env ## order-service, Temporal worker — фаза 3
	go run ./services/order/cmd/worker
run-thumbs: .env ## thumbnail-worker — фаза 1
	go run ./services/thumbnail-worker/cmd/thumbnail-worker

.PHONY: run-all
run-all: .env ## все готовые сервисы разом, Ctrl+C гасит всех
	@trap 'kill 0' EXIT INT TERM; \
	$(MAKE) --no-print-directory run-media   2>&1 | sed 's/^/[media  ] /' & \
	$(MAKE) --no-print-directory run-catalog 2>&1 | sed 's/^/[catalog] /' & \
	wait

# ─────────────────────────────────────────────────────────────────────────────
## Проверка фичи «загрузка фото»
# ─────────────────────────────────────────────────────────────────────────────

API  := http://localhost:58080

# U — id пользователя, F — файл. Короткие имена не случайны: USER и FILE
# make унаследовал бы из окружения (USER = имя твоего логина), и `?=`
# уже не смог бы подставить значение по умолчанию.
U ?= 1
F ?= /tmp/gosplash-demo.png

.PHONY: demo
demo: ## полный прогон фичи: загрузить от 4 пользователей и показать результат
	@$(MAKE) --no-print-directory demo-file
	@printf "\n\033[1m1. Загружаю от четырёх пользователей\033[0m\n"
	@printf "   media кладёт файл в MinIO, метаданные — в шард по hash(user_id),\n"
	@printf "   затем публикует media.photo.uploaded в Kafka.\n\n"
	@for u in 1 2 3 4; do \
		printf "   user %s → шард " $$u; \
		curl -sS -X POST $(API)/media/upload \
			-H "X-User-Id: $$u" \
			-F "file=@$(F)" \
			-F "title=фото пользователя $$u" | jq -r '.shard // .error'; \
	done
	@printf "\n\033[1m2. Прямой вызов media по gRPC\033[0m\n"
	@printf "   тот же сервис, но по контракту из proto/gosplash/media/v1/media.proto.\n\n"
	@id=$$(curl -sS "$(API)/media/photos?user_id=1&limit=1" | jq -r '.photos[0].id'); \
		go run ./tools/mediactl/cmd/mediactl -id $$id -user 1
	@printf "\n\033[1m3. Распределение по шардам\033[0m\n"
	@curl -sS $(API)/media/shards | jq
	@printf "\n\033[1m4. Каталог: карточки приехали через Kafka + gRPC\033[0m\n"
	@printf "   catalog получил событие, сходил в media по gRPC за подробностями\n"
	@printf "   и записал денормализованную карточку в свою базу.\n\n"
	@curl -sS "$(API)/catalog/listings?limit=10" | jq '.count, [.listings[] | {id, user_id, source_shard, title}]'
	@printf "\n\033[1m5. Репликация каталога\033[0m\n"
	@printf "   primary_rows — сколько записал консьюмер, replica_rows — сколько\n"
	@printf "   доехало по логической репликации. Разница = отставание реплики.\n\n"
	@curl -sS $(API)/catalog/replication | jq

.PHONY: demo-file
demo-file: ## создать тестовое изображение в /tmp (если его нет)
	@if [ ! -f $(F) ]; then \
		printf 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==' \
			| base64 --decode > $(F) && echo "создан $(F)"; \
	fi

.PHONY: upload
upload: demo-file ## загрузить фото (make upload F=~/photo.jpg U=7)
	@curl -sS -X POST $(API)/media/upload \
		-H "X-User-Id: $(U)" \
		-F "file=@$(F)" \
		-F "title=$(or $(TITLE),без названия)" | jq

.PHONY: shards
shards: ## сколько фото на каждом шарде media
	@curl -sS $(API)/media/shards | jq

.PHONY: my-photos
my-photos: ## мои фото из media (make my-photos U=1)
	@curl -sS "$(API)/media/photos?user_id=$(U)" | jq

.PHONY: feed
feed: ## лента каталога (читается с реплики)
	@curl -sS "$(API)/catalog/listings?limit=$(or $(LIMIT),20)" | jq

.PHONY: replication
replication: ## отставание реплики каталога: строки на primary и на реплике
	@printf "через API сервиса (dbresolver):\n"
	@curl -sS $(API)/catalog/replication | jq
	@printf "\nнапрямую в базах:\n"
	@printf "  primary  "; $(DC) exec -T pg-catalog $(PSQL) -tAc 'SELECT count(*) FROM listings' 2>/dev/null || echo "нет таблицы"
	@printf "  replica  "; $(DC) exec -T pg-catalog-replica $(PSQL) -tAc 'SELECT count(*) FROM listings' 2>/dev/null || echo "нет таблицы"
	@printf "\nсостояние подписки на реплике:\n"
	@$(DC) exec -T pg-catalog-replica $(PSQL) -c \
		"SELECT subname, pid IS NOT NULL AS running, received_lsn, latest_end_lsn \
		 FROM pg_stat_subscription" 2>/dev/null || true

.PHONY: grpc-media
grpc-media: ## вызвать media по gRPC (make grpc-media ID=<uuid> U=1)
	@go run ./tools/mediactl/cmd/mediactl -id $(ID) -user $(U)

.PHONY: grpc-list
grpc-list: ## какие gRPC-методы есть у media (нужен grpcurl)
	@grpcurl -plaintext localhost:9101 list gosplash.media.v1.MediaService

.PHONY: smoke
smoke: ## быстрый прогон публичных ручек через NGINX
	@printf "GET  /healthz            "; curl -s -o /dev/null -w '%{http_code}\n' $(API)/healthz
	@printf "GET  /media/healthz      "; curl -s -o /dev/null -w '%{http_code}\n' $(API)/media/healthz
	@printf "GET  /catalog/healthz    "; curl -s -o /dev/null -w '%{http_code}\n' $(API)/catalog/healthz
	@printf "GET  /media/readyz       "; curl -s -o /dev/null -w '%{http_code}\n' $(API)/media/readyz
	@printf "GET  /catalog/readyz     "; curl -s -o /dev/null -w '%{http_code}\n' $(API)/catalog/readyz
	@printf "GET  /catalog/listings   "; curl -s -o /dev/null -w '%{http_code}\n' $(API)/catalog/listings
	@printf "GET  /media/shards       "; curl -s -o /dev/null -w '%{http_code}\n' $(API)/media/shards

# ─────────────────────────────────────────────────────────────────────────────
## Наблюдаемость
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: metrics
metrics: ## сырые метрики сервиса (make metrics P=8201 — media, 8202 — catalog)
	@curl -sS http://localhost:$(or $(P),8201)/metrics | grep -v '^#' | grep -E 'http_server|kafka_|outbox_' | head -40

.PHONY: targets
targets: ## кого Prometheus реально скрейпит и с каким результатом
	@curl -sS http://localhost:59090/api/v1/targets \
		| jq -r '.data.activeTargets[] | "\(.labels.instance)  \(.health)  \(.lastError // "")"'

.PHONY: alerts
alerts: ## текущие алерты Prometheus
	@curl -sS http://localhost:59090/api/v1/rules \
		| jq -r '.data.groups[].rules[] | select(.type=="alerting") | "\(.state)  \(.name)"'

.PHONY: trace
trace: ## открыть трейс в Grafana по id (make trace ID=<trace_id>)
	@echo "http://localhost:53000/explore?schemaVersion=1&panes=%7B%22a%22:%7B%22datasource%22:%22tempo%22,%22queries%22:%5B%7B%22query%22:%22$(ID)%22,%22queryType%22:%22traceql%22%7D%5D%7D%7D&orgId=1"

.PHONY: logs-loki
logs-loki: ## последние строки логов из Loki (make logs-loki C=kafka)
	@curl -sS -G http://localhost:53100/loki/api/v1/query_range \
		--data-urlencode 'query={container="$(or $(C),kafka)"}' \
		--data-urlencode 'limit=20' \
		| jq -r '.data.result[].values[][1]' 2>/dev/null | tail -20

# ─────────────────────────────────────────────────────────────────────────────
## Демо по фазам (docs/PLAN.md)
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: demo-0
demo-0: ## фаза 0: проверить, что сервисы отдают метрики, трейсы и готовность
	@$(MAKE) --no-print-directory demo-file
	@printf "\n\033[1m1. Готовность сервисов\033[0m\n"
	@printf "   /healthz отвечает всегда, пока процесс жив.\n"
	@printf "   /readyz ходит в PostgreSQL и Kafka — останови любой из них,\n"
	@printf "   и он покраснеет, но перезапуска пода это не вызовет.\n\n"
	@printf "   media   "; curl -sS $(API)/media/readyz | jq -c '{ready, checks}'
	@printf "   catalog "; curl -sS $(API)/catalog/readyz | jq -c '{ready, checks}'
	@printf "\n\033[1m2. Загрузка — она же источник трейса\033[0m\n"
	@curl -sS -X POST $(API)/media/upload -H "X-User-Id: $(U)" -F "file=@$(F)" \
		-F "title=демо фазы 0" | jq -c '{id, shard, status}'
	@printf "\n\033[1m3. Метрики RED появились\033[0m\n\n"
	@curl -sS http://localhost:8201/metrics | grep -E '^http_server_requests_total' | head -5
	@printf "\n\033[1m4. Prometheus видит оба сервиса\033[0m\n\n"
	@$(MAKE) --no-print-directory targets
	@printf "\n\033[1m5. Трейс целиком\033[0m\n"
	@printf "   Grafana → Explore → Tempo → Search по service.name=media.\n"
	@printf "   В одном трейсе должно быть видно: HTTP-запрос → запрос в PostgreSQL →\n"
	@printf "   publish в Kafka → process в catalog → gRPC обратно в media.\n"
	@printf "   http://localhost:53000/explore\n\n"

# ─────────────────────────────────────────────────────────────────────────────
# Фрагменты по фазам.
#
# Цели, появляющиеся вместе с новой фазой, живут в отдельных файлах mk/*.mk,
# а не дописываются сюда. Причина простая: этот Makefile — общая для всех
# точка, и каждая фаза, правящая его середину, гарантированно сталкивается
# с соседней. Отдельный файл на тему такого столкновения не создаёт.
#
# Включается в конце, чтобы переменные выше (DC, API, PSQL, KAFKA) были уже
# определены, а секции фаз шли в `make help` после базовых.
-include mk/*.mk

.PHONY: clean
clean: ## удалить артефакты сборки
	rm -rf bin coverage.out

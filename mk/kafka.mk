# ─────────────────────────────────────────────────────────────────────────────
## Фаза 1: ретраи, DLQ, лаг (docs/PLAN.md, docs/adr/0007..0011)
# ─────────────────────────────────────────────────────────────────────────────
#
# Цели этого файла предполагают, что инфраструктура и сервисы уже подняты
# (`make up`, `make migrate`, и в отдельных терминалах `make run-media`,
# `make run-catalog`) — так же, как это уже предполагают `demo`/`demo-0`
# в главном Makefile. Здесь же ничего заново не поднимается — только
# сценарии поверх уже работающего стенда.

.PHONY: dlq-tail
dlq-tail: ## читать DLQ с начала (make dlq-tail T=media.photo.uploaded)
	$(DC) exec -it kafka $(KAFKA)/kafka-console-consumer.sh \
		--bootstrap-server $(BOOTSTRAP) --topic $(T).dlq --from-beginning \
		--property print.key=true --property print.partition=true --property print.headers=true

.PHONY: dlq-reprocess
dlq-reprocess: ## перелить DLQ обратно в топик (make dlq-reprocess T=media.photo.uploaded [LIMIT=10] [DRY=1])
	@if [ -z "$(T)" ]; then echo "нужен T=<топик, без .dlq>"; exit 1; fi
	cd tools/kafka-reprocess && KAFKA_BROKERS=$(BOOTSTRAP) go run ./cmd/kafka-reprocess \
		-topic $(T) $(if $(LIMIT),-limit $(LIMIT)) $(if $(DRY),-dry-run)

# Consumer group'ы проекта — в одном месте, чтобы не искать их по коду
# при каждой проверке лага. Ретрай-группы существуют, только пока где-то
# запущен соответствующий kafkax.RetryConsumer.
KAFKA_GROUPS := catalog thumbnail-worker catalog-retry thumbnail-worker-retry

.PHONY: consumer-lag
consumer-lag: ## отставание всех consumer group проекта (kafka-consumer-groups.sh + метрика kafka_consumer_lag)
	@printf "\033[1mCLI-срез (kafka-consumer-groups.sh)\033[0m\n"
	@for g in $(KAFKA_GROUPS); do \
		printf "\n\033[36m%s\033[0m\n" "$$g"; \
		$(DC) exec -T kafka $(KAFKA)/kafka-consumer-groups.sh \
			--bootstrap-server $(BOOTSTRAP) --describe --group $$g 2>/dev/null \
			|| echo "  группа ещё не создана — консьюмер этой группы не запускался"; \
	done
	@printf "\n\033[1mМетрика kafka_consumer_lag (Prometheus)\033[0m\n"
	@printf "   Та же величина, что и выше, но собранная через franz-go/kadm\n"
	@printf "   раз в 15с (pkg/kafkax/lag.go) — на неё смотрит дашборд и алерт.\n\n"
	@curl -sS 'http://localhost:59090/api/v1/query?query=kafka_consumer_lag' \
		| jq -c '.data.result[] | {group: .metric.group, topic: .metric.topic, partition: .metric.partition, lag: (.value[1] | tonumber)}' \
		|| echo "  Prometheus недоступен или метрика ещё не собрана"

# ─────────────────────────────────────────────────────────────────────────────
## Демо фазы 1 (docs/PLAN.md — «готово, когда проходят все четыре сценария»)
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: demo-kafka
demo-kafka: ## все 4 сценария фазы 1 разом (нужны: up, migrate, run-media, run-catalog)
	@printf "\033[1mФаза 1 — Kafka по-взрослому: 4 сценария (docs/PLAN.md)\033[0m\n"
	@$(MAKE) --no-print-directory demo-kafka-rebalance
	@$(MAKE) --no-print-directory demo-kafka-dlq
	@$(MAKE) --no-print-directory demo-kafka-outage
	@$(MAKE) --no-print-directory demo-kafka-reprocess

.PHONY: demo-kafka-rebalance
demo-kafka-rebalance: ## сценарий 1: ребаланс партиций между двумя инстансами catalog
	@printf "\n\033[1m1. Ребаланс на двух инстансах одной consumer group\033[0m\n"
	@printf "   Поднимаю ВТОРОЙ инстанс catalog в ТОЙ ЖЕ группе (KAFKA_CONSUMER_GROUP_CATALOG=catalog)\n"
	@printf "   на других портах, чтобы не конфликтовать с первым. В логе должно\n"
	@printf "   появиться 'kafka: партиции назначены' у обоих — Kafka сама поделила\n"
	@printf "   партиции media.photo.uploaded (6 штук) между двумя процессами.\n\n"
	@CATALOG_HTTP_ADDR=:8112 CATALOG_METRICS_ADDR=:8212 \
		go run ./services/catalog/cmd/catalog > /tmp/gosplash-catalog-2.log 2>&1 & \
		echo $$! > /tmp/gosplash-catalog-2.pid
	@sleep 6
	@printf "   Второй инстанс запущен, PID $$(cat /tmp/gosplash-catalog-2.pid). Его лог:\n\n"
	@grep -E "партиции (назначены|отозваны)" /tmp/gosplash-catalog-2.log \
		| sed 's/^/   /' || echo "   (сообщений о ребалансе ещё нет — смотри /tmp/gosplash-catalog-2.log)"
	@printf "\n   Останавливаю второй инстанс — партиции ребалансируются обратно первому.\n"
	@kill $$(cat /tmp/gosplash-catalog-2.pid) 2>/dev/null || true
	@rm -f /tmp/gosplash-catalog-2.pid

.PHONY: demo-kafka-dlq
demo-kafka-dlq: ## сценарий 2: битое сообщение уходит в DLQ, а не блокирует партицию
	@printf "\n\033[1m2. Битое сообщение → DLQ\033[0m\n"
	@printf "   Пишу мусорные байты (не protobuf) напрямую в media.photo.uploaded.\n"
	@printf "   catalog не сможет разобрать конверт — по коду это PERMANENT-ошибка\n"
	@printf "   (повторять бессмысленно), поэтому сообщение сразу уходит в DLQ\n"
	@printf "   с заголовками error/original_topic/original_partition/original_offset,\n"
	@printf "   а НЕ виснет на партиции навсегда.\n\n"
	@echo "не-protobuf-мусор-$$(date +%s)" | $(DC) exec -T kafka $(KAFKA)/kafka-console-producer.sh \
		--bootstrap-server $(BOOTSTRAP) --topic media.photo.uploaded
	@printf "   Сообщение отправлено. Читаю DLQ (до 15с ожидания):\n\n"
	@timeout 15 $(DC) exec -T kafka $(KAFKA)/kafka-console-consumer.sh \
		--bootstrap-server $(BOOTSTRAP) --topic media.photo.uploaded.dlq --from-beginning \
		--max-messages 1 --property print.headers=true --property print.key=true \
		|| echo "   ничего не прочитано за 15с — проверь, что run-catalog запущен"

.PHONY: demo-kafka-outage
demo-kafka-outage: ## сценарий 3: Kafka лежит 30с — ничего не теряется
	@printf "\n\033[1m3. Kafka недоступна 30 секунд\033[0m\n"
	@printf "   /readyz catalog должен покраснеть, пока Kafka лежит, и позеленеть\n"
	@printf "   обратно без перезапуска процесса — консьюмер сам переподключается\n"
	@printf "   (franz-go: PollFetches просто продолжает пытаться). Ничего не теряется:\n"
	@printf "   и то, что уже лежало в топике, и то, что придёт после восстановления,\n"
	@printf "   будет обработано штатно, offset'ы никуда не делись.\n\n"
	@printf "   readyz ДО остановки: "; curl -sS $(API)/catalog/readyz | jq -c '.checks.kafka // .checks'
	$(DC) stop kafka
	@printf "   Kafka остановлена, жду 30с...\n"
	@sleep 30
	@printf "   readyz ПОКА Kafka лежит: "; curl -sS $(API)/catalog/readyz | jq -c '.checks.kafka // .checks' || true
	$(DC) start kafka
	@printf "   Kafka снова поднимается, жду healthcheck...\n"
	@for i in $$(seq 1 30); do \
		$(DC) exec -T kafka $(KAFKA)/kafka-broker-api-versions.sh --bootstrap-server $(BOOTSTRAP) >/dev/null 2>&1 && break; \
		sleep 2; \
	done
	@printf "   readyz ПОСЛЕ восстановления: "; curl -sS $(API)/catalog/readyz | jq -c '.checks.kafka // .checks'
	@printf "\n   Обрати внимание: media публикует события СИНХРОННО (пока не подключён\n"
	@printf "   к pkg/outbox — это следующий шаг интеграции, docs/adr/0008-*), поэтому\n"
	@printf "   /media/upload, вызванный ИМЕННО в эти 30с, вернёт клиенту явную ошибку,\n"
	@printf "   а не потеряет событие молча. Загрузки ДО и ПОСЛЕ окна недоступности\n"
	@printf "   проходят и попадают в catalog как обычно.\n"

.PHONY: demo-kafka-reprocess
demo-kafka-reprocess: ## сценарий 4: переигровка DLQ через tools/kafka-reprocess
	@printf "\n\033[1m4. Переигровка DLQ обратно в топик\033[0m\n"
	@printf "   dry-run — только показать, что будет перелито:\n\n"
	@$(MAKE) --no-print-directory dlq-reprocess T=media.photo.uploaded LIMIT=5 DRY=1
	@printf "\n   Теперь по-настоящему:\n\n"
	@$(MAKE) --no-print-directory dlq-reprocess T=media.photo.uploaded LIMIT=5
	@printf "\n   Конверт и event_id при переливке не меняются (tools/kafka-reprocess\n"
	@printf "   копирует Value как есть) — именно по event_id идемпотентный consumer\n"
	@printf "   (processed_events, docs/PLAN.md 1.5 — таблица заводится в самих\n"
	@printf "   сервисах, вне зоны pkg/kafkax и tools/kafka-reprocess) отсекает событие,\n"
	@printf "   если оно уже было применено раньше по другому пути. Если причина\n"
	@printf "   попадания в DLQ (см. сценарий 2 — мусорные байты) не была исправлена,\n"
	@printf "   переигранное сообщение честно окажется в DLQ ещё раз — это ожидаемо:\n"
	@printf "   инструмент переливает, а не чинит данные.\n"

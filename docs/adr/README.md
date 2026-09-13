# Архитектурные решения

Каждый файл — одно решение по схеме **контекст → решение → последствия**.

Правило: раздел «последствия» обязан включать плохое. ADR, в котором решение
выглядит бесплатным, — это не решение, а реклама; через полгода никто не
вспомнит, чем за него заплатили, и первое же столкновение с ценой будет
воспринято как чей-то просчёт.

Номера выдаются заранее и не переиспользуются: ссылка вида «см. ADR 0009»
должна вести в одно и то же место всегда.

| № | Тема | Фаза | Статус |
|---|---|---|---|
| [0001](0001-struktura-repozitoriya.md) | Структура репозитория: миграция на раскладку спецификации | 0 | принято |
| [0002](0002-sohranyaem-shardirovanie-media.md) | Шардирование media сохраняется вопреки §13 | 0 | принято |
| [0003](0003-imenovanie-sobytiy-i-envelope.md) | Именование событий и обязательный Envelope | 0 | принято |
| [0004](0004-saga-srazu-na-temporal.md) | Сага сразу на Temporal | 0 | принято |
| [0005](0005-observability-stack.md) | Jaeger → OTel Collector + Tempo/Loki/Grafana | 0 | принято |
| [0006](0006-gorm-ostayotsya-pgx-tochechno.md) | GORM остаётся; pgx точечно | 0 | принято |
| [0007](0007-franz-go.md) | franz-go против sarama и kafka-go | 1 | принято |
| [0008](0008-transactional-outbox.md) | Transactional outbox против прямой публикации | 1 | принято |
| [0009](0009-klyuchi-particionirovaniya.md) | Ключи партиционирования топиков | 1 | принято |
| [0010](0010-retry-i-dlq.md) | Схема ретраев и DLQ | 1 | принято |
| [0011](0011-polling-relay-protiv-cdc.md) | Polling-relay против Debezium/CDC | 1 | принято |
| [0012](0012-redis-lock-protiv-advisory-lock.md) | Redis-лок против PostgreSQL advisory lock | 2 | принято |
| [0013](0013-cache-aside.md) | Cache-aside против write-through и refresh-ahead | 2 | принято |
| [0014](0014-ledger-append-only.md) | `ledger_entries` append-only с двойной записью | 3 | принято |
| [0015](0015-headless-service-dlya-grpc.md) | Headless Service для gRPC и балансировка HTTP/2 | 3 | принято |
| [0016](0016-clickhouse-kafka-engine.md) | ClickHouse через Kafka engine против push из Go | 4 | принято |
| [0017](0017-elasticsearch-protiv-tsvector.md) | Elasticsearch против tsvector + GIN в PostgreSQL | 5 | принято |
| [0018](0018-idempotency-http.md) | Идемпотентность HTTP: Redis против таблицы в PostgreSQL | 3 | принято |

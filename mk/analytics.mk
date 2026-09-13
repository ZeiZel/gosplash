# ─────────────────────────────────────────────────────────────────────────────
## Фаза 4: аналитика на ClickHouse (docs/PLAN.md, docs/adr/0016-*)
#
# Цели этого файла предполагают уже поднятую инфраструктуру (`make up`,
# который включает clickhouse.yml) — как и остальные demo-*/phase-* цели
# в проекте. DC, PSQL, API определены в главном Makefile.
# ─────────────────────────────────────────────────────────────────────────────

# Доступ к ClickHouse — те же значения, что в .env.example (CLICKHOUSE_*):
# HTTP-порт удобен для curl (простой текстовый протокол, POST-тело — это
# сам SQL, без клиента), native (59440) использует сам сервис и
# analytics-seed через clickhouse-go.
CH_HTTP := http://localhost:58123
CH_USER := gosplash
CH_PASS := gosplash
CH_DB   := gosplash

# curl к ClickHouse с уже подставленными кредами и базой — чтобы не повторять
# -u $(CH_USER):$(CH_PASS) в каждой цели этого файла.
CH_CURL := curl -s -u $(CH_USER):$(CH_PASS) "$(CH_HTTP)/?database=$(CH_DB)"

.PHONY: ch-cli
ch-cli: ## интерактивный clickhouse-client внутри контейнера
	@$(DC) exec -it clickhouse clickhouse-client --user $(CH_USER) --password $(CH_PASS) --database $(CH_DB)

.PHONY: ch-tables
ch-tables: ## список таблиц analytics и их размер на диске (system.parts)
	@$(CH_CURL) --data "SELECT table, formatReadableSize(sum(bytes_on_disk)) AS disk_size, sum(rows) AS row_count, count() AS parts FROM system.parts WHERE active AND database = '$(CH_DB)' GROUP BY table ORDER BY sum(rows) DESC FORMAT PrettyCompact" \
		|| echo "ClickHouse недоступен на $(CH_HTTP) — сервис поднят? (make up, или docker compose -f deploy/compose/docker-compose.yml up -d clickhouse)"

# Сколько строк засевать по умолчанию — то самое "5 миллионов" из задачи
# фазы. Переопределяется: make analytics-seed N=200000 для быстрой проверки
# без ожидания полного прогона.
ANALYTICS_SEED_N ?= 5000000

.PHONY: analytics-seed
analytics-seed: .env ## засеять N просмотров в photo_views батчами через clickhouse-go (make analytics-seed N=200000)
	@echo "Засеваю $(or $(N),$(ANALYTICS_SEED_N)) просмотров батчами (см. cmd/seed: почему не по одной строке)…"
	go run ./services/analytics/cmd/seed -n=$(or $(N),$(ANALYTICS_SEED_N))

# ─────────────────────────────────────────────────────────────────────────────
# demo-clickhouse — колоночное (ClickHouse) против строчного (PostgreSQL)
# хранения на ОДНОМ И ТОМ ЖЕ запросе: "топ-100 фото по просмотрам за неделю".
#
# ЧЕСТНОЕ ОГРАНИЧЕНИЕ ЭТОЙ ДЕМОНСТРАЦИИ: сравнение не претендует на строгий
# бенчмарк (общее железо под контейнерами, один прогон вместо серии, без
# отдельного прогрева) — оно иллюстративное, чтобы увидеть РАЗНИЦУ НА
# ПОРЯДОК, а не третий знак после запятой. Таблица в PostgreSQL создаётся
# заново при каждом прогоне (DROP+CREATE+INSERT) с тем же числом строк, что
# сейчас лежит в ClickHouse (make analytics-seed до этой цели), и с обычным
# btree-индексом по ts — то есть со схемой, которую честно завёл бы
# разработчик, попытавшись сделать то же самое в PostgreSQL, а не намеренно
# ухудшенной версией без индексов вообще.
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: demo-clickhouse
demo-clickhouse: ## топ-100 фото за неделю: ClickHouse vs PostgreSQL, оба числа (нужен analytics-seed)
	@rows=$$($(CH_CURL) --data "SELECT count() FROM photo_views" 2>/dev/null); \
	if [ -z "$$rows" ] || [ "$$rows" = "0" ]; then \
		echo "photo_views пуста — сначала make analytics-seed"; \
		exit 1; \
	fi; \
	echo "Строк в ClickHouse.photo_views: $$rows"; \
	echo; \
	echo "1) ClickHouse — топ-100 по просмотрам за неделю (photo_views_daily,"; \
	echo "   тот же запрос, что использует TopPhotos, см. internal/adapters/clickhouse/repository.go):"; \
	ch_time=$$(curl -s -o /dev/null -w '%{time_total}' -u $(CH_USER):$(CH_PASS) "$(CH_HTTP)/?database=$(CH_DB)" \
		--data "SELECT photo_id, sum(views) AS views FROM photo_views_daily WHERE day >= today() - 7 GROUP BY photo_id ORDER BY views DESC LIMIT 100 FORMAT Null"); \
	echo "   время: $${ch_time}s"; \
	echo; \
	echo "2) PostgreSQL — та же строка данных ($$rows строк), тот же запрос, обычный btree(ts)."; \
	echo "   Пересоздаю демо-таблицу под текущий объём (может занять время)…"; \
	$(DC) exec -T pg-catalog $(PSQL) -q -c " \
		DROP TABLE IF EXISTS analytics_demo_photo_views; \
		CREATE TABLE analytics_demo_photo_views ( \
			photo_id uuid NOT NULL, \
			author_id bigint NOT NULL, \
			viewer_id bigint, \
			ts timestamptz NOT NULL, \
			country text NOT NULL \
		); \
		INSERT INTO analytics_demo_photo_views (photo_id, author_id, viewer_id, ts, country) \
		SELECT \
			('11111111-1111-4111-8111-' || lpad(to_hex(1 + (g % 5000)), 12, '0'))::uuid, \
			1000 + (g % 500), \
			CASE WHEN g % 10 < 3 THEN NULL ELSE (g % 2000000) END, \
			now() - (random() * interval '30 days'), \
			(ARRAY['RU','US','DE','FR','KZ','BY','TR','IN','BR','CN'])[1 + (g % 10)] \
		FROM generate_series(1, $$rows) AS g; \
		CREATE INDEX ON analytics_demo_photo_views (ts); \
		ANALYZE analytics_demo_photo_views;" > /tmp/analytics-demo-pg-setup.log 2>&1; \
	if [ $$? -ne 0 ]; then \
		echo "   не смог создать демо-таблицу в pg-catalog — контейнер поднят? (make up)"; \
		tail -3 /tmp/analytics-demo-pg-setup.log; \
		exit 1; \
	fi; \
	pg_time=$$($(DC) exec -T pg-catalog $(PSQL) -X -c '\timing on' -c " \
		SELECT photo_id, count(*) AS views FROM analytics_demo_photo_views \
		WHERE ts >= now() - interval '7 days' \
		GROUP BY photo_id ORDER BY views DESC LIMIT 100" 2>&1 | grep -oE 'Time: [0-9.]+ ms' | tail -1); \
	if [ -z "$$pg_time" ]; then \
		echo "   не получил время выполнения от psql — вывод см. выше"; \
	else \
		echo "   время: $$pg_time"; \
	fi; \
	echo; \
	echo "Разница на порядок — и есть демонстрация: колоночное хранение читает"; \
	echo "только колонки (photo_id, ts, views), нужные ЭТОМУ запросу; строчное"; \
	echo "читает строку целиком (photo_id, author_id, viewer_id, ts, country)"; \
	echo "на каждую подходящую по индексу строку. Подробный разбор — README сервиса."

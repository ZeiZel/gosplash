# ─────────────────────────────────────────────────────────────────────────────
## Фаза 2: Redis — cache-aside, rate limit, лок
#
# DC и API уже определены в главном Makefile; сам Redis поднимается и
# инспектируется целями up-redis/redis-cli/redis-flush/redis-monitor оттуда же.
# Здесь — то, что появилось вместе с pkg/redisx: сравнение p99 с кэшем и без,
# hit ratio по метрикам и список Lua-скриптов, реально загруженных в Redis.
# ─────────────────────────────────────────────────────────────────────────────

# Сколько запросов в каждом прогоне demo-redis. 200 — компромисс: p99 из
# меньшей выборки — это фактически "почти самый медленный запрос", а не
# устойчивая оценка хвоста; больше — прогон становится заметно дольше без
# ощутимого выигрыша в точности для локальной демонстрации.
REDIS_DEMO_N ?= 200

.PHONY: demo-redis
demo-redis: .env ## фаза 2: p99 catalog listings с кэшем и без (make demo-redis N=500)
	@n=$(or $(N),$(REDIS_DEMO_N)); \
	url="$(API)/catalog/listings?limit=20"; \
	echo "URL: $$url  ($$n запросов на прогон)"; \
	echo; \
	echo "1. Без кэша — перед КАЖДЫМ запросом сбрасываю Redis, поэтому каждый"; \
	echo "   запрос гарантированно промахивается и идёт до PostgreSQL."; \
	times_cold=$$(for i in $$(seq 1 $$n); do \
		$(DC) exec -T redis redis-cli flushall >/dev/null; \
		curl -s -o /dev/null -w '%{time_total}\n' "$$url"; \
	done | sort -n); \
	echo "2. С кэшем — сбрасываю Redis один раз перед первым запросом, дальше"; \
	echo "   $$n запросов подряд на тот же URL: первый — промах и прогрев,"; \
	echo "   остальные — попадания в кэш."; \
	$(DC) exec -T redis redis-cli flushall >/dev/null; \
	times_warm=$$(for i in $$(seq 1 $$n); do \
		curl -s -o /dev/null -w '%{time_total}\n' "$$url"; \
	done | sort -n); \
	echo; \
	printf "               p50      p95      p99\n"; \
	printf "  без кэша  "; echo "$$times_cold" | awk '{a[NR]=$$1} END{ \
		p50=a[(NR>1)?int(NR*0.50)+1:1]; p95=a[(NR>1)?((int(NR*0.95)<NR)?int(NR*0.95)+1:NR):1]; \
		p99=a[(NR>1)?((int(NR*0.99)<NR)?int(NR*0.99)+1:NR):1]; \
		printf "%6.4fs  %6.4fs  %6.4fs\n", p50, p95, p99}'; \
	printf "  с кэшем   "; echo "$$times_warm" | awk '{a[NR]=$$1} END{ \
		p50=a[(NR>1)?int(NR*0.50)+1:1]; p95=a[(NR>1)?((int(NR*0.95)<NR)?int(NR*0.95)+1:NR):1]; \
		p99=a[(NR>1)?((int(NR*0.99)<NR)?int(NR*0.99)+1:NR):1]; \
		printf "%6.4fs  %6.4fs  %6.4fs\n", p50, p95, p99}'; \
	echo; \
	echo "Пока catalog не подключил pkg/redisx (шаг 2.2 плана) обе строки будут"; \
	echo "примерно равны — это не ошибка демо: разница появляется ровно тогда,"; \
	echo "когда сервис начинает реально кэшировать listing через GetOrLoad*."

.PHONY: redis-hitrate
redis-hitrate: ## hit ratio кэша из /metrics (make redis-hitrate S=catalog, порт по умолчанию 8202)
	@service="$(or $(S),catalog)"; \
	port="$(or $(P),8202)"; \
	raw=$$(curl -sS "http://localhost:$$port/metrics" 2>/dev/null); \
	if [ -z "$$raw" ]; then \
		echo "$$service недоступен на :$$port/metrics — сервис запущен? (make run-$$service)"; \
		exit 1; \
	fi; \
	found=$$(echo "$$raw" | grep -E '^redis_cache_(hits|misses)_total'); \
	if [ -z "$$found" ]; then \
		echo "у $$service ещё нет ни одного обращения через redisx.GetOrLoad* — метрик нет"; \
		exit 0; \
	fi; \
	echo "$$found"; \
	hits=$$(echo "$$raw" | awk '/^redis_cache_hits_total/{sum+=$$NF} END{print sum+0}'); \
	misses=$$(echo "$$raw" | awk '/^redis_cache_misses_total/{sum+=$$NF} END{print sum+0}'); \
	echo; \
	awk -v h=$$hits -v m=$$misses 'BEGIN{ \
		t=h+m; \
		if (t==0) { print "hit ratio: n/a (0 обращений)"; exit } \
		printf "hit ratio: %d/%d = %.1f%%\n", h, t, (h/t)*100 \
	}'

.PHONY: redis-lua
redis-lua: ## какие Lua-скрипты pkg/redisx реально загружены в Redis (EVALSHA-кэш)
	@echo "Источник — сами исходники pkg/redisx (token bucket из ratelimit.go,"; \
	echo "unlock/extend из lock.go), а не копия текста: тело скрипта вырезается"; \
	echo "прямо из .go-файла, поэтому SHA1 всегда совпадает с тем, что вычисляет"; \
	echo "redis.NewScript(...) внутри работающего сервиса."; \
	echo; \
	sha_tb=$$(sed -n '/^const tokenBucketScript = `$$/,/^`$$/p' pkg/redisx/ratelimit.go \
		| sed '1s/.*//; $$d' | $(DC) exec -T redis redis-cli -x script load | tr -d '"\r'); \
	ex_tb=$$($(DC) exec -T redis redis-cli script exists "$$sha_tb" | tr -d '\r'); \
	printf "  %-14s sha1=%s  cached=%s\n" "token-bucket" "$$sha_tb" "$$ex_tb"; \
	sha_un=$$(sed -n '/^const unlockScript = `$$/,/^`$$/p' pkg/redisx/lock.go \
		| sed '1s/.*//; $$d' | $(DC) exec -T redis redis-cli -x script load | tr -d '"\r'); \
	ex_un=$$($(DC) exec -T redis redis-cli script exists "$$sha_un" | tr -d '\r'); \
	printf "  %-14s sha1=%s  cached=%s\n" "unlock" "$$sha_un" "$$ex_un"; \
	sha_ex=$$(sed -n '/^const extendScript = `$$/,/^`$$/p' pkg/redisx/lock.go \
		| sed '1s/.*//; $$d' | $(DC) exec -T redis redis-cli -x script load | tr -d '"\r'); \
	ex_ex=$$($(DC) exec -T redis redis-cli script exists "$$sha_ex" | tr -d '\r'); \
	printf "  %-14s sha1=%s  cached=%s\n" "extend" "$$sha_ex" "$$ex_ex"

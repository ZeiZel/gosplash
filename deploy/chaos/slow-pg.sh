#!/usr/bin/env bash
#
# slow-pg.sh — вносит сетевую задержку на путь к PostgreSQL через tc netem.
#
# ЧТО ЛОМАЕМ И ЧЕМ
#   `tc qdisc add ... netem delay` меняет ПЛАНИРОВЩИК ПАКЕТОВ сетевого
#   интерфейса и требует capability NET_ADMIN у процесса, который его
#   вызывает. Ни один Postgres-сервис в deploy/compose/docker-compose.postgres.yml не
#   объявляет `cap_add: [NET_ADMIN]` (сознательно: он не нужен для работы
#   Postgres, и добавлять привилегию контейнеру ради одного chaos-скрипта —
#   расширять поверхность атаки постоянно ради теста, который запускается
#   изредка) — значит, `docker exec pg-catalog tc ...` гарантированно
#   упадёт с "Operation not permitted".
#
# PATTERN: сетевой sidecar в СЕТЕВОМ NAMESPACE цели, а не изменение самой
# цели. `docker run --network container:pg-catalog` подключает НОВЫЙ
# контейнер к сетевому стеку pg-catalog (тот же eth0, тот же IP) — и именно
# СЕТЕВОЙ SIDECAR, а не pg-catalog, нуждается в NET_ADMIN, потому что
# capability проверяется у процесса, который делает syscall, а не у
# владельца netns. Это значит: cap_add НЕ нужно дописывать в
# deploy/compose/docker-compose.postgres.yml (файл вне зоны ответственности этого скрипта
# и вообще не должен меняться ради теста) — весь chaos создаётся и
# разбирается СНАРУЖИ, обычным `docker run --rm`.
#
# ЧЕСТНО О ГРАНИЦАХ: это работает, только если сам docker-демон разрешает
# `--cap-add=NET_ADMIN` (обычный Docker Desktop/Linux — да; rootless docker
# или ограниченная политика — может быть нет). Если сайдкар не поднимается —
# ниже нет "тихого" пропуска шага, скрипт останавливается и печатает ДВА
# запасных варианта, которые НЕ требуют NET_ADMIN вообще:
#
#   А. pg_sleep в удерживаемой транзакции — не сеть, а конкретный запрос:
#        BEGIN; SELECT pg_sleep(30) FROM listings LIMIT 1 FOR UPDATE;
#      держит блокировку строки 30с в отдельной сессии (make psql-catalog),
#      конкурентные читатели той же строки встанут в очередь — годится,
#      чтобы показать поведение таймаутов запроса и пул соединений, но НЕ
#      эмулирует общую деградацию сети (round-trip каждого пакета).
#
#   Б. `docker update --cpus=0.1 pg-catalog` — не сеть, а ресурсы контейнера:
#      не требует NET_ADMIN и не меняет compose-файлы (это runtime-лимит,
#      не часть образа/конфига), режет Postgres по CPU и тем самым удлиняет
#      КАЖДЫЙ запрос под конкурентной нагрузкой — грубее netem (не даёт
#      контролируемую задержку в мс), но воспроизводимо и просто снимается
#      обратно (`docker update --cpus=0 pg-catalog`).
#
# КАК УБЕДИТЬСЯ, ЧТО ПОЧИНИЛОСЬ: qdisc снят (`tc qdisc show dev eth0` внутри
# netns цели — снова "qdisc noqueue" или пусто), задержка `psql -c 'SELECT 1'`
# вернулась к обычным долям миллисекунды.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DC=(docker compose -f "$ROOT/deploy/compose/docker-compose.yml")

TARGET="${1:-${TARGET:-pg-catalog}}"
DELAY="${DELAY:-500ms}"
JITTER="${JITTER:-100ms}"
DURATION="${DURATION:-60}"
NETSHOOT_IMAGE="${NETSHOOT_IMAGE:-nicolaka/netshoot:latest}"

if ! docker info >/dev/null 2>&1; then
	echo "docker недоступен (демон не запущен?)." >&2
	exit 1
fi

if ! "${DC[@]}" ps "$TARGET" --format '{{.State}}' 2>/dev/null | grep -q running; then
	echo "контейнер '$TARGET' сейчас не running — сначала 'make up' или 'make up-pg'." >&2
	exit 1
fi

sidecar_tc() {
	docker run --rm --cap-add=NET_ADMIN --network "container:$TARGET" "$NETSHOOT_IMAGE" "$@"
}

cleanup_ran=0
cleanup() {
	[[ "$cleanup_ran" == "1" ]] && return
	cleanup_ran=1
	echo
	echo "снимаю qdisc с $TARGET (если он там остался)..."
	sidecar_tc tc qdisc del dev eth0 root >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "── пробую сетевой sidecar (tc netem, требует NET_ADMIN) ────────────────"
if ! sidecar_tc tc qdisc add dev eth0 root netem delay "$DELAY" "$JITTER" distribution normal 2>/tmp/gosplash-slow-pg-err.log; then
	echo
	echo "НЕ ПОЛУЧИЛОСЬ добавить netem — вывод ошибки:" >&2
	sed 's/^/  /' /tmp/gosplash-slow-pg-err.log >&2 || true
	rm -f /tmp/gosplash-slow-pg-err.log
	cat >&2 <<EOF

Значит, у этого docker-демона --cap-add=NET_ADMIN недоступен даже для
отдельного sidecar-контейнера (см. заголовок файла). Запасные варианты,
НЕ требующие NET_ADMIN, — тоже описаны в заголовке файла:

  А. pg_sleep в удерживаемой транзакции (конкретный запрос, не вся сеть):
       make psql-catalog
       BEGIN;
       SELECT pg_sleep(30) FROM listings LIMIT 1 FOR UPDATE;
       -- в ДРУГОЙ вкладке параллельно читай /catalog/listings и смотри,
       -- как долго держится ответ, если запрос упирается в эту же строку.

  Б. Урезать ресурсы контейнера (грубее, но без привилегий):
       docker update --cpus=0.1 $TARGET
       ... нагрузка / make demo-redis ...
       docker update --cpus=0 $TARGET   # снять лимит обратно
EOF
	exit 1
fi
rm -f /tmp/gosplash-slow-pg-err.log

echo "задержка ${DELAY} ± ${JITTER} применена к $TARGET на ${DURATION}с."
echo
echo "проверь прямо сейчас (другой терминал):"
printf '  docker compose -f deploy/compose/docker-compose.yml exec -it %s \\\n' "$TARGET"
printf "      psql -U gosplash -d gosplash -c '\\\\timing on' -c 'SELECT 1'   # почувствуй задержку глазами\n"
echo "  make demo-redis N=50   # если TARGET=pg-catalog: p99 БЕЗ кэша должен заметно вырасти"
echo "  curl -s http://localhost:58080/catalog/readyz | jq .checks   # readyz может начать мигать, если превышен таймаут пинга"
echo

for ((i = 1; i <= DURATION; i++)); do
	printf "\r  осталось: %2ds" "$((DURATION - i + 1))"
	sleep 1
done
echo
echo "время вышло — снимаю задержку (trap уже сделает это при выходе)."

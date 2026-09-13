#!/usr/bin/env bash
#
# kill-kafka.sh — роняет Kafka на N секунд и поднимает обратно.
#
# ЧТО ЛОМАЕМ
#   Контейнер kafka (deploy/compose/kafka.yml) останавливается через
#   `docker compose stop`, а не `kill -9` изнутри контейнера: нас интересует
#   поведение КЛИЕНТОВ (media, catalog, thumbnail-worker) при недоступном
#   брокере, а не поведение самой Kafka при грязном шатдауне — это другой,
#   гораздо более узкий тест (не входит в объём фазы 6).
#
# ЧТО ДОЛЖНО ПРОИЗОЙТИ (docs/PLAN.md, фаза 1: «Kafka лежит 30с — ничего
# не теряется»; тот же сценарий уже есть как make demo-kafka-outage —
# этот скрипт делает то же самое, но как отдельный воспроизводимый chaos-
# артефакт, а не цель Makefile, специфичная для одного прогона демо):
#   1. media продолжает принимать POST /media/upload. Запись в PostgreSQL
#      (photos + outbox, ОДНА транзакция — pkg/outbox) не зависит от Kafka
#      вообще: событие ложится в outbox и ждёт публикации.
#   2. Relay (часть процесса media) видит недоступный брокер, ошибку
#      логирует, СТРОКУ В OUTBOX НЕ ПОМЕЧАЕТ published_at — вот почему
#      "ничего не теряется": то, что не уехало, остаётся в очереди, а не
#      исчезает.
#   3. /catalog/readyz краснеет по проверке Kafka (kafka metadata ping),
#      но САМ ПРОЦЕСС catalog не падает и не перезапускается — это
#      осознанная разница readiness/liveness (см. docs/PLAN.md 0.4).
#   4. После восстановления Kafka relay доставляет накопленное в течение
#      следующих нескольких OUTBOX_POLL_INTERVAL (по умолчанию 1с), и
#      outbox_pending возвращается к нулю без вмешательства человека.
#
# КАК УБЕДИТЬСЯ, ЧТО ПОЧИНИЛОСЬ — см. вывод скрипта внизу; коротко:
#   make media-outbox     — "не отправлено" должно сходить к 0 после подъёма
#   make consumer-lag     — лаг должен вернуться к обычному уровню
#   curl .../catalog/readyz | jq .checks.kafka   — снова true
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DC=(docker compose -f "$ROOT/deploy/compose/docker-compose.yml")
BOOTSTRAP="localhost:9092"
KAFKA_BIN="/opt/kafka/bin"

DOWNTIME="${1:-${DOWNTIME:-30}}"

if ! docker info >/dev/null 2>&1; then
	echo "docker недоступен (демон не запущен?) — поднять/уронить kafka нечем." >&2
	exit 1
fi

if ! "${DC[@]}" ps kafka --format '{{.State}}' 2>/dev/null | grep -q running; then
	echo "kafka сейчас не в состоянии running — сначала 'make up' или 'make up-kafka'." >&2
	exit 1
fi

echo "── до отключения ──────────────────────────────────────────────────────"
echo "readyz catalog (kafka-чек):"
curl -sS --max-time 2 http://localhost:58080/catalog/readyz 2>/dev/null \
	| jq -c '.checks.kafka // .checks // "недоступно"' 2>/dev/null || echo "  catalog недоступен на :58080 — readyz не проверить"

echo
echo "── роняю kafka на ${DOWNTIME}с ─────────────────────────────────────────"
"${DC[@]}" stop kafka

echo "загружай media прямо сейчас в ДРУГОМ терминале, например:"
echo "  make upload F=~/photo.jpg U=1"
echo "события лягут в outbox и останутся там НЕОТПРАВЛЕННЫМИ, пока Kafka лежит."
echo
for ((i = 1; i <= DOWNTIME; i++)); do
	printf "\r  осталось: %2ds" "$((DOWNTIME - i + 1))"
	sleep 1
done
echo

echo
echo "readyz catalog ПОКА Kafka лежит (ожидается ready=false / checks.kafka с ошибкой):"
curl -sS --max-time 2 http://localhost:58080/catalog/readyz 2>/dev/null \
	| jq -c '.checks.kafka // .checks // "недоступно"' 2>/dev/null || echo "  недоступно"

echo
echo "── поднимаю kafka обратно ──────────────────────────────────────────────"
"${DC[@]}" start kafka

echo "жду, пока брокер начнёт отвечать на api-versions (до 60с)..."
for _ in $(seq 1 30); do
	if "${DC[@]}" exec -T kafka "$KAFKA_BIN/kafka-broker-api-versions.sh" \
		--bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
		break
	fi
	sleep 2
done

echo
echo "readyz catalog ПОСЛЕ восстановления (ожидается снова ok):"
curl -sS --max-time 2 http://localhost:58080/catalog/readyz 2>/dev/null \
	| jq -c '.checks.kafka // .checks // "недоступно"' 2>/dev/null || echo "  недоступно"

cat <<'EOF'

── ЧТО ПРОВЕРИТЬ ДАЛЬШЕ ─────────────────────────────────────────────────────
  make media-outbox
      "не отправлено" по каждому шарду должно СХОДИТЬ К НУЛЮ в течение
      нескольких секунд (OUTBOX_POLL_INTERVAL) — Relay уже переподключился
      и разбирает накопленное. Если зависло — читай docs/runbooks/outbox.md.

  make consumer-lag
      лаг catalog/thumbnail-worker подрастёт за время простоя и должен
      вернуться к обычному уровню за разумное время после подъёма Kafka.
      Если продолжает расти — docs/runbooks/kafka-lag.md.

  make feed
      фото, загруженные ВО ВРЕМЯ простоя Kafka, обязаны появиться в ленте
      каталога после того, как relay их доставит — это и есть "ничего не
      потеряно", а не просто "сервис не упал".

  Grafana → Kafka и outbox (http://localhost:53000): визуально виден провал
  "Обработано сообщений" на время простоя и всплеск "Очередь outbox", оба
  возвращаются к норме без перезапуска процессов.
EOF

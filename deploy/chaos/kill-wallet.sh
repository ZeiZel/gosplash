#!/usr/bin/env bash
#
# kill-wallet.sh — убивает процесс wallet посреди саги покупки лицензии.
#
# ЧТО ЛОМАЕМ
#   wallet-service запущен НЕ в docker (`make run-wallet` — обычный `go run`
#   на хосте, см. корневой Makefile), поэтому "уронить" его — это найти PID,
#   слушающий WALLET_GRPC_ADDR (порт из .env, по умолчанию 9103), и убить
#   его `kill -9`. Это жёстче, чем Ctrl+C: имитирует настоящий крэш процесса,
#   а не штатное graceful shutdown (которое и так покрыто /readyz).
#
# ЧТО ДОЛЖНО ПРОИЗОЙТИ (docs/PLAN.md, критерий готовности фазы 3):
#   1. order размещается СРАЗУ (POST /orders не ждёт сагу, см. package doc
#      services/order/internal/adapters/http) — этот шаг wallet не трогает.
#   2. Temporal запускает PlaceOrderWorkflow: ReserveFunds — первый вызов
#      в wallet. Если wallet уже мёртв в этот момент — резерва не было,
#      компенсировать нечего (см. комментарий в workflow.go про
#      "деньги не были тронуты"), сага просто проваливается на первом шаге.
#   3. Если wallet умирает ПОЗЖЕ — после ReserveFunds, но до CommitFunds —
#      GrantLicense (catalog, от wallet не зависит) может успеть пройти,
#      а CommitFunds упадёт. Тогда failOrder прогоняет компенсации в
#      обратном порядке: RevokeLicense (catalog, не зависит от wallet —
#      сработает даже пока wallet мёртв) и ReleaseFunds (wallet,
#      criticalActivityOptions — БЕЗ ограничения попыток). ReleaseFunds
#      будет ретраиться, пока wallet не поднимется обратно — именно
#      поэтому "деньги возвращаются" может занять больше времени, чем
#      просто окно простоя wallet ниже.
#   4. Circuit breaker (pkg/resilience, см. его package doc) — после
#      нескольких неудачных вызовов подряд размыкается, и КАЖДЫЙ следующий
#      вызов в wallet возвращает ошибку за миллисекунды, НЕ дожидаясь
#      TCP-таймаута. Это и есть критерий "503 за 50мс, а не за 5с".
#
# ТОЧНОЕ окно "между ReserveFunds и CommitFunds" поймать бекэндом с точностью
# до шага саги отсюда невозможно — оба вызова локально занимают миллисекунды,
# а у этого скрипта нет доступа к внутренностям воркфлоу. Задержка ниже —
# ЛУЧШЕЕ ПРИБЛИЖЕНИЕ, не гарантия; скрипт честно проверяет и печатает, что
# получилось на самом деле, вместо того чтобы делать вид, что получилось
# ровно так, как задумано.
#
# КАК УБЕДИТЬСЯ, ЧТО ПОЧИНИЛОСЬ — см. вывод внизу; коротко: баланс покупателя
# после подъёма wallet и завершения компенсации должен совпасть с балансом
# ДО покупки (make wallet-balance / grpcurl GetBalance), а wallet-check
# (инвариант двойной записи) обязан сходиться в ноль.
set -euo pipefail

API="http://localhost:58080"
WALLET_GRPC_PORT="${WALLET_GRPC_PORT:-9103}"
WALLET_HTTP_PORT="${WALLET_HTTP_PORT:-8103}"
BUYER="${BUYER:-1}"
DOWNTIME="${1:-${DOWNTIME:-15}}"
PRE_KILL_DELAY="${PRE_KILL_DELAY:-0.2}" # см. пояснение выше

if ! command -v lsof >/dev/null 2>&1; then
	echo "нужен lsof, чтобы найти PID wallet по порту — на macOS он есть по умолчанию." >&2
	exit 1
fi

find_pid() {
	lsof -tiTCP:"$1" -sTCP:LISTEN 2>/dev/null | head -1
}

pid=$(find_pid "$WALLET_GRPC_PORT") || true
if [[ -z "$pid" ]]; then
	echo "на :$WALLET_GRPC_PORT никто не слушает — wallet уже не запущен?" >&2
	echo "подними его: make run-wallet" >&2
	exit 1
fi

listing="${LISTING_ID:-}"
if [[ -z "$listing" ]]; then
	listing=$(curl -sS --max-time 2 "$API/catalog/listings?limit=1" 2>/dev/null | jq -r '.listings[0].id // empty' 2>/dev/null) || true
fi
if [[ -z "$listing" ]]; then
	echo "нет опубликованной карточки для покупки — сначала make demo-media && make demo-catalog," >&2
	echo "либо передай LISTING_ID=<id> явно." >&2
	exit 1
fi

echo "── до убийства wallet ───────────────────────────────────────────────────"
echo "карточка: $listing"
echo "баланс покупателя $BUYER ДО покупки:"
grpcurl -plaintext -connect-timeout 2 -d "{\"account_id\":$BUYER}" \
	"localhost:$WALLET_GRPC_PORT" gosplash.wallet.v1.WalletService/GetBalance 2>/dev/null \
	|| echo "  grpcurl недоступен или wallet не отвечает — установи grpcurl (см. mk/grpc.mk)"

key="chaos-kill-wallet-$(date +%s%N)"
echo
echo "размещаю заказ (Idempotency-Key: $key)..."
order=$(curl -sS --max-time 5 -X POST "$API/orders" \
	-H "Idempotency-Key: $key" -H "Content-Type: application/json" \
	-d "{\"buyer_id\":$BUYER,\"listing_id\":\"$listing\"}")
echo "$order" | jq -c '.' 2>/dev/null || echo "$order"
order_id=$(echo "$order" | jq -r '.id // empty') || true
if [[ -z "$order_id" ]]; then
	echo "не удалось создать заказ, дальше нет смысла продолжать." >&2
	exit 1
fi

sleep "$PRE_KILL_DELAY"

echo
echo "── убиваю wallet (PID $pid, kill -9) ────────────────────────────────────"
kill -9 "$pid" 2>/dev/null || true

echo "первый вызов ПОСЛЕ убийства — насколько быстро он проваливается"
echo "(бьём напрямую в порт wallet, а не через NGINX: у /wallet/ в"
echo "gateway.conf нет отдельного /healthz, только проксирование как есть):"
curl -sS --max-time 5 -o /dev/null -w "  HTTP %{http_code}, %{time_total}s\n" \
	"http://localhost:$WALLET_HTTP_PORT/healthz" 2>/dev/null || echo "  connection refused (это и ожидается)"
echo "  (gRPC-вызов через wallet_saga увидит то же самое: connection refused"
echo "   почти мгновенно, а после нескольких таких подряд — ErrBreakerOpen,"
echo "   тоже мгновенно, без похода в сеть вообще; см. pkg/resilience/breaker.go)"

echo
echo "жду ${DOWNTIME}с, опрашивая статус заказа $order_id..."
for ((i = 1; i <= DOWNTIME; i++)); do
	status=$(curl -sS --max-time 2 "$API/orders/$order_id" 2>/dev/null | jq -r '.status // "?"') || true
	printf "  [%2ds] status=%s\n" "$i" "$status"
	[[ "$status" == "failed" || "$status" == "completed" ]] && break
	sleep 1
done

cat <<EOF

── подними wallet обратно ───────────────────────────────────────────────────
  make run-wallet

Пока wallet мёртв, ReleaseFunds (если резерв успел случиться) будет
бесконечно ретраиться (criticalActivityOptions, см. workflow.go) — сага
доедет до "failed" ТОЛЬКО после того, как wallet снова начнёт отвечать.
После make run-wallet подожди немного и проверь:

  curl -s $API/orders/$order_id | jq .
      ожидается status=failed (резерв был и откатился) ИЛИ status=completed
      (не успели — wallet умер уже после CommitFunds, деньги списаны штатно,
      это тоже валидный исход, просто не тот, что демонстрирует компенсацию).

  grpcurl -plaintext -d '{"account_id":$BUYER}' \\
      localhost:$WALLET_GRPC_PORT gosplash.wallet.v1.WalletService/GetBalance
      если status=failed — баланс должен совпасть с балансом ДО покупки
      (см. вывод в начале этого скрипта).

  make wallet-check
      инвариант двойной записи (SUM ledger_entries) обязан сходиться в 0
      независимо от исхода — docs/adr/0014-*.

  make wf-describe W=$order_id
      история саги: видно ReserveFunds/CommitFunds и, если понадобилось,
      ReleaseFunds/RevokeLicense как компенсации. UI: http://localhost:58233
EOF

#!/usr/bin/env bash
#
# kill-catalog.sh — убивает catalog так, чтобы GrantLicense в саге покупки
# гарантированно провалился, и показывает полную компенсацию.
#
# ЧТО ЛОМАЕМ
#   catalog-service, как и wallet, — процесс на хосте (`make run-catalog`),
#   а не docker-контейнер. Находим PID, слушающий CATALOG_GRPC_ADDR (порт
#   из .env, по умолчанию 9102 — заказ ходит в catalog по gRPC, не по REST),
#   и убиваем его `kill -9` ДО того, как размещаем заказ.
#
#   Слушатель порта каталог — не единственный способ поймать "перед
#   GrantLicense": шаги саги (ReserveFunds → GrantLicense → CommitFunds →
#   ConfirmOrder, workflow.go) выполняются локально за миллисекунды, поймать
#   catalog живым-но-убить-через-50мс скриптом снаружи ненадёжно. Поэтому
#   этот сценарий убивает catalog ЗАРАНЕЕ и держит мёртвым до конца — это
#   ровно тот сценарий, для которого написана вся ветка failOrder в
#   workflow.go, и ровно то, что docs/PLAN.md называет критерием готовности
#   фазы 3: "при падении catalog деньги возвращаются".
#
# ЧТО ДОЛЖНО ПРОИЗОЙТИ:
#   1. ReserveFunds (wallet) — catalog не нужен, проходит успешно. Деньги
#      покупателя РЕЗЕРВИРУЮТСЯ (ledger_entries), это ещё не списание.
#   2. GrantLicense (catalog) — catalog мёртв, вызов проваливается. По
#      forwardActivityOptions (workflow.go) Temporal ретраит до 5 раз с
#      экспоненциальным backoff (~30с суммарно), затем сдаётся.
#   3. failOrder прогоняет компенсации В ОБРАТНОМ порядке:
#        RevokeLicense(catalog) — тоже упадёт (catalog мёртв), но это
#          criticalActivityOptions: БЕЗ ограничения попыток, будет
#          ретраиться, пока catalog не поднимется (см. п. ниже).
#        ReleaseFunds(wallet) — catalog здесь не нужен, пройдёт успешно
#          НЕЗАВИСИМО от того, поднят catalog или нет.
#   4. Итог: заказ переходит в "compensating", а не сразу в "failed" — сага
#      ЖДЁТ, пока RevokeLicense получится (или пока не подымешь catalog).
#      Это ожидаемое поведение: RevokeLicense — не оптимизация, а гарантия,
#      что лицензия точно не останется висеть, если GrantLicense каким-то
#      образом всё же успел выполниться до сетевого сбоя (см. комментарий
#      про "GrantLicense технически выполнился, но activity вернула ошибку"
#      в workflow.go).
#
# КАК УБЕДИТЬСЯ, ЧТО ПОЧИНИЛОСЬ — см. вывод внизу.
set -euo pipefail

API="http://localhost:58080"
CATALOG_GRPC_PORT="${CATALOG_GRPC_PORT:-9102}"
CATALOG_HTTP_PORT="${CATALOG_HTTP_PORT:-8102}"
BUYER="${BUYER:-1}"
WALLET_GRPC_PORT="${WALLET_GRPC_PORT:-9103}"
POLL_TIMEOUT="${POLL_TIMEOUT:-60}"

if ! command -v lsof >/dev/null 2>&1; then
	echo "нужен lsof, чтобы найти PID catalog по порту — на macOS он есть по умолчанию." >&2
	exit 1
fi

find_pid() {
	lsof -tiTCP:"$1" -sTCP:LISTEN 2>/dev/null | head -1
}

# Карточку и баланс берём ДО убийства catalog: после — читать листинги
# уже не у кого (REST-фасад каталога сам ходит в свой же gRPC-сервер,
# см. package doc services/catalog/internal/adapters/http).
listing="${LISTING_ID:-}"
if [[ -z "$listing" ]]; then
	listing=$(curl -sS --max-time 2 "$API/catalog/listings?limit=1" 2>/dev/null | jq -r '.listings[0].id // empty' 2>/dev/null) || true
fi
if [[ -z "$listing" ]]; then
	echo "нет опубликованной карточки для покупки — сначала make demo-media && make demo-catalog," >&2
	echo "либо передай LISTING_ID=<id> явно." >&2
	exit 1
fi

echo "── до убийства catalog ──────────────────────────────────────────────────"
echo "карточка: $listing"
echo "баланс покупателя $BUYER ДО покупки:"
grpcurl -plaintext -connect-timeout 2 -d "{\"account_id\":$BUYER}" \
	"localhost:$WALLET_GRPC_PORT" gosplash.wallet.v1.WalletService/GetBalance 2>/dev/null \
	|| echo "  grpcurl недоступен или wallet не отвечает"

pid=$(find_pid "$CATALOG_GRPC_PORT") || true
if [[ -z "$pid" ]]; then
	echo
	echo "на :$CATALOG_GRPC_PORT никто не слушает — catalog уже не запущен, отлично," >&2
	echo "продолжаю без явного kill." >&2
else
	echo
	echo "── убиваю catalog (PID $pid, kill -9) ───────────────────────────────────"
	kill -9 "$pid" 2>/dev/null || true
	sleep 1
	if curl -sS -o /dev/null --max-time 1 "http://localhost:$CATALOG_HTTP_PORT/catalog/healthz" 2>/dev/null; then
		echo "catalog всё ещё отвечает на :$CATALOG_HTTP_PORT — возможно, запущен второй" >&2
		echo "инстанс (см. make demo-kafka-rebalance) — убей и его вручную." >&2
	fi
fi

key="chaos-kill-catalog-$(date +%s%N)"
echo
echo "размещаю заказ на карточку с мёртвым catalog (Idempotency-Key: $key)..."
order=$(curl -sS --max-time 5 -X POST "$API/orders" \
	-H "Idempotency-Key: $key" -H "Content-Type: application/json" \
	-d "{\"buyer_id\":$BUYER,\"listing_id\":\"$listing\"}")
echo "$order" | jq -c '.' 2>/dev/null || echo "$order"
order_id=$(echo "$order" | jq -r '.id // empty') || true
if [[ -z "$order_id" ]]; then
	echo "не удалось создать заказ, дальше нет смысла продолжать." >&2
	exit 1
fi

echo
echo "жду до ${POLL_TIMEOUT}с, пока ReserveFunds пройдёт, а GrantLicense"
echo "исчерпает ретраи (forwardActivityOptions — до 5 попыток, backoff до 30с):"
status="pending"
for ((i = 1; i <= POLL_TIMEOUT; i++)); do
	status=$(curl -sS --max-time 2 "$API/orders/$order_id" 2>/dev/null | jq -r '.status // "?"') || true
	printf "  [%2ds] status=%s\n" "$i" "$status"
	[[ "$status" == "failed" ]] && break
	sleep 1
done

echo
echo "статус заказа СЕЙЧАС ($status):"
curl -sS --max-time 2 "$API/orders/$order_id" 2>/dev/null | jq . || true

cat <<EOF

── подними catalog обратно ──────────────────────────────────────────────────
  make run-catalog

RevokeLicense (компенсация) — criticalActivityOptions, БЕЗ ограничения
попыток: пока catalog мёртв, эта activity будет ретраиться бесконечно, и
заказ останется в "compensating", а не дойдёт до "failed". Это НЕ зависание
саги (docs/runbooks/saga-stuck.md проводит эту границу отдельно) — Temporal
UI покажет активный pending activity RevokeLicense, а не остановленный
воркфлоу. После подъёма catalog она пройдёт при первой же попытке.

Проверка после подъёма catalog:

  curl -s $API/orders/$order_id | jq .status
      ожидается "failed" (RevokeLicense прошла, следом ReleaseFunds — саге
      больше ждать нечего).

  grpcurl -plaintext -d '{"account_id":$BUYER}' \\
      localhost:$WALLET_GRPC_PORT gosplash.wallet.v1.WalletService/GetBalance
      баланс обязан совпасть с балансом ДО покупки (см. вывод в начале).

  make wf-describe W=$order_id     # или make wf-show W=$order_id
      история саги целиком: ReserveFunds → (GrantLicense, все 5 неудачных
      попыток) → RevokeLicense/ReleaseFunds как компенсации.
      UI: http://localhost:58233

  make wallet-check
      SUM(ledger_entries) обязана сходиться в 0 — docs/adr/0014-*.
EOF

# Runbook: эксплуатация в Kubernetes (kind, фаза 6)

Относится к чартам `deploy/helm/**`, кластеру `deploy/kind/**` и целям
`mk/k8s.mk`. Namespace везде один — `gosplash` (см.
`deploy/kind/infra/namespace.yaml`, почему один, а не разделение
infra/apps). Пять сценариев ниже — те, что явно перечислены в задании
фазы 6; за консьюмерным лагом и `outbox_pending` **по существу** проблемы
(что именно пошло не так в Kafka/relay) — в `docs/runbooks/kafka-lag.md`
и `docs/runbooks/outbox.md`, здесь — только то, что добавляет K8s-обвязка
поверх этих же диагнозов.

## Под в CrashLoopBackOff

### Симптом

```bash
make k8s-status
# STATUS показывает CrashLoopBackOff, RESTARTS растёт
```

### Что это значит

Контейнер запускается и завершается с ошибкой ДО того, как liveness/
readinessProbe вообще успевают отработать (или сразу после) — Kubernetes
ждёт возрастающий backoff (10с, 20с, 40с, ..., до 5 минут) перед каждым
следующим перезапуском. Это НЕ то же самое, что "readiness красный" ниже:
там процесс жив и отвечает 503, здесь процесс вообще не держится.

### Как отличить причину

```bash
kubectl -n gosplash describe pod <под>       # секция Events внизу — часто уже написан диагноз
kubectl -n gosplash logs <под> --previous     # ЛОГ УМЕРШЕГО контейнера, не текущего рестарта
```

Три частых причины, в порядке вероятности:

1. **Обязательная переменная окружения не задана.** Каждый `main.go`
   сервиса падает с `os.Exit(1)` при ошибке подключения к обязательной
   зависимости (см. `pkg/config` — os.Getenv только там, но пустой DSN/
   адрес не ловится на этапе чтения конфига, а валится уже на попытке
   подключиться). В логе `--previous` будет одна из строк вида
   `"media: postgres" error=...` — сверь с ConfigMap/Secret чарта:

   ```bash
   kubectl -n gosplash get configmap <chart>-config -o yaml
   kubectl -n gosplash get secret <chart>-secret -o yaml   # значения в base64, см. ниже
   ```

   Секреты в `kubectl get secret -o yaml` — base64, не открытый текст;
   раскодировать одно значение:
   `kubectl -n gosplash get secret media-secret -o jsonpath='{.data.MEDIA_SHARD_0_DSN}' | base64 -d`.

2. **Зависимость (Postgres/Kafka/...) ещё не готова.** Особенно в первые
   минуты после `make kind-up` — сервис стартовал раньше, чем поднялась
   его база. Правило `k8s-infra-up` перед `k8s-deploy` в `mk/k8s.mk`
   существует именно для этого, но и с ним возможна гонка (Deployment
   инфраструктуры `available`, но конкретная реплика Postgres ещё
   довосстанавливается после своего рестарта). Обычно самолечится за
   несколько backoff-циклов — если через 2-3 минуты не прошло, это уже
   причина 1 или 3, а не гонка при старте.

3. **`order`/`order-worker`: недоступен Temporal.** В отличие от
   остальных зависимостей, `sdkclient.Dial` в обоих `cmd/order/main.go`
   выполняется СИНХРОННО при старте и завершает процесс при ошибке —
   это единственная зависимость проекта, у которой недоступность даёт
   не красный `/readyz`, а именно CrashLoopBackOff. Проверь:

   ```bash
   kubectl -n gosplash get pods -l app=temporal
   kubectl -n gosplash logs deployment/temporal --tail=50
   ```

### Как убедиться, что починилось

```bash
kubectl -n gosplash get pods -l app.kubernetes.io/name=<chart>
# STATUS Running, RESTARTS перестал расти
```

---

## readiness красный, а liveness зелёный

### Симптом

```bash
kubectl -n gosplash get pods
# READY 0/1, но STATUS Running (не CrashLoopBackOff!)
kubectl -n gosplash describe pod <под> | grep -A3 Readiness
```

### Почему это НОРМАЛЬНОЕ (ожидаемое) состояние, а не инцидент по себе

Это ровно то разделение, ради которого в `pkg/httpx.Health` два разных
эндпоинта (см. развёрнутый комментарий в package doc `pkg/httpx/health.go`
и в `deploy/helm/media/values.yaml`, раздел probes): `/healthz` отвечает
`ok`, если процесс вообще может ответить на HTTP-запрос, `/readyz` — если
доступны его внешние зависимости (Postgres/Kafka/Redis/ClickHouse/
Elasticsearch — по сервису). Под в состоянии `Running` + `READY 0/1`
означает: **процесс жив, но одна из его зависимостей недоступна ПРЯМО
СЕЙЧАС** — и Kubernetes намеренно НИЧЕГО не делает с самим подом (не
перезапускает), а только убирает его из Endpoints ClusterIP/headless
Service, пока `readinessProbe` не позеленеет. Это тот самый сценарий,
который не даёт короткой заминке базы устроить одновременный рестарт
всех реплик разом (см. `deploy/helm/media/values.yaml`, "Классическая
авария").

### Как найти причину (какая именно зависимость упала)

```bash
kubectl -n gosplash port-forward svc/<chart> 8080:<http-порт> &
curl -s localhost:8080/readyz | jq .checks
```

Пример для wallet (нет ClusterIP Service — порт-форвардить нужно прямо
под, см. `deploy/helm/wallet/values.yaml`, почему у wallet нет
`service-http.yaml`):

```bash
kubectl -n gosplash port-forward pod/<под-wallet> 8080:8103 &
curl -s localhost:8080/readyz | jq .checks
```

Ответ — карта `{"postgres": "ok", "kafka_uploaded": "connection refused", ...}` —
это ИМЕННО та зависимость, которую нужно чинить, без гадания.

### Как убедиться, что починилось

```bash
kubectl -n gosplash get pods -l app.kubernetes.io/name=<chart>
# READY 1/1
```

Если `/readyz` не зеленеет спустя разумное время после того, как сама
зависимость точно восстановилась (`kubectl get pods` по ней тоже
`Running`/`Ready`), проверь `periodSeconds`/`failureThreshold` пробы в
`values.yaml` чарта — возможно, проба ещё просто не успела перепроверить
(за это отвечает `periodSeconds`, не немедленная реакция).

---

## Лаг консьюмера растёт

Диагностика ПРИЧИНЫ (не запущен ли консьюмер, недоступна ли Kafka,
не успевает ли обработка) — целиком в `docs/runbooks/kafka-lag.md`,
он написан безотносительно к тому, где именно запущен процесс. Ниже —
только то, что специфично для Kubernetes: КАК посмотреть то же самое,
когда сервис запущен не через `go run`, а как под.

### Симптом

- Алерт `KafkaConsumerLag` (тот же, что и в `docs/runbooks/kafka-lag.md`) —
  алертинг общий для compose и kind, метрика приходит с `/metrics`
  сервиса независимо от того, где он запущен.
- `kubectl -n gosplash get hpa` показывает CPU у консьюмерных
  Deployment'ов (catalog/thumbnail-worker/analytics/search) НИЗКИМ, пока
  лаг растёт, — это НЕ повод считать HPA сломанным, это прямая
  демонстрация комментария в `deploy/helm/thumbnail-worker/values.yaml`
  ("HPA: CPU — грубая, но не бессмысленная метрика для консьюмера"):
  CPU не видит отставание, вызванное ожиданием сети/диска, а не счётом.

### K8s-специфичная диагностика

```bash
# сколько реплик РЕАЛЬНО получают партиции этого топика прямо сейчас
kubectl -n gosplash get pods -l app.kubernetes.io/name=catalog
# лаг по группе — Kafka не знает про Kubernetes, поэтому команда та же,
# что и в docker-compose, просто через под, а не через host:port
kubectl -n gosplash exec deploy/kafka -- \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group catalog
```

Если реплик МЕНЬШЕ, чем партиций топика (6 у `media.photo.uploaded`,
см. `deploy/kind/infra/kafka.yaml`) — HPA ещё не доскейлился или упёрся
в `maxReplicas` чарта (`autoscaling.maxReplicas` в `values.yaml`): подними
его руками для диагностики (`kubectl -n gosplash scale deployment/catalog
--replicas=6`) и посмотри, снижается ли лаг — если да, дело в CPU-метрике
HPA (см. выше), а не в самом коде консьюмера.

Если реплик БОЛЬШЕ, чем партиций, — лишние реплики физически не могут
получить ни одной партиции при ребалансировке и будут простаивать
(`READY 1/1`, лаг не двигается ни туда, ни сюда со стороны этой реплики) —
это не баг, а прямое следствие того, что партиция обрабатывается ровно
одним консьюмером группы одновременно; `replicaCount`/`autoscaling.
maxReplicas` в values.yaml согласованы с числом партиций намеренно, менять
одно без другого бессмысленно.

### Как убедиться, что починилось

Тот же критерий, что и в `docs/runbooks/kafka-lag.md`: лаг ниже порога
алерта, `for: 5m` в `deploy/observability/alerts.yaml` выдержан, алерт
`KafkaConsumerLag` погас сам, без ручного вмешательства в offset'ы.

---

## `outbox_pending` растёт

Диагностика ПРИЧИНЫ — целиком в `docs/runbooks/outbox.md` (relay не
запущен / Kafka недоступна / relay не успевает). В Kubernetes добавляется
ровно один новый вопрос, которого не было в docker-compose: "а не убило
ли под ПОСРЕДИ батча, до того как relay успел закоммитить `published_at`".

### K8s-специфичная диагностика

```bash
# видел ли под недавний рестарт — если RESTARTS только что вырос,
# именно это и произошло: SIGKILL посреди работы relay
kubectl -n gosplash get pod <под> -o jsonpath='{.status.containerStatuses[0].restartCount}'
kubectl -n gosplash describe pod <под> | grep -A5 "Last State"
```

Если `Last State: Terminated, Reason: OOMKilled` — это не проблема relay,
а `resources.limits.memory` в `values.yaml` чарта, см. раздел "Ресурсы"
там же (лимит по памяти абсолютен, троттлинга, как у CPU, не бывает).

Если `Last State: Terminated, Reason: Error` с ненулевым кодом посреди
`terminationGracePeriodSeconds` (kubelet прислал SIGKILL, не дождавшись
штатной остановки, — см. следующий раздел, "под не завершается по
SIGTERM") — это ровно тот случай, из-за которого `terminationGracePeriodSeconds`
в каждом values.yaml посчитан с запасом (см. разбор чисел там же): при
недостаточном значении относительно кода сервиса недосланные batch'и
outbox — прямое следствие обрыва на середине, а не бага relay.

Само по себе это ВОССТАНАВЛИВАЕТСЯ без вмешательства: `published_at`
не проставлен, значит на следующем тике (или после рестарта пода) relay
подхватит те же строки снова — паттерн outbox именно для этого и
существует (см. `docs/adr/0008-*`). Ручное вмешательство нужно, только
если рестарты продолжаются (см. первый сценарий, CrashLoopBackOff).

### Как убедиться, что починилось

Тот же критерий, что в `docs/runbooks/outbox.md`: метрика `outbox_pending`
идёт к нулю и алерт `OutboxBacklog` гаснет сам по истечении `for: 5m`.

---

## Под не завершается по SIGTERM

### Симптом

```bash
kubectl -n gosplash delete pod <под>
kubectl -n gosplash get pod <под> -w
# STATUS остаётся Terminating дольше terminationGracePeriodSeconds чарта,
# и под исчезает РЕЗКО (kubelet прислал SIGKILL) вместо плавного ухода
```

### Что это значит и как отличить причину

`terminationGracePeriodSeconds` каждого чарта — не круглое число "с
запасом на всякий случай", а сумма конкретных таймаутов кода сервиса
(разбор — в `values.yaml` каждого чарта, поищи `terminationGracePeriodSeconds`).
Если под всё равно не укладывается — один из шагов внутри процесса
занял БОЛЬШЕ своего собственного таймаута, а не просто "стало чуть
медленнее":

```bash
kubectl -n gosplash logs <под> --previous --tail=100
```

Ищи ПОСЛЕДНЮЮ строку лога перед обрывом — она называет конкретный шаг:

1. **`"... shutdown не уложился в таймаут"`** (`pkg/httpx.Shutdown` /
   `pkg/grpcx.Shutdown`) — активное соединение держалось дольше 15с.
   Частая причина — клиент с очень долгим запросом (загрузка большого
   файла в media, см. `httpCfg.WriteTimeout = 5 * time.Minute` в
   `cmd/media/main.go` — специально увеличенный таймаут САМОГО HTTP-
   сервера, который переживает 15-секундный бюджет shutdown без проблем
   в штатном случае, но при реальной ретрансляции 50 МБ по медленному
   каналу может исчерпать оба).
2. **`"... консьюмер не остановился вовремя"`** — сообщение застряло в
   обработчике дольше бюджета `waitAll`/`waitFor` (15с). Смотри логи
   САМОГО обработчика (индексация фото, ресайз изображения) — что он
   делал в этот момент; сам факт "не успел" не говорит, где узкое место.
3. **thumbnail-worker специфично: три последовательных таймаута по 15с.**
   В отличие от catalog/analytics (`waitAll` с ОБЩИМ бюджетом на
   несколько каналов), `cmd/thumbnail-worker/main.go` ждёт
   consumer/retry-consumer/outbox-relay ТРЁМЯ независимыми
   `time.After(15s)` подряд (см. разбор в `deploy/helm/thumbnail-worker/
   values.yaml`) — 90с чарта уже заложены с расчётом на этот факт;
   если под всё равно не укладывается, отставание СУЩЕСТВЕННО больше
   штатных 45с на этом шаге, и это уже не "чуть не хватило времени",
   а отдельная проблема (например, зависший S3-запрос без собственного
   таймаута).
4. **order-worker: SDK Temporal сам решает, сколько ждать.**
   `terminationGracePeriodSeconds: 90` там — заведомо консервативная
   ОЦЕНКА (см. комментарий в values.yaml), а не расчёт по коду проекта:
   если под систематически не укладывается, смотри Temporal Web UI
   (`kubectl -n gosplash port-forward svc/temporal-ui 8080:8080`,
   `http://localhost:8080`) на предмет activity, которая реально
   выполнялась дольше 90с в момент удаления пода — это первый факт,
   который нужно измерить, а не гадать по коду этого проекта (см.
   `docs/runbooks/saga-stuck.md`, тот же UI, для сравнения "воркфлоу
   висит" vs "воркфлоу просто долгий").

### Что делать

Краткосрочно — поднять `terminationGracePeriodSeconds` в `values.yaml`
чарта (`helm upgrade` передеплоит Deployment с новым значением). Это
лечит СИМПТОМ (обрыв на SIGKILL), но не причину — если шаг стабильно
не укладывается в СВОЙ внутренний таймаут (15с у httpx/grpcx, тот же
пункт 1-2 выше), увеличение только внешнего `terminationGracePeriodSeconds`
не поможет: внутренний `context.WithTimeout` всё равно оборвёт попытку
раньше, просто под будет дольше "висеть" в Terminating после этого,
ожидая уже ничего не делающий процесс.

### Как убедиться, что починилось

```bash
kubectl -n gosplash delete pod <под>
kubectl -n gosplash get pod <под> -w
```

Под должен исчезнуть САМ (Terminating → удалён) в пределах
`terminationGracePeriodSeconds`, БЕЗ разницы между временем ухода и
временем, которое логи самого процесса показывают как "остановлен"
(`slog.Info("<сервис>: остановлен")` — последняя строка штатного
шатдауна каждого `main.go`).

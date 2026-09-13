# ─────────────────────────────────────────────────────────────────────────────
## Фаза 6: Kubernetes/kind (docs/PLAN.md, deploy/helm/**, deploy/kind/**)
# ─────────────────────────────────────────────────────────────────────────────
#
# Docker-демон должен быть запущен, kind и helm — установлены (`brew install
# kind helm` на macOS). Если kind не установлен, `kind-up`/`kind-down` честно
# упадут на первой команде — это ожидаемо, а не баг цели: см. ограничение
# задания в отчёте по фазе 6.
#
# Порядок kind-up НЕ случаен и важен именно в этом порядке:
#   1. кластер (узлы существуют, но пустые);
#   2. образы (собраны И загружены В УЗЛЫ — до этого шага в кластере
#      физически нечего деплоить: registry для этих образов не существует,
#      см. deploy/docker/*.Dockerfile и values.yaml любого чарта, комментарий
#      про image.pullPolicy);
#   3. инфраструктура (Postgres/Kafka/Redis/MinIO/ClickHouse/Elasticsearch/
#      Temporal) и ожидание её готовности — сервисам НЕЧЕГО ПРОВЕРЯТЬ
#      в /readyz, если Postgres ещё не Ready;
#   4. миграции (deploy/kind/jobs/migrate.yaml) — СХЕМА должна появиться
#      ДО того, как реплики сервисов начнут запрашивать /readyz=ok, иначе
#      k8s-deploy технически "успеет", но все поды повиснут в 0/1 Ready;
#   5. Helm-релизы сервисов.
# Пропуск любого шага не роняет kind-up с ошибкой — Kubernetes самовосстановится
# (поды будут перезапускаться, пока зависимость не появится), но неоправданно
# долго и с CrashLoopBackOff в первые минуты, что и демонстрирует, зачем
# нужен именно такой порядок.

KIND_CLUSTER := gosplash
K8S_NAMESPACE := gosplash

# Каждый образ — своя пара (Dockerfile, тег). automigrate не является
# "сервисом" в смысле services/**, но собирается и грузится в кластер
# точно так же — см. разбор в deploy/docker/automigrate.Dockerfile.
K8S_IMAGES := media catalog wallet order order-worker thumbnail-worker analytics search automigrate

# Порядок установки Helm-релизов НЕ важен для corretness (Kubernetes сам
# ретраит недоступные зависимости — Service существует, даже когда за ним
# пока 0 Ready подов, DNS отвечает NXDOMAIN только для несуществующего
# имени, а не для "пока нет живых подов"), но важен для СКОРОСТИ первого
# up: гRPC-клиенты (pkg/grpcx.Dial) не блокируются на недоступности target'а
# при старте, поэтому порядок здесь исключительно для читаемости лога.
K8S_CHARTS := media catalog wallet order order-worker thumbnail-worker analytics search

# НЕОЧЕВИДНОЕ РЕШЕНИЕ: инфраструктурные образы (Postgres/Kafka/Redis/...)
# грузятся в kind ТЕМ ЖЕ способом, что и образы сервисов (`kind load
# docker-image`), а не оставлены на откуп containerd-у самих узлов, как
# это обычно и делается (Kubernetes сам тянет публичные образы по имени).
# Причина конкретная и проверенная: containerd ВНУТРИ kind-узла (это
# отдельный контейнер, со своим сетевым стеком) наследует HTTP(S)_PROXY
# из окружения. Если прокси слушает на localhost хоста — а так устроено
# большинство корпоративных и отладочных прокси, — то ИЗНУТРИ узла этот
# адрес указывает в пустоту, и попытка docker.elastic.co/
# registry-1.docker.io/... падает connection refused, хотя `docker pull`
# с ХОСТА (тот же Docker Desktop) работает нормально. `kind load
# docker-image` копирует уже локально существующий образ напрямую в
# containerd узла, минуя сетевой pull целиком — тот же трюк, что уже
# применяется для gosplash/* образов, здесь просто расширен на
# инфраструктуру. Ценой становится то, что первый `make kind-up` не
# запускается мгновенно (образы тянутся docker pull'ом на хосте), но
# это уже не проблема самого kind — если у пользователя проблем с
# доступом к Docker Hub нет, `docker pull` в цели ниже отработает
# от локального кеша daemon'а почти бесплатно.
K8S_INFRA_IMAGES := postgres:18-alpine redis:8-alpine apache/kafka:4.0.0 \
	clickhouse/clickhouse-server:25.8 docker.elastic.co/elasticsearch/elasticsearch:8.19.11 \
	temporalio/auto-setup:1.29.0 temporalio/ui:2.42.1 \
	quay.io/minio/minio:latest quay.io/minio/mc:latest \
	registry.k8s.io/metrics-server/metrics-server:v0.9.0

# ─────────────────────────────────────────────────────────────────────────────
## kind: поднять / снести кластер
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: kind-up
kind-up: ## поднять kind, собрать образы, поставить инфраструктуру+миграции, задеплоить сервисы
	@printf "\033[1m1/5 Кластер kind ($(KIND_CLUSTER))\033[0m\n"
	@kind get clusters 2>/dev/null | grep -qx "$(KIND_CLUSTER)" \
		&& echo "  уже существует, пропускаю create" \
		|| kind create cluster --name $(KIND_CLUSTER) --config deploy/kind/cluster.yaml
	@$(MAKE) --no-print-directory k8s-images
	@$(MAKE) --no-print-directory k8s-infra-images
	@printf "\n\033[1m3/5 Инфраструктура\033[0m\n"
	@$(MAKE) --no-print-directory k8s-infra-up
	@printf "\n\033[1m4/5 Миграции\033[0m\n"
	@$(MAKE) --no-print-directory k8s-migrate
	@printf "\n\033[1m5/5 Сервисы\033[0m\n"
	@$(MAKE) --no-print-directory k8s-deploy
	@printf '\n\033[1mГотово.\033[0m Смотри make k8s-status. Порт-форвард примеры — в docs/runbooks/kubernetes.md.\n'

.PHONY: kind-down
kind-down: ## снести kind-кластер целиком (все данные в emptyDir теряются — это ожидаемо)
	kind delete cluster --name $(KIND_CLUSTER)

# ─────────────────────────────────────────────────────────────────────────────
## Образы
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: k8s-images
k8s-images: ## собрать бинарники (make build-linux) и образы сервисов (deploy/docker/*.Dockerfile), загрузить их в kind
	@printf "\033[1m2/5 Образы сервисов\033[0m\n"
	@printf "  бинарники (build-linux)...\n"
	@# Централизованная сборка: один `go build` на все девять образов вместо
	@# builder-стадии в каждом Dockerfile — см. разбор цены и порядок шагов
	@# в deploy/docker/media.Dockerfile. ARCH по умолчанию — архитектура
	@# хоста (см. Makefile): kind-узлы — это контейнеры того же Docker
	@# Desktop, что собирает образы, поэтому "родная" архитектура здесь и
	@# есть правильная по умолчанию, без эмуляции.
	@$(MAKE) --no-print-directory build-linux
	@for img in $(K8S_IMAGES); do \
		printf "  собираю gosplash/$$img:dev... "; \
		docker build -q -f deploy/docker/$$img.Dockerfile -t gosplash/$$img:dev . >/tmp/gosplash-build-$$img.log 2>&1 \
			&& echo ok \
			|| { echo FAILED; echo "  лог: /tmp/gosplash-build-$$img.log"; exit 1; }; \
		printf "  загружаю в kind ($(KIND_CLUSTER))... "; \
		kind load docker-image gosplash/$$img:dev --name $(KIND_CLUSTER) >/dev/null \
			&& echo ok; \
	done

# УТОЧНЕНИЕ, добавленное ПОСЛЕ первого реального прогона на этой машине
# (см. отчёт по фазе 6): `kind load docker-image`/`kind load image-archive`
# для этих образов падают с "ctr: content digest ...: not found", даже
# когда docker pull отработал и образ виден в `docker images`. Причина —
# несовместимость между containerd-backed image store текущей версии
# Docker Desktop (он экспортирует через `docker save` OCI-индекс
# МУЛЬТИПЛАТФОРМЕННОГО манифеста для образов вроде postgres/redis/kafka,
# даже когда локально скачан только один платформенный вариант) и флагом
# `--all-platforms`, который `kind load` жёстко использует внутри себя
# при вызове `ctr images import`, — тот пытается импортировать ВСЕ
# платформы из индекса и не находит блобы отсутствующих. gosplash/*
# образы (K8S_IMAGES выше) эту проблему не показывают, потому что
# `docker build` производит образ ОДНОЙ платформы без индекса — манифест-
# лист просто неоткуда взяться.
#
# Обходной путь ниже — тот же самый экспорт (`docker save`), но
# ИМПОРТИРУЕТСЯ НАПРЯМУЮ через `ctr images import` БЕЗ `--all-platforms`
# (проверено вручную на этой машине — работает на всех типах узлов kind).
# Это не задокументированный флаг kind, а byte-в-byte то же, что делает
# `kind load` внутри себя, минус один флаг, — поэтому решение хрупкое
# к будущим версиям kind/containerd и стоит считать временной меркой,
# а не постоянной архитектурой; при обновлении kind стоит сначала
# попробовать заново `kind load docker-image`.
.PHONY: k8s-infra-images
k8s-infra-images: ## docker pull + импорт образов инфраструктуры в containerd каждого узла kind
	@printf "\033[1mОбразы инфраструктуры\033[0m\n"
	@nodes=$$(kind get nodes --name $(KIND_CLUSTER)); \
	for img in $(K8S_INFRA_IMAGES); do \
		printf "  docker pull $$img... "; \
		docker pull -q $$img >/dev/null 2>/tmp/gosplash-pull-$$(echo $$img | tr '/:' '--').log \
			&& echo ok \
			|| { echo FAILED; echo "  лог: /tmp/gosplash-pull-$$(echo $$img | tr '/:' '--').log"; exit 1; }; \
		for node in $$nodes; do \
			printf "  импортирую в $$node... "; \
			docker save $$img | docker exec -i $$node ctr --namespace=k8s.io images import --digests --snapshotter=overlayfs - >/dev/null \
				&& echo ok \
				|| { echo FAILED; exit 1; }; \
		done; \
	done

# ─────────────────────────────────────────────────────────────────────────────
## Инфраструктура и миграции
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: k8s-infra-up
k8s-infra-up: ## поставить Postgres/Kafka/Redis/MinIO/ClickHouse/Elasticsearch/Temporal/metrics-server и дождаться готовности
	kubectl apply -f deploy/kind/infra/namespace.yaml
	@# Temporal читает свой dynamicconfig из ConfigMap, СОБРАННОГО из
	@# deploy/temporal/dynamicconfig/development-sql.yaml (существующий
	@# файл, вне зоны этой задачи) — а не задублированного статическим
	@# YAML здесь, см. комментарий в deploy/kind/infra/temporal.yaml.
	kubectl create configmap temporal-dynamicconfig \
		--from-file=deploy/temporal/dynamicconfig/development-sql.yaml \
		-n $(K8S_NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f deploy/kind/infra/postgres.yaml
	kubectl apply -f deploy/kind/infra/kafka.yaml
	kubectl apply -f deploy/kind/infra/redis.yaml
	kubectl apply -f deploy/kind/infra/minio.yaml
	kubectl apply -f deploy/kind/infra/clickhouse.yaml
	kubectl apply -f deploy/kind/infra/elasticsearch.yaml
	kubectl apply -f deploy/kind/infra/temporal.yaml
	kubectl apply -f deploy/kind/infra/metrics-server.yaml
	@printf "  жду Deployment'ы инфраструктуры (до 180с)...\n"
	@kubectl -n $(K8S_NAMESPACE) wait --for=condition=available --timeout=180s \
		deployment/pg-shard-0 deployment/pg-shard-1 deployment/pg-catalog \
		deployment/pg-catalog-replica deployment/pg-wallet deployment/pg-order \
		deployment/kafka deployment/redis deployment/minio \
		deployment/clickhouse deployment/elasticsearch \
		deployment/pg-temporal deployment/temporal deployment/temporal-ui
	@kubectl -n kube-system wait --for=condition=available --timeout=120s deployment/metrics-server
	@printf "  жду Job'ы инициализации (топики Kafka, бакеты MinIO)...\n"
	@kubectl -n $(K8S_NAMESPACE) wait --for=condition=complete --timeout=120s \
		job/kafka-topics-init job/minio-bucket-init

.PHONY: k8s-migrate
k8s-migrate: ## накатить схему во все базы (Job на tools/automigrate)
	kubectl -n $(K8S_NAMESPACE) delete job automigrate --ignore-not-found
	kubectl apply -f deploy/kind/jobs/migrate.yaml
	@kubectl -n $(K8S_NAMESPACE) wait --for=condition=complete --timeout=180s job/automigrate \
		|| { echo "миграции не завершились — смотри: kubectl -n $(K8S_NAMESPACE) logs job/automigrate"; exit 1; }

# ─────────────────────────────────────────────────────────────────────────────
## Сервисы (Helm)
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: k8s-deploy
k8s-deploy: ## helm upgrade --install для всех чартов сервисов
	@for c in $(K8S_CHARTS); do \
		printf "  helm upgrade --install $$c... "; \
		helm upgrade --install $$c deploy/helm/$$c \
			--namespace $(K8S_NAMESPACE) --create-namespace \
			--wait --timeout 3m \
			> /tmp/gosplash-helm-$$c.log 2>&1 \
			&& echo ok \
			|| { echo FAILED; echo "  лог: /tmp/gosplash-helm-$$c.log"; exit 1; }; \
	done

.PHONY: k8s-status
k8s-status: ## снимок состояния namespace: релизы, поды, HPA, события
	@printf "\033[1mHelm-релизы\033[0m\n"
	@helm list -n $(K8S_NAMESPACE)
	@printf "\n\033[1mПоды\033[0m\n"
	@kubectl -n $(K8S_NAMESPACE) get pods -o wide
	@printf "\n\033[1mHPA\033[0m\n"
	@kubectl -n $(K8S_NAMESPACE) get hpa
	@printf "\n\033[1mПоследние события (полезно при CrashLoopBackOff/Pending)\033[0m\n"
	@kubectl -n $(K8S_NAMESPACE) get events --sort-by=.lastTimestamp | tail -20

.PHONY: k8s-logs
k8s-logs: ## логи сервиса (make k8s-logs S=media [F=1] — F=1 значит -f/follow)
	@if [ -z "$(S)" ]; then echo "нужен S=<имя чарта/релиза, например media>"; exit 1; fi
	kubectl -n $(K8S_NAMESPACE) logs -l app.kubernetes.io/name=$(S) --all-containers --tail=200 $(if $(F),-f)

# ─────────────────────────────────────────────────────────────────────────────
## Helm без кластера — проверка чартов
# ─────────────────────────────────────────────────────────────────────────────
#
# Обе цели ниже НЕ требуют ни kind, ни docker: `helm lint`/`helm template`
# работают со статическими файлами чарта. Это та часть проверки, которую
# можно (и нужно) гонять в CI без поднятия кластера.

.PHONY: helm-lint
helm-lint: ## helm lint по всем чартам deploy/helm/**
	@fail=0; \
	for c in $(K8S_CHARTS); do \
		printf "\033[1m== %s ==\033[0m\n" "$$c"; \
		helm lint deploy/helm/$$c || fail=1; \
	done; \
	exit $$fail

.PHONY: helm-template
helm-template: ## отрендерить чарт(ы) без деплоя (make helm-template [S=media])
	@for c in $(if $(S),$(S),$(K8S_CHARTS)); do \
		printf "\033[1m== %s ==\033[0m\n" "$$c"; \
		helm template $$c deploy/helm/$$c || exit 1; \
	done

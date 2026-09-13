# ─────────────────────────────────────────────────────────────────────────────
# tools/automigrate — образ ДЛЯ KUBERNETES JOB (deploy/kind/jobs/migrate.yaml),
# не для services/*. Формально задание просило Dockerfile "на сервис", но без
# накатанной схемы каждый чарт вечно сидел бы в /readyz=503 (health.Register
# "postgres"/"clickhouse" и т.д. никогда не станет ok на пустой базе) — то
# есть `make kind-up` без этого образа технически "поднимает кластер", но не
# даёт РАБОТАЮЩИЙ демо-стенд. Кладём его сюда же (deploy/docker/), а не
# куда-то отдельно: место определяется тем, что это тоже "образ для
# деплоя", а не тем, что automigrate лежит в tools/, а не в services/.
#
# tools/automigrate — ОТДЕЛЬНЫЙ go-модуль (docs/STYLE.md, раздел
# "Конфигурация": "Единственное исключение — утилиты в tools/"), но
# импортирует migrations-пакеты сразу нескольких сервисов (см. package doc
# main.go: gosplash/services/*/migrations) — это ЕДИНСТВЕННОЕ место в
# проекте, которому это разрешено. Раньше это было проблемой ИМЕННО для
# Dockerfile: builder-стадия внутри контейнера не видела go.work и была
# вынуждена вручную перечислять через COPY, какие internal-подпакеты каждого
# сервиса реально нужны migrations/ (adapters/pg, domain, ports — но не
# internal/app и не остальные adapters/*), чтобы не тащить в контекст сборки
# вообще все services/*/internal/**.
#
# Централизованная сборка снимает эту проблему целиком: `go build` теперь
# выполняется СНАРУЖИ (`make build-linux`, корневой Makefile), где действует
# go.work и весь граф зависимостей резолвится штатно, без ручного отбора
# пакетов и без CGO_ENABLED=0 GOOS=linux руками — эти флаги уже в
# build-linux. Dockerfile просто забирает готовый файл. Разбор цены переноса
# сборки из Dockerfile в ./build (образ перестаёт быть самодостаточным) — в
# deploy/docker/media.Dockerfile, канонический комментарий на эту тему.
#
# distroless, не alpine — тот же разбор, что в media.Dockerfile. НЕ :nonroot:
# ошибка была бы здесь, если бы Job'у был нужен root, — но Job запускается
# один раз до старта остальных сервисов, ничего постоянно не слушает и не
# хранит секретов дольше своего жизненного цикла: root vs nonroot здесь
# равнозначны по риску, но у Job'а нет причин НЕ соблюдать то же правило
# "непривилегированный пользователь", поэтому :nonroot всё равно
# используется — исключений из общего требования задания нет.
# ─────────────────────────────────────────────────────────────────────────────

FROM gcr.io/distroless/static-debian12:nonroot

# Бинарник уже собран под linux/$(ARCH) снаружи — см. `make build-linux`
# в корневом Makefile и разбор цены в deploy/docker/media.Dockerfile.
COPY build/automigrate /usr/local/bin/automigrate

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/automigrate"]

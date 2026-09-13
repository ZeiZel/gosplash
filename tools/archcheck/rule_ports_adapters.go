package archcheck

import (
	"fmt"
	"strconv"
	"strings"
)

const rulePortsAdapters = "ports-adapters"

// forbiddenInApp — инфраструктурные импорты, которых не должно быть в
// internal/app (docs/STYLE.md, «Раскладка сервиса — ports & adapters»):
// «Появился в internal/app импорт gorm, kgo, redis, grpc или net/http —
// сценарий прорастает в инфраструктуру, и его больше нельзя протестировать
// без докера».
//
// gosplash/pkg/kafkax и gosplash/pkg/outbox сюда НЕ входят, хотя формально
// это тоже "инфраструктура". Это уже абстракции проекта НАД инфраструктурой
// (конверты, классификация ошибок retryable/permanent, таблица outbox) —
// а не сами библиотеки, и internal/app реально ими пользуется (см.
// services/*/internal/app/*.go: kafkax.NewEnvelope, kafkax.Permanent,
// kafkax.Handler как тип обработчика) именно для того, чтобы protobuf-
// и Kafka-детали не просочились ещё на слой ниже, в domain. Запрет этих
// пакетов сломал бы существующий, архитектурно правильный код.
var forbiddenInApp = []string{
	"gorm.io/",
	"github.com/twmb/franz-go",
	"github.com/redis/go-redis",
	"google.golang.org/grpc",
	"net/http",
	"github.com/ClickHouse/clickhouse-go",
	"github.com/elastic/go-elasticsearch",
}

// forbiddenInDomain — то же самое плюс gen/go: domain, в отличие от app,
// не имеет права знать даже о существовании protobuf-типов событий
// (STYLE.md: «internal/domain: без тегов gorm/json/protobuf, без импортов
// инфраструктуры и gen/go»). Конвертация Envelope → доменное значение —
// работа адаптера или app-сценария, но не domain.
var forbiddenInDomain = append(append([]string{}, forbiddenInApp...), "gosplash/gen/go")

// checkPortsAdapters проверяет импорты файлов internal/app и internal/domain
// (любого сервиса — путь ищется по подстроке "/internal/app/" или
// "/internal/domain/", а не по конкретному модулю: правило одинаково для
// всех сервисов и не завязано на список модулей).
func checkPortsAdapters(files []*file) []Violation {
	var out []Violation
	for _, f := range files {
		var forbidden []string
		var layer string
		switch {
		case strings.Contains(f.relPath, "/internal/domain/"):
			forbidden, layer = forbiddenInDomain, "internal/domain"
		case strings.Contains(f.relPath, "/internal/app/"):
			forbidden, layer = forbiddenInApp, "internal/app"
		default:
			continue
		}

		for _, imp := range f.ast.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				continue
			}
			for _, bad := range forbidden {
				if !matchesImportPrefix(path, bad) {
					continue
				}
				out = append(out, Violation{
					File: f.relPath,
					Line: f.fset.Position(imp.Pos()).Line,
					Rule: rulePortsAdapters,
					Message: fmt.Sprintf(
						"%s импортирует %q — ports & adapters (docs/STYLE.md) "+
							"запрещает этому слою знать об инфраструктуре напрямую",
						layer, path,
					),
				})
				break // одного совпадения по одному импорту достаточно
			}
		}
	}
	return out
}

// matchesImportPrefix — совпадение по префиксу пути импорта. Для net/http
// нужна отдельная ветка: наивный strings.HasPrefix(path, "net/http") задел
// бы и гипотетический сторонний пакет "net/http2" (тот же префикс как
// строка, но другой модуль) — а нам нужно поймать именно net/http и его
// вложенные пакеты (net/http/httptest, net/http/httputil), для которых
// после префикса всегда идёт "/".
func matchesImportPrefix(path, prefix string) bool {
	if prefix == "net/http" {
		return path == "net/http" || strings.HasPrefix(path, "net/http/")
	}
	return strings.HasPrefix(path, prefix)
}

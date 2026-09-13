package archcheck

import (
	"fmt"
	"regexp"
	"strconv"
)

const ruleServiceIsolation = "service-isolation"

// currentServiceRe вытаскивает имя сервиса из пути ФАЙЛА: services/<name>/...
var currentServiceRe = regexp.MustCompile(`^services/([^/]+)/`)

// importedServiceRe вытаскивает имя сервиса из ПУТИ ИМПОРТА:
// gosplash/services/<name>/... — ровно тот же путь, каким сервисы
// адресовали бы друг друга, если бы это было разрешено. go.work делает
// пакеты соседних сервисов видимыми компилятору (это плата за единое
// рабочее пространство), поэтому такой импорт синтаксически валиден и его
// некому, кроме archcheck, запретить.
var importedServiceRe = regexp.MustCompile(`^gosplash/services/([^/]+)/`)

// checkServiceIsolation проверяет, что файлы одного сервиса не импортируют
// пакеты другого сервиса напрямую — единственный разрешённый способ
// связи между сервисами: контракт (gen/go, порождённый из proto/) поверх
// gRPC или Kafka.
func checkServiceIsolation(files []*file) []Violation {
	var out []Violation
	for _, f := range files {
		m := currentServiceRe.FindStringSubmatch(f.relPath)
		if m == nil {
			continue
		}
		own := m[1]

		for _, imp := range f.ast.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				continue
			}
			im := importedServiceRe.FindStringSubmatch(path)
			// len(im) < 2 невозможно при непустом im: у importedServiceRe
			// ровно одна группа захвата, и FindStringSubmatch либо вернёт
			// nil (нет совпадения), либо срез длиной ровно 2 (совпадение +
			// группа). Проверка len — только чтобы gocritic (weakCond) не
			// считал это неполной защитой от паники.
			if len(im) < 2 || im[1] == own {
				continue
			}
			out = append(out, Violation{
				File: f.relPath,
				Line: f.fset.Position(imp.Pos()).Line,
				Rule: ruleServiceIsolation,
				Message: fmt.Sprintf(
					"сервис %s импортирует %q — сервисы монорепы не должны "+
						"импортировать друг друга напрямую, только через контракт "+
						"(gen/go из proto/) поверх gRPC или Kafka",
					own, path,
				),
			})
		}
	}
	return out
}

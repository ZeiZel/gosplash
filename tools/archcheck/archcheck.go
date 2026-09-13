// Package archcheck — статический анализатор архитектурных правил gosplash,
// описанных в docs/STYLE.md:
//
//  1. direct-produce — публикация в Kafka разрешена только через outbox
//     (pkg/outbox) либо из самого pkg/kafkax; всё остальное — нарушение,
//     если явно не разрешено (см. allowlist.go и rule_direct_produce.go).
//  2. ports-adapters — internal/app и internal/domain не импортируют
//     инфраструктуру напрямую (gorm, kgo, go-redis, grpc, net/http,
//     clickhouse, elasticsearch); internal/domain вдобавок не импортирует
//     gen/go.
//  3. service-isolation — services/A не импортирует services/B.
//
// Сознательно НЕ используется go/types и golang.org/x/tools/go/packages.
// Каждый сервис монорепы — отдельный Go-модуль (см. go.work), и загрузка
// типов для всех трёх правил разом означала бы для каждого модуля свой
// packages.Load с резолвом всех транзитивных зависимостей (gorm, franz-go,
// grpc, testcontainers...) — дорого по времени CI и не нужно по существу:
// все три правила проверяются по ИМПОРТАМ файла и ИМЕНАМ вызовов, а это
// целиком доступно из чистого AST. go/parser разбирает файл как текст, не
// резолвя ни одного импортируемого пакета — поэтому у самого анализатора
// нет вообще никаких внешних зависимостей в рантайме (см. go.mod) и он
// работает одинаково быстро что на одном файле, что на всех одиннадцати
// модулях репозитория.
//
// Цена такого выбора: правило direct-produce не различает метод Publish
// пакета kafkax от случайно одноимённого метода в чужом коде — оно ловит
// вызовы ПО ИМЕНИ (Publish/Produce/ProduceSync), не по типу получателя.
// На практике это не проблема (см. отчёт о прогоне по репозиторию — других
// методов Publish/Produce в проекте нет), но при росте кодовой базы
// эвристика может потребовать сужения. Осознанный компромисс простоты
// против точности, а не недосмотр.
package archcheck

import (
	"fmt"
	"sort"
)

// Violation — одно найденное нарушение: файл, строка и объяснение.
type Violation struct {
	// File — путь относительно корня сканирования, всегда со слэшем "/"
	// (независимо от ОС), чтобы вывод был воспроизводим в CI.
	File    string
	Line    int
	Rule    string // короткий id правила: direct-produce, ports-adapters, service-isolation
	Message string
}

// String — формат «файл:строка: сообщение», как требует спецификация CI.
// Имя правила остаётся частью сообщения (в квадратных скобках), а не
// отдельным полем формата: так вывод остаётся однострочным и грепается
// по одному правилу через `grep '\[direct-produce\]'`.
func (v Violation) String() string {
	return fmt.Sprintf("%s:%d: [%s] %s", v.File, v.Line, v.Rule, v.Message)
}

// Run сканирует дерево начиная с root и возвращает все найденные нарушения,
// отсортированные по файлу и строке. Сортировка — не косметика: без неё
// порядок вывода зависел бы от порядка обхода файловой системы, и одинаковый
// код на двух запусках CI показывал бы разный diff.
func Run(root string, verbose bool) ([]Violation, error) {
	files, err := collectFiles(root)
	if err != nil {
		return nil, fmt.Errorf("archcheck: обход %s: %w", root, err)
	}

	var out []Violation
	out = append(out, checkDirectProduce(files, verbose)...)
	out = append(out, checkPortsAdapters(files)...)
	out = append(out, checkServiceIsolation(files)...)

	sort.Slice(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}
		if out[i].Line != out[j].Line {
			return out[i].Line < out[j].Line
		}
		return out[i].Rule < out[j].Rule
	})
	return out, nil
}

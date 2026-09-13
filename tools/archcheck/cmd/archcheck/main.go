// Команда archcheck — статическая проверка архитектурных правил монорепы
// (docs/STYLE.md): outbox как единственный путь публикации в Kafka,
// ports & adapters (internal/app и internal/domain не знают об
// инфраструктуре напрямую) и изоляция сервисов друг от друга.
//
// Запускается из CI (job lint, .github/workflows/ci.yml) и руками:
//
//	go run ./tools/archcheck/cmd/archcheck -root . -v
//	make archcheck   # mk/ci.mk
//
// Флаг -fix не предусмотрен: все три правила — про структуру импортов и
// вызовов, автоматически «починить» нарушение значило бы переписать чужой
// код за автора решения (перенести Publish в outbox, вынести инфраструктуру
// из app) — а такое решение нельзя принимать автоматически.
package main

import (
	"flag"
	"fmt"
	"os"

	"gosplash/archcheck"
)

func main() {
	root := flag.String("root", ".", "корень репозитория для сканирования")
	verbose := flag.Bool("v", false, "подробный вывод: какие исключения сработали и почему")
	flag.Parse()

	violations, err := archcheck.Run(*root, *verbose)
	if err != nil {
		fmt.Fprintln(os.Stderr, "archcheck:", err)
		os.Exit(2)
	}
	if len(violations) == 0 {
		fmt.Println("archcheck: нарушений не найдено")
		return
	}
	for _, v := range violations {
		fmt.Println(v.String())
	}
	fmt.Fprintf(os.Stderr, "archcheck: найдено нарушений: %d\n", len(violations))
	os.Exit(1)
}

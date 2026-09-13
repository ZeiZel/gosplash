// tools/archcheck — статический анализатор архитектурных правил монорепы
// (docs/STYLE.md): единственный путь в Kafka — через outbox, ports &
// adapters (internal/app и internal/domain не знают об инфраструктуре),
// сервисы не импортируют друг друга.
//
// Отдельный модуль, а не пакет в корневом gosplash: это АНАЛИЗАТОР кода,
// который сам не является частью рантайма ни одного сервиса и не должен
// тянуть их транзитивные зависимости (gorm, franz-go, grpc...). По этой же
// причине он намеренно не использует go/types и golang.org/x/tools/go/packages
// (см. package doc в archcheck.go) — единственная его зависимость это testify
// для тестов самого анализатора.
module gosplash/archcheck

go 1.27.1

require github.com/stretchr/testify v1.12.1

require go.yaml.in/yaml/v3 v3.0.5 // indirect

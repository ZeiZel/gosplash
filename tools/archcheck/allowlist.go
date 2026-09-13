package archcheck

// directProduceAllowlist — известные, осознанные исключения из правила
// direct-produce, для файлов, где нельзя поставить комментарий-маркер
// //archcheck:allow direct-produce прямо на месте.
//
// Маркер в комментарии — предпочтительный способ разрешить исключение:
// он живёт рядом с кодом, виден в диффе ревьюеру и требует объяснения на
// той же строке. Этот список — запасной путь на тот случай, когда правку
// файла делать нельзя (например: файл вне зоны ответственности задачи,
// которая заводит archcheck, — см. регламент фазы CI, где правки
// services/**, pkg/** и прочего чужого кода запрещены отдельно от
// .github/**, .golangci.yml, tools/archcheck/** и mk/ci.mk).
//
// Ключ — путь файла относительно корня сканирования (тот же вид, что и
// в Violation.File): "services/catalog/internal/adapters/kafka/..._batcher.go".
var directProduceAllowlist = map[string]string{
	"services/catalog/internal/adapters/kafka/view_batcher.go": "catalog публикует photo.viewed батчами напрямую, минуя outbox — " +
		"решение и его цена подробно разобраны в doc-комментарии пакета " +
		"(view_batcher.go, package kafka, раздел «НЕОЧЕВИДНОЕ РЕШЕНИЕ»): " +
		"объём просмотров на порядки выше остальных событий каталога, " +
		"потеря части при падении процесса допустима, а путь чтения " +
		"(ListingService.Get) не должен зависеть от доступности Kafka.",
}

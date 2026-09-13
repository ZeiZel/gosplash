// Package migrations — схема media-сервиса.
//
// Мигрирует ОДИН шард. Вызывающий (tools/automigrate) прогоняет эту функцию
// по каждому шарду: схема на них обязана быть идентичной, иначе один и тот же
// код будет работать с одной базой и падать на другой.
//
// Почему пакет не в internal/: его импортирует tools/automigrate, который
// лежит вне services/media, а из internal импортировать снаружи нельзя.
package migrations

import (
	"fmt"

	"gorm.io/gorm"

	"gosplash/pkg/idempotency"
	"gosplash/pkg/outbox"
	"gosplash/services/media/internal/adapters/pg"
)

// Migrate приводит схему шарда к текущим моделям.
//
// Источник схемы — структуры адаптера PostgreSQL, а не доменные типы: теги
// gorm живут там, и это правильное место для знания о том, как выглядит
// таблица (см. комментарий к PhotoRow).
//
// AutoMigrate создаёт таблицы, добавляет недостающие колонки и индексы.
// Чего он НЕ делает: не удаляет колонки, не меняет их тип «сужающе»
// и не переименовывает. Для учебного проекта этого достаточно; в проде
// рядом обычно живёт инструмент с версионированными миграциями
// (goose, migrate), потому что там нужен откат и предсказуемый порядок.
//
// outbox.OutboxRow и idempotency.ProcessedEvent — служебные таблицы паттернов
// (docs/adr/0008-*), а не бизнес-схема media, но живут в той же функции
// Migrate: вызывающий (tools/automigrate) прогоняет ЭТУ функцию по ОБОИМ
// шардам, а значит, ровно так же по обоим шардам разъедутся outbox и
// processed_events — и это то самое следствие шардирования media, о котором
// говорит docs/adr/0002-*: у каждого шарда своя, независимая копия обеих
// таблиц, а не одна общая на весь сервис.
func Migrate(db *gorm.DB) error {
	if err := db.AutoMigrate(&pg.PhotoRow{}); err != nil {
		return fmt.Errorf("photos: %w", err)
	}
	if err := outbox.Migrate(db, "media"); err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	if err := db.AutoMigrate(&idempotency.ProcessedEvent{}); err != nil {
		return fmt.Errorf("processed_events: %w", err)
	}
	return nil
}

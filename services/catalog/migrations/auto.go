// Package migrations — схема catalog-сервиса и настройка репликации.
//
// Здесь два разных дела:
//
//	Migrate            — таблицы. Через GORM AutoMigrate, как везде.
//	SetupReplication   — связь primary → реплика. Это уже не GORM, а обычный
//	                     SQL: CREATE PUBLICATION и CREATE SUBSCRIPTION.
//
// Репликация настраивается отсюда, а не скриптами в docker-compose,
// намеренно: так её видно из кода, она идемпотентна и применяется той же
// командой make migrate, что и всё остальное.
package migrations

import (
	"fmt"
	"strings"

	"gorm.io/gorm"

	"gosplash/pkg/idempotency"
	"gosplash/pkg/outbox"
	"gosplash/services/catalog/internal/adapters/pg"
)

// Имена объектов репликации. Публикация живёт на primary, подписка — на реплике.
const (
	publicationName  = "catalog_pub"
	subscriptionName = "catalog_sub"
)

// Migrate создаёт таблицы. Применяется И к primary, И к реплике.
//
// Реплику мигрировать тоже нужно — это ключевое отличие ЛОГИЧЕСКОЙ репликации
// от физической. Физическая копирует файлы кластера целиком, поэтому схема
// приезжает сама. Логическая передаёт только строки (INSERT/UPDATE/DELETE),
// а DDL не реплицируется вовсе: таблица на подписчике должна существовать
// заранее и совпадать по колонкам.
//
// Практическое следствие: добавил колонку — накати миграцию на обе базы,
// иначе репликация встанет с ошибкой.
//
// processed_events (pkg/idempotency) и outbox (pkg/outbox) — тоже сюда:
// оба пакета сознательно НЕ создают свои таблицы сами (см. их комментарии
// пакета), это обязанность вызывающего сервиса, чтобы схема была видна
// целиком в одном месте, а не размазана по AutoMigrate разных пакетов.
func Migrate(db *gorm.DB) error {
	if err := db.AutoMigrate(
		&pg.ListingRow{},
		&pg.AuthorSnapshotRow{},
		&pg.LicenseRow{},
		&idempotency.ProcessedEvent{},
	); err != nil {
		return fmt.Errorf("схема каталога: %w", err)
	}
	// Очередь outbox — отдельным вызовом: её имя зависит от сервиса
	// (outbox_catalog), и знает об этом сам пакет, а не вызывающий.
	if err := outbox.Migrate(db, "catalog"); err != nil {
		return err
	}
	if err := ensureTagsIndex(db); err != nil {
		return err
	}
	return nil
}

// ensureTagsIndex создаёт GIN-индекс на listings.tags отдельным SQL:
// AutoMigrate умеет создавать обычные btree-индексы по тегу gorm:"index",
// но не умеет выбрать METHOD (GIN) — без него оператор && (используется в
// ListListings при фильтрации по tags) выполнял бы последовательный скан
// таблицы карточек на каждый запрос ленты с фильтром.
func ensureTagsIndex(db *gorm.DB) error {
	err := db.Exec(
		"CREATE INDEX IF NOT EXISTS idx_listings_tags_gin ON listings USING GIN (tags)",
	).Error
	if err != nil {
		return fmt.Errorf("индекс по тегам: %w", err)
	}
	return nil
}

// SetupPublication объявляет на primary, что его изменения можно читать.
//
// FOR ALL TABLES — «публиковать всё, что есть и появится». Для учебного
// проекта это то, что нужно; в проде обычно перечисляют таблицы явно,
// чтобы случайно не начать реплицировать что-нибудь тяжёлое.
//
// Требует wal_level=logical на сервере — он выставлен в
// deploy/compose/postgres.yml.
func SetupPublication(db *gorm.DB) error {
	var exists bool
	err := db.Raw(
		"SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = ?)", publicationName,
	).Scan(&exists).Error
	if err != nil {
		return fmt.Errorf("проверка публикации: %w", err)
	}
	if exists {
		return nil
	}

	// Имя объекта нельзя передать параметром — параметры бывают только
	// у значений, не у идентификаторов. Здесь константа, так что это безопасно.
	if err := db.Exec("CREATE PUBLICATION " + publicationName + " FOR ALL TABLES").Error; err != nil {
		return fmt.Errorf("создание публикации: %w", err)
	}
	return nil
}

// SetupSubscription подписывает реплику на изменения primary.
//
// conn — строка подключения, которую будет использовать СЕРВЕР РЕПЛИКИ,
// а не твой ноутбук. Поэтому в ней docker-адрес pg-catalog:5432, а не
// localhost:55434. Классическая ошибка на этом месте: подставить туда тот же
// DSN, которым подключается приложение, и полчаса смотреть на «connection
// refused» в логах реплики.
//
// При создании подписка сначала КОПИРУЕТ текущее содержимое таблиц, а потом
// переходит в режим потока изменений. Поэтому порядок в automigrate такой:
// сначала схема на обеих базах, потом публикация, потом подписка.
func SetupSubscription(db *gorm.DB, conn string) error {
	var exists bool
	err := db.Raw(
		"SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = ?)", subscriptionName,
	).Scan(&exists).Error
	if err != nil {
		return fmt.Errorf("проверка подписки: %w", err)
	}
	if exists {
		return nil
	}

	// ВАЖНО: CREATE SUBSCRIPTION нельзя выполнить внутри транзакции —
	// команда создаёт слот репликации на другом сервере, а это действие
	// нельзя откатить. Поэтому tools/automigrate не оборачивает миграции
	// в db.Transaction. В той же компании: CREATE DATABASE, CREATE INDEX
	// CONCURRENTLY, VACUUM.
	stmt := fmt.Sprintf(
		"CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s",
		subscriptionName, quote(conn), publicationName,
	)
	if err := db.Exec(stmt).Error; err != nil {
		return fmt.Errorf("создание подписки: %w", err)
	}
	return nil
}

// quote оборачивает строку в одинарные кавычки для SQL-литерала.
func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

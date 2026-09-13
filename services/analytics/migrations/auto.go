// Package migrations — схема ClickHouse для analytics-сервиса.
//
// GORM здесь НЕ используется, в отличие от всех Postgres-сервисов монорепы
// (services/catalog/migrations, services/media/...). Три причины, и каждой
// по отдельности достаточно:
//
//  1. gorm.io/driver/clickhouse моделирует ClickHouse как ЕЩЁ ОДНУ SQL-базу
//     с обычными таблицами — AutoMigrate умеет CREATE TABLE, но не умеет
//     сказать ENGINE = MergeTree, PARTITION BY, ORDER BY или TTL: это не
//     опции столбца или индекса в терминах GORM, а сама суть движка
//     ClickHouse. Пришлось бы либо мигрировать GORM'ом и ДОБАВЛЯТЬ руками
//     ALTER TABLE после него, либо не мигрировать GORM'ом вовсе — второе
//     честнее, чем наполовину рабочая абстракция.
//  2. AutoMigrate рассчитан на добавление недостающих колонок к уже
//     существующей таблице (эволюция схемы). В ClickHouse смена ORDER BY,
//     PARTITION BY или движка задним числом — это не ALTER, а пересоздание
//     таблицы (детали — в README сервиса, раздел "почему нет UPDATE"):
//     ключевое решение о структуре хранения принимается ОДИН раз при
//     создании, и представлять его как эволюционируемую GORM-модель значит
//     обещать то, чего движок не может дать.
//  3. Материализованное представление (photo_views_daily_mv) — это вообще
//     не таблица в понимании GORM, а сохранённый SELECT, который движок сам
//     перевыполняет на каждый INSERT в источник. Для него нет ни отдалённого
//     аналога в GORM, ни смысла его туда впихивать.
//
// Итог: схема — обычные SQL-строки, применяемые idempotent-но
// (CREATE ... IF NOT EXISTS) при старте сервиса (см. cmd/analytics/main.go).
// Это ровно то же самое, что делает CREATE PUBLICATION в
// services/catalog/migrations/auto.go — там GORM тоже неприменим по той же
// причине (DDL, которого нет в его словаре), и решение то же: обычный SQL.
package migrations

import (
	"context"
	"fmt"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// statements — DDL в порядке, в котором его обязан увидеть ClickHouse:
// таблица-источник, таблица-приёмник материализованного представления,
// само представление (ему нужны обе), и независимая таблица purchases.
var statements = []string{
	// photo_views — сырые события. ORDER BY (photo_id, ts), а не (ts) и не
	// (author_id, ts): подробный разбор — в README сервиса. Коротко: обе
	// сводки сервиса (PhotoStats и материализованное представление ниже)
	// фильтруют и группируют ПО ФОТО, а тонкий физический порядок на диске
	// (разреженный индекс по гранулам, см. README) выгоден только тем
	// запросам, чей WHERE/GROUP BY совпадает с первыми колонками ORDER BY.
	//
	// TTL на год — просмотры годовой давности не нужны ни одному отчёту
	// проекта (period ограничен day/week/month, см. proto), а хранить их
	// вечно значило бы бесплатно растить диск под данные, которые никто
	// никогда не прочитает.
	`CREATE TABLE IF NOT EXISTS photo_views
	(
		photo_id  UUID,
		author_id Int64,
		-- Nullable(Int64), а не 0 "как в protobuf": ноль — валидный
		-- viewer_id, и COUNT/uniqCombined обязаны отличать "аноним" от
		-- "зритель номер ноль". Конвертация 0→NULL происходит на границе
		-- Kafka→домен, см. internal/app/ingest.go.
		viewer_id Nullable(Int64),
		ts        DateTime64(3),
		-- LowCardinality(String), а не String: страна — код из фиксированного
		-- набора (меньше 300 значений на всей Земле), и словарное кодирование
		-- ЗДЕСЬ выигрывает: ClickHouse хранит не строку на каждую строку
		-- таблицы, а маленький словарь ("RU", "US", "DE"...) и индекс в нём
		-- (обычно 1 байт), плюс GROUP BY country сравнивает целые числа,
		-- а не строки. Проигрывает LowCardinality там, где значений МНОГО
		-- (email, url, свободный текст) — тогда словарь разрастается почти
		-- до размера самих данных, а перестройка словаря на каждую вставку
		-- новых уникальных значений становится дополнительной стоимостью
		-- без всякой экономии.
		country   LowCardinality(String)
	)
	ENGINE = MergeTree
	PARTITION BY toYYYYMM(ts)
	ORDER BY (photo_id, ts)
	TTL toDateTime(ts) + INTERVAL 1 YEAR`,

	// photo_views_daily — предагрегированная витрина для TopPhotos: без неё
	// каждый запрос топа считал бы sum() по ВСЕМ сырым просмотрам таблицы
	// photo_views за период. SummingMergeTree складывает строки с одинаковым
	// ORDER BY (photo_id, day) В ФОНЕ, во время мержа партов, — то есть
	// иногда после SELECT'а на диске всё ещё лежит несколько
	// несведённых строк на одну и ту же пару (photo_id, day). Явный
	// sum(views) в запросах (см. adapters/clickhouse/repository.go) —
	// это не перестраховка "на всякий случай", а обязательное следствие
	// того, как SummingMergeTree устроен: он ГОТОВИТ данные к дешёвому
	// sum(), а не гарантирует, что каждая пара ключа уже единственна
	// на момент чтения.
	`CREATE TABLE IF NOT EXISTS photo_views_daily
	(
		day      Date,
		photo_id UUID,
		views    UInt64
	)
	ENGINE = SummingMergeTree(views)
	PARTITION BY toYYYYMM(day)
	ORDER BY (photo_id, day)`,

	// photo_views_daily_mv — MATERIALIZED VIEW ... TO <таблица>: в отличие
	// от обычного MV (который завёл бы свою СКРЫТУЮ таблицу-приёмник с
	// автосгенерированным именем), TO указывает уже созданную выше
	// photo_views_daily явно — так её движок (SummingMergeTree) и её
	// TTL/партиционирование видны в этом же файле одной DDL-командой выше,
	// а не спрятаны в недрах автоматически заведённой таблицы.
	//
	// Работает по триггеру: на каждый INSERT в photo_views ClickHouse
	// прогоняет SELECT ниже по ТОЛЬКО ЧТО вставленному блоку строк (не по
	// всей таблице) и результат сам вставляет в photo_views_daily. Поэтому
	// исторические данные, вставленные ДО создания представления, в
	// photo_views_daily не попадут — представление видит только то, что
	// приходит после его создания. Для чистого запуска сервиса это не
	// проблема (миграция создаёт всё с нуля), но это ровно то, из-за чего
	// в проде реальные MV сопровождают разовым INSERT SELECT по истории.
	`CREATE MATERIALIZED VIEW IF NOT EXISTS photo_views_daily_mv
	TO photo_views_daily
	AS
	SELECT
		toDate(ts) AS day,
		photo_id,
		count() AS views
	FROM photo_views
	GROUP BY day, photo_id`,

	// purchases — из order.order.paid. ORDER BY (photo_id, ts) — та же
	// причина, что и у photo_views: PhotoStats фильтрует и по фото, и
	// TopPhotos джойнит по фото (adapters/clickhouse/repository.go).
	`CREATE TABLE IF NOT EXISTS purchases
	(
		order_id    UUID,
		photo_id    UUID,
		author_id   Int64,
		buyer_id    Int64,
		price_cents Int64,
		ts          DateTime64(3)
	)
	ENGINE = MergeTree
	PARTITION BY toYYYYMM(ts)
	ORDER BY (photo_id, ts)`,
}

// Migrate применяет схему. Идемпотентна (IF NOT EXISTS везде) — безопасно
// вызывать на каждом старте сервиса, как и Migrate у остальных сервисов
// монорепы (см. services/catalog/migrations/auto.go).
func Migrate(ctx context.Context, conn chdriver.Conn) error {
	for i, stmt := range statements {
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("миграция ClickHouse #%d: %w", i, err)
		}
	}
	return nil
}

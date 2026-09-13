// tools/automigrate — миграции всех баз проекта одной командой.
//
//	make migrate                 все базы
//	make migrate ONLY=media      только шарды media
//
// Почему отдельный бинарник, а не миграция при старте сервиса:
//   - сервис может подняться в трёх репликах, и они начнут мигрировать
//     наперегонки, ловя дедлоки на DDL;
//   - удобно накатить всё сразу после docker compose up;
//   - рантайм сервиса не тянет за собой лишние зависимости.
//
// Про изоляцию: это единственное место в монорепе, которому разрешено знать
// про все сервисы сразу. Сами сервисы друг друга по-прежнему не импортируют.
// Поэтому пакет со схемой у каждого сервиса называется migrations, а не
// internal/migrations — из internal его отсюда импортировать было бы нельзя.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"gorm.io/gorm"

	"gosplash/pkg/dbx"
	catalogmig "gosplash/services/catalog/migrations"
	mediamig "gosplash/services/media/migrations"
	ordermig "gosplash/services/order/migrations"
	thumbmig "gosplash/services/thumbnail-worker/migrations"
	walletmig "gosplash/services/wallet/migrations"
)

// target — одна база, которую нужно привести в порядок.
type target struct {
	service string               // для фильтра ONLY
	name    string               // для логов
	envKey  string               // переменная с DSN
	migrate func(*gorm.DB) error // схема этой базы
}

func main() {
	only := flag.String("only", "", "мигрировать только один сервис: media|thumbnail|catalog|wallet|order")
	flag.Parse()

	if err := godotenv.Load(); err != nil {
		log.Println("automigrate: .env не найден, читаю переменные окружения")
	}

	targets := []target{
		// Шарды media — два target'а с ОДНОЙ функцией миграции.
		// Схема на шардах обязана быть идентичной: код не знает, на какой
		// сервер попадёт, и любое расхождение вылезет случайной ошибкой
		// у половины пользователей.
		{"media", "media shard 0", "MEDIA_SHARD_0_DSN", mediamig.Migrate},
		{"media", "media shard 1", "MEDIA_SHARD_1_DSN", mediamig.Migrate},

		// thumbnail-worker живёт на ТЕХ ЖЕ шардах, что и media: отметка об
		// обработке события и смена статуса фото обязаны попасть в одну
		// транзакцию, а транзакция между двумя серверами невозможна
		// (docs/adr/0002-*). Свои таблицы (processed_events и собственную
		// очередь outbox_thumbnail-worker) он создаёт отдельной миграцией —
		// у каждого сервиса своя очередь, даже когда база общая.
		{"thumbnail", "thumbs shard 0", "MEDIA_SHARD_0_DSN", thumbmig.Migrate},
		{"thumbnail", "thumbs shard 1", "MEDIA_SHARD_1_DSN", thumbmig.Migrate},

		// Каталог: primary и реплика. Реплику тоже мигрируем — при логической
		// репликации DDL не передаётся, таблицы на подписчике создаём сами.
		{"catalog", "catalog primary", "CATALOG_DSN", catalogmig.Migrate},
		{"catalog", "catalog replica", "CATALOG_REPLICA_DSN", catalogmig.Migrate},

		// Деньги и заказы: по одной базе на сервис, без шардов и без реплик.
		// Шардировать счета нельзя в принципе — перевод между двумя счетами
		// на разных серверах перестал бы быть транзакцией, и это уже не
		// «медленнее», а «неверно».
		{"wallet", "wallet", "WALLET_DSN", walletmig.Migrate},
		{"order", "order", "ORDER_DSN", ordermig.Migrate},
	}

	failed := 0
	for _, t := range targets {
		if *only != "" && t.service != *only {
			continue
		}
		if err := run(t); err != nil {
			log.Printf("✗ %-16s %v", t.name, err)
			failed++
			continue
		}
		log.Printf("✓ %-16s схема применена", t.name)
	}

	// Репликация настраивается ПОСЛЕ схемы: подписка при создании копирует
	// содержимое таблиц, а копировать в несуществующую таблицу нельзя.
	if *only == "" || *only == "catalog" {
		if err := setupReplication(); err != nil {
			log.Printf("✗ %-16s %v", "репликация", err)
			failed++
		}
	}

	if failed > 0 {
		os.Exit(1)
	}
}

func run(t target) error {
	dsn := os.Getenv(t.envKey)
	if dsn == "" {
		return fmt.Errorf("не задан %s", t.envKey)
	}

	conn, err := open(dsn)
	if err != nil {
		return err
	}
	defer closeDB(conn)

	// Транзакции вокруг миграции здесь намеренно НЕТ.
	//
	// Postgres умеет транзакционный DDL, и обычно обернуть миграцию в
	// транзакцию — хорошая идея. Но CREATE SUBSCRIPTION внутри транзакции
	// выполнить нельзя: команда идёт на другой сервер и создаёт там слот
	// репликации, а такое действие не откатывается. В той же компании
	// CREATE DATABASE, CREATE INDEX CONCURRENTLY и VACUUM.
	//
	// AutoMigrate при этом идемпотентен: повторный запуск ничего не сломает.
	return t.migrate(conn)
}

// setupReplication связывает primary и реплику каталога.
//
// Порядок важен: сначала публикация на primary, потом подписка на реплике.
// Обе операции идемпотентны — повторный make migrate ничего не сломает.
func setupReplication() error {
	primaryDSN := os.Getenv("CATALOG_DSN")
	replicaDSN := os.Getenv("CATALOG_REPLICA_DSN")
	if primaryDSN == "" || replicaDSN == "" {
		log.Printf("• %-16s пропускаю: не задан CATALOG_DSN или CATALOG_REPLICA_DSN", "репликация")
		return nil
	}

	// Строку подключения использует СЕРВЕР РЕПЛИКИ, поэтому адрес здесь
	// внутренний, docker'овский (pg-catalog:5432), а не localhost:55434.
	conn := os.Getenv("CATALOG_REPLICATION_CONN")
	if conn == "" {
		return fmt.Errorf("не задан CATALOG_REPLICATION_CONN")
	}

	primary, err := open(primaryDSN)
	if err != nil {
		return err
	}
	defer closeDB(primary)

	if err := catalogmig.SetupPublication(primary); err != nil {
		return err
	}
	log.Printf("✓ %-16s публикация на primary", "репликация")

	replica, err := open(replicaDSN)
	if err != nil {
		return err
	}
	defer closeDB(replica)

	if err := catalogmig.SetupSubscription(replica, conn); err != nil {
		return err
	}
	log.Printf("✓ %-16s подписка на реплике", "репликация")
	return nil
}

// open подключается и ждёт, пока Postgres поднимется: сразу после
// docker compose up сервер может ещё инициализироваться.
func open(dsn string) (*gorm.DB, error) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := dbx.Open(dsn)
		if err == nil {
			return conn, nil
		}
		// Ошибка не про доступность (неверный пароль, нет базы) — ждать нечего.
		if !strings.Contains(err.Error(), "connection refused") &&
			!strings.Contains(err.Error(), "starting up") {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("база не поднялась за 30с: %w", err)
		}
		time.Sleep(time.Second)
	}
}

func closeDB(conn *gorm.DB) {
	if sqlDB, err := conn.DB(); err == nil {
		_ = sqlDB.Close()
	}
}

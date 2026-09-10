// tools/automigrate — прогоняет миграции по всем базам проекта.
//
// Запуск:   go run ./tools/automigrate            (все базы)
//
//	go run ./tools/automigrate -only media (только шарды media)
//
// Почему отдельный бинарник, а не миграция при старте каждого сервиса:
//   - сервис может подняться в 3 репликах и они начнут мигрировать наперегонки;
//   - удобно прогнать всё одной командой после docker compose up;
//   - миграции не тянут в рантайм сервиса лишних зависимостей.
//
// Важно про изоляцию: этот тул импортирует пакет migrations КАЖДОГО сервиса.
// Это единственное место в монорепе, которому разрешено знать про все
// сервисы сразу. Сами сервисы друг друга по-прежнему не импортируют.
// Поэтому пакет называется migrations, а не internal/migrations —
// из internal его отсюда импортировать нельзя.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	catalogmig "photostock/services/catalog/migrations"
	mediamig "photostock/services/media/migrations"
	ordermig "photostock/services/order/migrations"
	walletmig "photostock/services/wallet/migrations"
)

// target — одна база, которую нужно смигрировать.
type Target struct {
	service string               // к какому сервису относится (для фильтра -only)
	name    string               // человекочитаемое имя для логов
	envKey  string               // переменная окружения с DSN
	def     string               // DSN по умолчанию (docker-compose с хоста)
	migrate func(*gorm.DB) error // функция миграции из пакета сервиса
}

// Все базы проекта. Шарды media — два target'а с ОДНОЙ функцией миграции:
// схема на шардах обязана быть идентичной.
var targets = []Target{
	{"media", "media shard 0", "MEDIA_SHARD_0_DSN", dsn(5432), mediamig.Migrate},
	{"media", "media shard 1", "MEDIA_SHARD_1_DSN", dsn(5433), mediamig.Migrate},
	{"catalog", "catalog", "CATALOG_DSN", dsn(5434), catalogmig.Migrate},
	{"wallet", "wallet", "WALLET_DSN", dsn(5435), walletmig.Migrate},
	{"order", "order", "ORDER_DSN", dsn(5436), ordermig.Migrate},
}

func dsn(port int) string {
	return fmt.Sprintf(
		"host=localhost port=%d user=photostock password=photostock dbname=photostock sslmode=disable",
		port,
	)
}

func main() {
	only := flag.String("only", "", "мигрировать только один сервис: media|catalog|wallet|order")
	flag.Parse()

	failed := 0
	for _, t := range targets {
		if *only != "" && t.service != *only {
			continue
		}
		if err := run(t); err != nil {
			log.Printf("✗ %-15s %v", t.name, err)
			failed++
			continue
		}
		log.Printf("✓ %-15s ok", t.name)
	}

	if failed > 0 {
		os.Exit(1)
	}
}

func run(t target) error {
	d := os.Getenv(t.envKey)
	if d == "" {
		d = t.def
	}

	db, err := gorm.Open(postgres.Open(d), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	// Postgres в compose может быть ещё не готов — ждём до 30 секунд.
	if err := waitReady(db, 30*time.Second); err != nil {
		return err
	}

	// Вся миграция одной транзакцией: либо применилась целиком, либо нет.
	// Postgres умеет транзакционный DDL, в отличие от MySQL.
	return db.Transaction(func(tx *gorm.DB) error {
		return t.migrate(tx)
	})
}

func waitReady(db *gorm.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var one int
		err := db.Raw("SELECT 1").Scan(&one).Error
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("db not ready after %s: %w", timeout, err)
		}
		if !strings.Contains(err.Error(), "connection refused") {
			return err // другая ошибка (неверный пароль и т.п.) — не ждём
		}
		time.Sleep(time.Second)
	}
}

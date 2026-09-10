package db

import (
	"gosplash/configs"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Timestamps struct {
	CreatedAt time.Time
	UpdatedAt time.Time
}

type DB struct {
	*gorm.DB
}

func NewDB(conf *configs.Config) *DB {
	db, err := gorm.Open(postgres.Open(conf.DB.dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	return &DB{db}
}

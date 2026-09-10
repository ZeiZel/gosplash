package configs

import (
	"log"
	"os"
)

type DBConfig struct {
	DSN string
}

type AuthConfig struct {
	Secret string
}

type Config struct {
	DB   DBConfig
	Auth AuthConfig
}

func LoadConfig() *Config {
	err := godotenv.Load()

	if err != nil {
		log.Println("Error loading of .env")
	}

	return &Config{
		DB: DBConfig{
			DSN: os.Getenv("DSN")
		},
		Auth: AuthConfig{
			Secret: os.Getenv("SECRET"),
		},
	}
}


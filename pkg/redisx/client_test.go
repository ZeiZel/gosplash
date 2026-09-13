package redisx

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKey_StroitNamespaceGosplashServiceParts(t *testing.T) {
	c := &Client{service: "catalog"}

	assert.Equal(t, "gosplash:catalog:listing:42", c.Key("listing", "42"))
	assert.Equal(t, "gosplash:catalog", c.Key(),
		"без частей ключ — это просто namespace сервиса, тоже законный случай")
}

func TestKey_RazniyeServisyNeStalkivayutsya(t *testing.T) {
	// Смысл namespace целиком в этом: два сервиса с одинаковым "логическим"
	// именем ключа обязаны получить РАЗНЫЕ физические ключи в общем Redis
	// (см. deploy/compose/redis.yml — инстанс один на весь проект).
	catalog := &Client{service: "catalog"}
	media := &Client{service: "media"}

	assert.NotEqual(t, catalog.Key("photo", "1"), media.Key("photo", "1"))
}

func TestKindFromKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{"обычный ключ", "gosplash:catalog:listing:42", "listing"},
		{"ключ без хвоста", "gosplash:catalog:listing", "listing"},
		{"составной хвост", "gosplash:catalog:views:20260101", "views"},
		{"меньше трёх сегментов — не наш формат", "gosplash:catalog", "unknown"},
		{"вообще не ключ redisx", "просто-строка", "unknown"},
		{"пустая строка", "", "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, kindFromKey(tt.key))
		})
	}
}

func TestNew_TrebuetServiceNameIAddr(t *testing.T) {
	// Обе проверки не должны требовать живого Redis: это ошибки конфигурации,
	// а не сети, и обнаруживаться они обязаны до первой попытки подключиться.
	_, err := New(Config{Addr: "localhost:0"})
	require.Error(t, err, "пустой ServiceName обязан быть отвергнут")
	assert.Contains(t, err.Error(), "ServiceName")

	_, err = New(Config{ServiceName: "catalog"})
	require.Error(t, err, "пустой Addr обязан быть отвергнут")
	assert.Contains(t, err.Error(), "Addr")
}

func TestConfig_PolyaSootvetstvuyutSpecifikacii(t *testing.T) {
	// Фиксирует набор полей Config — контракт, на который опирается
	// pkg/config при заполнении редис-конфигурации сервисов.
	cfg := Config{
		Addr:        "localhost:6379",
		Password:    "секрет",
		DB:          1,
		PoolSize:    20,
		MaxRetries:  3,
		DialTimeout: 5 * time.Second,
		ReadTimeout: 3 * time.Second,
		ServiceName: "catalog",
	}
	assert.Equal(t, "catalog", cfg.ServiceName)
	assert.Equal(t, 20, cfg.PoolSize)
}

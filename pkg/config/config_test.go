package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnv(t *testing.T) {
	t.Setenv("GOSPLASH_TEST_KEY", "значение")

	assert.Equal(t, "значение", env("GOSPLASH_TEST_KEY", "по умолчанию"))
	assert.Equal(t, "по умолчанию", env("GOSPLASH_TEST_NET_TAKOY", "по умолчанию"))

	// Пустая строка трактуется как «не задано». Это сознательный выбор:
	// в docker-compose переменная без значения приезжает именно пустой
	// строкой, и считать её осмысленным значением почти всегда не то,
	// чего хотел автор конфига.
	t.Setenv("GOSPLASH_TEST_KEY", "")
	assert.Equal(t, "по умолчанию", env("GOSPLASH_TEST_KEY", "по умолчанию"))
}

func TestDuration(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{"минуты", "15m", 15 * time.Minute},
		{"секунды", "30s", 30 * time.Second},
		{"не задано", "", time.Hour},
		{"мусор — берём значение по умолчанию", "пятнадцать минут", time.Hour},
		{"число без единицы измерения тоже мусор", "15", time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("GOSPLASH_TEST_TTL", tt.value)
			assert.Equal(t, tt.want, duration("GOSPLASH_TEST_TTL", time.Hour))
		})
	}
}

func TestInteger(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  int
	}{
		{"число", "42", 42},
		{"ноль — валидное значение", "0", 0},
		{"не задано", "", 7},
		{"мусор", "много", 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("GOSPLASH_TEST_INT", tt.value)
			assert.Equal(t, tt.want, integer("GOSPLASH_TEST_INT", 7))
		})
	}
}

func TestRatio(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  float64
	}{
		{"доля", "0.05", 0.05},
		{"ноль — выключить трассировку", "0", 0},
		{"единица", "1", 1},
		{"больше единицы — не доля", "2", 1.0},
		{"отрицательное — не доля", "-0.5", 1.0},
		{"мусор", "половина", 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("GOSPLASH_TEST_RATIO", tt.value)
			assert.Equal(t, tt.want, ratio("GOSPLASH_TEST_RATIO", 1.0))
		})
	}
}

func TestShardDSNs_ChitaetPodryadIOstanavlivaetsyaNaProbele(t *testing.T) {
	t.Setenv("MEDIA_SHARD_0_DSN", "dsn-0")
	t.Setenv("MEDIA_SHARD_1_DSN", "dsn-1")
	// Третьей переменной нет — значит, шардов два.
	t.Setenv("MEDIA_SHARD_3_DSN", "dsn-3")

	// Порядок в срезе — это и есть номера шардов, поэтому проверяем именно
	// последовательность, а не множество. Перепутанный порядок означал бы,
	// что hash(user_id) указывает на чужую базу.
	assert.Equal(t, []string{"dsn-0", "dsn-1"}, shardDSNs(),
		"чтение обязано остановиться на первой пропущенной переменной, "+
			"иначе дыра в нумерации молча сдвинет все шарды")
}

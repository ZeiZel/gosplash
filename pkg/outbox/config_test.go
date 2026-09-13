package outbox

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfig_WithDefaults_PodstavlyaetNulevye(t *testing.T) {
	cfg := Config{ServiceName: "media", Brokers: []string{"kafka:9092"}, DSNs: []string{"dsn"}}

	got := cfg.withDefaults()

	assert.Equal(t, DefaultConfig().PollInterval, got.PollInterval)
	assert.Equal(t, DefaultConfig().BatchSize, got.BatchSize)
	// Явно заданные поля не трогаем.
	assert.Equal(t, "media", got.ServiceName)
	assert.Equal(t, []string{"dsn"}, got.DSNs)
}

func TestConfig_WithDefaults_NeTrogaetYavnoZadannye(t *testing.T) {
	cfg := Config{PollInterval: 5 * time.Second, BatchSize: 50}

	got := cfg.withDefaults()

	assert.Equal(t, 5*time.Second, got.PollInterval)
	assert.Equal(t, 50, got.BatchSize)
}

func TestConfig_WithDefaults_OtritsatelnyeTozheZamenyayutsya(t *testing.T) {
	cfg := Config{PollInterval: -1, BatchSize: -1}

	got := cfg.withDefaults()

	assert.Positive(t, got.PollInterval)
	assert.Positive(t, got.BatchSize)
}

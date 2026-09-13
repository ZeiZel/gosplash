package redisx

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWithJitter_PopadaetVDiapazonDesyatiProcentov(t *testing.T) {
	ttl := 10 * time.Minute
	lower := time.Duration(float64(ttl) * (1 - jitterFraction))
	upper := time.Duration(float64(ttl) * (1 + jitterFraction))

	for i := 0; i < 500; i++ {
		got := withJitter(ttl)
		assert.GreaterOrEqualf(t, got, lower, "джиттер не должен опускать TTL ниже -10%%: %v", got)
		assert.LessOrEqualf(t, got, upper, "джиттер не должен поднимать TTL выше +10%%: %v", got)
	}
}

func TestWithJitter_ZначенияRazlichayutsyaMezhduVyzovami(t *testing.T) {
	// Смысл джиттера — размазать протухание по времени. Если бы withJitter
	// всегда возвращал одно и то же значение (например, из-за общего
	// зафиксированного seed), тысяча ключей продолжала бы протухать
	// синхронно, и весь смысл функции терялся бы, несмотря на формально
	// «случайную» формулу внутри.
	ttl := 10 * time.Minute

	seen := make(map[time.Duration]struct{})
	for i := 0; i < 50; i++ {
		seen[withJitter(ttl)] = struct{}{}
	}

	assert.Greaterf(t, len(seen), 1,
		"withJitter вернул одно и то же значение %d раз подряд — джиттер не работает", 50)
}

func TestWithJitter_NulevoyTTLOstaetsyaNulevym(t *testing.T) {
	// TTL=0 в go-redis означает "без истечения". Джиттер не должен превращать
	// "кэшируем навсегда" в "кэшируем на случайные несколько наносекунд".
	assert.Equal(t, time.Duration(0), withJitter(0))
}

func TestWithJitter_OtritsatelnyyTTLNeMenyaetsya(t *testing.T) {
	assert.Equal(t, time.Duration(-1), withJitter(-1))
}

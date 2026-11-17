//go:build !integration

package redisx

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain ловит утечки горутин между тестами: redisx поднимает горутины
// фоновой очистки (TTL-наблюдатели, ratelimit), и забытый Close был бы
// незаметен без проверки. Под тегом integration выключен — testcontainers
// (Redis) сам оставляет горутины после себя.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

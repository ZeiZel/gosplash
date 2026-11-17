//go:build !integration

package resilience

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain ловит утечки горутин между тестами: resilience не должен
// порождать неостанавливаемых горутин — breaker и retrier обходятся
// без них, поэтому любой посторонний след в стеке это баг теста или
// кода. Под тегом integration выключен — тесты с контейнерами сами
// оставляют горутины после себя.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

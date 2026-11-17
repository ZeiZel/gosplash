//go:build !integration

package kafkax

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain ловит утечки горутин между тестами: kafkax крутит движки с
// фоновыми горутинами (consumer, DLQ-писатель), и забытый Stop был бы
// незаметен без проверки. Под тегом integration выключен — testcontainers
// сам оставляет горутины после себя.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

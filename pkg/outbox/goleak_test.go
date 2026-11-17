//go:build !integration

package outbox

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain ловит утечки горутин между тестами: relay крутит горутину
// опроса outbox-таблицы, и забытая остановка была бы незаметна без
// проверки. Под тегом integration выключен — testcontainers (Postgres)
// сам оставляет горутины после себя.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

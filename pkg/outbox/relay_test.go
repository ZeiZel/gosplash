package outbox

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestHeadersToRecordHeaders_ParsitJSON(t *testing.T) {
	headers, err := headersToRecordHeaders([]byte(`{"event_id":"e-1","event_type":"media.photo.uploaded"}`))
	require.NoError(t, err)
	require.Len(t, headers, 2)

	get := func(key string) (string, bool) {
		for _, h := range headers {
			if h.Key == key {
				return string(h.Value), true
			}
		}
		return "", false
	}

	v, ok := get("event_id")
	assert.True(t, ok)
	assert.Equal(t, "e-1", v)
}

func TestHeadersToRecordHeaders_PustyeVhodnyeDannye(t *testing.T) {
	headers, err := headersToRecordHeaders(nil)
	require.NoError(t, err)
	assert.Empty(t, headers)

	headers, err = headersToRecordHeaders([]byte(`{}`))
	require.NoError(t, err)
	assert.Empty(t, headers)
}

func TestHeadersToRecordHeaders_BitiyJSONVozvrashaetOshibku(t *testing.T) {
	_, err := headersToRecordHeaders([]byte(`не json`))
	assert.Error(t, err)
}

func TestHasHeader(t *testing.T) {
	headers := []kgo.RecordHeader{
		{Key: "event_id", Value: []byte("e-1")},
		{Key: "producer", Value: []byte("media")},
	}

	assert.True(t, hasHeader(headers, "producer"))
	assert.False(t, hasHeader(headers, "traceparent"))
	assert.False(t, hasHeader(nil, "producer"))
}

// Механика «чистить не чаще раза в интервал»: нулевой lastCleanup (шард ещё
// ни разу не чистился) — пора; недавняя чистка — не пора, даже если интервал
// крошечный; ровно по истечении интервала — снова пора. Сам DELETE требует
// Postgres и живёт в интеграционных тестах (relay_integration_test.go),
// здесь проверяется только решение, которое легко сломать рефакторингом.
func TestCleanupDue_NeChashcheRazaVInterval(t *testing.T) {
	now := time.Now()
	interval := time.Hour

	// Ни разу не чистили (нулевой lastCleanup): первая же возможность.
	assert.True(t, cleanupDue(now, time.Time{}, interval))

	// Чистили только что.
	assert.False(t, cleanupDue(now, now.Add(-interval/2), interval))

	// Ровно интервал назад — граница включительно.
	assert.True(t, cleanupDue(now, now.Add(-interval), interval))

	// Интервал прошёл с запасом.
	assert.True(t, cleanupDue(now, now.Add(-3*interval), interval))
}

// Retention=0 — выключено: maybeCleanup обязан уйти до любого обращения
// к пулу (шард здесь вообще без пула — nil), и метка lastCleanup не должна
// сдвинуться.
func TestMaybeCleanup_VyvklyuchennayaChistkaNeTrogaetShard(t *testing.T) {
	r := &Relay{cfg: Config{Retention: 0, CleanupInterval: time.Hour}}
	s := &shard{index: 0} // pool намеренно nil: доказательство, что до него не дойдут

	r.maybeCleanup(context.Background(), s)

	assert.True(t, s.lastCleanup.IsZero(), "выключенная чистка не должна отмечаться выполненной")
}

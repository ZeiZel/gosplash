package redisx

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdempotencyStatus_TriRazlichnyhZnacheniya(t *testing.T) {
	// Три исхода обязаны быть попарно различимы — иначе вызывающий код
	// не сможет положиться на switch по Status и часть веток станет
	// недостижимой незаметно для компилятора.
	statuses := []IdempotencyStatus{IdempotencyFree, IdempotencyInProgress, IdempotencyDone}
	seen := make(map[IdempotencyStatus]bool)
	for _, s := range statuses {
		assert.False(t, seen[s], "статус %v встречается повторно", s)
		seen[s] = true
	}
}

func TestIdempotencyResult_ZeroValueEstSvobodno(t *testing.T) {
	// Нулевое значение IdempotencyResult{} обязано читаться как IdempotencyFree
	// (iota == 0) — это подстраховка на случай, если кто-то забудет
	// инициализировать Status явно.
	var r IdempotencyResult
	assert.Equal(t, IdempotencyFree, r.Status)
	assert.Nil(t, r.Response)
}

func TestIdempotencyEnvelope_JSONRoundTrip(t *testing.T) {
	// envelope — внутренний формат хранения в Redis. Тест фиксирует, что он
	// переживает marshal/unmarshal без потерь, включая бинарный Response
	// (encoding/json сам base64-кодирует []byte).
	original := idempotencyEnvelope{
		Status:   envelopeDone,
		Response: []byte(`{"order_id":"abc-123"}`),
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var restored idempotencyEnvelope
	require.NoError(t, json.Unmarshal(data, &restored))

	assert.Equal(t, original, restored)
}

func TestIdempotencyEnvelope_InProgressBezResponse(t *testing.T) {
	env := idempotencyEnvelope{Status: envelopeInProgress}

	data, err := json.Marshal(env)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "response",
		"omitempty обязан убрать поле response для in_progress — иначе Begin "+
			"мог бы случайно принять null за пустой, но валидный сохранённый ответ")
}

func TestDefaultIdempotencyTTL(t *testing.T) {
	assert.Equal(t, 24*time.Hour, DefaultIdempotencyTTL)
}

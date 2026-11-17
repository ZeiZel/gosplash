package httpx

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testPayload struct {
	Title string `json:"title"`
}

func TestBody_DekodiruetJSON(t *testing.T) {
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(`{"title":"закат"}`))

	got, err := Body[testPayload](r)

	require.NoError(t, err)
	assert.Equal(t, "закат", got.Title)
}

func TestBody_OgranichivaetRazmerTela(t *testing.T) {
	// MaxBytesReader обязан обрывать чтение ЗАРАНЕЕ, а не после того, как
	// гигабайт уже прочитан: oversized-тело — ошибка декодирования, а не
	// успешный decode ценой съеденной памяти.
	big := `{"title":"` + strings.Repeat("а", int(DefaultMaxJSONBody)) + `"}` // заведомо больше 1 МиБ
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", bytes.NewReader([]byte(big)))

	_, err := Body[testPayload](r)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "некорректный JSON")
}

func TestBody_DisallowUnknownFields(t *testing.T) {
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(`{"titel":"опечатка"}`))

	_, err := Body[testPayload](r)

	require.Error(t, err, "опечатка в имени поля обязана стать ошибкой, а не молчалим нулём")
}

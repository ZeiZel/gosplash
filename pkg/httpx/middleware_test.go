package httpx

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecover_PanikaStanovitsya500(t *testing.T) {
	// Без этого middleware паника в хендлере обрывает соединение без ответа,
	// и клиент видит не 500, а разорванное соединение — диагностировать это
	// со стороны клиента практически невозможно.
	handler := Chain(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("что-то пошло не так")
		}),
		Recover(),
	)

	rec := httptest.NewRecorder()
	require.NotPanics(t, func() {
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	})

	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "внутренняя ошибка", body["error"],
		"наружу уходит общая формулировка: подробности паники — в лог, не клиенту")
}

func TestChain_PoryadokVypolneniya(t *testing.T) {
	// Порядок аргументов Chain — это порядок ВЫПОЛНЕНИЯ. Тест фиксирует
	// именно это, потому что перепутанный порядок ставит Recover внутрь
	// и делает его бесполезным.
	var order []string

	mark := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name+":до")
				next.ServeHTTP(w, r)
				order = append(order, name+":после")
			})
		}
	}

	handler := Chain(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			order = append(order, "хендлер")
		}),
		mark("внешний"), mark("внутренний"),
	)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	assert.Equal(t, []string{
		"внешний:до", "внутренний:до", "хендлер", "внутренний:после", "внешний:после",
	}, order)
}

func TestStatusRecorder(t *testing.T) {
	rec := httptest.NewRecorder()
	sr := &statusRecorder{ResponseWriter: rec, status: http.StatusOK}

	_, err := sr.Write([]byte("привет"))
	require.NoError(t, err)

	// WriteHeader не вызывали — значит, статус остался 200, как и у net/http.
	assert.Equal(t, http.StatusOK, sr.status)
	assert.Equal(t, len("привет"), sr.written)

	sr2 := &statusRecorder{ResponseWriter: httptest.NewRecorder(), status: http.StatusOK}
	sr2.WriteHeader(http.StatusTeapot)
	assert.Equal(t, http.StatusTeapot, sr2.status)
}

func TestJSONiError(t *testing.T) {
	rec := httptest.NewRecorder()
	JSON(rec, map[string]int{"n": 1}, http.StatusCreated)

	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"n":1}`, rec.Body.String())

	rec = httptest.NewRecorder()
	Error(rec, "не найдено", http.StatusNotFound)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.JSONEq(t, `{"error":"не найдено"}`, rec.Body.String())

	// nil-тело — это законный ответ без содержимого (204 и подобные).
	rec = httptest.NewRecorder()
	JSON(rec, nil, http.StatusNoContent)
	assert.Empty(t, rec.Body.String())
}

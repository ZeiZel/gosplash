package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newHealthServer(t *testing.T, configure func(*Health)) (*Health, *http.ServeMux) {
	t.Helper()

	health := NewHealth()
	if configure != nil {
		configure(health)
	}
	router := http.NewServeMux()
	health.Handle(router, "test")
	return health, router
}

func do(t *testing.T, router *http.ServeMux, path string) (int, map[string]any) {
	t.Helper()

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	return rec.Code, body
}

func TestHealthz_NeZavisitOtZavisimostey(t *testing.T) {
	// Ключевое поведение: liveness отвечает 200 ДАЖЕ когда база лежит.
	// Иначе моргнувший PostgreSQL приводит к перезапуску всех подов разом,
	// и они добивают базу пачкой переподключений.
	_, router := newHealthServer(t, func(h *Health) {
		h.Register("postgres", func(context.Context) error {
			return errors.New("connection refused")
		})
	})

	code, body := do(t, router, "/healthz")

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "ok", body["status"])
}

func TestReadyz_ZdorovyySlucay(t *testing.T) {
	_, router := newHealthServer(t, func(h *Health) {
		h.Register("postgres", func(context.Context) error { return nil })
		h.Register("kafka", func(context.Context) error { return nil })
	})

	code, body := do(t, router, "/readyz")

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, true, body["ready"])

	checks := body["checks"].(map[string]any)
	assert.Equal(t, "ok", checks["postgres"])
	assert.Equal(t, "ok", checks["kafka"])
}

func TestReadyz_UpavshayaZavisimost(t *testing.T) {
	// 503 убирает под из балансировки, но НЕ перезапускает его. Разница
	// с liveness принципиальна: сервис жив, просто временно бесполезен.
	_, router := newHealthServer(t, func(h *Health) {
		h.Register("postgres", func(context.Context) error { return nil })
		h.Register("kafka", func(context.Context) error {
			return errors.New("no brokers available")
		})
	})

	code, body := do(t, router, "/readyz")

	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, false, body["ready"])

	checks := body["checks"].(map[string]any)
	assert.Equal(t, "ok", checks["postgres"], "здоровые проверки всё равно показываются")
	assert.Contains(t, checks["kafka"], "no brokers available",
		"причина должна быть видна прямо в ответе, а не только в логах")
}

func TestReadyz_PosleNotReady(t *testing.T) {
	// Первый шаг graceful shutdown: сервис ещё обслуживает текущие запросы,
	// но новых получать не должен.
	health, router := newHealthServer(t, func(h *Health) {
		h.Register("postgres", func(context.Context) error { return nil })
	})

	code, _ := do(t, router, "/readyz")
	require.Equal(t, http.StatusOK, code)

	health.NotReady()

	code, body := do(t, router, "/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, false, body["ready"])
	assert.Contains(t, body["checks"].(map[string]any), "shutdown")

	// Liveness при этом обязан оставаться зелёным: под ещё доделывает работу,
	// и перезапускать его сейчас — значит оборвать её на середине.
	code, _ = do(t, router, "/healthz")
	assert.Equal(t, http.StatusOK, code)
}

func TestReadyz_MedlennayaProverkaNeVeshaetProbu(t *testing.T) {
	// У всех проверок общий бюджет времени. Без него одна зависшая
	// зависимость подвешивает пробу целиком, и kubelet объявит под
	// неготовым по таймауту — без единой полезной строки в ответе.
	_, router := newHealthServer(t, func(h *Health) {
		h.Register("slow", func(ctx context.Context) error {
			select {
			case <-time.After(30 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	})

	started := time.Now()
	code, _ := do(t, router, "/readyz")

	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Less(t, time.Since(started), 5*time.Second,
		"проба обязана уложиться в свой бюджет, а не ждать зависимость")
}

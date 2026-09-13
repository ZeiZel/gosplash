package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
	ordergrpc "gosplash/services/order/internal/adapters/grpc"
	"gosplash/services/order/internal/app"
	"gosplash/services/order/internal/domain"
	"gosplash/services/order/internal/ports"
)

// Фейки портов — тот же приём, что и в internal/app/order_service_test.go
// (см. его комментарий), но продублирован здесь: HTTP-тест проверяет
// ПОЛНЫЙ путь запроса (маршрутизация → декодирование JSON → grpc.Server →
// app.OrderService), а не сценарий в изоляции, и ему нужны собственные
// экземпляры фейков.

type fakeOrderRepo struct {
	byID map[string]*domain.Order
}

func newFakeOrderRepo() *fakeOrderRepo { return &fakeOrderRepo{byID: map[string]*domain.Order{}} }

func (r *fakeOrderRepo) Create(_ context.Context, order *domain.Order, _ *eventsv1.Envelope) error {
	cp := *order
	r.byID[order.ID] = &cp
	return nil
}

func (r *fakeOrderRepo) GetByID(_ context.Context, id string) (*domain.Order, error) {
	if o, ok := r.byID[id]; ok {
		return o, nil
	}
	return nil, domain.ErrNotFound
}

type fakeIdempotencyStore struct {
	done        map[string][]byte
	forceStatus map[string]ports.IdempotencyStatus
}

func newFakeIdempotencyStore() *fakeIdempotencyStore {
	return &fakeIdempotencyStore{done: map[string][]byte{}, forceStatus: map[string]ports.IdempotencyStatus{}}
}

func (s *fakeIdempotencyStore) Begin(_ context.Context, key string) (ports.IdempotencyResult, error) {
	if status, ok := s.forceStatus[key]; ok {
		return ports.IdempotencyResult{Status: status}, nil
	}
	if response, ok := s.done[key]; ok {
		return ports.IdempotencyResult{Status: ports.IdempotencyDone, Response: response}, nil
	}
	return ports.IdempotencyResult{Status: ports.IdempotencyFree}, nil
}

func (s *fakeIdempotencyStore) Complete(_ context.Context, key string, response []byte) error {
	s.done[key] = response
	return nil
}

type fakeListingReader struct{}

func (fakeListingReader) GetListing(_ context.Context, listingID string) (ports.Listing, error) {
	if listingID != "listing-1" {
		return ports.Listing{}, domain.ErrListingNotFound
	}
	return ports.Listing{ID: "listing-1", AuthorID: 7, PriceCents: 2500, Currency: "RUB", Status: "published"}, nil
}

type fakeWorkflowStarter struct{}

func (fakeWorkflowStarter) StartPlaceOrder(context.Context, *domain.Order) error { return nil }

func newTestRouter() (*http.ServeMux, *fakeIdempotencyStore) {
	repo := newFakeOrderRepo()
	idem := newFakeIdempotencyStore()
	service := app.NewOrderService(repo, idem, fakeListingReader{}, fakeWorkflowStarter{})
	grpcServer := ordergrpc.NewServer(service)

	router := http.NewServeMux()
	Register(router, grpcServer)
	return router, idem
}

func TestPlaceOrder_BezZagolovkaIdempotencyKey_400(t *testing.T) {
	router, _ := newTestRouter()

	req := httptest.NewRequest(http.MethodPost, "/orders",
		strings.NewReader(`{"buyer_id":1,"listing_id":"listing-1"}`))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code, "без Idempotency-Key ожидается 400")
}

func TestPlaceOrder_NovyZapros_201(t *testing.T) {
	router, _ := newTestRouter()

	req := httptest.NewRequest(http.MethodPost, "/orders",
		strings.NewReader(`{"buyer_id":1,"listing_id":"listing-1"}`))
	req.Header.Set("Idempotency-Key", "key-1")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var got orderView
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
	assert.Equal(t, int64(2500), got.PriceCents)
	assert.Equal(t, domain.StatusPending, got.Status)
}

func TestPlaceOrder_PovtorSGotovymOtvetom_TotZheZakaz(t *testing.T) {
	router, _ := newTestRouter()

	makeReq := func() *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/orders",
			strings.NewReader(`{"buyer_id":1,"listing_id":"listing-1"}`))
		r.Header.Set("Idempotency-Key", "key-1")
		return r
	}

	rec1 := httptest.NewRecorder()
	router.ServeHTTP(rec1, makeReq())
	require.Equal(t, http.StatusOK, rec1.Code)
	var first orderView
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&first))

	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, makeReq())
	require.Equal(t, http.StatusOK, rec2.Code)
	var second orderView
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&second))

	assert.Equal(t, first.ID, second.ID, "повтор с тем же ключом обязан вернуть тот же заказ")
}

func TestPlaceOrder_ParallelnyPovtor_409(t *testing.T) {
	router, idem := newTestRouter()
	idem.forceStatus["place-order:key-1"] = ports.IdempotencyInProgress

	req := httptest.NewRequest(http.MethodPost, "/orders",
		strings.NewReader(`{"buyer_id":1,"listing_id":"listing-1"}`))
	req.Header.Set("Idempotency-Key", "key-1")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code, "параллельный повтор с тем же ключом обязан получить 409")
}

func TestGetOrder_NeNayden_404(t *testing.T) {
	router, _ := newTestRouter()

	req := httptest.NewRequest(http.MethodGet, "/orders/unknown", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

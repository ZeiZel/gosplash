package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
	"gosplash/services/order/internal/domain"
	"gosplash/services/order/internal/ports"
)

// fakeOrderRepo — подделка ports.OrderRepository, без базы.
type fakeOrderRepo struct {
	byID        map[string]*domain.Order
	createCalls int
	lastEvent   *eventsv1.Envelope
}

func newFakeOrderRepo() *fakeOrderRepo {
	return &fakeOrderRepo{byID: map[string]*domain.Order{}}
}

func (r *fakeOrderRepo) Create(_ context.Context, order *domain.Order, placedEvent *eventsv1.Envelope) error {
	r.createCalls++
	r.lastEvent = placedEvent
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

// fakeIdempotencyStore — подделка ports.IdempotencyStore, воспроизводящая
// SET NX семантику pkg/redisx.Client.Begin/Complete в памяти.
type fakeIdempotencyStore struct {
	entries     map[string]ports.IdempotencyResult
	beginCalls  int
	forceStatus map[string]ports.IdempotencyStatus // для теста параллельного повтора
}

func newFakeIdempotencyStore() *fakeIdempotencyStore {
	return &fakeIdempotencyStore{
		entries:     map[string]ports.IdempotencyResult{},
		forceStatus: map[string]ports.IdempotencyStatus{},
	}
}

func (s *fakeIdempotencyStore) Begin(_ context.Context, key string) (ports.IdempotencyResult, error) {
	s.beginCalls++
	if status, ok := s.forceStatus[key]; ok {
		return ports.IdempotencyResult{Status: status}, nil
	}
	if result, ok := s.entries[key]; ok {
		return result, nil
	}
	return ports.IdempotencyResult{Status: ports.IdempotencyFree}, nil
}

func (s *fakeIdempotencyStore) Complete(_ context.Context, key string, response []byte) error {
	s.entries[key] = ports.IdempotencyResult{Status: ports.IdempotencyDone, Response: response}
	return nil
}

// fakeListingReader — подделка ports.ListingReader.
type fakeListingReader struct {
	byID map[string]ports.Listing
	err  error
}

func (r *fakeListingReader) GetListing(_ context.Context, listingID string) (ports.Listing, error) {
	if r.err != nil {
		return ports.Listing{}, r.err
	}
	l, ok := r.byID[listingID]
	if !ok {
		return ports.Listing{}, domain.ErrListingNotFound
	}
	return l, nil
}

// fakeWorkflowStarter — подделка ports.WorkflowStarter.
type fakeWorkflowStarter struct {
	started []string
	err     error
}

func (s *fakeWorkflowStarter) StartPlaceOrder(_ context.Context, order *domain.Order) error {
	if s.err != nil {
		return s.err
	}
	s.started = append(s.started, order.ID)
	return nil
}

func newService() (*OrderService, *fakeOrderRepo, *fakeIdempotencyStore, *fakeWorkflowStarter) {
	repo := newFakeOrderRepo()
	idem := newFakeIdempotencyStore()
	listings := &fakeListingReader{byID: map[string]ports.Listing{
		"listing-1": {ID: "listing-1", AuthorID: 7, PriceCents: 1500, Currency: "RUB", Status: "published"},
	}}
	starter := &fakeWorkflowStarter{}
	return NewOrderService(repo, idem, listings, starter), repo, idem, starter
}

func TestOrderService_PlaceOrder_BezZagolovkaIdempotencyKey(t *testing.T) {
	service, _, _, _ := newService()

	_, err := service.PlaceOrder(context.Background(), PlaceOrderCommand{
		BuyerID: 1, ListingID: "listing-1", IdempotencyKey: "",
	})

	require.ErrorIs(t, err, domain.ErrIdempotencyKeyRequired)
}

func TestOrderService_PlaceOrder_NovyZakaz(t *testing.T) {
	service, repo, _, starter := newService()

	order, err := service.PlaceOrder(context.Background(), PlaceOrderCommand{
		BuyerID: 1, ListingID: "listing-1", IdempotencyKey: "key-1",
	})

	require.NoError(t, err)
	assert.Equal(t, int64(1500), order.PriceCents, "цена берётся из catalog в момент создания заказа")
	assert.Equal(t, domain.StatusPending, order.Status)
	assert.Equal(t, 1, repo.createCalls)
	require.NotNil(t, repo.lastEvent, "order.order.placed обязан уйти в outbox вместе с созданием заказа")
	assert.Contains(t, starter.started, order.ID, "сага обязана быть запущена с workflow_id = order.ID")
}

func TestOrderService_PlaceOrder_PovtorSGotovymOtvetom(t *testing.T) {
	service, repo, _, starter := newService()

	first, err := service.PlaceOrder(context.Background(), PlaceOrderCommand{
		BuyerID: 1, ListingID: "listing-1", IdempotencyKey: "key-1",
	})
	require.NoError(t, err)

	second, err := service.PlaceOrder(context.Background(), PlaceOrderCommand{
		BuyerID: 1, ListingID: "listing-1", IdempotencyKey: "key-1",
	})
	require.NoError(t, err)

	assert.Equal(t, first.ID, second.ID, "повтор с тем же ключом обязан вернуть ТОТ ЖЕ заказ")
	assert.Equal(t, 1, repo.createCalls, "повтор не должен создавать вторую строку заказа")
	assert.Len(t, starter.started, 1, "повтор не должен запускать сагу второй раз")
}

func TestOrderService_PlaceOrder_ParallelnyPovtor409(t *testing.T) {
	service, repo, idem, starter := newService()
	idem.forceStatus["place-order:key-1"] = ports.IdempotencyInProgress

	_, err := service.PlaceOrder(context.Background(), PlaceOrderCommand{
		BuyerID: 1, ListingID: "listing-1", IdempotencyKey: "key-1",
	})

	require.ErrorIs(t, err, domain.ErrIdempotencyInProgress)
	assert.Equal(t, 0, repo.createCalls, "запрос в процессе не должен ничего создавать")
	assert.Empty(t, starter.started)
}

func TestOrderService_PlaceOrder_KartochkaNeNaydena(t *testing.T) {
	service, repo, _, starter := newService()

	_, err := service.PlaceOrder(context.Background(), PlaceOrderCommand{
		BuyerID: 1, ListingID: "listing-unknown", IdempotencyKey: "key-1",
	})

	require.ErrorIs(t, err, domain.ErrListingNotFound)
	assert.Equal(t, 0, repo.createCalls, "без карточки заказ создавать нечего")
	assert.Empty(t, starter.started)
}

func TestOrderService_GetOrder_NeNayden(t *testing.T) {
	service, _, _, _ := newService()

	_, err := service.GetOrder(context.Background(), "unknown")

	require.ErrorIs(t, err, domain.ErrNotFound)
}

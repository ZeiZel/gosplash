package app

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/catalog/internal/domain"
	"gosplash/services/catalog/internal/ports"
)

// fakeListingRepo — подделка ports.ListingRepository (сторона чтения).
type fakeListingRepo struct {
	byID       map[string]*domain.Listing
	getCalls   int
	nextCursor string
}

func (r *fakeListingRepo) GetByID(_ context.Context, id string) (*domain.Listing, error) {
	r.getCalls++
	if l, ok := r.byID[id]; ok {
		return l, nil
	}
	return nil, domain.ErrNotFound
}

func (r *fakeListingRepo) List(context.Context, int, string, []string) ([]domain.Listing, string, error) {
	return nil, r.nextCursor, nil
}

func (r *fakeListingRepo) CountOn(context.Context, string) (int64, error) { return 0, nil }

// fakeViewCounter — подделка ports.ViewCounter.
type fakeViewCounter struct {
	incremented []string
	top         []ports.TopEntry
}

func (v *fakeViewCounter) IncrView(_ context.Context, id string, _ time.Time) error {
	v.incremented = append(v.incremented, id)
	return nil
}

func (v *fakeViewCounter) Top(context.Context, time.Time, int) ([]ports.TopEntry, error) {
	return v.top, nil
}

// fakeViewPublisher — подделка ports.ViewPublisher.
type fakeViewPublisher struct {
	enqueued []string
}

func (p *fakeViewPublisher) Enqueue(photoID string, _, _ int64, _ string) {
	p.enqueued = append(p.enqueued, photoID)
}

func TestListingService_Get_SchitaetProsmotr(t *testing.T) {
	repo := &fakeListingRepo{byID: map[string]*domain.Listing{
		"photo-1": {ID: "photo-1", AuthorID: 42, Title: "закат"},
	}}
	views := &fakeViewCounter{}
	pub := &fakeViewPublisher{}
	service := NewListingService(repo, &fakeCache{}, views, pub)

	listing, err := service.Get(context.Background(), "photo-1")

	require.NoError(t, err)
	assert.Equal(t, "закат", listing.Title)
	assert.Contains(t, views.incremented, "photo-1", "каждый показ карточки обязан увеличивать счётчик")
	assert.Contains(t, pub.enqueued, "photo-1", "каждый показ карточки обязан ставить событие аналитики в очередь")
}

func TestListingService_Get_NeNaydena(t *testing.T) {
	repo := &fakeListingRepo{byID: map[string]*domain.Listing{}}
	views := &fakeViewCounter{}
	pub := &fakeViewPublisher{}
	service := NewListingService(repo, &fakeCache{}, views, pub)

	_, err := service.Get(context.Background(), "нет-такой")

	require.ErrorIs(t, err, domain.ErrNotFound)
	assert.Empty(t, views.incremented, "несуществующую карточку не считаем просмотренной")
}

func TestListingService_Top_DelegiruetSchyotchiku(t *testing.T) {
	views := &fakeViewCounter{top: []ports.TopEntry{{ListingID: "photo-1", Views: 10}}}
	service := NewListingService(&fakeListingRepo{}, &fakeCache{}, views, &fakeViewPublisher{})

	top, err := service.Top(context.Background(), time.Now(), 100)

	require.NoError(t, err)
	assert.Equal(t, views.top, top)
}

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
	"gosplash/pkg/kafkax"

	"gosplash/services/analytics/internal/app"
	"gosplash/services/analytics/internal/domain"
)

type fakeViewSink struct {
	added []domain.PhotoView
	err   error
}

func (f *fakeViewSink) Add(_ context.Context, v domain.PhotoView) error {
	f.added = append(f.added, v)
	return f.err
}

type fakePurchaseSink struct {
	added []domain.Purchase
	err   error
}

func (f *fakePurchaseSink) Add(_ context.Context, p domain.Purchase) error {
	f.added = append(f.added, p)
	return f.err
}

func TestHandlePhotoViewed_RazbiraetKonvertIZovyotIngest(t *testing.T) {
	views := &fakeViewSink{}
	c := &Consumers{ingest: app.NewIngest(views, &fakePurchaseSink{})}

	env, err := kafkax.NewEnvelope(kafkax.EventPhotoViewed, "photo-1", &eventsv1.PhotoViewed{
		PhotoId:  "photo-1",
		AuthorId: 10,
		ViewerId: 777,
		Country:  "RU",
	})
	require.NoError(t, err)

	require.NoError(t, c.handlePhotoViewed(context.Background(), env))
	require.Len(t, views.added, 1)
	assert.Equal(t, "photo-1", views.added[0].PhotoID)
	assert.Equal(t, int64(10), views.added[0].AuthorID)
	require.NotNil(t, views.added[0].ViewerID)
	assert.Equal(t, int64(777), *views.added[0].ViewerID)
	assert.Equal(t, "RU", views.added[0].Country)
	assert.WithinDuration(t, kafkax.OccurredAt(env), views.added[0].OccurredAt, time.Millisecond)
}

func TestHandlePhotoViewed_BityyPayloadPostoyannayaOshibka(t *testing.T) {
	c := &Consumers{ingest: app.NewIngest(&fakeViewSink{}, &fakePurchaseSink{})}

	// Конверт с payload ДРУГОГО типа — типичный сценарий "не тот event_type
	// приехал в топик": anypb.UnmarshalTo обязан вернуть ошибку, а не молча
	// заполнить структуру мусором (см. events.proto, комментарий про Any).
	env, err := kafkax.NewEnvelope(kafkax.EventOrderPaid, "order-1", &eventsv1.OrderPaid{OrderId: "order-1"})
	require.NoError(t, err)

	err = c.handlePhotoViewed(context.Background(), env)

	require.Error(t, err)
	assert.ErrorIs(t, err, kafkax.ErrPermanent, "битый/чужой payload не станет валидным после повтора")
}

func TestHandlePhotoViewed_PustoyPayloadPostoyannayaOshibka(t *testing.T) {
	c := &Consumers{ingest: app.NewIngest(&fakeViewSink{}, &fakePurchaseSink{})}

	env := &eventsv1.Envelope{
		EventId:     "evt-1",
		EventType:   kafkax.EventPhotoViewed,
		AggregateId: "photo-1",
		Payload:     &anypb.Any{}, // пустой Any — типичный битый конверт
	}

	err := c.handlePhotoViewed(context.Background(), env)

	require.Error(t, err)
	assert.ErrorIs(t, err, kafkax.ErrPermanent)
}

func TestHandleOrderPaid_ListingIdStanovitsyaPhotoID(t *testing.T) {
	purchases := &fakePurchaseSink{}
	c := &Consumers{ingest: app.NewIngest(&fakeViewSink{}, purchases)}

	env, err := kafkax.NewEnvelope(kafkax.EventOrderPaid, "order-1", &eventsv1.OrderPaid{
		OrderId:    "order-1",
		BuyerId:    20,
		AuthorId:   10,
		ListingId:  "photo-1", // см. комментарий handleOrderPaid: listing_id == photo_id в этом проекте
		PriceCents: 1999,
	})
	require.NoError(t, err)

	require.NoError(t, c.handleOrderPaid(context.Background(), env))
	require.Len(t, purchases.added, 1)
	assert.Equal(t, "order-1", purchases.added[0].OrderID)
	assert.Equal(t, "photo-1", purchases.added[0].PhotoID)
	assert.Equal(t, int64(10), purchases.added[0].AuthorID)
	assert.Equal(t, int64(20), purchases.added[0].BuyerID)
	assert.Equal(t, int64(1999), purchases.added[0].PriceCents)
}

func TestHandleOrderPaid_ChuzhoyPayloadPostoyannayaOshibka(t *testing.T) {
	c := &Consumers{ingest: app.NewIngest(&fakeViewSink{}, &fakePurchaseSink{})}

	env, err := kafkax.NewEnvelope(kafkax.EventPhotoViewed, "photo-1", &eventsv1.PhotoViewed{PhotoId: "photo-1"})
	require.NoError(t, err)

	err = c.handleOrderPaid(context.Background(), env)

	require.Error(t, err)
	assert.ErrorIs(t, err, kafkax.ErrPermanent)
}

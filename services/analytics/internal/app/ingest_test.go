package app_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

type fakeUploadSink struct {
	added []domain.Upload
	err   error
}

func (f *fakeUploadSink) Add(_ context.Context, u domain.Upload) error {
	f.added = append(f.added, u)
	return f.err
}

type fakeThumbnailSink struct {
	added []domain.Thumbnail
	err   error
}

func (f *fakeThumbnailSink) Add(_ context.Context, th domain.Thumbnail) error {
	f.added = append(f.added, th)
	return f.err
}

func TestIngest_PhotoViewed_NulevoyViewerStanovitsyaNil(t *testing.T) {
	views := &fakeViewSink{}
	ingest := app.NewIngest(views, &fakePurchaseSink{}, &fakeUploadSink{}, &fakeThumbnailSink{})
	occurredAt := time.Now()

	err := ingest.PhotoViewed(context.Background(), "photo-1", 10, 0, "RU", occurredAt)

	require.NoError(t, err)
	require.Len(t, views.added, 1)
	// viewer_id=0 в protobuf — аноним (см. events.proto), в ClickHouse это
	// NULL, а не число 0 — см. комментарий у domain.PhotoView.
	assert.Nil(t, views.added[0].ViewerID)
	assert.Equal(t, "photo-1", views.added[0].PhotoID)
	assert.Equal(t, int64(10), views.added[0].AuthorID)
	assert.Equal(t, "RU", views.added[0].Country)
	assert.Equal(t, occurredAt, views.added[0].OccurredAt)
}

func TestIngest_PhotoViewed_NenulevoyViewerSohranyaetsya(t *testing.T) {
	views := &fakeViewSink{}
	ingest := app.NewIngest(views, &fakePurchaseSink{}, &fakeUploadSink{}, &fakeThumbnailSink{})

	err := ingest.PhotoViewed(context.Background(), "photo-1", 10, 777, "DE", time.Now())

	require.NoError(t, err)
	require.Len(t, views.added, 1)
	require.NotNil(t, views.added[0].ViewerID)
	assert.Equal(t, int64(777), *views.added[0].ViewerID)
}

func TestIngest_PhotoViewed_PustoyPhotoIDOshibka(t *testing.T) {
	views := &fakeViewSink{}
	ingest := app.NewIngest(views, &fakePurchaseSink{}, &fakeUploadSink{}, &fakeThumbnailSink{})

	err := ingest.PhotoViewed(context.Background(), "", 10, 0, "RU", time.Now())

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrEmptyPhotoID)
	assert.Empty(t, views.added, "невалидное событие не должно доходить до sink'а")
}

func TestIngest_PhotoViewed_OshibkaSinkaProbrasyvaetsyaSKontekstom(t *testing.T) {
	sinkErr := errors.New("batch insert failed")
	views := &fakeViewSink{err: sinkErr}
	ingest := app.NewIngest(views, &fakePurchaseSink{}, &fakeUploadSink{}, &fakeThumbnailSink{})

	err := ingest.PhotoViewed(context.Background(), "photo-1", 10, 0, "RU", time.Now())

	require.Error(t, err)
	assert.ErrorIs(t, err, sinkErr)
}

func TestIngest_OrderPaid_UspeshnayaZapis(t *testing.T) {
	purchases := &fakePurchaseSink{}
	ingest := app.NewIngest(&fakeViewSink{}, purchases, &fakeUploadSink{}, &fakeThumbnailSink{})
	occurredAt := time.Now()

	err := ingest.OrderPaid(context.Background(), "order-1", "photo-1", 10, 20, 1999, occurredAt)

	require.NoError(t, err)
	require.Len(t, purchases.added, 1)
	assert.Equal(t, domain.Purchase{
		OrderID:    "order-1",
		PhotoID:    "photo-1",
		AuthorID:   10,
		BuyerID:    20,
		PriceCents: 1999,
		OccurredAt: occurredAt,
	}, purchases.added[0])
}

func TestIngest_OrderPaid_PustyeIdentifikatoryOshibka(t *testing.T) {
	purchases := &fakePurchaseSink{}
	ingest := app.NewIngest(&fakeViewSink{}, purchases, &fakeUploadSink{}, &fakeThumbnailSink{})

	// Оба идентификатора проверяются сантинел-ошибками домена, а не голым
	// fmt.Errorf: консьюмер Kafka различает их через errors.Is, чтобы безнадёжное
	// событие сразу ушло в DLQ (см. classifyIngestErr в adapters/kafka).
	err := ingest.OrderPaid(context.Background(), "", "photo-1", 10, 20, 1999, time.Now())
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrEmptyOrderID)

	err = ingest.OrderPaid(context.Background(), "order-1", "", 10, 20, 1999, time.Now())
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrEmptyPhotoID)
	assert.Empty(t, purchases.added)
}

func TestIngest_PhotoUploaded_UspeshnayaZapis(t *testing.T) {
	uploads := &fakeUploadSink{}
	ingest := app.NewIngest(&fakeViewSink{}, &fakePurchaseSink{}, uploads, &fakeThumbnailSink{})
	occurredAt := time.Now()

	err := ingest.PhotoUploaded(context.Background(), "photo-1", 10, occurredAt)

	require.NoError(t, err)
	require.Len(t, uploads.added, 1)
	// user_id события == AuthorID факта: переименование на границе, см.
	// комментарий у domain.Upload.
	assert.Equal(t, domain.Upload{
		PhotoID:    "photo-1",
		AuthorID:   10,
		OccurredAt: occurredAt,
	}, uploads.added[0])
}

func TestIngest_PhotoUploaded_PustoyPhotoIDOshibka(t *testing.T) {
	uploads := &fakeUploadSink{}
	ingest := app.NewIngest(&fakeViewSink{}, &fakePurchaseSink{}, uploads, &fakeThumbnailSink{})

	err := ingest.PhotoUploaded(context.Background(), "", 10, time.Now())

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrEmptyPhotoID)
	assert.Empty(t, uploads.added, "невалидное событие не должно доходить до sink'а")
}

func TestIngest_ThumbnailReady_UspeshnayaZapis(t *testing.T) {
	thumbnails := &fakeThumbnailSink{}
	ingest := app.NewIngest(&fakeViewSink{}, &fakePurchaseSink{}, &fakeUploadSink{}, thumbnails)
	occurredAt := time.Now()

	err := ingest.ThumbnailReady(context.Background(), "photo-1", occurredAt)

	require.NoError(t, err)
	require.Len(t, thumbnails.added, 1)
	assert.Equal(t, domain.Thumbnail{
		PhotoID:    "photo-1",
		OccurredAt: occurredAt,
	}, thumbnails.added[0])
}

func TestIngest_ThumbnailReady_PustoyPhotoIDOshibka(t *testing.T) {
	thumbnails := &fakeThumbnailSink{}
	ingest := app.NewIngest(&fakeViewSink{}, &fakePurchaseSink{}, &fakeUploadSink{}, thumbnails)

	err := ingest.ThumbnailReady(context.Background(), "", time.Now())

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrEmptyPhotoID)
	assert.Empty(t, thumbnails.added, "невалидное событие не должно доходить до sink'а")
}

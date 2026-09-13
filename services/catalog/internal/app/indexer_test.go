package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/pkg/kafkax"
	"gosplash/services/catalog/internal/domain"
	"gosplash/services/catalog/internal/ports"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// fakeUnitOfWork — подделка ports.UnitOfWork: claimed имитирует таблицу
// processed_events (pkg/idempotency), без базы. Повторный WithClaim с тем же
// event_id возвращает applied=false и НЕ вызывает fn — ровно то поведение,
// которое реальная реализация (adapters/pg) получает от idempotency.Claim.
type fakeUnitOfWork struct {
	claimed map[string]bool
	tx      *fakeIndexTx
}

func newFakeUnitOfWork(tx *fakeIndexTx) *fakeUnitOfWork {
	return &fakeUnitOfWork{claimed: map[string]bool{}, tx: tx}
}

func (u *fakeUnitOfWork) WithClaim(_ context.Context, eventID, _, _ string, fn func(ports.IndexTx) error) (bool, error) {
	if u.claimed[eventID] {
		return false, nil
	}
	if err := fn(u.tx); err != nil {
		// Откат: событие НЕ помечается обработанным, повтор должен снова
		// дойти до fn — так же, как gorm.Transaction откатывает Claim при
		// ошибке бизнес-логики.
		return false, err
	}
	u.claimed[eventID] = true
	return true, nil
}

// fakeIndexTx — подделка ports.IndexTx.
type fakeIndexTx struct {
	upserted []domain.Listing
	authors  map[int64]string
	outbox   []*eventsv1.Envelope

	applyResult *domain.Listing
	applyErr    error
}

func (t *fakeIndexTx) UpsertListing(_ context.Context, listing *domain.Listing) error {
	t.upserted = append(t.upserted, *listing)
	return nil
}

func (t *fakeIndexTx) ApplyThumbnails(_ context.Context, _ string, thumbnails map[string]string, publishedAt time.Time) (*domain.Listing, error) {
	if t.applyErr != nil {
		return nil, t.applyErr
	}
	result := *t.applyResult
	result.Thumbnails = thumbnails
	result.PublishedAt = publishedAt
	result.Status = domain.StatusPublished
	return &result, nil
}

func (t *fakeIndexTx) UpsertAuthorSnapshot(_ context.Context, authorID int64, name string) error {
	if t.authors == nil {
		t.authors = map[int64]string{}
	}
	t.authors[authorID] = name
	return nil
}

func (t *fakeIndexTx) Outbox(_ context.Context, env *eventsv1.Envelope, _ string) error {
	t.outbox = append(t.outbox, env)
	return nil
}

// fakeMedia — подделка ports.PhotoFetcher.
type fakeMedia struct {
	calls int
	photo *domain.Listing
	err   error
}

func (m *fakeMedia) Fetch(_ context.Context, photoID string, authorID int64) (*domain.Listing, error) {
	m.calls++
	if m.err != nil {
		return nil, m.err
	}
	photo := *m.photo
	photo.ID = photoID
	photo.AuthorID = authorID
	return &photo, nil
}

// fakeCache — подделка ports.ListingCache: GetOrLoad просто вызывает load
// (кэш "всегда промахивается" — индексатору это неважно, он только
// инвалидирует), Invalidate считает вызовы.
type fakeCache struct {
	invalidated []string
}

func (c *fakeCache) GetOrLoad(ctx context.Context, _ string, load func(context.Context) (*domain.Listing, error)) (*domain.Listing, error) {
	return load(ctx)
}

func (c *fakeCache) Invalidate(_ context.Context, id string) error {
	c.invalidated = append(c.invalidated, id)
	return nil
}

func uploadedEnvelope(t *testing.T, photoID string, authorID int64) *eventsv1.Envelope {
	t.Helper()
	env, err := kafkax.NewEnvelope(kafkax.EventPhotoUploaded, photoID, &eventsv1.PhotoUploaded{
		PhotoId: photoID,
		UserId:  authorID,
	})
	require.NoError(t, err)
	return env
}

func thumbnailEnvelope(t *testing.T, photoID string, authorID int64, thumbs ...*eventsv1.Thumbnail) *eventsv1.Envelope {
	t.Helper()
	env, err := kafkax.NewEnvelope(kafkax.EventPhotoThumbnailReady, photoID, &eventsv1.PhotoThumbnailReady{
		PhotoId:    photoID,
		UserId:     authorID,
		Thumbnails: thumbs,
	})
	require.NoError(t, err)
	return env
}

func TestHandlePhotoUploaded_SozdayotChernovik(t *testing.T) {
	tx := &fakeIndexTx{}
	uow := newFakeUnitOfWork(tx)
	media := &fakeMedia{photo: &domain.Listing{Title: "закат", Width: 1920, Height: 1080}}
	cache := &fakeCache{}
	indexer := NewIndexer(uow, media, cache, NewWatchHub())

	env := uploadedEnvelope(t, "photo-1", 42)
	require.NoError(t, indexer.HandlePhotoUploaded(context.Background(), env))

	require.Len(t, tx.upserted, 1)
	got := tx.upserted[0]
	assert.Equal(t, "photo-1", got.ID)
	assert.Equal(t, int64(42), got.AuthorID)
	assert.Equal(t, "закат", got.Title)

	// Заглушка имени автора заведена ОДНОВРЕМЕННО с карточкой, в той же
	// транзакции.
	assert.Equal(t, "author-42", tx.authors[42])

	// Кэш инвалидирован — на случай, если карточка уже была в нём.
	assert.Contains(t, cache.invalidated, "photo-1")
}

func TestHandlePhotoUploaded_PovtornoeSobytieNePrimenyaetsyaDvazhdy(t *testing.T) {
	// PATTERN: idempotent consumer. Kafka даёт at-least-once — то же событие
	// может приехать дважды (ребаланс, ретрай доставки offset'а). В отличие
	// от фазы 0 (идемпотентность через upsert — обработчик вызывался бы
	// оба раза с одинаковым результатом), теперь первым действием стоит
	// Claim, и повтор НЕ должен дойти до бизнес-логики вовсе.
	tx := &fakeIndexTx{}
	uow := newFakeUnitOfWork(tx)
	media := &fakeMedia{photo: &domain.Listing{Title: "закат"}}
	cache := &fakeCache{}
	indexer := NewIndexer(uow, media, cache, NewWatchHub())

	env := uploadedEnvelope(t, "photo-1", 42)
	require.NoError(t, indexer.HandlePhotoUploaded(context.Background(), env))
	require.NoError(t, indexer.HandlePhotoUploaded(context.Background(), env))

	assert.Len(t, tx.upserted, 1, "UpsertListing обязан вызваться РОВНО один раз")
	assert.Equal(t, 1, media.calls, "повтор не должен даже сходить в media — Claim первым действием экономит и сетевой вызов")
}

func TestHandlePhotoUploaded_MediaNedostupna_Retryable(t *testing.T) {
	tx := &fakeIndexTx{}
	uow := newFakeUnitOfWork(tx)
	media := &fakeMedia{err: errors.New("connection refused")}
	cache := &fakeCache{}
	indexer := NewIndexer(uow, media, cache, NewWatchHub())

	env := uploadedEnvelope(t, "photo-1", 42)
	err := indexer.HandlePhotoUploaded(context.Background(), env)

	require.Error(t, err)
	assert.True(t, errors.Is(err, kafkax.ErrRetryable), "недоступность media — временная ошибка")
	assert.Empty(t, tx.upserted)
	assert.False(t, uow.claimed[env.GetEventId()], "Claim обязан откатиться вместе с бизнес-ошибкой")

	// Проверяем, что откат — не просто флаг: повторная доставка ТОГО ЖЕ
	// event_id действительно доходит до media ещё раз, а не пропускается
	// как "уже обработанное".
	media.err = nil
	media.photo = &domain.Listing{Title: "закат"}
	require.NoError(t, indexer.HandlePhotoUploaded(context.Background(), env))
	assert.Equal(t, 2, media.calls)
	assert.Len(t, tx.upserted, 1)
}

func TestHandlePhotoUploaded_FotoNeSuschestvuet_Permanent(t *testing.T) {
	tx := &fakeIndexTx{}
	uow := newFakeUnitOfWork(tx)
	media := &fakeMedia{err: domain.ErrPhotoNotFound}
	cache := &fakeCache{}
	indexer := NewIndexer(uow, media, cache, NewWatchHub())

	err := indexer.HandlePhotoUploaded(context.Background(), uploadedEnvelope(t, "photo-1", 42))

	require.Error(t, err)
	assert.True(t, errors.Is(err, kafkax.ErrPermanent), "несуществующее фото — постоянная ошибка, повтор не поможет")
	assert.Empty(t, tx.upserted)
}

func TestHandlePhotoUploaded_BituyPayload_Permanent(t *testing.T) {
	// В отличие от фазы 0 (тихий пропуск, return nil), теперь в проекте есть
	// DLQ (pkg/kafkax): битый payload классифицируется Permanent и уезжает
	// в dead letter queue, а не теряется молча.
	tx := &fakeIndexTx{}
	uow := newFakeUnitOfWork(tx)
	media := &fakeMedia{photo: &domain.Listing{}}
	cache := &fakeCache{}
	indexer := NewIndexer(uow, media, cache, NewWatchHub())

	// Конверт с payload'ом чужого типа — самый реалистичный вид "битого":
	// кто-то отправил не то событие не в тот топик.
	broken, err := kafkax.NewEnvelope(kafkax.EventPhotoDeleted, "photo-1", &eventsv1.PhotoDeleted{PhotoId: "photo-1"})
	require.NoError(t, err)

	handleErr := indexer.HandlePhotoUploaded(context.Background(), broken)

	require.Error(t, handleErr)
	assert.True(t, errors.Is(handleErr, kafkax.ErrPermanent))
	assert.Zero(t, media.calls, "до похода в media дело не должно дойти")
	assert.Empty(t, tx.upserted)
}

func TestHandlePhotoThumbnailReady_PublikuetIInvalidiruetKesh(t *testing.T) {
	tx := &fakeIndexTx{applyResult: &domain.Listing{
		ID: "photo-1", AuthorID: 42, AuthorName: "author-42", Title: "закат",
	}}
	uow := newFakeUnitOfWork(tx)
	cache := &fakeCache{}
	indexer := NewIndexer(uow, &fakeMedia{}, cache, NewWatchHub())

	env := thumbnailEnvelope(t, "photo-1", 42, &eventsv1.Thumbnail{Size: "small", StorageKey: "thumbs/small.jpg"})
	require.NoError(t, indexer.HandlePhotoThumbnailReady(context.Background(), env))

	require.Len(t, tx.outbox, 1, "после публикации карточки событие обязано уйти в outbox")
	assert.Equal(t, kafkax.EventListingPublished, tx.outbox[0].GetEventType())
	assert.Contains(t, cache.invalidated, "photo-1")
}

func TestHandlePhotoThumbnailReady_KartochkaEschoNeSozdana_Retryable(t *testing.T) {
	// thumbnail-ready и uploaded — независимые топики (разные consumer
	// group, см. main.go), относительный порядок между ними не
	// гарантирован. Если превью обогнали создание карточки, это ВРЕМЕННАЯ
	// ситуация — карточка появится, как только применится uploaded.
	tx := &fakeIndexTx{applyErr: domain.ErrNotFound}
	uow := newFakeUnitOfWork(tx)
	cache := &fakeCache{}
	indexer := NewIndexer(uow, &fakeMedia{}, cache, NewWatchHub())

	env := thumbnailEnvelope(t, "photo-1", 42, &eventsv1.Thumbnail{Size: "small", StorageKey: "x"})
	err := indexer.HandlePhotoThumbnailReady(context.Background(), env)

	require.Error(t, err)
	assert.True(t, errors.Is(err, kafkax.ErrRetryable))
	assert.Empty(t, cache.invalidated, "инвалидация не должна случиться, пока применение не удалось")
}

func TestHandlePhotoThumbnailReady_BituyPayload_Permanent(t *testing.T) {
	tx := &fakeIndexTx{}
	uow := newFakeUnitOfWork(tx)
	indexer := NewIndexer(uow, &fakeMedia{}, &fakeCache{}, NewWatchHub())

	broken, err := kafkax.NewEnvelope(kafkax.EventPhotoDeleted, "photo-1", &eventsv1.PhotoDeleted{PhotoId: "photo-1"})
	require.NoError(t, err)

	handleErr := indexer.HandlePhotoThumbnailReady(context.Background(), broken)
	require.Error(t, handleErr)
	assert.True(t, errors.Is(handleErr, kafkax.ErrPermanent))
}

func TestHandlePhotoThumbnailReady_PovtornoeSobytieNePrimenyaetsyaDvazhdy(t *testing.T) {
	tx := &fakeIndexTx{applyResult: &domain.Listing{ID: "photo-1", AuthorID: 42}}
	uow := newFakeUnitOfWork(tx)
	cache := &fakeCache{}
	indexer := NewIndexer(uow, &fakeMedia{}, cache, NewWatchHub())

	env := thumbnailEnvelope(t, "photo-1", 42, &eventsv1.Thumbnail{Size: "small", StorageKey: "x"})
	require.NoError(t, indexer.HandlePhotoThumbnailReady(context.Background(), env))
	require.NoError(t, indexer.HandlePhotoThumbnailReady(context.Background(), env))

	assert.Len(t, tx.outbox, 1, "повтор не должен опубликовать catalog.listing.published второй раз")
}

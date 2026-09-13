package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/thumbnail-worker/internal/domain"
	"gosplash/services/thumbnail-worker/internal/ports"
)

// Весь смысл ports & adapters виден в этом файле: сценарий прогоняется
// целиком без PostgreSQL, MinIO, Redis и реального декодирования картинок —
// подделки ниже реализуют интерфейсы ports и работают мгновенно.

// ── fakeRepo ─────────────────────────────────────────────────────────────

type commitCall struct {
	in ports.ReadyCommit
}

type fakeRepo struct {
	mu sync.Mutex

	original    domain.PhotoOriginal
	originalErr error

	commits     []commitCall
	claimed     bool
	commitErr   error
	alreadyDone map[string]bool // event_id -> уже применено (для теста дедупликации)
}

func newFakeRepo(original domain.PhotoOriginal) *fakeRepo {
	return &fakeRepo{original: original, claimed: true, alreadyDone: map[string]bool{}}
}

func (r *fakeRepo) GetOriginal(context.Context, int64, string) (domain.PhotoOriginal, error) {
	if r.originalErr != nil {
		return domain.PhotoOriginal{}, r.originalErr
	}
	return r.original, nil
}

func (r *fakeRepo) CommitReady(_ context.Context, in ports.ReadyCommit) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commits = append(r.commits, commitCall{in: in})
	if r.commitErr != nil {
		return false, r.commitErr
	}
	if r.alreadyDone[in.EventID] {
		return false, nil
	}
	r.alreadyDone[in.EventID] = true
	return r.claimed, nil
}

// ── fakeStorage ──────────────────────────────────────────────────────────

type fakeStorage struct {
	mu sync.Mutex

	originalBody string
	getErr       error

	puts   map[string][]byte // key -> содержимое
	putErr error
}

func newFakeStorage(originalBody string) *fakeStorage {
	return &fakeStorage{originalBody: originalBody, puts: map[string][]byte{}}
}

func (s *fakeStorage) Get(context.Context, string, string) (io.ReadCloser, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return io.NopCloser(bytes.NewReader([]byte(s.originalBody))), nil
}

func (s *fakeStorage) Put(_ context.Context, _, key string, reader io.Reader, _ int64, _ string) error {
	if s.putErr != nil {
		return s.putErr
	}
	data, _ := io.ReadAll(reader)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts[key] = data
	return nil
}

func (s *fakeStorage) putCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.puts)
}

// ── fakeLock / fakeLocker ────────────────────────────────────────────────

type fakeLock struct {
	unlocked  bool
	unlockErr error
}

func (l *fakeLock) Unlock(context.Context) error {
	l.unlocked = true
	return l.unlockErr
}

type fakeLocker struct {
	busy    bool // Acquire вернёт (nil, nil) — лок занят кем-то другим
	err     error
	granted []*fakeLock
}

func (l *fakeLocker) AcquirePhotoLock(context.Context, string, time.Duration) (ports.Lock, error) {
	if l.err != nil {
		return nil, l.err
	}
	if l.busy {
		return nil, nil
	}
	lock := &fakeLock{}
	l.granted = append(l.granted, lock)
	return lock, nil
}

// ── fakeImageProcessor ───────────────────────────────────────────────────
//
// Реализует ТУ ЖЕ арифметику, что и настоящий адаптер (через
// domain.FitLongSide), — иначе тест "апскейла не происходит" проверял бы
// только сам себя, а не то, что сценарий честно передаёт исходные размеры
// в порт и не подменяет результат.

type fakeImageProcessor struct {
	decodeErr error
	width     int
	height    int
}

func (p *fakeImageProcessor) Decode(io.Reader) (ports.DecodedImage, error) {
	if p.decodeErr != nil {
		return ports.DecodedImage{}, p.decodeErr
	}
	return ports.DecodedImage{Width: p.width, Height: p.height}, nil
}

func (p *fakeImageProcessor) Thumbnail(img ports.DecodedImage, target int) (ports.EncodedThumbnail, error) {
	w, h := domain.FitLongSide(img.Width, img.Height, target)
	return ports.EncodedThumbnail{
		Data:   []byte(fmt.Sprintf("превью-%dx%d", w, h)),
		Width:  w,
		Height: h,
		Format: "jpeg",
		Ext:    "jpg",
	}, nil
}

// ── тестовая сборка сервиса ──────────────────────────────────────────────

type harness struct {
	service *Service
	repo    *fakeRepo
	storage *fakeStorage
	locker  *fakeLocker
	imgs    *fakeImageProcessor
}

func newHarness(width, height int) *harness {
	repo := newFakeRepo(domain.PhotoOriginal{ID: "photo-1", UserID: 42, StorageKey: "originals/42/photo-1.jpg"})
	storage := newFakeStorage("байты-оригинала")
	locker := &fakeLocker{}
	imgs := &fakeImageProcessor{width: width, height: height}

	service := NewService(repo, storage, locker, imgs,
		"gosplash-originals", "gosplash-thumbnails",
		[]int{320, 800, 1600}, 30*time.Second, 2)

	return &harness{service: service, repo: repo, storage: storage, locker: locker, imgs: imgs}
}

func baseInput() ProcessInput {
	return ProcessInput{
		EventID:   "event-1",
		EventType: "media.photo.uploaded",
		Topic:     "media.photo.uploaded",
		PhotoID:   "photo-1",
		UserID:    42,
	}
}

// ── тесты ────────────────────────────────────────────────────────────────

func TestProcess_UspeshnyySlucay(t *testing.T) {
	h := newHarness(2000, 1000) // горизонтальное фото, больше всех трёх целей

	err := h.service.Process(context.Background(), baseInput())

	require.NoError(t, err)
	assert.Equal(t, 3, h.storage.putCount(), "три превью — 320/800/1600")

	require.Len(t, h.repo.commits, 1)
	commit := h.repo.commits[0].in
	assert.Equal(t, "event-1", commit.EventID)
	assert.Equal(t, int64(42), commit.UserID)
	assert.Equal(t, "photo-1", commit.PhotoID)
	require.Len(t, commit.Thumbnails, 3)

	labels := map[string]bool{}
	for _, th := range commit.Thumbnails {
		labels[th.Label] = true
		assert.Equal(t, "jpeg", th.Format)
		assert.Contains(t, th.StorageKey, "thumbnails/42/photo-1_")
	}
	assert.True(t, labels["small"] && labels["medium"] && labels["large"])

	// Лок обязан быть взят и снят ровно один раз.
	require.Len(t, h.locker.granted, 1)
	assert.True(t, h.locker.granted[0].unlocked)
}

func TestProcess_NeIzobrazhenie_OshibkaPostoyannaya(t *testing.T) {
	h := newHarness(0, 0)
	h.imgs.decodeErr = fmt.Errorf("оборачиваем: %w", domain.ErrNotAnImage)

	err := h.service.Process(context.Background(), baseInput())

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrNotAnImage,
		"ошибка обязана дойти до adapters/kafka классифицируемой как permanent")
	assert.Zero(t, h.storage.putCount(), "до генерации превью дело не должно дойти")
	assert.Empty(t, h.repo.commits, "фиксировать нечего")
}

func TestProcess_ApskeylaNeProishodit(t *testing.T) {
	// Оригинал 300x200 — меньше всех трёх целевых размеров (320/800/1600).
	h := newHarness(300, 200)

	err := h.service.Process(context.Background(), baseInput())

	require.NoError(t, err)
	require.Len(t, h.repo.commits, 1)

	for _, th := range h.repo.commits[0].in.Thumbnails {
		assert.LessOrEqual(t, th.Width, 300, "ширина превью не должна превышать оригинал")
		assert.LessOrEqual(t, th.Height, 200, "высота превью не должна превышать оригинал")
		assert.Equal(t, 300, th.Width, "без апскейла размер равен оригиналу")
		assert.Equal(t, 200, th.Height)
	}
}

func TestProcess_LokZanyat_RabotaPropuskaetsyaBezOshibki(t *testing.T) {
	h := newHarness(2000, 1000)
	h.locker.busy = true

	err := h.service.Process(context.Background(), baseInput())

	require.NoError(t, err, "занятый лок — не ошибка, это нормальный проигрыш в гонке")
	assert.Zero(t, h.storage.putCount(), "работа не должна была начаться")
	assert.Empty(t, h.repo.commits)
}

func TestProcess_PovtornoeSobytieOtsekaetsya(t *testing.T) {
	h := newHarness(2000, 1000)
	ctx := context.Background()
	in := baseInput()

	// Первая обработка — обычный успешный путь.
	require.NoError(t, h.service.Process(ctx, in))
	require.Len(t, h.repo.commits, 1)

	// Вторая обработка ТОГО ЖЕ event_id (повторная доставка Kafka) —
	// CommitReady фиксирует claimed=false, и это не ошибка.
	err := h.service.Process(ctx, in)

	require.NoError(t, err, "повтор — не ошибка, это ожидаемая идемпотентность")
	assert.Len(t, h.repo.commits, 2, "CommitReady вызывается оба раза")
}

func TestProcess_OshibkaHranilischa_NeKlassificirovannayaOshibkaProhoditKakEst(t *testing.T) {
	h := newHarness(2000, 1000)
	h.storage.getErr = errors.New("minio: connection refused")

	err := h.service.Process(context.Background(), baseInput())

	require.Error(t, err)
	// НЕ domain.ErrNotAnImage/ErrPhotoNotFound/ErrOriginalMissing — эта
	// ошибка должна остаться неклассифицированной, чтобы kafkax по
	// умолчанию считал её retryable (см. package doc adapters/kafka).
	assert.False(t,
		errors.Is(err, domain.ErrNotAnImage) || errors.Is(err, domain.ErrPhotoNotFound) || errors.Is(err, domain.ErrOriginalMissing),
		"техническая ошибка хранилища не должна маскироваться под доменную")
}

func TestProcess_LokNeSnyalsya_LogiruetsyaNoNeLomaetSsenariy(t *testing.T) {
	h := newHarness(2000, 1000)
	// Подменяем locker так, чтобы Unlock вернул ошибку — сценарий обязан
	// всё равно завершиться успешно: владение локом не гарантия (package doc).
	h.locker.granted = nil
	origAcquire := h.locker
	_ = origAcquire

	lock := &fakeLock{unlockErr: errors.New("лок уже перехвачен")}
	h.locker.busy = false
	// Подсовываем лок с ошибкой на Unlock через обёртку.
	h.service.locker = lockerWithFixedLock{lock: lock}

	err := h.service.Process(context.Background(), baseInput())

	require.NoError(t, err)
	assert.True(t, lock.unlocked)
}

type lockerWithFixedLock struct {
	lock ports.Lock
}

func (l lockerWithFixedLock) AcquirePhotoLock(context.Context, string, time.Duration) (ports.Lock, error) {
	return l.lock, nil
}

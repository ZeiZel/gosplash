package http

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/png"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/media/internal/app"
	"gosplash/services/media/internal/domain"
)

// Свои подделки портов, а не переиспользование fakeRepo/fakeStorage из
// internal/app: те unexported и живут в другом пакете. Тесты этого файла
// проверяют HTTP-слой (маршрутизацию, коды ответа, заголовки), а не
// прикладной сценарий — он уже покрыт internal/app/service_test.go.

var validImage = func() []byte {
	img := image.NewRGBA(image.Rect(0, 0, 4, 3))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		panic(err)
	}
	return buf.Bytes()
}()

type fakePhotoRepo struct {
	created   []domain.Photo
	deleted   []string
	createErr error
	delErr    error
}

func (r *fakePhotoRepo) Create(_ context.Context, photo *domain.Photo) error {
	if r.createErr != nil {
		return r.createErr
	}
	photo.CreatedAt = time.Now()
	r.created = append(r.created, *photo)
	return nil
}

func (r *fakePhotoRepo) GetByID(context.Context, int64, string) (*domain.Photo, error) {
	return nil, domain.ErrNotFound
}
func (r *fakePhotoRepo) ListByUser(context.Context, int64, int) ([]domain.Photo, error) {
	return nil, nil
}
func (r *fakePhotoRepo) ShardOf(userID int64) int                      { return int(userID % 2) }
func (r *fakePhotoRepo) CountByShard(context.Context) ([]int64, error) { return []int64{0, 0}, nil }

func (r *fakePhotoRepo) Delete(_ context.Context, _ int64, id string) error {
	if r.delErr != nil {
		return r.delErr
	}
	r.deleted = append(r.deleted, id)
	return nil
}

type fakeObjectStorage struct {
	putKeys []string
	err     error
}

func (s *fakeObjectStorage) Put(_ context.Context, _, key string, reader io.Reader, _ int64, _ string) error {
	if s.err != nil {
		return s.err
	}
	_, _ = io.Copy(io.Discard, reader)
	s.putKeys = append(s.putKeys, key)
	return nil
}

func (s *fakeObjectStorage) PresignedURL(_ context.Context, _, key string, _ time.Duration) (string, error) {
	return "https://example.test/" + key, nil
}

// fakeLimiter — подделка RateLimiter (Key + Allow, см. её объявление
// в handler.go). Allowed/RetryAfter/Err настраиваются под сценарий теста.
type fakeLimiter struct {
	allowed    bool
	retryAfter time.Duration
	err        error
	calls      int
}

func (f *fakeLimiter) Key(parts ...string) string { return "test:" + strings.Join(parts, ":") }

func (f *fakeLimiter) Allow(context.Context, string, int, float64) (bool, time.Duration, error) {
	f.calls++
	return f.allowed, f.retryAfter, f.err
}

func newUploadRequest(t *testing.T, userID, filename string, content []byte) *http.Request {
	t.Helper()

	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	part, err := w.CreateFormFile("file", filename)
	require.NoError(t, err)
	_, err = part.Write(content)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	req := httptest.NewRequest(http.MethodPost, "/media/upload", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("X-User-Id", userID)
	return req
}

func newTestRouter(deps Deps) *http.ServeMux {
	router := http.NewServeMux()
	Register(router, deps)
	return router
}

func TestUpload_RateLimit_Prevyshen(t *testing.T) {
	// Лимит исчерпан: 429 и Retry-After в ЦЕЛЫХ секундах, округлённых
	// ВВЕРХ — 2.5с не должны превратиться в "2", иначе клиент, честно
	// подождавший 2 секунды, снова получит 429.
	storage := &fakeObjectStorage{}
	service := app.NewPhotoService(&fakePhotoRepo{}, storage, "originals")
	limiter := &fakeLimiter{allowed: false, retryAfter: 2500 * time.Millisecond}

	router := newTestRouter(Deps{
		Service:                service,
		MaxBodyBytes:           1 << 20,
		Limiter:                limiter,
		UploadRateCapacity:     1,
		UploadRateRefillPerSec: 1,
	})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, newUploadRequest(t, "1", "a.png", validImage))

	assert.Equal(t, http.StatusTooManyRequests, rec.Code)
	assert.Equal(t, "3", rec.Header().Get("Retry-After"))
	assert.Equal(t, 1, limiter.calls)
	assert.Empty(t, storage.putKeys, "лимит должен останавливать запрос ДО обращения к хранилищу")
}

func TestUpload_RateLimit_FailOpen(t *testing.T) {
	// Redis недоступен (Allow вернул ошибку) — загрузка НЕ должна упасть
	// из-за этого: rate limiter — защита инфраструктуры, а не часть
	// контракта "принять файл" (см. комментарий к allowUpload).
	storage := &fakeObjectStorage{}
	service := app.NewPhotoService(&fakePhotoRepo{}, storage, "originals")
	limiter := &fakeLimiter{err: errors.New("redis: connection refused")}

	router := newTestRouter(Deps{
		Service:                service,
		MaxBodyBytes:           1 << 20,
		Limiter:                limiter,
		UploadRateCapacity:     1,
		UploadRateRefillPerSec: 1,
	})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, newUploadRequest(t, "1", "a.png", validImage))

	assert.Equal(t, http.StatusCreated, rec.Code, "недоступный лимитер не должен блокировать загрузку")
	assert.Len(t, storage.putKeys, 1)
}

func TestUpload_BezLimitera(t *testing.T) {
	// Limiter == nil — конфигурация без Redis вообще (например, тест или
	// локальный запуск без rate limiting). Поведение то же, что и allowed=true.
	storage := &fakeObjectStorage{}
	service := app.NewPhotoService(&fakePhotoRepo{}, storage, "originals")

	router := newTestRouter(Deps{Service: service, MaxBodyBytes: 1 << 20})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, newUploadRequest(t, "1", "a.png", validImage))

	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestUpload_NeIzobrazhenie(t *testing.T) {
	// Байты не декодируются ни одним зарегистрированным форматом — 400
	// с понятным текстом, а не 500 и не "принято, разберёмся позже".
	storage := &fakeObjectStorage{}
	service := app.NewPhotoService(&fakePhotoRepo{}, storage, "originals")

	router := newTestRouter(Deps{Service: service, MaxBodyBytes: 1 << 20})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, newUploadRequest(t, "1", "a.jpg", []byte("это точно не картинка")))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), domain.ErrNotAnImage.Error())
	assert.Empty(t, storage.putKeys, "не-изображение не должно доезжать до S3")
}

func TestDelete_Uspeshno(t *testing.T) {
	repo := &fakePhotoRepo{}
	service := app.NewPhotoService(repo, &fakeObjectStorage{}, "originals")

	router := newTestRouter(Deps{Service: service, MaxBodyBytes: 1 << 20})

	req := httptest.NewRequest(http.MethodDelete, "/media/photos/photo-1?user_id=1", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, []string{"photo-1"}, repo.deleted)
}

func TestDelete_NeNaydeno(t *testing.T) {
	repo := &fakePhotoRepo{delErr: domain.ErrNotFound}
	service := app.NewPhotoService(repo, &fakeObjectStorage{}, "originals")

	router := newTestRouter(Deps{Service: service, MaxBodyBytes: 1 << 20})

	req := httptest.NewRequest(http.MethodDelete, "/media/photos/photo-1?user_id=1", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDelete_BezUserID(t *testing.T) {
	service := app.NewPhotoService(&fakePhotoRepo{}, &fakeObjectStorage{}, "originals")
	router := newTestRouter(Deps{Service: service, MaxBodyBytes: 1 << 20})

	req := httptest.NewRequest(http.MethodDelete, "/media/photos/photo-1", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

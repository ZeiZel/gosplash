package app

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/png"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/media/internal/domain"
)

// Весь смысл ports & adapters виден в этом файле: тест прогоняет сценарий
// загрузки целиком, не поднимая ни PostgreSQL, ни MinIO, ни Kafka. Подделки
// ниже занимают тридцать строк и работают мгновенно — именно поэтому
// интерфейсы объявлены в ports, а не взяты у реализаций.

// validImage — валидный PNG 4×3, сгенерированный один раз для всех тестов.
// Sniffing по сигнатуре (см. sniffImage в service.go) требует настоящих
// байт изображения: строка вида "байты!" или "x" больше не проходит через
// Upload, и это ЦЕЛЬ фичи из docs/PLAN.md, а не случайность теста.
var validImage = func() []byte {
	img := image.NewRGBA(image.Rect(0, 0, 4, 3))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		panic(err) // package-level init: упасть здесь значит упасть на старте тестового бинаря, что и нужно
	}
	return buf.Bytes()
}()

func validUploadInput(userID int64, filename string) UploadInput {
	return UploadInput{
		UserID:   userID,
		Filename: filename,
		Size:     int64(len(validImage)),
		Content:  bytes.NewReader(validImage),
	}
}

type fakeRepo struct {
	created []domain.Photo
	deleted []string
	err     error
	delErr  error
}

func (r *fakeRepo) Create(_ context.Context, photo *domain.Photo) error {
	if r.err != nil {
		return r.err
	}
	photo.CreatedAt = time.Now()
	r.created = append(r.created, *photo)
	return nil
}

func (r *fakeRepo) GetByID(context.Context, int64, string) (*domain.Photo, error) {
	return nil, domain.ErrNotFound
}
func (r *fakeRepo) ListByUser(context.Context, int64, int) ([]domain.Photo, error) { return nil, nil }
func (r *fakeRepo) ShardOf(userID int64) int                                       { return int(userID % 2) }
func (r *fakeRepo) CountByShard(context.Context) ([]int64, error)                  { return []int64{0, 0}, nil }

func (r *fakeRepo) Delete(_ context.Context, _ int64, id string) error {
	if r.delErr != nil {
		return r.delErr
	}
	r.deleted = append(r.deleted, id)
	return nil
}

type fakeStorage struct {
	putKeys []string
	err     error
	// body — что реально доехало до хранилища. Проверяем, что сервис
	// передаёт поток, а не теряет его по дороге.
	body string
}

func (s *fakeStorage) Put(_ context.Context, _, key string, reader io.Reader, _ int64, _ string) error {
	if s.err != nil {
		return s.err
	}
	data, _ := io.ReadAll(reader)
	s.body = string(data)
	s.putKeys = append(s.putKeys, key)
	return nil
}

func (s *fakeStorage) PresignedURL(_ context.Context, _, key string, _ time.Duration) (string, error) {
	return "https://example.test/" + key, nil
}

func newService() (*PhotoService, *fakeRepo, *fakeStorage) {
	repo := &fakeRepo{}
	storage := &fakeStorage{}
	return NewPhotoService(repo, storage, "originals"), repo, storage
}

func TestUpload_UspeshnyySlucay(t *testing.T) {
	service, repo, storage := newService()

	photo, err := service.Upload(context.Background(), UploadInput{
		UserID:   42,
		Title:    "закат",
		Filename: "Sunset.JPEG",
		Size:     int64(len(validImage)),
		Content:  bytes.NewReader(validImage),
	})

	require.NoError(t, err)
	assert.NotEmpty(t, photo.ID)
	assert.Equal(t, domain.StatusUploaded, photo.Status)

	// Ключ объекта детерминирован и начинается с user_id: по префиксу видно,
	// чьи файлы, и их удобно чистить целиком.
	require.Len(t, storage.putKeys, 1)
	assert.True(t, strings.HasPrefix(storage.putKeys[0], "originals/42/"),
		"ключ должен начинаться с originals/<user_id>/, получено: %s", storage.putKeys[0])
	// Расширение приводится к нижнему регистру: иначе один и тот же файл
	// даст разные ключи в зависимости от того, как его назвали в проводнике.
	assert.True(t, strings.HasSuffix(storage.putKeys[0], ".jpeg"))

	assert.Equal(t, validImage, []byte(storage.body),
		"содержимое обязано доехать до хранилища БЕЗ ИЗМЕНЕНИЙ, включая байты, "+
			"уже прочитанные sniffImage при определении формата")
	require.Len(t, repo.created, 1)

	// Mime и width/height определены по СИГНАТУРЕ файла (sniffImage), а не
	// взяты из клиентского Content-Type, которого в UploadInput больше нет.
	assert.Equal(t, "image/png", photo.Mime)
	assert.Equal(t, 4, photo.Width)
	assert.Equal(t, 3, photo.Height)
}

func TestUpload_PorydokShagov_HranilischeUpalo(t *testing.T) {
	// Хранилище первое: если упало оно, в базе не должно появиться ничего.
	// Иначе в каталоге была бы карточка, ведущая в пустоту.
	service, repo, storage := newService()
	storage.err = errors.New("minio недоступен")

	_, err := service.Upload(context.Background(), validUploadInput(1, "a.jpg"))

	require.Error(t, err)
	assert.Empty(t, repo.created, "в базу писать нельзя, пока файл не сохранён")
}

func TestUpload_AtomicnostMetadannyhISobytiya(t *testing.T) {
	// История этого теста важна для понимания, что именно изменилось.
	// Раньше (фаза 0, см. git-историю: TestUpload_SobytiePoteryano_
	// ZagruzkaVsyoRavnoUspeshna) публикация факта была ОТДЕЛЬНЫМ шагом
	// ПОСЛЕ коммита метаданных, через отдельный порт EventPublisher.
	// Kafka лежит — Create уже отработал успешно, publisher.PhotoUploaded
	// падает, ошибка просто логируется, а Upload возвращает успех: фото
	// есть, события нет, данные разъехались НАВСЕГДА (пока кто-то руками
	// не разберётся). Тест фиксировал это поведение как заведомо
	// несовершенное и был обязан упасть, когда появится outbox, — ровно
	// это сейчас и произошло: порта EventPublisher больше нет вообще,
	// service.Upload физически не может скомпилироваться со старой
	// сигнатурой newService(), которая его принимала.
	//
	// Новое поведение проверяется здесь с ДРУГОЙ стороны, доступной без
	// поднятия реальной БД: PhotoRepository.Create теперь отвечает и за
	// строку photos, и за факт "фото загружено" ОДНОЙ операцией
	// (docs/adr/0008-*, adapters/pg.PhotoRepository.Create — там же и
	// разбор, почему это вообще возможно в шардированном media). Раз
	// отдельного метода "опубликовать после" у порта больше нет, сценарию
	// просто нечего вызывать по отдельности — а значит, разъехаться
	// метаданным и событию НЕЧЕМ СТРУКТУРНО, а не потому что кто-то не
	// забыл добавить ретрай. Здесь это видно как: Create падает → Upload
	// возвращает ошибку → ни фото, ни (невидимого отсюда, но обязанного
	// быть частью той же транзакции) события не остаётся НИГДЕ.
	service, repo, storage := newService()
	repo.err = errors.New("outbox: запись события photo.uploaded: контекст отменён")

	photo, err := service.Upload(context.Background(), validUploadInput(1, "a.jpg"))

	require.Error(t, err, "если единая операция Create не удалась, загрузка не может считаться успешной")
	assert.Nil(t, photo)
	assert.Empty(t, repo.created, "ни фото, ни событие не могут появиться отдельно друг от друга")
	// Объект в S3 при этом остаётся — это ОСОЗНАННЫЙ мусор (см. комментарий
	// к Upload), а не то, что чинит транзакционность Postgres: у Postgres
	// и MinIO нет общего протокола коммита, и не может быть.
	assert.Len(t, storage.putKeys, 1)
}

func TestUpload_Validaciya(t *testing.T) {
	tests := []struct {
		name  string
		input UploadInput
		want  error
	}{
		{
			name:  "без user_id — неизвестен шард",
			input: UploadInput{Filename: "a.jpg", Content: strings.NewReader("x")},
			want:  domain.ErrNoUserID,
		},
		{
			name:  "отрицательный user_id",
			input: UploadInput{UserID: -1, Filename: "a.jpg", Content: strings.NewReader("x")},
			want:  domain.ErrNoUserID,
		},
		{
			name:  "без имени файла — неоткуда взять расширение",
			input: UploadInput{UserID: 1, Content: strings.NewReader("x")},
			want:  domain.ErrEmptyFileName,
		},
		{
			// Ключевой новый случай фазы 1-2: "x" не декодируется НИ ОДНИМ
			// из зарегистрированных форматов (jpeg/png/gif), и такой файл
			// обязан быть отвергнут ДО того, как что-либо уедет в S3 —
			// не потому что клиент не указал Content-Type правильно
			// (Content-Type в UploadInput больше нет вовсе), а потому что
			// байты объективно не являются изображением.
			name:  "не изображение — сигнатура не распознана ни одним декодером",
			input: UploadInput{UserID: 1, Filename: "a.jpg", Content: strings.NewReader("это точно не картинка")},
			want:  domain.ErrNotAnImage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, _, storage := newService()

			_, err := service.Upload(context.Background(), tt.input)

			require.ErrorIs(t, err, tt.want)
			assert.Empty(t, storage.putKeys, "невалидный запрос не должен доходить до хранилища")
		})
	}
}

func TestUpload_IDUnikalenDlyaKazhdoyZagruzki(t *testing.T) {
	service, _, _ := newService()

	seen := make(map[string]struct{})
	for range 100 {
		photo, err := service.Upload(context.Background(), validUploadInput(1, "a.jpg"))
		require.NoError(t, err)

		_, duplicate := seen[photo.ID]
		require.False(t, duplicate, "повторный id фото: %s", photo.ID)
		seen[photo.ID] = struct{}{}
	}
}

func TestDownloadURL(t *testing.T) {
	service, _, _ := newService()

	url, err := service.DownloadURL(context.Background(),
		&domain.Photo{StorageKey: "originals/1/abc.jpg"}, time.Minute)

	require.NoError(t, err)
	assert.Equal(t, "https://example.test/originals/1/abc.jpg", url)
}

func TestDelete_UspeshnyySlucay(t *testing.T) {
	service, repo, _ := newService()

	err := service.Delete(context.Background(), 1, "photo-1")

	require.NoError(t, err)
	assert.Equal(t, []string{"photo-1"}, repo.deleted)
}

func TestDelete_NeNaydeno(t *testing.T) {
	// domain.ErrNotFound должен дойти до вызывающего КАК ЕСТЬ (errors.Is),
	// без оборачивания: HTTP-адаптер сравнивает именно с этим сигналом,
	// чтобы вернуть 404, а не 500 (см. adapters/http.Handler.delete).
	service, repo, _ := newService()
	repo.delErr = domain.ErrNotFound

	err := service.Delete(context.Background(), 1, "photo-1")

	require.ErrorIs(t, err, domain.ErrNotFound)
}

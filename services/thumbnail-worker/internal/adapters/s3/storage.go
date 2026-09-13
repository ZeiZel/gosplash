// Package s3 — адаптер объектного хранилища thumbnail-worker'а.
//
// pkg/s3x (общий пакет проекта) даёт только Put и PresignedURL — этого
// хватало media, которая сама байты оригинала никогда не читает: клиент
// скачивает файл напрямую по подписанной ссылке, минуя сервис. thumbnail-
// worker устроен ровно наоборот: ему обязательно нужно прочитать байты
// оригинала самому, чтобы их пересжать, — а метода Get в pkg/s3x нет, и
// добавлять его нельзя (pkg/** вне зоны ответственности этой задачи).
// Поэтому здесь свой тонкий клиент поверх того же minio-go: Put повторяет
// pkg/s3x.Client.Put (совместим с ports.ObjectStorage), Get — то, чего
// не хватало.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"gosplash/services/thumbnail-worker/internal/domain"
)

type Client struct {
	minio *minio.Client
}

func New(endpoint, accessKey, secretKey string, useSSL bool) (*Client, error) {
	m, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio: %w", err)
	}
	return &Client{minio: m}, nil
}

// Get отдаёт байты объекта потоком.
//
// Объект отсутствует → domain.ErrOriginalMissing: это ПОСТОЯННАЯ ошибка,
// повторная попытка не заставит объект появиться в хранилище. Любая другая
// ошибка (сеть, MinIO недоступен) уходит как есть — по умолчанию kafkax
// считает неклассифицированную ошибку retryable, а это ровно то, что нужно
// для временного сбоя инфраструктуры (см. internal/adapters/kafka).
func (c *Client) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	obj, err := c.minio.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get %s/%s: %w", bucket, key, err)
	}

	// GetObject у minio-go ленивый: ошибка "объекта нет" появляется только
	// при первом реальном обращении к телу (Stat/Read), а не здесь. Дёргаем
	// Stat сразу же — иначе Decode получил бы на входе просто пустой поток
	// и honestly отчитался "не изображение" вместо "оригинал отсутствует",
	// и классификация ошибки в adapters/kafka сработала бы неверно
	// (домены разные: ErrNotAnImage — про содержимое, ErrOriginalMissing —
	// про сам факт наличия объекта, хотя обе и permanent).
	if _, statErr := obj.Stat(); statErr != nil {
		_ = obj.Close()
		if isNoSuchKey(statErr) {
			return nil, fmt.Errorf("get %s/%s: %w", bucket, key, domain.ErrOriginalMissing)
		}
		return nil, fmt.Errorf("stat %s/%s: %w", bucket, key, statErr)
	}
	return obj, nil
}

// Put — идентичен pkg/s3x.Client.Put: реализует ту же сигнатуру, потому
// что превью загружаются точно так же, как media грузит оригиналы.
func (c *Client) Put(ctx context.Context, bucket, key string, reader io.Reader, size int64, contentType string) error {
	_, err := c.minio.PutObject(ctx, bucket, key, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("put %s/%s: %w", bucket, key, err)
	}
	return nil
}

// Ping — для /readyz. bucket передаёт main.go (обычно бакет оригиналов):
// BucketExists — самый дешёвый запрос, который честно проверяет, что MinIO
// отвечает и настроен на нужный бакет, не читая и не пиша данные.
func (c *Client) Ping(ctx context.Context, bucket string) error {
	ok, err := c.minio.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("minio ping: %w", err)
	}
	if !ok {
		return fmt.Errorf("minio ping: бакет %s не существует", bucket)
	}
	return nil
}

func isNoSuchKey(err error) bool {
	var errResp minio.ErrorResponse
	if errors.As(err, &errResp) {
		return errResp.Code == "NoSuchKey"
	}
	// ToErrorResponse — задокументированный способ minio-go развернуть ЛЮБУЮ
	// ошибку (включая ту, что не implement errors.As-совместимую цепочку)
	// в единообразный ErrorResponse с кодом.
	return minio.ToErrorResponse(err).Code == "NoSuchKey"
}

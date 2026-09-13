// Package s3x — работа с объектным хранилищем (локально это MinIO).
//
// В базе лежат только метаданные и ключ объекта. Сами байты в PostgreSQL
// не попадают никогда: blob'ы раздувают WAL, дампы и репликацию, а отдавать
// файл из БД через приложение — лишний прыжок.
package s3x

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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

// Put заливает объект потоком.
//
// reader — это io.Reader, а не []byte, и это принципиально: файл на 50 МБ
// проходит через сервис кусками и не оседает в памяти целиком. При десятке
// параллельных загрузок разница между стримингом и «прочитать всё в срез» —
// это разница между работающим сервисом и OOM.
//
// size обязателен: S3 должен знать длину заранее. Для multipart-загрузки его
// даёт FileHeader.Size. Если размер неизвестен, передают -1, и клиент
// переключается на многочастную загрузку.
func (c *Client) Put(ctx context.Context, bucket, key string, reader io.Reader, size int64, contentType string) error {
	_, err := c.minio.PutObject(ctx, bucket, key, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("put %s/%s: %w", bucket, key, err)
	}
	return nil
}

// PresignedURL — временная ссылка на приватный объект.
//
// Подпись считается локально, поход в MinIO не нужен. Клиент скачивает файл
// напрямую из хранилища, минуя сервис: тот не тратит ни трафик, ни горутины.
// Так отдают оригиналы тем, кто купил лицензию.
func (c *Client) PresignedURL(ctx context.Context, bucket, key string, ttl time.Duration) (string, error) {
	u, err := c.minio.PresignedGetObject(ctx, bucket, key, ttl, url.Values{})
	if err != nil {
		return "", fmt.Errorf("presign %s/%s: %w", bucket, key, err)
	}
	return u.String(), nil
}

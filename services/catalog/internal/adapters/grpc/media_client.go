// Package grpc — адаптер каталога к media-сервису и реализация собственного
// gRPC-сервиса каталога (server.go).
//
// Реализует порт ports.PhotoFetcher. Это единственное место в catalog,
// которое знает про gRPC и про контракт media.
package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mediav1 "gosplash/gen/go/gosplash/media/v1"
	"gosplash/services/catalog/internal/domain"
)

type MediaClient struct {
	client mediav1.MediaServiceClient
}

func NewMediaClient(client mediav1.MediaServiceClient) *MediaClient {
	return &MediaClient{client: client}
}

// Fetch запрашивает подробности о фото и сразу переводит их в доменный тип
// каталога.
//
// Перевод делается здесь, а не в прикладном слое, намеренно: иначе
// сгенерированный тип mediav1.Photo протёк бы в app, и катaлог оказался бы
// связан с контрактом чужого сервиса не только по сети, но и по коду.
//
// NotFound от media переводится в domain.ErrPhotoNotFound — единственная
// ошибка этого метода, которую прикладной слой обязан распознавать (см.
// internal/app/indexer.go: она постоянная, всё остальное — временное).
// Перевод именно здесь, а не в app, — по той же причине, что и выше: app не
// должен импортировать google.golang.org/grpc/codes, чтобы оставаться
// тестируемым без сети (docs/STYLE.md).
func (c *MediaClient) Fetch(ctx context.Context, photoID string, userID int64) (*domain.Listing, error) {
	resp, err := c.client.GetPhoto(ctx, &mediav1.GetPhotoRequest{
		Id: photoID,
		// user_id обязателен: без ключа шардирования media не знает,
		// на какой базе искать строку.
		UserId: userID,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, fmt.Errorf("%w: %s", domain.ErrPhotoNotFound, photoID)
		}
		return nil, err
	}

	photo := resp.GetPhoto()
	return &domain.Listing{
		ID:         photo.GetId(),
		AuthorID:   photo.GetUserId(),
		Title:      photo.GetTitle(),
		StorageKey: photo.GetStorageKey(),
		Width:      photo.GetWidth(),
		Height:     photo.GetHeight(),
		// Tags, Thumbnails, PriceCents, Currency — у media таких данных нет,
		// остаются нулевыми значениями до тех пор, пока их не заполнит
		// HandlePhotoThumbnailReady (Tags/Thumbnails) или будущая фаза,
		// добавляющая цену (см. domain.Listing и отчёт о работе).
	}, nil
}

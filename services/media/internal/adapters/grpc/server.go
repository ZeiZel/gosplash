// Package grpc — адаптер синхронного межсервисного API media.
//
// Клиент у него ровно один: catalog. Получив из Kafka факт «фото загружено»,
// он идёт сюда за подробностями. Это и есть разделение ролей: Kafka несёт
// СОБЫТИЕ, gRPC отдаёт ДАННЫЕ.
package grpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mediav1 "gosplash/gen/go/gosplash/media/v1"
	"gosplash/services/media/internal/app"
	"gosplash/services/media/internal/domain"
)

// Server — реализация сервиса из proto/gosplash/media/v1/media.proto.
//
// UnimplementedMediaServiceServer встроен намеренно (так советует сам gRPC):
// когда в .proto добавится новый метод, сервис продолжит компилироваться
// и будет отвечать Unimplemented, а не падать при сборке.
type Server struct {
	mediav1.UnimplementedMediaServiceServer
	service *app.PhotoService
}

func NewServer(service *app.PhotoService) *Server {
	return &Server{service: service}
}

func (s *Server) GetPhoto(ctx context.Context, req *mediav1.GetPhotoRequest) (*mediav1.GetPhotoResponse, error) {
	if req.GetUserId() <= 0 {
		// В gRPC ошибки — это коды из codes, а не HTTP-статусы. Клиент по ним
		// принимает решения: InvalidArgument ретраить бессмысленно,
		// Unavailable — наоборот, нужно.
		return nil, status.Error(codes.InvalidArgument, "user_id обязателен: это ключ шардирования")
	}

	photo, err := s.service.Get(ctx, req.GetUserId(), req.GetId())
	if errors.Is(err, domain.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "фото не найдено")
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &mediav1.GetPhotoResponse{Photo: toProto(photo, s.service.ShardOf(photo.UserID))}, nil
}

// toProto переводит доменную модель в контракт. Ручное отображение выглядит
// скучным, но именно оно не даёт внутренней модели протечь в публичный API:
// добавишь поле — оно не появится в контракте, пока ты этого не захочешь.
func toProto(p *domain.Photo, shard int) *mediav1.Photo {
	return &mediav1.Photo{
		Id:         p.ID,
		UserId:     p.UserID,
		Title:      p.Title,
		StorageKey: p.StorageKey,
		Mime:       p.Mime,
		SizeBytes:  p.SizeBytes,
		Status:     p.Status,
		Shard:      int32(shard),
		Width:      int32(p.Width),
		Height:     int32(p.Height),
	}
}

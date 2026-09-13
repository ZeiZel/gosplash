package app

import (
	"context"

	"gosplash/services/catalog/internal/domain"
	"gosplash/services/catalog/internal/ports"
)

// LicenseService — шаги саги заказа: выдача и отзыв лицензии. Тонкий слой
// над репозиторием намеренно: сама идемпотентность по order_id — свойство
// РЕАЛИЗАЦИИ (adapters/pg), сервис лишь называет операцию по имени, которое
// понятно вызывающему (gRPC-адаптеру), не протаскивая детали хранения выше.
type LicenseService struct {
	repo ports.LicenseRepository
}

func NewLicenseService(repo ports.LicenseRepository) *LicenseService {
	return &LicenseService{repo: repo}
}

func (s *LicenseService) Grant(ctx context.Context, listingID string, buyerID int64, orderID string) (*domain.License, error) {
	return s.repo.Grant(ctx, listingID, buyerID, orderID)
}

func (s *LicenseService) Revoke(ctx context.Context, orderID, reason string) (bool, error) {
	return s.repo.Revoke(ctx, orderID, reason)
}

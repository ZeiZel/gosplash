package app

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gosplash/services/catalog/internal/domain"
)

// fakeLicenseRepo — подделка ports.LicenseRepository, воспроизводящая
// идемпотентный контракт adapters/pg/license_repository.go (ON CONFLICT /
// UPDATE ... WHERE status = granted) в памяти, без базы.
type fakeLicenseRepo struct {
	byOrderID map[string]*domain.License
	grants    int // сколько раз РЕАЛЬНО создана новая строка
}

func newFakeLicenseRepo() *fakeLicenseRepo {
	return &fakeLicenseRepo{byOrderID: map[string]*domain.License{}}
}

func (r *fakeLicenseRepo) Grant(_ context.Context, listingID string, buyerID int64, orderID string) (*domain.License, error) {
	if existing, ok := r.byOrderID[orderID]; ok {
		return existing, nil
	}
	r.grants++
	l := &domain.License{
		ID: "license-" + orderID, ListingID: listingID, BuyerID: buyerID,
		OrderID: orderID, Status: domain.LicenseGranted, GrantedAt: time.Now(),
	}
	r.byOrderID[orderID] = l
	return l, nil
}

func (r *fakeLicenseRepo) Revoke(_ context.Context, orderID, reason string) (bool, error) {
	l, ok := r.byOrderID[orderID]
	if !ok || l.Status != domain.LicenseGranted {
		return false, nil
	}
	l.Status = domain.LicenseRevoked
	l.Reason = reason
	l.RevokedAt = time.Now()
	return true, nil
}

func TestLicenseService_Grant_IdempotentnyPoOrderID(t *testing.T) {
	repo := newFakeLicenseRepo()
	service := NewLicenseService(repo)

	first, err := service.Grant(context.Background(), "listing-1", 7, "order-1")
	require.NoError(t, err)

	second, err := service.Grant(context.Background(), "listing-1", 7, "order-1")
	require.NoError(t, err)

	assert.Equal(t, first.ID, second.ID, "повтор с тем же order_id обязан вернуть ТУ ЖЕ лицензию")
	assert.Equal(t, 1, repo.grants, "новая строка создаётся ровно один раз")
}

func TestLicenseService_Revoke_IdempotentnyPoOrderID(t *testing.T) {
	repo := newFakeLicenseRepo()
	service := NewLicenseService(repo)
	_, err := service.Grant(context.Background(), "listing-1", 7, "order-1")
	require.NoError(t, err)

	first, err := service.Revoke(context.Background(), "order-1", "возврат")
	require.NoError(t, err)
	second, err := service.Revoke(context.Background(), "order-1", "возврат")
	require.NoError(t, err)

	assert.True(t, first, "первый отзыв выданной лицензии обязан вернуть true")
	assert.False(t, second, "повторный отзыв уже отозванной — false, без ошибки")
}

func TestLicenseService_Revoke_NevydannayaLitsenziya(t *testing.T) {
	// Компенсирующий шаг саги обязан пережить отмену шага, который не
	// выполнился: RevokeLicense на несуществующий order_id — НЕ ошибка.
	repo := newFakeLicenseRepo()
	service := NewLicenseService(repo)

	revoked, err := service.Revoke(context.Background(), "order-nikogda-ne-bylo", "отмена")

	require.NoError(t, err)
	assert.False(t, revoked)
}

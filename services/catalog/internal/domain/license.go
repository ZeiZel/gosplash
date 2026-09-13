package domain

import "time"

// Статусы лицензии.
const (
	LicenseGranted = "granted"
	LicenseRevoked = "revoked"
)

// License — право покупателя на использование карточки, выданное шагом саги
// заказа (GrantLicense) и отзываемое компенсирующим шагом (RevokeLicense).
//
// OrderID — ключ идемпотентности обеих операций: сага заказа умеет упасть
// и повториться на любом шаге, и повтор GrantLicense/RevokeLicense с тем же
// order_id обязан быть безопасным (см. adapters/pg/license_repository.go).
type License struct {
	ID        string
	ListingID string
	BuyerID   int64
	OrderID   string
	Status    string // granted | revoked
	Reason    string // причина отзыва, пусто для granted
	GrantedAt time.Time
	RevokedAt time.Time // нулевое, пока не отозвана
}

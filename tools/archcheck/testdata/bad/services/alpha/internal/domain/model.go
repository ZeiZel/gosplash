// Фикстура: domain, нарушающий ports & adapters дважды — прямой импорт
// инфраструктуры (gorm.io/gorm) и gen/go (запрещён именно для domain,
// в отличие от app).
package domain

import (
	"gorm.io/gorm"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

type Row struct {
	gorm.Model
	Event *eventsv1.Envelope
}

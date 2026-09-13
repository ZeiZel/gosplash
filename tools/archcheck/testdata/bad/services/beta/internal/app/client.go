// Фикстура: сервис beta импортирует internal/domain сервиса alpha напрямую
// — единственное нарушение service-isolation в этом фикстурном дереве.
package app

import "gosplash/services/alpha/internal/domain"

type Client struct {
	widget domain.Widget
}

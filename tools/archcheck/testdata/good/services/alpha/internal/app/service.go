// Фикстура: чистый app — импортирует только domain и ports (и kafkax,
// как в реальном проекте: это абстракция, а не сама инфраструктура,
// см. rule_ports_adapters.go).
package app

import (
	"context"

	"gosplash/pkg/kafkax"
	"gosplash/services/alpha/internal/domain"
)

type Service struct{}

func (s *Service) Handle(ctx context.Context, w domain.Widget) error {
	_ = kafkax.TopicPhotoUploaded
	return nil
}

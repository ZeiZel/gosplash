// Фикстура: прямая публикация в Kafka вне pkg/outbox и pkg/kafkax, без
// какого-либо маркера-исключения — должна дать ровно одно нарушение
// direct-produce на строке вызова Publish.
package views

import "context"

type publisher struct{}

func (p *publisher) Publish(ctx context.Context, topic string, payload []byte) error { return nil }

func Send(ctx context.Context) error {
	p := &publisher{}
	return p.Publish(ctx, "views.photo.viewed", nil)
}

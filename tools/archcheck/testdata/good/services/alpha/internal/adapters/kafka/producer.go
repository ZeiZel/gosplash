// Фикстура: адаптер сервиса, публикующий напрямую, но с явным маркером-
// исключением — должен пройти direct-produce без единого нарушения.
package kafka

import "context"

type publisher struct{}

func (p *publisher) Publish(ctx context.Context, topic string, payload []byte) error { return nil }

func Send(ctx context.Context) error {
	p := &publisher{}
	// archcheck:allow direct-produce тестовая фикстура — маркер строкой выше вызова.
	return p.Publish(ctx, "alpha.widget.created", nil)
}

func SendInline(ctx context.Context) error {
	p := &publisher{}
	return p.Publish(ctx, "alpha.widget.created", nil) // archcheck:allow direct-produce маркер на той же строке, что и вызов
}

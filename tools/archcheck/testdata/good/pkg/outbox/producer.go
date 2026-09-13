// Фикстура для тестов archcheck: pkg/outbox — «дом» direct-produce,
// здесь Publish/ProduceSync разрешены без единого маркера.
package outbox

import "context"

type client struct{}

func (c *client) ProduceSync(ctx context.Context, topic string) error { return nil }

type producer struct{}

func (p *producer) Publish(ctx context.Context, topic string, payload []byte) error { return nil }

func Write(ctx context.Context) error {
	c := &client{}
	if err := c.ProduceSync(ctx, "outbox.topic"); err != nil {
		return err
	}
	p := &producer{}
	return p.Publish(ctx, "outbox.topic", nil)
}

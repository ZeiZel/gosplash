// Фикстура: обычный пакет без публикации в Kafka и без запрещённых
// импортов — не должен давать ни одного нарушения ни по одному правилу.
package somepkg

import "context"

func DoSomething(ctx context.Context) error { return nil }

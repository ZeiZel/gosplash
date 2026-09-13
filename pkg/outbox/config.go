package outbox

import "time"

// Config — параметры Relay. Собственный тип пакета, а не часть pkg/config:
// pkg/config читает переменные окружения (docs/STYLE.md запрещает делать
// это где-либо ещё), а Relay нужен не всем сервисам и не должен обязывать
// pkg/config знать про DSN шардов конкретного сервиса. Разбор переменных
// окружения в структуру Config — забота main.go вызывающего сервиса.
type Config struct {
	// ServiceName попадает в заголовок producer публикуемых записей —
	// та же роль, что у одноимённого параметра kafkax.NewProducer.
	ServiceName string

	// Brokers — адреса Kafka, через которые Relay публикует события.
	Brokers []string

	// DSNs — ОДИН DSN на шард. Порядок неважен (в отличие от pkg/dbx.Shards,
	// где по индексу считается hash % N): Relay не решает, где лежат данные,
	// он просто обходит ВСЕ переданные базы по очереди (см. docs/adr/0002-*
	// про то, почему в шардированном media таблица outbox — по одной копии
	// НА ШАРД, а не одна общая). У сервиса с одной базой (wallet, order)
	// здесь просто один DSN.
	DSNs []string

	// PollInterval — как часто Relay опрашивает outbox на новые строки.
	PollInterval time.Duration

	// BatchSize — сколько строк вычитывает и публикует за один заход
	// (LIMIT в SELECT). Слишком маленький — Relay не успевает за потоком
	// записи при всплеске нагрузки; слишком большой — один медленный брокер
	// держит транзакцию (и, соответственно, блокировки FOR UPDATE) дольше,
	// чем нужно, мешая другому релею того же шарда SKIP LOCKED'ом обойти
	// эти строки стороной, но не мешая писать НОВЫЕ строки бизнес-коду.
	BatchSize int
}

// DefaultConfig — разумные значения для локальной разработки и тестов.
// ServiceName, Brokers и DSNs осмысленного дефолта не имеют и обязаны
// быть заданы вызывающим.
func DefaultConfig() Config {
	return Config{
		PollInterval: 2 * time.Second,
		BatchSize:    100,
	}
}

// withDefaults подставляет DefaultConfig() там, где вызывающий оставил
// нулевое значение — так New(cfg) не падает на "PollInterval: 0", то есть
// на релей, до предела нагружающий и Postgres, и Kafka пустыми SELECT'ами.
func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.PollInterval <= 0 {
		c.PollInterval = d.PollInterval
	}
	if c.BatchSize <= 0 {
		c.BatchSize = d.BatchSize
	}
	return c
}

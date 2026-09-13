package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"gosplash/pkg/kafkax"
)

// PATTERN: relay на pgx, а не на GORM (docs/adr/0006-*).
//
// SELECT ... FOR UPDATE SKIP LOCKED в цикле, постоянно, — это горячий путь
// с особым требованием: несколько экземпляров Relay (несколько подов одного
// сервиса) должны уметь работать ПАРАЛЛЕЛЬНО, разбирая разные строки одной
// таблицы, а не сериализоваться на блокировке строки, которую кто-то уже
// взял в обработку. SKIP LOCKED — это ровно то, что делает возможным
// «несколько воркеров, одна очередь» без централизованного диспетчера:
// каждый релей берёт первые NEPUBLISHED строки, которые НЕ заблокированы
// чужой транзакцией, и молча пропускает остальные, вместо того чтобы висеть
// в очереди на блокировку и тем самым сериализовать работу, которую и так
// можно делать параллельно.
//
// pgx здесь напрямую, а не через GORM: `Clauses(clause.Locking{...})`
// в GORM работает, но главная строка этого файла — SKIP LOCKED — читается
// хуже сырого SQL, а рефлексия GORM при разборе шести колонок без единой
// связи ничего не выигрывает.

// shard — одно соединение с одной базой media. Relay должен уметь работать
// с НЕСКОЛЬКИМИ базами: media шардирован (docs/adr/0002-*), и у каждого
// шарда — своя копия таблицы outbox. Обход ВСЕХ шардов на каждом тике —
// прямое следствие этого решения, а не недосмотр: строка, которая должна
// уехать в Kafka, может лежать на любом из них.
type shard struct {
	index int
	pool  *pgxpool.Pool
}

// Relay — фоновый процесс, публикующий накопленные в outbox события.
type Relay struct {
	cfg      Config
	shards   []shard
	producer *kgo.Client

	pending metric.Int64Gauge
}

// New открывает пул на каждый DSN и клиент Kafka. Соединения открываются
// сразу (а не лениво при первом Run), чтобы ошибку конфигурации — опечатку
// в DSN, недоступный брокер — было видно на старте сервиса, а не через
// PollInterval после того, как всё остальное уже приняло трафик.
func New(cfg Config) (*Relay, error) {
	cfg = cfg.withDefaults()
	if len(cfg.DSNs) == 0 {
		return nil, fmt.Errorf("outbox: relay: не задан ни один DSN")
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("outbox: relay: не заданы брокеры Kafka")
	}

	r := &Relay{cfg: cfg}

	for i, dsn := range cfg.DSNs {
		pool, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("outbox: relay: шард %d: %w", i, err)
		}
		r.shards = append(r.shards, shard{index: i, pool: pool})
	}

	// Тот же набор гарантий доставки, что и у pkg/kafkax.Producer (см. там
	// producerOpts) — outbox существует, чтобы НЕ терять события, и было бы
	// странно доверять этой отправке слабее, чем прямой публикации. Опции
	// продублированы, а не переиспользованы: producerOpts не экспортирован
	// из kafkax, и протаскивать его в публичный API пакета ради единственного
	// внешнего вызывающего дороже, чем пять строк дублирования.
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RecordDeliveryTimeout(10*time.Second),
		kgo.RecordRetries(5),
	)
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("outbox: relay: kafka producer: %w", err)
	}
	r.producer = producer

	meter := otel.Meter("gosplash/outbox")
	r.pending, err = meter.Int64Gauge(
		"outbox_pending",
		metric.WithDescription("Строк outbox, ожидающих публикации"),
	)
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("outbox: метрика outbox_pending: %w", err)
	}

	return r, nil
}

// Close закрывает пулы и продюсер. Вызывается при graceful shutdown ПОСЛЕ
// того, как Run вернул управление.
func (r *Relay) Close() {
	for _, s := range r.shards {
		s.pool.Close()
	}
	if r.producer != nil {
		r.producer.Close()
	}
}

// Ping проверяет все шарды — тем же принципом, что и pkg/dbx.Shards.Ping:
// один недоступный шард означает, что relay не может опубликовать события
// пользователей, чьи данные лежат именно там.
func (r *Relay) Ping(ctx context.Context) error {
	for _, s := range r.shards {
		if err := s.pool.Ping(ctx); err != nil {
			return fmt.Errorf("outbox: шард %d: %w", s.index, err)
		}
	}
	return nil
}

// Run опрашивает все шарды раз в PollInterval, пока не отменят ctx.
//
// Завершается по ctx, ДОПИСАВ текущий батч: тик, попавший в работу до
// отмены, доводится до конца (produce + UPDATE published_at) на контексте,
// отвязанном от отмены родителя (context.WithoutCancel). Обрывать батч на
// половине хуже, чем доработать его на секунду дольше: недописанный батч —
// это события, которые уже могли уйти в Kafka, но не помечены published_at,
// и они гарантированно уйдут ТУДА ЖЕ ещё раз при следующем запуске —
// то есть ровно тот дубликат, ради ограничения количества которых
// и стоило потратить секунду на завершение батча.
func (r *Relay) Run(ctx context.Context) error {
	slog.Info("outbox: relay запущен", "shards", len(r.shards), "interval", r.cfg.PollInterval, "batch", r.cfg.BatchSize)

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("outbox: relay останавливается")
			return nil
		case <-ticker.C:
			batchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.PollInterval*10)
			r.pollAllShards(batchCtx)
			cancel()
		}
	}
}

// pollAllShards — обход ВСЕХ шардов на каждом тике. Это следствие
// docs/adr/0002-*: у media несколько независимых баз, и в каждой — своя
// копия outbox. Один relay на процесс проще для эксплуатации, чем один
// relay-подпроцесс на шард, а цена — последовательный, а не параллельный
// обход шардов; при двух-трёх шардах это несущественно, при десятках —
// уже стоило бы распараллелить (не делаем: нет цифр, см. ADR 0006).
func (r *Relay) pollAllShards(ctx context.Context) {
	for _, s := range r.shards {
		if err := r.pollShard(ctx, s); err != nil {
			slog.Error("outbox: relay: шард", "shard", s.index, "error", err)
		}
	}
}

type batchRow struct {
	id      string
	topic   string
	key     []byte
	headers []byte
	payload []byte
}

// pollShard — одна итерация: снять метрику, выбрать батч, опубликовать,
// пометить опубликованное.
func (r *Relay) pollShard(ctx context.Context, s shard) error {
	// Имя таблицы подставляется в SQL СТРОКОЙ, а не параметром: параметры
	// бывают только у значений, не у идентификаторов. Это безопасно ровно
	// потому, что значение приходит не от пользователя, а из TableFor()
	// поверх имени сервиса, заданного в main.go при старте.
	table := TableFor(r.cfg.ServiceName)

	if err := r.reportPending(ctx, s); err != nil {
		// Метрика — не повод останавливать публикацию, поэтому только лог.
		slog.Warn("outbox: relay: outbox_pending", "shard", s.index, "error", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	// Rollback после успешного Commit в pgx — no-op (документированное
	// поведение библиотеки), поэтому безусловный defer безопасен для обоих
	// исходов: и коммита, и раннего return по ошибке.
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, aggregate_id, topic, key, headers, payload
		FROM `+table+`
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`,
		r.cfg.BatchSize,
	)
	if err != nil {
		return fmt.Errorf("select: %w", err)
	}

	var batch []batchRow
	for rows.Next() {
		var row batchRow
		var aggregateID string
		if err := rows.Scan(&row.id, &aggregateID, &row.topic, &row.key, &row.headers, &row.payload); err != nil {
			rows.Close()
			return fmt.Errorf("scan: %w", err)
		}
		batch = append(batch, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}
	if len(batch) == 0 {
		return nil
	}

	published := r.publishBatch(ctx, batch)
	if len(published) == 0 {
		// Ни одна запись не ушла (например, Kafka целиком недоступна).
		// Транзакция откатывается через defer выше: блокировки FOR UPDATE
		// снимаются, published_at не тронут, и следующий тик подберёт эти
		// же строки заново — ничего не потеряно, просто отложено.
		return fmt.Errorf("publish: ни одна запись из %d не отправлена", len(batch))
	}

	if _, err := tx.Exec(ctx, `UPDATE `+table+` SET published_at = now() WHERE id = ANY($1)`, published); err != nil {
		return fmt.Errorf("update published_at: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	slog.Info("outbox: relay опубликовал батч",
		"shard", s.index, "published", len(published), "batch", len(batch))
	if len(published) < len(batch) {
		slog.Warn("outbox: relay: часть батча не опубликована, останется в очереди",
			"shard", s.index, "failed", len(batch)-len(published))
	}
	return nil
}

// publishBatch публикует строки батча ПОСЛЕДОВАТЕЛЬНО и синхронно.
//
// Не параллельно и не асинхронным Produce с колбэками: батч — 100 строк
// максимум, а код, который нужно понять с одного взгляда (это тот самый
// принцип из docs/adr/0006-* про relay), важнее выигрыша в пропускной
// способности, для которого пока нет ни одной цифры, доказывающей, что он
// нужен. Строка, чья публикация не удалась, просто остаётся в очереди —
// ошибка одной строки не должна ронять весь батч.
func (r *Relay) publishBatch(ctx context.Context, batch []batchRow) []string {
	published := make([]string, 0, len(batch))
	for _, row := range batch {
		headers, err := headersToRecordHeaders(row.headers)
		if err != nil {
			slog.Error("outbox: relay: битые headers в строке, публикую без них",
				"id", row.id, "error", err)
		}
		// producer проставлен ещё в Write — тем сервисом, который событие
		// породил. Дописываем своё имя, только если его там почему-то нет
		// (строка от старой версии кода): перетирать чужую атрибуцию нельзя,
		// relay — доставщик, а не автор.
		if !hasHeader(headers, kafkax.HeaderProducer) {
			headers = append(headers, kgo.RecordHeader{Key: kafkax.HeaderProducer, Value: []byte(r.cfg.ServiceName)})
		}

		record := &kgo.Record{Topic: row.topic, Key: row.key, Value: row.payload, Headers: headers}
		if err := r.producer.ProduceSync(ctx, record).FirstErr(); err != nil {
			slog.Error("outbox: relay: produce не удался, строка останется в очереди",
				"id", row.id, "topic", row.topic, "error", err)
			continue
		}
		published = append(published, row.id)
	}
	return published
}

// headersToRecordHeaders разбирает JSON-объект строка→строка, записанный
// в Write (outbox.go), обратно в заголовки kgo.Record.
func headersToRecordHeaders(raw []byte) ([]kgo.RecordHeader, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("unmarshal headers: %w", err)
	}
	headers := make([]kgo.RecordHeader, 0, len(m))
	for k, v := range m {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return headers, nil
}

func (r *Relay) reportPending(ctx context.Context, s shard) error {
	var count int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+TableFor(r.cfg.ServiceName)+` WHERE published_at IS NULL`).Scan(&count); err != nil {
		return fmt.Errorf("count: %w", err)
	}
	r.pending.Record(ctx, count, metric.WithAttributes(
		attribute.String("service", r.cfg.ServiceName),
		attribute.Int("shard", s.index),
	))
	return nil
}

// hasHeader — есть ли уже такой заголовок. Нужен, чтобы relay не перетирал
// атрибуцию, проставленную при записи в outbox.
func hasHeader(headers []kgo.RecordHeader, key string) bool {
	for _, h := range headers {
		if h.Key == key {
			return true
		}
	}
	return false
}

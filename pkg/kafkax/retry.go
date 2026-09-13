package kafkax

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// MaxRetryRounds — сколько раз RetryConsumer готов продлить retry_at и
// вернуть сообщение обратно в <topic>.retry, прежде чем сдаться и уйти
// в DLQ. Раунд — это НЕ то же самое, что MaxRetryAttempts: attempts — это
// попытки «на месте» внутри одной горутины партиции основного консьюмера;
// rounds — это отдельные визиты в retry-топик, каждый со своим окном
// ожидания (обоснование схемы — docs/adr/0010-*).
const MaxRetryRounds = 3

// headerValue — общий разбор заголовка kgo.Record по имени.
func headerValue(headers []kgo.RecordHeader, key string) (string, bool) {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// setHeader заменяет значение заголовка, если он уже есть, и добавляет новый
// иначе. Без этой функции повторная отправка сообщения (retry → retry,
// retry → dlq) дублировала бы заголовки: у записи оказалось бы два
// "retry_count" с разными значениями, и непонятно, какой из них правда.
func setHeader(headers []kgo.RecordHeader, key, value string) []kgo.RecordHeader {
	for i, h := range headers {
		if h.Key == key {
			headers[i].Value = []byte(value)
			return headers
		}
	}
	return append(headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

// parseRetryAt читает заголовок retry_at (unix-миллисекунды).
//
// Отсутствие или битое значение трактуется как «уже пора»: сообщение без
// валидного retry_at безопаснее обработать немедленно, чем ждать неизвестно
// сколько (или, того хуже, забыть про него — реализация ждала бы time.Time{},
// то есть 1 января года 1, что и так «в прошлом», но лучше явно, чем случайно).
func parseRetryAt(record *kgo.Record) time.Time {
	v, ok := headerValue(record.Headers, HeaderRetryAt)
	if !ok {
		return time.Time{}
	}
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

// retryRound читает заголовок retry_count. Ноль — сообщение ещё не проходило
// через RetryConsumer ни разу (пришло из основного консьюмера впервые).
func retryRound(record *kgo.Record) int {
	v, ok := headerValue(record.Headers, HeaderRetryCount)
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return n
}

// sendToRetry публикует запись в <topic>.retry — вызывается ОСНОВНЫМ
// консьюмером, когда MaxRetryAttempts попыток «на месте» исчерпаны.
//
// retry_count выставляется в "1": это первый визит в retry-топик.
// original_* фиксируют топик-партицию-offset ИСХОДНОГО сообщения — они же
// попадут в DLQ, если RetryConsumer тоже не справится, и именно по ним
// потом можно понять, что за сообщение и откуда, глядя только на DLQ.
func (c *Consumer) sendToRetry(ctx context.Context, record *kgo.Record, delay time.Duration) error {
	headers := append([]kgo.RecordHeader{}, record.Headers...)
	headers = setHeader(headers, HeaderRetryAt, strconv.FormatInt(time.Now().Add(delay).UnixMilli(), 10))
	headers = setHeader(headers, HeaderRetryCount, "1")
	headers = setHeader(headers, HeaderOriginalTopic, record.Topic)
	headers = setHeader(headers, HeaderOriginalPartition, strconv.Itoa(int(record.Partition)))
	headers = setHeader(headers, HeaderOriginalOffset, strconv.FormatInt(record.Offset, 10))

	rec := &kgo.Record{
		Topic:   RetryTopic(c.topic),
		Key:     record.Key,
		Value:   record.Value,
		Headers: headers,
	}
	if err := c.sideProducer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("retry produce в %s: %w", rec.Topic, err)
	}
	return nil
}

// sendToDLQ публикует запись в <topic>.dlq. Вызывается основным консьюмером
// напрямую (permanent-ошибка или неразбираемый конверт).
func (c *Consumer) sendToDLQ(ctx context.Context, record *kgo.Record, cause error) error {
	rec := &kgo.Record{
		Topic:   DLQTopic(c.topic),
		Key:     record.Key,
		Value:   record.Value,
		Headers: dlqHeaders(record.Headers, cause, record.Topic, record.Partition, record.Offset),
	}
	if err := c.sideProducer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("dlq produce в %s: %w", rec.Topic, err)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// RETRY-КОНСЬЮМЕР
// ─────────────────────────────────────────────────────────────────────────────

// RetryConsumer — отдельный запускаемый процесс, читающий <topic>.retry.
//
// PATTERN: retry queue с отложенной доставкой. Kafka не умеет «доставить
// сообщение через 30 секунд» — партиция строго FIFO, и задержанное
// сообщение неизбежно оказывается ПЕРЕД более новыми в той же партиции.
// Решение — отдельный топик и отдельный процесс, который явно ждёт нужный
// момент (retry_at), прежде чем вернуть событие в тот же Handler, что
// и основной консьюмер.
//
// Почему это ОТДЕЛЬНЫЙ процесс, а не часть Consumer.Run: время ожидания
// retry_at может быть заметным (секунды-минуты, см. docs/adr/0010-*), и всё
// это время консьюмер занят ИМЕННО ожиданием, а не приёмом новых сообщений.
// Смешивать это с обработкой основного потока значит рисковать, что окно
// ожидания одного ретрая начнёт задерживать обработку свежих событий.
type RetryConsumer struct {
	client       *kgo.Client
	sideProducer *kgo.Client
	sourceTopic  string // "media.photo.uploaded" — топик, из которого этот retry произошёл
	retryTopic   string // "media.photo.uploaded.retry" — то, что реально читаем
	group        string

	engine atomic.Pointer[partitionEngine]

	meter     metric.Meter
	processed metric.Int64Counter
	duration  metric.Float64Histogram
}

// NewRetryConsumer подключается к <sourceTopic>.retry в составе своей
// группы (по умолчанию — group + "-retry", но группа передаётся явно,
// потому что у media и thumbnail-worker разные соглашения об именах групп).
func NewRetryConsumer(brokers []string, group, sourceTopic string) (*RetryConsumer, error) {
	rc := &RetryConsumer{
		sourceTopic: sourceTopic,
		retryTopic:  RetryTopic(sourceTopic),
		group:       group,
	}

	client, err := kgo.NewClient(append(
		[]kgo.Opt{
			kgo.SeedBrokers(brokers...),
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(rc.retryTopic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.DisableAutoCommit(),
			// Тот же приём goroutine-per-partition, что и у основного
			// консьюмера (kafka.go) — подробное обоснование там.
			kgo.BlockRebalanceOnPoll(),
			kgo.OnPartitionsAssigned(func(ctx context.Context, cl *kgo.Client, assigned map[string][]int32) {
				slog.Info("kafka retry: партиции назначены", "topic", rc.retryTopic, "assigned", assigned)
				if e := rc.engine.Load(); e != nil {
					e.assigned(ctx, cl, assigned)
				}
			}),
			kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
				slog.Info("kafka retry: партиции отозваны", "topic", rc.retryTopic, "revoked", revoked)
				if e := rc.engine.Load(); e != nil {
					e.revoked(ctx, cl, revoked)
				}
			}),
			kgo.OnPartitionsLost(func(ctx context.Context, cl *kgo.Client, lost map[string][]int32) {
				slog.Warn("kafka retry: партиции потеряны", "topic", rc.retryTopic, "lost", lost)
				if e := rc.engine.Load(); e != nil {
					e.lost(ctx, cl, lost)
				}
			}),
		},
		producerOpts(brokers)..., // тот же клиент читает и пишет: сообщения короткие, отдельный TCP-пул не оправдан
	)...)
	if err != nil {
		return nil, fmt.Errorf("kafka retry-consumer: %w", err)
	}
	rc.client = client

	sideProducer, err := kgo.NewClient(producerOpts(brokers)...)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka retry-consumer: side-producer: %w", err)
	}
	rc.sideProducer = sideProducer

	rc.meter = otel.Meter("gosplash/kafkax")
	rc.processed, err = rc.meter.Int64Counter(
		"kafka_messages_processed_total",
		metric.WithDescription("Обработано сообщений из Kafka"),
	)
	if err != nil {
		return nil, fmt.Errorf("метрика kafka_messages_processed_total: %w", err)
	}
	rc.duration, err = rc.meter.Float64Histogram(
		"kafka_message_processing_duration_seconds",
		metric.WithDescription("Время обработки одного сообщения"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("метрика kafka_message_processing_duration_seconds: %w", err)
	}

	return rc, nil
}

func (rc *RetryConsumer) Close() {
	rc.client.Close()
	rc.sideProducer.Close()
}

// Ping — проверка живости для /readyz.
func (rc *RetryConsumer) Ping(ctx context.Context) error { return rc.client.Ping(ctx) }

// Run — как Consumer.Run, но перед вызовом Handler ждёт retry_at.
func (rc *RetryConsumer) Run(ctx context.Context, handle Handler) error {
	slog.Info("kafka retry ← слушаю", "topic", rc.retryTopic)

	// Лаг retry-топика — не менее важный сигнал, чем лаг основного: растущий
	// лаг здесь означает, что сообщения не успевают дождаться retry_at
	// и продолжают копиться (docs/adr/0010-*, раздел «чем платим»).
	stopLag := startLagMetrics(ctx, rc.client, rc.group, rc.meter)
	defer stopLag()

	engine := newPartitionEngine(ctx, func(recordCtx context.Context, record *kgo.Record) {
		rc.processOne(recordCtx, record, handle)
	})
	rc.engine.Store(engine)
	defer func() {
		rc.engine.Store(nil)
		engine.stopAll()
	}()

	for {
		fetches := rc.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			// Та же причина, что и в Consumer.Run (kafka.go): PollFetches
			// с BlockRebalanceOnPoll ставит "poller" перед КАЖДЫМ возвратом,
			// в том числе на фиктивный fetch при отменённом ctx. Без явного
			// AllowRebalance() здесь счётчик не обнулялся, и RetryConsumer.
			// Close() (→ kgo Client.Close() → LeaveGroupContext) зависал
			// навсегда в ожидании этого обнуления — под не покидал группу
			// и держал партиции retry-топика до SIGKILL по grace period.
			rc.client.AllowRebalance()
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			rc.client.AllowRebalance()
			return fmt.Errorf("retry fetch: %w", errs[0].Err)
		}

		engine.dispatch(fetches)
		rc.client.AllowRebalance()
	}
}

// processOne — тело обработки одной retry-записи: подождать retry_at,
// вызвать Handler, по результату либо закоммитить, либо продлить раунд,
// либо уйти в DLQ.
func (rc *RetryConsumer) processOne(ctx context.Context, record *kgo.Record, handle Handler) {
	if wait := time.Until(parseRetryAt(record)); wait > 0 {
		if !rc.waitForRetryAt(ctx, record, wait) {
			// Ждать перестали, потому что сервис останавливается: НЕ коммитим
			// — offset не сдвинут, и при следующем запуске это же сообщение
			// подождёт ещё раз (retry_at не пересчитывается, так что ждать
			// придётся меньше или нисколько — это не баг, а следствие того,
			// что retry_at абсолютное время, а не относительная задержка).
			return
		}
	}

	var env eventsv1.Envelope
	if err := proto.Unmarshal(record.Value, &env); err != nil {
		rc.finishWithDLQ(ctx, record, fmt.Errorf("разбор конверта в retry: %w", err))
		return
	}

	started := time.Now()
	err := handle(ctx, &env)
	elapsed := time.Since(started)

	if err == nil {
		rc.commit(ctx, record)
		rc.observe(ctx, "ok", elapsed)
		return
	}
	if isPermanent(err) {
		rc.finishWithDLQ(ctx, record, err)
		return
	}

	round := retryRound(record)
	if round >= MaxRetryRounds {
		rc.finishWithDLQ(ctx, record, fmt.Errorf("исчерпаны раунды retry (%d): %w", round, err))
		return
	}
	rc.reescalate(ctx, record, round+1, err)
}

// waitForRetryAt ждёт наступления retry_at, ставя ИМЕННО ЭТУ партицию
// на паузу — а не блокируя партицию простым time.Sleep без паузы.
//
// Разница принципиальна, когда wait измеряется минутами (см. docs/adr/0010-*
// про выбор между пассивным ожиданием и pause/resume): без паузы клиент
// продолжает фетчить новые батчи ЭТОЙ партиции, они копятся в буферизованном
// канале горутины-обработчика, и как только буфер заполняется, ГЛАВНЫЙ цикл
// Run — тот, что вызывает dispatch() для ВСЕХ партиций разом — блокируется
// на попытке отправить в переполненный канал. Один долгий ретрай в одной
// партиции останавливает вообще весь консьюмер. PauseFetchPartitions убирает
// эту партицию из опроса брокера на время ожидания: канал не растёт, а
// PollFetches по остальным партициям идёт как ни в чём не бывало.
func (rc *RetryConsumer) waitForRetryAt(ctx context.Context, record *kgo.Record, wait time.Duration) bool {
	paused := rc.client.PauseFetchPartitions(map[string][]int32{record.Topic: {record.Partition}})
	defer rc.client.ResumeFetchPartitions(paused)

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// reescalate продлевает retry: увеличивает retry_count, ставит новый
// retry_at (с тем же backoff+jitter, что и в основном консьюмере, только
// «продолжая» последовательность попыток с MaxRetryAttempts) и публикует
// запись обратно в тот же retry-топик.
func (rc *RetryConsumer) reescalate(ctx context.Context, record *kgo.Record, round int, cause error) {
	dctx, cancel := detachedTimeout(ctx, 10*time.Second)
	defer cancel()

	delay := Backoff(MaxRetryAttempts + round)
	headers := append([]kgo.RecordHeader{}, record.Headers...)
	headers = setHeader(headers, HeaderRetryCount, strconv.Itoa(round))
	headers = setHeader(headers, HeaderRetryAt, strconv.FormatInt(time.Now().Add(delay).UnixMilli(), 10))

	next := &kgo.Record{Topic: rc.retryTopic, Key: record.Key, Value: record.Value, Headers: headers}
	if err := rc.sideProducer.ProduceSync(dctx, next).FirstErr(); err != nil {
		slog.ErrorContext(ctx, "kafka retry ✗ не смог продлить раунд, offset не двигаю",
			"topic", rc.retryTopic, "error", err)
		rc.observe(ctx, "retry_failed", 0)
		return
	}
	rc.commit(ctx, record)
	rc.observe(ctx, "retry", 0)
	slog.WarnContext(ctx, "kafka retry: раунд исчерпан, продлеваю",
		"topic", rc.retryTopic, "round", round, "delay", delay, "error", cause)
}

func (rc *RetryConsumer) finishWithDLQ(ctx context.Context, record *kgo.Record, cause error) {
	dctx, cancel := detachedTimeout(ctx, 10*time.Second)
	defer cancel()

	rec := &kgo.Record{
		Topic:   DLQTopic(rc.sourceTopic),
		Key:     record.Key,
		Value:   record.Value,
		Headers: dlqHeaders(record.Headers, cause, originalTopic(record), originalPartition(record), originalOffset(record)),
	}
	if err := rc.sideProducer.ProduceSync(dctx, rec).FirstErr(); err != nil {
		slog.ErrorContext(ctx, "kafka retry ✗ не смог отправить в DLQ, offset не двигаю",
			"topic", rc.retryTopic, "error", err)
		rc.observe(ctx, "dlq_failed", 0)
		return
	}
	rc.commit(ctx, record)
	rc.observe(ctx, "dlq", 0)
	slog.ErrorContext(ctx, "kafka retry ✗ исчерпаны попытки, сообщение в DLQ",
		"topic", rc.retryTopic, "original_topic", originalTopic(record), "error", cause)
}

func (rc *RetryConsumer) commit(ctx context.Context, record *kgo.Record) {
	cctx, cancel := detachedTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rc.client.CommitRecords(cctx, record); err != nil {
		slog.Error("kafka retry ✗ commit", "error", err)
	}
}

func (rc *RetryConsumer) observe(ctx context.Context, result string, elapsed time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("topic", rc.retryTopic),
		attribute.String("result", result),
	)
	rc.processed.Add(ctx, 1, attrs)
	if elapsed > 0 {
		rc.duration.Record(ctx, elapsed.Seconds(), attrs)
	}
}

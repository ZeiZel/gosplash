// Package kafkax — тонкая обёртка над franz-go: продюсер и консьюмер,
// которые умеют ровно то, что нужно проекту.
//
// Сообщения сериализуются в protobuf теми же типами, что и gRPC
// (proto/gosplash/events/v1). Один язык контрактов и для синхронного,
// и для асинхронного общения.
//
// Консьюмер даёт at-least-once с явной классификацией ошибок (errors.go):
// retryable-ошибка получает несколько попыток на месте и окно в отдельном
// retry-топике (retry.go), permanent — сразу уходит в DLQ (dlq.go). Каждая
// партиция обрабатывается своей горутиной (engine.go) — так одно медленное
// или битое сообщение не останавливает весь топик.
package kafkax

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// Заголовки Kafka. Дублируют часть полей конверта намеренно: заголовок можно
// прочитать, не разбирая тело сообщения, — а для фильтрации в Kafka UI,
// маршрутизации в DLQ и трассировки этого достаточно.
const (
	HeaderEventID   = "event_id"
	HeaderEventType = "event_type"
	HeaderProducer  = "producer"
	// HeaderTraceparent — W3C trace context. Заполняется пропагатором OTel
	// (см. trace.go): без него трейс рвётся на границе топика, потому что
	// HTTP-заголовки до консьюмера, разумеется, не доезжают.
	HeaderTraceparent = "traceparent"

	// Заголовки схемы ретраев и DLQ (docs/adr/0010-*).
	HeaderError             = "error"              // текст ошибки, из-за которой сообщение в DLQ
	HeaderOriginalTopic     = "original_topic"     // топик, из которого сообщение УШЛО в retry/dlq
	HeaderOriginalPartition = "original_partition" // партиция в original_topic
	HeaderOriginalOffset    = "original_offset"    // offset в original_topic
	HeaderRetryAt           = "retry_at"           // unix ms — когда RetryConsumer может повторить попытку
	HeaderRetryCount        = "retry_count"        // сколько раз сообщение уже возвращалось в retry-топик
)

// ─────────────────────────────────────────────────────────────────────────────
// ПРОДЮСЕР
// ─────────────────────────────────────────────────────────────────────────────

// producerOpts — гарантии доставки продюсера. Общие для основного Producer
// и служебных клиентов, которыми Consumer/RetryConsumer шлют в retry/DLQ:
// DLQ, в который сообщения теряются на общих основаниях, бесполезен —
// то, ради чего его завели.
func producerOpts(brokers []string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		// Ждать подтверждения от всех реплик, прежде чем считать запись
		// успешной. У нас брокер один, так что разницы не видно, но с этой
		// настройкой при падении лидера сообщение не потеряется.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Snappy — быстрое сжатие с низкой ценой CPU, а не максимальная
		// плотность (той нужен был бы zstd ценой заметно большей нагрузки
		// на продюсер). Envelope с protobuf и так компактен; сжатие здесь
		// в основном экономит место на диске брокера и трафик до него.
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		// Через сколько сдаться и вернуть ошибку вызывающему, если брокер
		// не подтверждает запись (недоступен, лидер переизбирается и т.п.).
		// 10с — дольше, чем любой отдельный ретрай внутри клиента, но
		// достаточно коротко, чтобы вызывающий (HTTP-хендлер, outbox-relay)
		// не завис на нём надолго.
		kgo.RecordDeliveryTimeout(10 * time.Second),
		// Сколько раз клиент повторит отправку САМ, прежде чем сдаться
		// и вернуть ошибку — до RecordDeliveryTimeout. Идемпотентный
		// продюсер (включён по умолчанию) гарантирует, что брокер отбросит
		// дубликат при повторной отправке после сетевого сбоя, поэтому
		// повторять можно смело.
		kgo.RecordRetries(5),
	}
}

type Producer struct {
	client *kgo.Client
	// name — имя сервиса, попадает в заголовок producer. Когда в топике
	// окажется неожиданное сообщение, первый вопрос будет «кто это прислал».
	name string
}

func NewProducer(brokers []string, serviceName string) (*Producer, error) {
	client, err := kgo.NewClient(producerOpts(brokers)...)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Producer{client: client, name: serviceName}, nil
}

// Publish отправляет событие и ЖДЁТ подтверждения брокера.
//
// Синхронно — намеренно: если Kafka недоступна, вызывающий должен узнать об
// этом сразу, а не обнаружить через час, что событий нет. Асинхронная отправка
// (client.Produce с колбэком) быстрее, но её нельзя использовать, не имея
// outbox: процесс умрёт вместе с неотправленным буфером.
//
// Ключ сообщения берётся из конверта (aggregate_id) и не передаётся отдельным
// аргументом — чтобы нельзя было отправить событие фото с ключом заказа.
// Все сообщения с одним ключом попадают в одну партицию, а значит,
// обрабатываются строго по порядку; события разных агрегатов обрабатываются
// параллельно.
func (p *Producer) Publish(ctx context.Context, topic string, env *eventsv1.Envelope) error {
	payload, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	ctx, span := startProduceSpan(ctx, topic, env.GetEventType())
	defer span.End()

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(env.GetAggregateId()),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: HeaderEventID, Value: []byte(env.GetEventId())},
			{Key: HeaderEventType, Value: []byte(env.GetEventType())},
			{Key: HeaderProducer, Value: []byte(p.name)},
		},
	}
	// traceparent кладётся ПОСЛЕ создания спана и ДО отправки: в заголовок
	// должен попасть идентификатор именно этого спана, иначе консьюмер
	// продолжит трейс не с того места.
	injectTrace(ctx, record)

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "produce failed")
		return fmt.Errorf("produce в %s: %w", topic, err)
	}
	slog.Info("kafka →",
		"topic", topic,
		"partition", record.Partition,
		"offset", record.Offset,
		"key", env.GetAggregateId(),
		"event_type", env.GetEventType(),
	)
	return nil
}

func (p *Producer) Close() { p.client.Close() }

// Ping — проверка живости брокера для /readyz.
//
// Запрашивает метаданные кластера. Это единственный честный способ проверить
// Kafka: TCP-соединение может быть установлено с брокером, который сам не
// в кластере и ничего обслужить не сможет.
func (p *Producer) Ping(ctx context.Context) error { return p.client.Ping(ctx) }

// ─────────────────────────────────────────────────────────────────────────────
// КОНСЬЮМЕР
// ─────────────────────────────────────────────────────────────────────────────

type Consumer struct {
	client *kgo.Client
	topic  string
	group  string

	// sideProducer — отдельный клиент-продюсер для retry/DLQ. Отдельный,
	// а не переиспользуем client (который здесь работает на ЧТЕНИЕ):
	// kgo.Client, сконфигурированный ConsumerGroup, не настроен как продюсер
	// (нет RequiredAcks/compression/retries из producerOpts), а заводить
	// эти опции на consumer-клиенте бессмысленно — они ни на что не влияют.
	sideProducer *kgo.Client

	// engine хранится за atomic.Pointer, потому что создаётся в Run(...),
	// уже ПОСЛЕ того, как колбэки OnPartitionsAssigned/Revoked/Lost переданы
	// в kgo.NewClient (см. комментарий в NewConsumer). До первого Run они
	// читают nil и ничего не делают — а первый вызов PollFetches, который
	// и запускает у kgo протокол вступления в группу, происходит только
	// внутри Run, уже после того, как engine записан.
	engine atomic.Pointer[partitionEngine]

	meter     metric.Meter
	processed metric.Int64Counter
	duration  metric.Float64Histogram
}

// NewConsumer подключается к топику в составе consumer group.
//
// Группа — это механизм разделения работы. Партиции топика делятся между
// участниками группы: 6 партиций и 2 процесса → по 3 партиции каждому.
// Запусти третий — Kafka сама всё перераспределит (ребалансировка).
// Больше процессов, чем партиций, смысла не имеет: лишние будут простаивать.
func NewConsumer(brokers []string, group, topic string) (*Consumer, error) {
	c := &Consumer{topic: topic, group: group}

	// Тем же клиентом читаем — но не пишем: сообщения в retry/DLQ уходят
	// через sideProducer ниже, поэтому producerOpts здесь не участвуют.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		// Новая группа начинает читать топик с САМОГО НАЧАЛА, а не с конца.
		// Иначе первый запуск консьюмера пропустил бы всё, что уже лежит
		// в топике, и отладка превратилась бы в загадку.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Автокоммит выключен: offset двигаем сами, ПОСЛЕ успешной обработки.
		// С автокоммитом Kafka отметит сообщение прочитанным по таймеру,
		// и падение обработчика означало бы потерю сообщения.
		kgo.DisableAutoCommit(),
		// Придерживает вызов OnPartitionsRevoked/Lost до явного
		// client.AllowRebalance() в конце итерации Run — подробности
		// и почему это необходимо для горутины-на-партицию — в engine.go.
		kgo.BlockRebalanceOnPoll(),
		// Логи ребалансировки — чтобы было видно глазами, как партиции
		// переезжают между инстансами, когда поднимаешь второй процесс.
		// Само управление горутинами партиций делегировано partitionEngine.
		kgo.OnPartitionsAssigned(func(ctx context.Context, cl *kgo.Client, assigned map[string][]int32) {
			slog.Info("kafka: партиции назначены", "assigned", assigned)
			if e := c.engine.Load(); e != nil {
				e.assigned(ctx, cl, assigned)
			}
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
			slog.Info("kafka: партиции отозваны", "revoked", revoked)
			if e := c.engine.Load(); e != nil {
				e.revoked(ctx, cl, revoked)
			}
		}),
		kgo.OnPartitionsLost(func(ctx context.Context, cl *kgo.Client, lost map[string][]int32) {
			slog.Warn("kafka: партиции потеряны", "lost", lost)
			if e := c.engine.Load(); e != nil {
				e.lost(ctx, cl, lost)
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	c.client = client

	sideProducer, err := kgo.NewClient(producerOpts(brokers)...)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka consumer: side-producer: %w", err)
	}
	c.sideProducer = sideProducer

	c.meter = otel.Meter("gosplash/kafkax")
	c.processed, err = c.meter.Int64Counter(
		"kafka_messages_processed_total",
		metric.WithDescription("Обработано сообщений из Kafka"),
	)
	if err != nil {
		return nil, fmt.Errorf("метрика kafka_messages_processed_total: %w", err)
	}
	c.duration, err = c.meter.Float64Histogram(
		"kafka_message_processing_duration_seconds",
		metric.WithDescription("Время обработки одного сообщения"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("метрика kafka_message_processing_duration_seconds: %w", err)
	}

	return c, nil
}

// Handler обрабатывает одно событие.
//
// Возвращаемая ошибка ОБЯЗАНА быть классифицирована через Retryable(err)
// или Permanent(err) (errors.go). Неклассифицированная ошибка считается
// retryable — это безопасный дефолт, а не молчаливое решение «как получится».
type Handler func(ctx context.Context, env *eventsv1.Envelope) error

// Run читает топик, пока не отменят ctx.
//
// Схема доставки получается at-least-once: сообщение обработается минимум
// один раз, но может и дважды — например, если процесс умер между успешной
// обработкой и коммитом offset'а. Поэтому обработчик ОБЯЗАН быть идемпотентным.
//
// Exactly-once в распределённой системе в общем случае недостижим; на практике
// его всегда заменяют на «at-least-once + идемпотентность».
func (c *Consumer) Run(ctx context.Context, handle Handler) error {
	slog.Info("kafka ← слушаю", "topic", c.topic)

	stopLag := startLagMetrics(ctx, c.client, c.group, c.meter)
	defer stopLag()

	engine := newPartitionEngine(ctx, func(recordCtx context.Context, record *kgo.Record) {
		c.processOne(recordCtx, record, handle)
	})
	c.engine.Store(engine)
	defer func() {
		c.engine.Store(nil)
		engine.stopAll()
	}()

	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			// НАСТОЯЩАЯ ПРИЧИНА зависания пода в Terminating на все
			// terminationGracePeriodSeconds: PollFetches с BlockRebalanceOnPoll
			// ставит "poller" (блокирует ребаланс) ПЕРЕД КАЖДЫМ возвратом —
			// включая фиктивный fetch с ctx.Err(), которым PollFetches отвечает
			// на отменённый контекст, даже не обращаясь к брокеру (см.
			// twmb/franz-go/pkg/kgo/consumer.go: "we still need to add
			// a poller ... since we are returning a fetch"). Раньше мы
			// возвращались здесь, ни разу не позвав AllowRebalance() —
			// счётчик poller'ов оставался НАВСЕГДА больше нуля.
			//
			// Consumer.Close() зовёт kgo Client.Close(), а тот ПЕРЕД тем,
			// как покинуть группу, ждёт как раз обнуления этого счётчика
			// (godoc Client.Close: "will hang if you polled, did not allow
			// rebalances, and want to close"). Ждать было нечего — вызвать
			// AllowRebalance() было уже некому, PollFetches этот цикл больше
			// не выполнял. В k8s это выглядело как под, который не покидает
			// consumer group и продолжает держать партиции: Kubernetes
			// добивал его SIGKILL'ом по истечении grace period, и только
			// тогда партиции наконец освобождались.
			//
			// Раз PollFetches уже поставил poller, снимаем его немедленно —
			// раздавать по горутинам партиций здесь нечего (fetches пуст),
			// поэтому, в отличие от успешного пути ниже, ждать нечего и
			// перед возвратом это безопасно.
			c.client.AllowRebalance()
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			c.client.AllowRebalance()
			return fmt.Errorf("fetch: %w", errs[0].Err)
		}

		engine.dispatch(fetches)
		// Без этого вызова BlockRebalanceOnPoll держит ребаланс замороженным
		// до следующего Poll: он специально ждёт явного разрешения, чтобы
		// мы успели раздать записи по горутинам партиций ДО того, как
		// какую-то из них могут отозвать.
		c.client.AllowRebalance()
	}
}

func (c *Consumer) Close() {
	c.client.Close()
	c.sideProducer.Close()
}

// Ping — проверка живости брокера для /readyz.
func (c *Consumer) Ping(ctx context.Context) error { return c.client.Ping(ctx) }

// processOne обрабатывает одну запись целиком: разбор конверта, попытки
// «на месте» с backoff+jitter, и по исходу — коммит, retry-топик или DLQ.
//
// Вызывается из горутины конкретной партиции (engine.go), поэтому может
// себе позволить синхронный sleep между попытками: это блокирует только
// ЭТУ партицию, а не весь консьюмер.
func (c *Consumer) processOne(ctx context.Context, record *kgo.Record, handle Handler) {
	var env eventsv1.Envelope
	if err := proto.Unmarshal(record.Value, &env); err != nil {
		// Битый конверт — permanent по определению: сколько ни повторяй,
		// байты не станут валидным protobuf.
		c.finishWithDLQ(ctx, record, fmt.Errorf("разбор конверта: %w", err))
		return
	}

	// Спан продолжает трейс продюсера — контекст приехал в заголовке
	// traceparent. Именно здесь «склеиваются» два сервиса, между которыми
	// нет ни одного синхронного вызова.
	spanCtx, span := startConsumeSpan(ctx, record, env.GetEventType(), env.GetEventId())
	started := time.Now()

	var lastErr error
	for attempt := 1; attempt <= MaxRetryAttempts; attempt++ {
		lastErr = handle(spanCtx, &env)
		if lastErr == nil || isPermanent(lastErr) {
			break
		}
		if attempt < MaxRetryAttempts {
			delay := Backoff(attempt)
			slog.WarnContext(spanCtx, "kafka: временная ошибка, повтор на месте",
				"topic", record.Topic, "partition", record.Partition, "offset", record.Offset,
				"attempt", attempt, "delay", delay, "error", lastErr)
			time.Sleep(delay)
		}
	}
	elapsed := time.Since(started)

	if lastErr == nil {
		span.End()
		c.commit(ctx, record)
		c.observe(ctx, record.Topic, "ok", elapsed)
		return
	}

	span.RecordError(lastErr)
	span.SetStatus(otelcodes.Error, "handler failed")
	span.End()

	if isPermanent(lastErr) {
		c.finishWithDLQ(ctx, record, lastErr)
		return
	}
	c.finishWithRetry(ctx, record, lastErr)
}

// finishWithDLQ публикует запись в DLQ и коммитит исходное сообщение.
//
// Если публикация в DLQ сама не удалась (Kafka недоступна), offset НЕ
// коммитим: сообщение придёт на следующем Poll снова, и мы попробуем ещё
// раз. Это единственный способ ничего не потерять, если DLQ временно
// недоступен вместе с остальной Kafka — коммит без успешной записи в DLQ
// был бы тихой потерей события.
func (c *Consumer) finishWithDLQ(ctx context.Context, record *kgo.Record, cause error) {
	dctx, cancel := detachedTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := c.sendToDLQ(dctx, record, cause); err != nil {
		slog.ErrorContext(ctx, "kafka ✗ не смог отправить в DLQ, offset не двигаю",
			"topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
		c.observe(ctx, record.Topic, "dlq_failed", 0)
		return
	}
	c.commit(ctx, record)
	c.observe(ctx, record.Topic, "dlq", 0)
	slog.ErrorContext(ctx, "kafka ✗ постоянная ошибка, сообщение в DLQ",
		"topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", cause)
}

// finishWithRetry публикует запись в <topic>.retry и коммитит исходное
// сообщение — той же логикой отказа, что и finishWithDLQ выше.
func (c *Consumer) finishWithRetry(ctx context.Context, record *kgo.Record, cause error) {
	dctx, cancel := detachedTimeout(ctx, 10*time.Second)
	defer cancel()

	// Задержка для ПЕРВОГО визита в retry-топик продолжает ту же
	// экспоненциальную последовательность, что и попытки на месте:
	// Backoff(MaxRetryAttempts) — это следующий шаг после исчерпанных
	// MaxRetryAttempts попыток, а не сброс на начало.
	delay := Backoff(MaxRetryAttempts)
	if err := c.sendToRetry(dctx, record, delay); err != nil {
		slog.ErrorContext(ctx, "kafka ✗ не смог отправить в retry, offset не двигаю",
			"topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
		c.observe(ctx, record.Topic, "retry_failed", 0)
		return
	}
	c.commit(ctx, record)
	c.observe(ctx, record.Topic, "retry", 0)
	slog.WarnContext(ctx, "kafka: попытки на месте исчерпаны, отправляю в retry",
		"topic", record.Topic, "partition", record.Partition, "offset", record.Offset,
		"delay", delay, "error", cause)
}

func (c *Consumer) commit(ctx context.Context, record *kgo.Record) {
	cctx, cancel := detachedTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.client.CommitRecords(cctx, record); err != nil {
		slog.Error("kafka ✗ commit", "error", err)
	}
}

// observe пишет метрики обработки. Отдельный метод, потому что вызывается
// из нескольких мест и все разы легко забыть.
func (c *Consumer) observe(ctx context.Context, topic, result string, elapsed time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.String("result", result),
	)
	c.processed.Add(ctx, 1, attrs)
	if elapsed > 0 {
		c.duration.Record(ctx, elapsed.Seconds(), attrs)
	}
}

// detachedTimeout — контекст с собственным таймаутом, ОТВЯЗАННЫЙ от отмены
// родителя (context.WithoutCancel), но не от его значений (trace_id и т.п.).
//
// Используется для commit/produce в момент, когда родительский ctx мог уже
// начать отменяться (Run завершается по сигналу остановки сервиса): без
// отвязки эти операции немедленно вернули бы context.Canceled и мы не
// смогли бы дописать уже принятое решение (DLQ/retry/commit) для сообщения,
// которое к этому моменту уже обработано, — то есть был бы риск повторной
// обработки при следующем запуске там, где повтора можно было избежать.
func detachedTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), d)
}

// tools/kafka-reprocess — переливка сообщений из <topic>.dlq обратно
// в <topic>, после того как причину попадания в DLQ нашли и починили.
//
//	go run ./cmd/kafka-reprocess -topic media.photo.uploaded
//	go run ./cmd/kafka-reprocess -topic media.photo.uploaded -limit 10 -dry-run
//
// PATTERN: разбор DLQ ручным инструментом, а не автоматическим повтором
// внутри сервиса. DLQ (pkg/kafkax, docs/adr/0010-*) специально устроен так,
// что его никто не читает сам: сообщение оказывается там либо из-за
// постоянной ошибки (повторять бессмысленно, пока не поправлен код или
// данные), либо после исчерпанных ретраев (повторять уже пробовали).
// Автоматический реплей DLQ обратно в топик без вмешательства человека
// превратил бы «сообщение обработать нельзя» в бесконечный цикл — то
// самое, ради избежания чего DLQ и завели. Поэтому реплей — отдельная
// команда, которую запускают осознанно, после того как причина устранена.
//
// Почему ОТДЕЛЬНЫЙ МОДУЛЬ (свой go.mod), а не пакет в корневом модуле:
// то же соображение, что и у tools/automigrate — инструмент, который
// администратор запускает руками с ноутбука или из CI, не должен тянуть
// за собой транзитивные зависимости всего монорепо (gRPC, S3, Redis…),
// когда ему на самом деле нужен только Kafka-клиент.
//
// Конверт (Envelope) и его event_id НЕ трогаются: Value записи копируется
// как есть, байт в байт, из DLQ-сообщения в исходный топик. Это осознанное
// решение, а не недосмотр — event_id внутри payload'а обязан остаться
// прежним, чтобы дедупликация на стороне consumer'а (processed_events)
// отсекла это событие, если оно каким-то образом уже было применено
// раньше по другому пути.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"gosplash/pkg/kafkax"
)

// reprocessGroup — ФИКСИРОВАННОЕ имя consumer group инструмента.
//
// Не эфемерная группа на каждый запуск: DLQ — это накопительная очередь,
// в которую между двумя запусками kafka-reprocess могут прийти НОВЫЕ
// сообщения. Стабильная группа коммитит offset после каждого успешно
// перелитого сообщения, и повторный запуск (например, завтра, после того
// как накопится ещё десяток) продолжает с места, на котором остановился
// предыдущий, вместо того чтобы заново переливать всё, что уже переливали.
const reprocessGroup = "kafka-reprocess"

// idleTimeout — после скольких секунд без новых сообщений инструмент
// считает, что DLQ на данный момент разобран, и завершается. DLQ никто
// не читает непрерывно (в этом и была его цель — см. комментарий пакета),
// поэтому пятисекундная пауза — надёжный сигнал «дальше пусто», а не
// признак временной задержки брокера.
const idleTimeout = 5 * time.Second

func main() {
	topic := flag.String("topic", "", "исходный топик, БЕЗ суффикса .dlq (обязателен)")
	limit := flag.Int("limit", 0, "сколько сообщений перелить за запуск (0 = все, что накопилось к моменту запуска)")
	dryRun := flag.Bool("dry-run", false, "только показать, что было бы перелито; ничего не отправлять и не коммитить")
	flag.Parse()

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "kafka-reprocess: -topic обязателен")
		flag.Usage()
		os.Exit(2)
	}

	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:59094"), ",")
	dlqTopic := kafkax.DLQTopic(*topic)

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(reprocessGroup),
		kgo.ConsumeTopics(dlqTopic),
		// С НАЧАЛА: свежая группа обязана увидеть весь текущий backlog DLQ,
		// а не только то, что придёт после запуска, — иначе первый запуск
		// молча пропустит всё, что уже накопилось.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Коммитим сами, ПОСЛЕ успешной публикации в исходный топик — та же
		// причина, что и у pkg/kafkax.Consumer: коммит раньше публикации
		// означал бы риск пометить сообщение "перелитым", хотя оно на самом
		// деле не ушло (например, брокер недоступен).
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("kafka-reprocess: consumer: %v", err)
	}
	defer consumer.Close()

	// В dry-run продюсер не нужен вовсе: -dry-run обязан не иметь ПОБОЧНЫХ
	// эффектов, включая открытие TCP-соединения на запись.
	var producer *kgo.Client
	if !*dryRun {
		producer, err = kgo.NewClient(
			kgo.SeedBrokers(brokers...),
			kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.ProducerBatchCompression(kgo.SnappyCompression()),
			kgo.RecordDeliveryTimeout(10*time.Second),
			kgo.RecordRetries(5),
		)
		if err != nil {
			log.Fatalf("kafka-reprocess: producer: %v", err)
		}
		defer producer.Close()
	}

	sent, err := run(context.Background(), consumer, producer, *topic, *limit, *dryRun)
	if err != nil {
		log.Fatalf("kafka-reprocess: %v", err)
	}

	suffix := ""
	if *dryRun {
		suffix = " (dry-run, ничего не отправлено и не закоммичено)"
	}
	log.Printf("kafka-reprocess: готово — %d сообщений из %s → %s%s", sent, dlqTopic, *topic, suffix)
}

// run — основной цикл: читает dlqTopic, пока не наберёт limit сообщений
// (limit == 0 — пока не исчерпает backlog) или не истечёт idleTimeout без
// новых записей, и для каждой публикует копию в исходный topic.
func run(ctx context.Context, consumer, producer *kgo.Client, topic string, limit int, dryRun bool) (int, error) {
	sent := 0
	for limit <= 0 || sent < limit {
		pollCtx, cancel := context.WithTimeout(ctx, idleTimeout)
		fetches := consumer.PollFetches(pollCtx)
		cancel()

		if len(fetches) == 0 {
			// PollFetches вернул пусто — это либо idle-таймаут (backlog
			// закончился), либо fetches.IsClientClosed(); в обоих случаях
			// продолжать нечего.
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return sent, fmt.Errorf("fetch: %w", errs[0].Err)
		}

		stop := false
		fetches.EachRecord(func(record *kgo.Record) {
			if stop || (limit > 0 && sent >= limit) {
				stop = true
				return
			}
			if err := reprocessOne(ctx, producer, topic, record, dryRun); err != nil {
				log.Fatalf("kafka-reprocess: produce в %s: %v", topic, err)
			}
			sent++
			if !dryRun {
				if err := consumer.CommitRecords(ctx, record); err != nil {
					log.Printf("kafka-reprocess: commit: %v", err)
				}
			}
		})
		if stop {
			break
		}
	}
	return sent, nil
}

// reprocessOne публикует одну запись из DLQ обратно в исходный топик.
func reprocessOne(ctx context.Context, producer *kgo.Client, topic string, record *kgo.Record, dryRun bool) error {
	eventID, _ := headerValue(record.Headers, kafkax.HeaderEventID)
	eventType, _ := headerValue(record.Headers, kafkax.HeaderEventType)
	dlqError, _ := headerValue(record.Headers, kafkax.HeaderError)

	if dryRun {
		slog.Info("kafka-reprocess: (dry-run) перелил бы",
			"topic", topic, "event_id", eventID, "event_type", eventType, "dlq_error", dlqError)
		return nil
	}

	out := &kgo.Record{
		Topic: topic,
		// Key и Value — БЕЗ ИЗМЕНЕНИЙ: Value это proto.Marshal(Envelope)
		// с исходным event_id внутри, а Key — исходный ключ партиционирования
		// (aggregate_id). Переигранное событие обязано попасть в ту же
		// партицию, где были бы остальные события того же агрегата.
		Key:     record.Key,
		Value:   record.Value,
		Headers: dropDLQHeaders(record.Headers),
	}
	if err := producer.ProduceSync(ctx, out).FirstErr(); err != nil {
		return err
	}
	slog.Info("kafka-reprocess: перелито", "topic", topic, "event_id", eventID, "event_type", eventType)
	return nil
}

// dropDLQHeaders убирает служебные заголовки схемы ретраев/DLQ
// (pkg/kafkax/kafka.go): в исходном топике им не место — там их не ждёт
// ни один consumer, а error/original_* только вводят в заблуждение, когда
// событие уже успешно уходит на новый круг.
func dropDLQHeaders(headers []kgo.RecordHeader) []kgo.RecordHeader {
	drop := map[string]bool{
		kafkax.HeaderError:             true,
		kafkax.HeaderOriginalTopic:     true,
		kafkax.HeaderOriginalPartition: true,
		kafkax.HeaderOriginalOffset:    true,
		kafkax.HeaderRetryAt:           true,
		kafkax.HeaderRetryCount:        true,
	}
	kept := make([]kgo.RecordHeader, 0, len(headers))
	for _, h := range headers {
		if !drop[h.Key] {
			kept = append(kept, h)
		}
	}
	return kept
}

func headerValue(headers []kgo.RecordHeader, key string) (string, bool) {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

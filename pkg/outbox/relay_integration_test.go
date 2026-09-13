//go:build integration

package outbox

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"

	"gosplash/pkg/kafkax"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// TestRelay_PadenieMezhduProduceIUpdateNeTeryayetSobytie — ключевой
// интеграционный тест фазы 1 (docs/PLAN.md, шаг 1.9): «outbox доставляет
// ровно один раз при падении relay между produce и update».
//
// «Ровно один раз» здесь — гарантия для ОБРАБОТКИ, а не для доставки Kafka.
// Сам relay даёт at-least-once: produce в Kafka происходит СНАРУЖИ
// транзакции Postgres (иначе паттерн был бы бессмыслен — не бывает единой
// транзакции на два разных хранилища), и если процесс падает ровно между
// успешным produce и UPDATE published_at, при перезапуске строка всё ещё
// не помечена опубликованной и уйдёт в Kafka ПОВТОРНО. Тест воспроизводит
// именно это падение и проверяет два свойства:
//
//  1. ничего не потеряно — событие рано или поздно оказывается в Kafka;
//  2. дубликат, порождённый падением, останавливается на дедупликации
//     по event_id (в проекте это делает таблица processed_events —
//     см. docs/adr/0008-*, здесь дедупликация показана впрямую, той же
//     проверкой, которую в реальном consumer'е делает processed_events).
func TestRelay_PadenieMezhduProduceIUpdateNeTeryayetSobytie(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pgContainer, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("gosplash"),
		tcpostgres.WithUsername("gosplash"),
		tcpostgres.WithPassword("gosplash"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, testcontainers.TerminateContainer(pgContainer)) }()

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	kafkaContainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0",
		tckafka.WithClusterID("outbox-relay-test"),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, testcontainers.TerminateContainer(kafkaContainer)) }()

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	gdb, err := gorm.Open(gormpostgres.Open(dsn))
	require.NoError(t, err)
	require.NoError(t, gdb.AutoMigrate(&OutboxRow{}))

	const topic = "media.photo.uploaded"
	env, err := kafkax.NewEnvelope(kafkax.EventPhotoUploaded, "photo-outbox-1", &eventsv1.PhotoUploaded{
		PhotoId: "photo-outbox-1",
		UserId:  1,
	})
	require.NoError(t, err)

	// Шаг 1 — бизнес-транзакция: ровно то, что сделал бы настоящий сервис
	// (media.internal.app), только без реальной таблицы photos рядом —
	// для теста самого outbox она не нужна, важна только атомарность
	// с ЧЕМ-ТО, а Write работает одинаково независимо от соседа по tx.
	require.NoError(t, gdb.Transaction(func(tx *gorm.DB) error {
		return Write(ctx, tx, env, topic)
	}))

	relay, err := New(Config{
		ServiceName: "media-test",
		Brokers:     brokers,
		DSNs:        []string{dsn},
		BatchSize:   10,
	})
	require.NoError(t, err)
	defer relay.Close()

	// Шаг 2 — имитация падения relay РОВНО между успешным produce и UPDATE.
	simulateCrashAfterProduce(ctx, t, relay)

	var row OutboxRow
	require.NoError(t, gdb.Where("aggregate_id = ?", env.GetAggregateId()).First(&row).Error)
	require.Nil(t, row.PublishedAt, "после имитации падения строка обязана остаться неопубликованной")

	// Шаг 3 — обычная работа relay: строка ещё не published (предыдущий
	// "процесс" не дошёл до UPDATE), поэтому SKIP LOCKED её не пропустит,
	// и она уйдёт в Kafka повторно.
	relay.pollAllShards(ctx)

	require.NoError(t, gdb.Where("aggregate_id = ?", env.GetAggregateId()).First(&row).Error)
	require.NotNil(t, row.PublishedAt, "после штатного прохода relay строка обязана быть помечена опубликованной")

	// Шаг 4 — читаем Kafka и проверяем оба свойства теста.
	envelopes := consumeEnvelopes(ctx, t, brokers, topic, 20*time.Second)
	require.NotEmpty(t, envelopes, "событие не должно потеряться")

	var withOurID int
	for _, e := range envelopes {
		if e.GetEventId() == env.GetEventId() {
			withOurID++
		}
	}
	require.GreaterOrEqual(t, withOurID, 1, "хотя бы одна копия события обязана дойти")
	// Ожидаемая (а не желательная) цена паттерна: падение между produce
	// и update дало дубликат. Если это когда-нибудь перестанет быть так
	// (например, relay переедет на транзакционный producer Kafka), тест
	// начнёт падать здесь — и это тот случай "теста, который должен упасть,
	// когда придёт время" из docs/STYLE.md: сигнал обновить ADR 0008,
	// а не тихо поправить assert.
	require.Equal(t, 2, withOurID, "падение между produce и update обязано давать ровно один дубликат")

	// Дедупликация по event_id — то, что в реальном consumer'е делает
	// таблица processed_events (docs/adr/0008-*, вне зоны ответственности
	// пакета outbox). Здесь она показана впрямую: несмотря на дубликат
	// в Kafka, применяем факт РОВНО один раз.
	applied := 0
	seen := make(map[string]struct{})
	for _, e := range envelopes {
		if e.GetEventId() != env.GetEventId() {
			continue
		}
		if _, ok := seen[e.GetEventId()]; ok {
			continue
		}
		seen[e.GetEventId()] = struct{}{}
		applied++
	}
	require.Equal(t, 1, applied, "идемпотентный обработчик обязан применить факт ровно один раз")
}

// simulateCrashAfterProduce воспроизводит ключевой сценарий теста: SELECT
// ... FOR UPDATE SKIP LOCKED, успешный produce в Kafka — и обрыв ДО UPDATE
// published_at и ДО commit. Настоящий крэш процесса воспроизвести в тесте
// нельзя, но эффект идентичен: соединение с открытой транзакцией закрывается,
// Postgres откатывает её и снимает блокировки, а Kafka уже честно получила
// сообщение, потому что producer работает вне транзакции Postgres.
func simulateCrashAfterProduce(ctx context.Context, t *testing.T, relay *Relay) {
	t.Helper()
	require.Len(t, relay.shards, 1)
	s := relay.shards[0]

	tx, err := s.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }() // "падение" — откатываем вместо commit

	rows, err := tx.Query(ctx, `
		SELECT id, aggregate_id, topic, key, headers, payload
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`,
		relay.cfg.BatchSize,
	)
	require.NoError(t, err)

	var batch []batchRow
	for rows.Next() {
		var row batchRow
		var aggregateID string
		require.NoError(t, rows.Scan(&row.id, &aggregateID, &row.topic, &row.key, &row.headers, &row.payload))
		batch = append(batch, row)
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Len(t, batch, 1, "в outbox должна быть ровно одна неопубликованная строка")

	published := relay.publishBatch(ctx, batch)
	require.Len(t, published, 1, "produce должен успешно уйти в Kafka ДО имитации падения")

	// Дальше — намеренно ничего: ни UPDATE published_at, ни Commit.
}

// consumeEnvelopes вычитывает топик с начала в течение timeout — для теста
// этого достаточно: обе публикации (штатная и повторная после "падения")
// происходят ДО вызова этой функции, поэтому ждать больше нет смысла.
func consumeEnvelopes(ctx context.Context, t *testing.T, brokers []string, topic string, timeout time.Duration) []*eventsv1.Envelope {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer client.Close()

	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var envelopes []*eventsv1.Envelope
	for cctx.Err() == nil {
		fetches := client.PollFetches(cctx)
		fetches.EachRecord(func(r *kgo.Record) {
			var env eventsv1.Envelope
			if err := proto.Unmarshal(r.Value, &env); err == nil {
				envelopes = append(envelopes, &env)
			}
		})
	}
	return envelopes
}

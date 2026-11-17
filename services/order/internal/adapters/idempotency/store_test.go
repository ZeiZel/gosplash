package idempotency

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"gosplash/pkg/redisx"
	"gosplash/services/order/internal/adapters/pg"
	"gosplash/services/order/internal/ports"
)

// newTestStore — Store над sqlite в памяти и «мёртвым» Redis: Begin получает
// от недоступного Redis транспортную ошибку и проваливается в beginPG —
// ровно тот путь PostgreSQL-истины, поведение которого здесь проверяется
// (см. Store.Begin, ветка err != nil).
//
// Сборка без CGO (CGO_ENABLED=0) получает от mattn/go-sqlite3 заглушку
// вместо драйвера — в таком окружении тесты пропускаются: они проверяют
// ПРАВИЛА перезанятия и чистки, а не окружение.
func newTestStore(t *testing.T, ttl time.Duration) *Store {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil && strings.Contains(err.Error(), "requires cgo") {
		t.Skip("sqlite недоступен: сборка без CGO (mattn/go-sqlite3 требует cgo)")
	}
	require.NoError(t, err, "sqlite в памяти обязана открыться без ошибок")
	require.NoError(t, db.AutoMigrate(&pg.IdempotencyKeyRow{}),
		"схема idempotency_keys обязана создаваться без ошибок")

	// 127.0.0.1:1 — соединение отвергается сразу; без ретраев и с коротким
	// DialTimeout, чтобы каждый Begin падал в beginPG за миллисекунды.
	redisClient, err := redisx.New(redisx.Config{
		Addr:        "127.0.0.1:1",
		ServiceName: "order-test",
		DialTimeout: 100 * time.Millisecond,
		MaxRetries:  -1,
	})
	require.NoError(t, err)

	return New(redisClient, db, ttl)
}

func rowStatus(t *testing.T, s *Store, key string) (status string, createdAt time.Time) {
	t.Helper()
	var row pg.IdempotencyKeyRow
	require.NoError(t, s.db.Where("key = ?", key).First(&row).Error)
	return row.Status, row.CreatedAt
}

func keysInTable(t *testing.T, s *Store) map[string]bool {
	t.Helper()
	var rows []pg.IdempotencyKeyRow
	require.NoError(t, s.db.Find(&rows).Error)
	set := make(map[string]bool, len(rows))
	for _, r := range rows {
		set[r.Key] = true
	}
	return set
}

// TestBegin_NovyyKlyuchZanimayetsyaAPovtorViditInProgress — первый Begin
// занимает ключ (Free), параллельный повтор видит in_progress (409).
func TestBegin_NovyyKlyuchZanimayetsyaAPovtorViditInProgress(t *testing.T) {
	store := newTestStore(t, time.Hour)
	ctx := context.Background()

	first, err := store.Begin(ctx, "k-new")
	require.NoError(t, err)
	assert.Equal(t, ports.IdempotencyFree, first.Status, "первый Begin обязан занять свободный ключ")

	second, err := store.Begin(ctx, "k-new")
	require.NoError(t, err)
	assert.Equal(t, ports.IdempotencyInProgress, second.Status,
		"живой in_progress нельзя ни пройти молча, ни перезанять — это и есть защита от параллельного повтора")
}

// TestComplete_PovtorVozvrashchaetSohranennyjOtvet — после Complete Begin
// возвращает готовый ответ из PostgreSQL, не выполняя операцию заново.
func TestComplete_PovtorVozvrashchaetSohranennyjOtvet(t *testing.T) {
	store := newTestStore(t, time.Hour)
	ctx := context.Background()

	first, err := store.Begin(ctx, "k-done")
	require.NoError(t, err)
	require.Equal(t, ports.IdempotencyFree, first.Status)

	require.NoError(t, store.Complete(ctx, "k-done", []byte(`{"order":"o-1"}`)))

	again, err := store.Begin(ctx, "k-done")
	require.NoError(t, err)
	assert.Equal(t, ports.IdempotencyDone, again.Status)
	assert.JSONEq(t, `{"order":"o-1"}`, string(again.Response))
}

// TestBegin_ProtuhshijInProgressPerezanimayetsya — след падения между Begin
// и Complete: in_progress старше TTL не блокирует ключ навсегда, а даёт
// повтору «свободно», начиная НОВУЮ эпоху занятия (created_at обновляется).
func TestBegin_ProtuhshijInProgressPerezanimayetsya(t *testing.T) {
	ttl := time.Hour
	store := newTestStore(t, ttl)
	ctx := context.Background()

	// Ключ занят «погибшим» процессом два TTL назад — уже протух.
	stale := time.Now().Add(-2 * ttl)
	require.NoError(t, store.db.Create(&pg.IdempotencyKeyRow{
		Key: "k-stale", Status: pg.IdempotencyStatusInProgress, CreatedAt: stale, UpdatedAt: stale,
	}).Error)

	claimed, err := store.Begin(ctx, "k-stale")
	require.NoError(t, err)
	assert.Equal(t, ports.IdempotencyFree, claimed.Status,
		"протухший in_progress обязан перезаниматься, а не отдавать 409 вечно")

	_, createdAt := rowStatus(t, store, "k-stale")
	assert.True(t, createdAt.After(stale), "перезанятие обязано начать новую эпоху (created_at обновлён)")

	// Перезанятый ключ для СЛЕДУЮЩЕГО повтора — свежий in_progress.
	next, err := store.Begin(ctx, "k-stale")
	require.NoError(t, err)
	assert.Equal(t, ports.IdempotencyInProgress, next.Status)
}

// TestComplete_UdalyaetStaryeStroki — Complete заодно чистит строки старше
// TTL+expiredRetention: завершённые (по updated_at) и брошенные
// in_progress (по created_at); свежие строки не трогает.
func TestComplete_UdalyaetStaryeStroki(t *testing.T) {
	ttl := time.Hour
	store := newTestStore(t, ttl)
	ctx := context.Background()

	ancient := time.Now().Add(-(ttl + expiredRetention + time.Hour))
	recent := time.Now().Add(-time.Minute)
	seed := []pg.IdempotencyKeyRow{
		{Key: "done-old", Status: pg.IdempotencyStatusDone, Response: []byte("x"),
			CreatedAt: ancient, UpdatedAt: ancient}, // завершён давно — удалить
		{Key: "done-fresh", Status: pg.IdempotencyStatusDone, Response: []byte("x"),
			CreatedAt: ancient, UpdatedAt: recent}, // завершён только что — оставить
		{Key: "ip-ancient", Status: pg.IdempotencyStatusInProgress,
			CreatedAt: ancient, UpdatedAt: ancient}, // брошен давно — удалить
		{Key: "ip-fresh", Status: pg.IdempotencyStatusInProgress,
			CreatedAt: recent, UpdatedAt: recent}, // живой — оставить
	}
	for i := range seed {
		require.NoError(t, store.db.Create(&seed[i]).Error)
	}

	require.NoError(t, store.Complete(ctx, "ip-fresh", []byte("done-now")))

	left := keysInTable(t, store)
	assert.True(t, left["ip-fresh"], "завершённый только что ключ обязан остаться")
	assert.True(t, left["done-fresh"], "недавно завершённый ключ обязан остаться (TTL ещё не вышел)")
	assert.False(t, left["done-old"], "done старше TTL+retention подлежит удалению")
	assert.False(t, left["ip-ancient"], "брошенный in_progress старше TTL+retention подлежит удалению")
}

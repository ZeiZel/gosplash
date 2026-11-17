//go:build integration

// Интеграционный тест адаптера ClickHouse — требует Docker (testcontainers-go
// поднимает тот же образ, что и deploy/compose/clickhouse.yml).
// Запуск: go test -tags=integration ./internal/adapters/clickhouse/...
//
// Без тега пакет собирается и тестируется (batch_test.go, repository_test.go)
// без какой-либо инфраструктуры — см. docs/STYLE.md.
package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"

	"gosplash/services/analytics/internal/domain"
	"gosplash/services/analytics/migrations"
)

// newTestClient поднимает отдельный контейнер ClickHouse на тест и
// применяет к нему миграции сервиса — тот же приём, что в
// pkg/redisx/integration_test.go: отдельный контейнер, а не общий на пакет,
// чтобы TopPhotos/PhotoStats одного теста не видели строки, вставленные
// другим.
func newTestConn(t *testing.T) chdriver.Conn {
	t.Helper()
	ctx := context.Background()

	container, err := tcclickhouse.Run(ctx, "clickhouse/clickhouse-server:25.8",
		tcclickhouse.WithUsername("gosplash"),
		tcclickhouse.WithPassword("gosplash"),
		tcclickhouse.WithDatabase("gosplash"),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, container.Terminate(context.Background()))
	})

	host, err := container.ConnectionHost(ctx)
	require.NoError(t, err)

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{host},
		Auth: clickhouse.Auth{Database: "gosplash", Username: "gosplash", Password: "gosplash"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })

	migrateCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, migrations.Migrate(migrateCtx, conn))

	return conn
}

func TestViewWriterIRepository_ProsmotrPopadaetVOtchety(t *testing.T) {
	conn := newTestConn(t)
	ctx := context.Background()

	// batchSize=1 — каждый Add() флашится немедленно: тесту не нужно ждать
	// таймер, а нужна детерминированная видимость строки сразу после Add.
	writer := NewViewWriter(conn, 1, time.Hour)
	repo := NewRepository(conn)

	viewerA := int64(100)
	viewerB := int64(101)
	now := time.Now()

	require.NoError(t, writer.Add(ctx, domain.PhotoView{
		PhotoID: "10000000-0000-4000-8000-000000000001", AuthorID: 1, ViewerID: &viewerA, Country: "RU", OccurredAt: now,
	}))
	require.NoError(t, writer.Add(ctx, domain.PhotoView{
		PhotoID: "10000000-0000-4000-8000-000000000001", AuthorID: 1, ViewerID: &viewerB, Country: "RU", OccurredAt: now,
	}))
	require.NoError(t, writer.Add(ctx, domain.PhotoView{
		PhotoID: "10000000-0000-4000-8000-000000000001", AuthorID: 1, ViewerID: nil, Country: "US", OccurredAt: now,
	}))

	// Материализованное представление photo_views_daily срабатывает
	// АСИНХРОННО по отношению к INSERT (см. комментарий в migrations/auto.go)
	// — обычно почти мгновенно на такой маленькой вставке, но не гарантированно
	// в тот же миллисекунд. eventually избегает флакающего теста без
	// произвольного sleep большего, чем нужно.
	require.Eventually(t, func() bool {
		ranks, err := repo.TopPhotos(ctx, domain.PeriodDay, 10)
		return err == nil && len(ranks) == 1 && ranks[0].Views == 3
	}, 10*time.Second, 100*time.Millisecond, "photo_views_daily не досчитал просмотры вовремя")

	ranks, err := repo.TopPhotos(ctx, domain.PeriodDay, 10)
	require.NoError(t, err)
	require.Len(t, ranks, 1)
	assert.Equal(t, "10000000-0000-4000-8000-000000000001", ranks[0].PhotoID)
	assert.Equal(t, int64(1), ranks[0].AuthorID)
	assert.Equal(t, int64(3), ranks[0].Views)

	stats, err := repo.PhotoStats(ctx, "10000000-0000-4000-8000-000000000001", domain.PeriodDay)
	require.NoError(t, err)
	assert.Equal(t, int64(3), stats.Views)
	// 2 уникальных НЕанонимных зрителя (viewerA, viewerB); анонимный
	// просмотр (viewer_id=NULL) не считается — см. domain.PhotoView.
	assert.Equal(t, int64(2), stats.UniqueViewersApprox)
}

func TestPurchaseWriterIRepository_PokupkaPopadaetVOtchety(t *testing.T) {
	conn := newTestConn(t)
	ctx := context.Background()

	writer := NewPurchaseWriter(conn, 1, time.Hour)
	repo := NewRepository(conn)

	require.NoError(t, writer.Add(ctx, domain.Purchase{
		OrderID:  "20000000-0000-4000-8000-000000000001",
		PhotoID:  "10000000-0000-4000-8000-000000000002",
		AuthorID: 2, BuyerID: 5, PriceCents: 1999, OccurredAt: time.Now(),
	}))

	stats, err := repo.PhotoStats(ctx, "10000000-0000-4000-8000-000000000002", domain.PeriodDay)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.Purchases)
	assert.Equal(t, int64(1999), stats.RevenueCents)
}

func TestWriter_DublirovaniePriPovtorePosleUpavshegoOffseta(t *testing.T) {
	// Честная демонстрация цены, разобранной в комментарии Writer[T].Add:
	// повторная доставка ОДНОЙ И ТОЙ ЖЕ строки (например, после того как
	// Kafka прислала сообщение снова, потому что offset не успел
	// закоммититься) не отбрасывается ClickHouse — вставляется ВТОРОЙ раз.
	// Тест фиксирует это поведение, а не борется с ним: у ClickHouse нет
	// уникальности по umolчанию (см. README сервиса).
	conn := newTestConn(t)
	ctx := context.Background()
	writer := NewViewWriter(conn, 1, time.Hour)

	view := domain.PhotoView{PhotoID: "10000000-0000-4000-8000-000000000003", AuthorID: 1, Country: "RU", OccurredAt: time.Now()}
	require.NoError(t, writer.Add(ctx, view))
	require.NoError(t, writer.Add(ctx, view)) // повтор той же строки

	repo := NewRepository(conn)
	require.Eventually(t, func() bool {
		stats, err := repo.PhotoStats(ctx, view.PhotoID, domain.PeriodDay)
		return err == nil && stats.Views == 2
	}, 10*time.Second, 100*time.Millisecond)
}

// TestFunnelAndAuthorStats — воронка и авторский срез на живом движке:
// две загрузки, одна из которых «застряла» без превью (ровно тот случай
// ready_percent < 100, ради которого метод и существует), плюс просмотры и
// покупка автора для AuthorStats.
func TestFunnelAndAuthorStats(t *testing.T) {
	conn := newTestConn(t)
	ctx := context.Background()

	uploadWriter := NewUploadWriter(conn, 1, time.Hour)
	thumbnailWriter := NewThumbnailWriter(conn, 1, time.Hour)
	viewWriter := NewViewWriter(conn, 1, time.Hour)
	purchaseWriter := NewPurchaseWriter(conn, 1, time.Hour)
	repo := NewRepository(conn)

	const author = int64(7)
	const ready = "10000000-0000-4000-8000-000000000010"
	const stuck = "10000000-0000-4000-8000-000000000011"

	base := time.Now().Add(-time.Hour)
	require.NoError(t, uploadWriter.Add(ctx, domain.Upload{PhotoID: ready, AuthorID: author, OccurredAt: base}))
	require.NoError(t, uploadWriter.Add(ctx, domain.Upload{PhotoID: stuck, AuthorID: author, OccurredAt: base}))
	// Превью готово через 1.5с после загрузки — это же число ожидаем в p50.
	require.NoError(t, thumbnailWriter.Add(ctx, domain.Thumbnail{PhotoID: ready, OccurredAt: base.Add(1500 * time.Millisecond)}))
	// stuck превью не получает — половина воронки «застряла».

	viewer := int64(500)
	require.NoError(t, viewWriter.Add(ctx, domain.PhotoView{PhotoID: ready, AuthorID: author, ViewerID: &viewer, Country: "RU", OccurredAt: base}))
	require.NoError(t, purchaseWriter.Add(ctx, domain.Purchase{
		OrderID:    "30000000-0000-4000-8000-000000000001",
		PhotoID:    ready,
		AuthorID:   author,
		BuyerID:    9,
		PriceCents: 1999,
		OccurredAt: base,
	}))

	// Счётчики воронки идут через витрины uploads_daily/thumbnails_daily —
	// они досчитываются MV асинхронно (как photo_views_daily выше).
	require.Eventually(t, func() bool {
		stats, err := repo.FunnelStats(ctx, domain.PeriodDay)
		return err == nil && stats.Uploads == 2 && stats.Thumbnails == 1
	}, 10*time.Second, 100*time.Millisecond, "витрины воронки не досчитались вовремя")

	funnel, err := repo.FunnelStats(ctx, domain.PeriodDay)
	require.NoError(t, err)
	assert.InDelta(t, 50.0, funnel.ReadyPercent, 0.001, "1 из 2 загрузок с превью")
	// Единственная пара upload→thumbnail даёт лаг 1500мс: p50 == p95 == 1500.
	assert.Equal(t, int64(1500), funnel.LatencyP50Ms)
	assert.Equal(t, int64(1500), funnel.LatencyP95Ms)

	authorStats, err := repo.AuthorStats(ctx, author, domain.PeriodDay)
	require.NoError(t, err)
	assert.Equal(t, int64(1), authorStats.Views)
	assert.Equal(t, int64(1), authorStats.UniqueViewersApprox)
	assert.Equal(t, int64(1), authorStats.Purchases)
	assert.Equal(t, int64(1999), authorStats.RevenueCents)
	assert.Equal(t, int64(2), authorStats.Uploads)

	// Чужой автор за тот же период — пустая сводка, а не ошибка: «нет
	// данных» и «не бывает такого автора» для отчёта одно и то же.
	other, err := repo.AuthorStats(ctx, 999, domain.PeriodDay)
	require.NoError(t, err)
	assert.Equal(t, domain.AuthorStats{}, other)
}

package clickhouse

import (
	"context"
	"fmt"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"gosplash/services/analytics/internal/domain"
)

// ViewWriter — реализация ports.ViewSink: batch-вставка в таблицу
// photo_views. Каждый Add() — это одна строка одной из накопленных пачек
// (см. batch.go про то, почему Add блокирует, а не просто складывает в
// канал).
type ViewWriter struct {
	batch *Writer[domain.PhotoView]
}

// NewViewWriter создаёт батчер просмотров. conn передаётся напрямую (а не
// *Client) — так этот файл и его тест не зависят от структуры client.go,
// только от chdriver.Conn.
func NewViewWriter(conn chdriver.Conn, batchSize int, flushInterval time.Duration) *ViewWriter {
	w := &ViewWriter{}
	w.batch = NewWriter(batchSize, flushInterval, func(ctx context.Context, rows []domain.PhotoView) error {
		return insertViews(ctx, conn, rows)
	})
	return w
}

func (w *ViewWriter) Add(ctx context.Context, view domain.PhotoView) error {
	return w.batch.Add(ctx, view)
}

func insertViews(ctx context.Context, conn chdriver.Conn, rows []domain.PhotoView) error {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO photo_views (photo_id, author_id, viewer_id, ts, country)")
	if err != nil {
		return fmt.Errorf("clickhouse: prepare batch photo_views: %w", err)
	}
	for _, v := range rows {
		// viewer_id передаётся указателем: nil сериализуется в NULL
		// Nullable(Int64)-колонки, значение — в само число. Ровно так
		// clickhouse-go ожидает Nullable-параметры (lib/column/nullable.go).
		if err := batch.Append(v.PhotoID, v.AuthorID, v.ViewerID, v.OccurredAt, v.Country); err != nil {
			return fmt.Errorf("clickhouse: append photo_views: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: send photo_views (%d строк): %w", len(rows), err)
	}
	return nil
}

// PurchaseWriter — реализация ports.PurchaseSink: batch-вставка в purchases.
type PurchaseWriter struct {
	batch *Writer[domain.Purchase]
}

func NewPurchaseWriter(conn chdriver.Conn, batchSize int, flushInterval time.Duration) *PurchaseWriter {
	w := &PurchaseWriter{}
	w.batch = NewWriter(batchSize, flushInterval, func(ctx context.Context, rows []domain.Purchase) error {
		return insertPurchases(ctx, conn, rows)
	})
	return w
}

func (w *PurchaseWriter) Add(ctx context.Context, purchase domain.Purchase) error {
	return w.batch.Add(ctx, purchase)
}

func insertPurchases(ctx context.Context, conn chdriver.Conn, rows []domain.Purchase) error {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO purchases (order_id, photo_id, author_id, buyer_id, price_cents, ts)")
	if err != nil {
		return fmt.Errorf("clickhouse: prepare batch purchases: %w", err)
	}
	for _, p := range rows {
		if err := batch.Append(p.OrderID, p.PhotoID, p.AuthorID, p.BuyerID, p.PriceCents, p.OccurredAt); err != nil {
			return fmt.Errorf("clickhouse: append purchases: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: send purchases (%d строк): %w", len(rows), err)
	}
	return nil
}

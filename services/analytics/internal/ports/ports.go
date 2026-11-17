// Package ports — интерфейсы между internal/app и внешним миром.
//
// Как и в catalog (services/catalog/internal/ports), интерфейс объявлен
// рядом с тем, кто им пользуется (internal/app), а не рядом с реализацией
// (internal/adapters/clickhouse) — так internal/app остаётся тестируемым
// поддельными реализациями без единого импорта clickhouse-go или franz-go.
package ports

import (
	"context"

	"gosplash/services/analytics/internal/domain"
)

// StatsReader — чтение сводок из ClickHouse. Единственный порт, которым
// пользуется internal/app/stats.go (сценарий gRPC-запросов).
type StatsReader interface {
	// TopPhotos — топ фото по сумме просмотров за период, лимит уже
	// провалидирован и подставлен вызывающим (internal/app).
	TopPhotos(ctx context.Context, period domain.Period, limit int32) ([]domain.PhotoRank, error)
	// PhotoStats — сводка по одному фото за период.
	PhotoStats(ctx context.Context, photoID string, period domain.Period) (domain.PhotoStats, error)
	// FunnelStats — воронка «загружено → превью готово» за период.
	FunnelStats(ctx context.Context, period domain.Period) (domain.FunnelStats, error)
	// AuthorStats — сводка по одному автору за период.
	AuthorStats(ctx context.Context, authorID int64, period domain.Period) (domain.AuthorStats, error)
}

// ViewSink — приёмник фактов "просмотр" со стороны Kafka-консьюмера
// (internal/app/ingest.go). Add блокирует вызывающего, пока факт не окажется
// частью УСПЕШНО завершённой пачечной вставки в ClickHouse — почему это
// важно именно здесь и чем это отличается от catalog.ViewPublisher.Enqueue
// (который никогда не блокирует), разобрано в
// internal/adapters/clickhouse/batch.go и в ADR 0016 (раздел «цена»).
type ViewSink interface {
	Add(ctx context.Context, view domain.PhotoView) error
}

// PurchaseSink — приёмник фактов "покупка". Отдельный порт от ViewSink,
// хотя оба сейчас реализует один и тот же адаптер (adapters/clickhouse):
// у покупок на порядки меньше объём и на порядки выше цена потерянной
// строки (выручка в отчёте), поэтому у них разные параметры батчинга
// (см. internal/config), и разделение портов оставляет это видимым в
// сигнатуре, а не только в конфиге.
type PurchaseSink interface {
	Add(ctx context.Context, purchase domain.Purchase) error
}

// UploadSink и ThumbnailSink — приёмники фактов воронки «загружено →
// превью готово». Они могли бы быть одним FunnelSink с двумя методами, но
// остаются двумя по образцу PurchaseSink: события воронки редкие, как
// покупки (загрузка — это осознанное действие человека, не просмотр
// карточки), поэтому их батчингу нужны СВОИ малые пределы — а раз пределы
// свои, то и порт свой, иначе разница видна только в конфиге и легко
// «выравнивается» одним общим Writer'ом по ошибке.
type UploadSink interface {
	Add(ctx context.Context, upload domain.Upload) error
}

type ThumbnailSink interface {
	Add(ctx context.Context, thumbnail domain.Thumbnail) error
}

// Package ports — интерфейсы, через которые прикладной слой каталога
// разговаривает с внешним миром. Интерфейс объявлен рядом с тем, кто им
// пользуется, а не рядом с реализацией (см. такой же приём в media):
// внутри одного пакета internal/app два разных сценария (Indexer,
// ListingService) делят часть портов — это НЕ нарушение принципа, он про
// границу пакетов, а не про то, что у каждого потребителя обязан быть свой
// собственный интерфейс.
package ports

import (
	"context"
	"time"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
	"gosplash/services/catalog/internal/domain"
)

// ListingRepository — витрина каталога, ТОЛЬКО ЧТЕНИЕ.
//
// Запись сознательно вынесена в UnitOfWork/IndexTx ниже: единственный
// писатель каталога — идемпотентный консьюмер, и его транзакционная граница
// (Claim + бизнес-изменение атомарно) не влезает в обычный CRUD-интерфейс
// без протаскивания *gorm.DB в internal/app, что запрещено docs/STYLE.md.
type ListingRepository interface {
	GetByID(ctx context.Context, id string) (*domain.Listing, error)
	// List — лента ТОЛЬКО published-карточек, keyset-пагинация по курсору
	// (см. adapters/pg/cursor.go: почему не OFFSET). Пустой cursor — первая
	// страница. Пустой nextCursor в ответе — страниц больше нет.
	List(ctx context.Context, limit int, cursor string, tags []string) (listings []domain.Listing, nextCursor string, err error)
	// CountOn — сколько строк на конкретном узле ("primary" | "replica").
	// Диагностика отставания реплики, унаследована из фазы 0.
	CountOn(ctx context.Context, node string) (int64, error)
}

// PhotoFetcher — подробности о фото из media.
//
// Порт назван по смыслу («достань фото»), а не по транспорту: прикладной
// слой не знает, что за ним gRPC. ErrPhotoNotFound — единственная ошибка,
// которую вызывающий обязан уметь распознать (см. domain.ErrPhotoNotFound);
// остальные трактуются как временные.
type PhotoFetcher interface {
	Fetch(ctx context.Context, photoID string, userID int64) (*domain.Listing, error)
}

// IndexTx — операции индексатора ВНУТРИ ОДНОЙ транзакции БД, той же, в
// которой сделан idempotency.Claim (см. UnitOfWork ниже). Реализация
// (adapters/pg) обязана выполнять их на том же *gorm.DB-tx, что и Claim —
// иначе гарантия «одна транзакция на отметку и бизнес-изменение» превращается
// в фикцию, а вместе с ней и вся идемпотентность консьюмера.
type IndexTx interface {
	// UpsertListing создаёт черновик карточки. ON CONFLICT DO NOTHING, а не
	// UpdateAll — см. обоснование в adapters/pg/tx.go.
	UpsertListing(ctx context.Context, listing *domain.Listing) error
	// ApplyThumbnails дописывает превью к уже существующей карточке и
	// переводит её в published. domain.ErrNotFound — карточка ещё не
	// создана (событие thumbnail-ready обогнало uploaded: топики
	// независимы по порядку) — вызывающий обязан считать это ВРЕМЕННОЙ
	// ошибкой, а не постоянной.
	ApplyThumbnails(ctx context.Context, photoID string, thumbnails map[string]string, publishedAt time.Time) (*domain.Listing, error)
	// UpsertAuthorSnapshot — заводит запись об авторе при первом появлении.
	// ON CONFLICT DO NOTHING, а не UPDATE — см. authors_snapshot в
	// adapters/pg/listing_repository.go.
	UpsertAuthorSnapshot(ctx context.Context, authorID int64, name string) error
	// Outbox — событие каталога В ТОЙ ЖЕ транзакции (PATTERN: transactional
	// outbox, pkg/outbox).
	Outbox(ctx context.Context, env *eventsv1.Envelope, topic string) error
}

// UnitOfWork — граница идемпотентного консьюмера.
//
// PATTERN: idempotent consumer (pkg/idempotency). WithClaim делает Claim
// ПЕРВЫМ действием транзакции: если событие уже применено, fn не вызывается
// вовсе — ни одного лишнего запроса, включая внешние (media по gRPC).
// Обратная сторона этого выбора — транзакция БД держится открытой на всё
// время работы fn, в том числе на время сетевого похода в media внутри неё
// (см. Indexer.HandlePhotoUploaded): это плата за то, что Claim и бизнес-
// изменение обязаны коммититься или откатываться АТОМАРНО одним куском —
// разнеси их, и на повторной доставке события можно получить «отметка есть,
// а карточки нет» ровно ту дыру, которую Claim должен закрывать.
type UnitOfWork interface {
	// WithClaim возвращает applied=false, если событие уже обработано
	// (Claim увидел конфликт по event_id) — тогда err всегда nil и fn не
	// вызывался. applied=true — fn выполнен и его результат зафиксирован.
	WithClaim(ctx context.Context, eventID, eventType, topic string, fn func(IndexTx) error) (applied bool, err error)
}

// ListingCache — cache-aside карточки (pkg/redisx.GetOrLoadProto) плюс
// инвалидация. Один интерфейс на обоих потребителей (ListingService читает,
// Indexer инвалидирует после записи) — они всё равно смотрят в один и тот же
// namespace ключей, и разносить их по разным портам ничего не даёт.
type ListingCache interface {
	GetOrLoad(ctx context.Context, id string, load func(context.Context) (*domain.Listing, error)) (*domain.Listing, error)
	// Invalidate, а не обновление кэша на месте (write-through) — см.
	// комментарий в internal/app/indexer.go у места вызова: два консьюмера
	// на разных партициях/топиках могут применяться в любом порядке, и
	// write-through рискует записать в кэш УСТАРЕВШЕЕ значение поверх
	// свежего. Invalidate просто убирает ключ — следующий читатель
	// перечитает актуальную строку из базы.
	Invalidate(ctx context.Context, id string) error
}

// TopEntry — одна строка топа просмотров. Копия redisx.TopEntry: домену и
// прикладному слою не нужно знать, что за ней Redis Sorted Set.
type TopEntry struct {
	ListingID string
	Views     float64
}

// ViewCounter — счётчик просмотров (pkg/redisx: Sorted Set, топ-N).
type ViewCounter interface {
	IncrView(ctx context.Context, id string, at time.Time) error
	Top(ctx context.Context, at time.Time, n int) ([]TopEntry, error)
}

// ViewPublisher — постановка события просмотра в очередь на батч-публикацию
// в Kafka. Enqueue не возвращает ошибку и не блокирует вызывающего —
// см. подробный разбор решения в adapters/kafka/view_batcher.go: outbox
// здесь намеренно не используется, потеря части событий аналитики допустима.
type ViewPublisher interface {
	Enqueue(photoID string, authorID, viewerID int64, country string)
}

// LicenseRepository — лицензии. Оба метода идемпотентны по order_id — это
// свойство гарантирует РЕАЛИЗАЦИЯ (adapters/pg), а не вызывающий код.
type LicenseRepository interface {
	// Grant выдаёт лицензию. Повтор с тем же order_id возвращает УЖЕ
	// выданную лицензию, не создавая вторую строку.
	Grant(ctx context.Context, listingID string, buyerID int64, orderID string) (*domain.License, error)
	// Revoke отзывает лицензию по order_id. revoked=false (без ошибки) —
	// если лицензии с таким order_id нет или она уже отозвана: компенсация
	// обязана быть безопасной при повторе и при отмене шага, который не
	// выполнился (см. proto: RevokeLicenseResponse.revoked).
	Revoke(ctx context.Context, orderID, reason string) (revoked bool, err error)
}

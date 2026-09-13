// Package app — прикладной слой поиска: наполнение индекса из Kafka и сам
// поиск. Импортирует только domain и ports (docs/STYLE.md) — ни ES, ни
// gRPC, ни Kafka здесь не появляются напрямую, только через интерфейсы.
package app

import (
	"errors"
	"fmt"

	"context"

	"gosplash/pkg/kafkax"
	"gosplash/services/search/internal/domain"
	"gosplash/services/search/internal/ports"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// Indexer — консьюмер catalog.listing.published → индекс/удаление документа.
//
// PATTERN: идемпотентность через версионирование документа вместо таблицы
// processed_events (как это сделано в catalog, см. pkg/idempotency).
//
// Почему здесь достаточно версии, а отдельная таблица не нужна:
// processed_events нужна ТОГДА, когда "применить событие" — это несколько
// операций, которые обязаны либо все произойти, либо ни одна (Claim +
// бизнес-изменение в одной транзакции БД). У Elasticsearch нет
// транзакций — здесь одна операция ровно на один документ, и версия решает
// ТУ ЖЕ задачу (не дать старому событию перезаписать новое состояние)
// без необходимости в отдельной "базе отметок": каждый Index-запрос несёт
// version=occurred_at (мс) и version_type=external_gte, и Elasticsearch
// САМ отклоняет запись, если в индексе уже лежит документ с версией
// не младше присланной (409 version_conflict_engine_exception, который
// classifyESErr ниже трактует как УСПЕХ, а не ошибку: пришло устаревшее
// событие, документ и так уже актуален).
//
// Зачем это вообще нужно: события могут прийти не по порядку. Ретрай после
// временной ошибки, переигровка DLQ вручную, две партиции с разной
// скоростью обработки одного и того же listing_id (тут не должно
// случиться, ключ — listing_id, но защита от полного класса подобных
// багов на уровне поколения событий — это ровно то, ради чего версия и
// нужна, а не "потому что могло бы") — без версии "более раннее по факту,
// но более позднее по доставке" событие тихо затёрло бы актуальное.
type Indexer struct {
	writer ports.IndexWriter
}

func NewIndexer(writer ports.IndexWriter) *Indexer {
	return &Indexer{writer: writer}
}

// HandleListingPublished — kafkax.Handler для catalog.listing.published.
func (i *Indexer) HandleListingPublished(ctx context.Context, env *eventsv1.Envelope) error {
	var event eventsv1.ListingPublished
	if err := kafkax.UnmarshalPayload(env, &event); err != nil {
		// Битый payload не станет валидным от повторов — permanent, в DLQ.
		return kafkax.Permanent(fmt.Errorf("разбор payload catalog.listing.published: %w", err))
	}

	// Версия — occurred_at конверта в миллисекундах, а НЕ время обработки
	// здесь: важно, КОГДА факт произошёл у продюсера, а не когда консьюмер
	// до него добрался (иначе два ретрая одного и того же события в разном
	// порядке дали бы разные версии одного и того же события).
	version := env.GetOccurredAtUnixMs()

	if event.GetStatus() == domain.StatusRemoved {
		if err := i.writer.DeleteListing(ctx, event.GetListingId(), version); err != nil {
			return classifyESErr(fmt.Errorf("удаление документа %s: %w", event.GetListingId(), err))
		}
		return nil
	}

	doc := domain.ListingDoc{
		ListingID:  event.GetListingId(),
		AuthorID:   event.GetAuthorId(),
		AuthorName: event.GetAuthorName(),
		Title:      event.GetTitle(),
		Tags:       event.GetTags(),
		PriceCents: event.GetPriceCents(),
		Currency:   event.GetCurrency(),
		Status:     event.GetStatus(),
		// НЕОЧЕВИДНОЕ РЕШЕНИЕ: ListingPublished не несёт published_at —
		// у события в принципе нет такого поля (см. proto). occurred_at
		// конверта — честная замена: это и есть момент публикации/
		// изменения карточки с точки зрения producer'а (catalog), тот же
		// смысл, который поле должно нести.
		PublishedAt: kafkax.OccurredAt(env),
	}
	if err := i.writer.IndexListing(ctx, doc, version); err != nil {
		return classifyESErr(fmt.Errorf("индексация документа %s: %w", doc.ListingID, err))
	}
	return nil
}

// classifyESErr решает retryable/permanent по ошибке от адаптера ES.
//
// ES недоступен (сеть, 503, 429) — retryable: как только кластер оживёт,
// то же самое сообщение обработается успешно. Документ не прошёл валидацию
// маппинга (adapters/es/mapping.go — dynamic: strict, поэтому лишнее или
// перепутанное по типу поле роняет запись, а не тихо игнорируется) —
// permanent: сколько ни повторяй запись с тем же битым документом,
// маппинг не изменится, и результат будет тот же.
func classifyESErr(err error) error {
	if errors.Is(err, domain.ErrInvalidDocument) {
		return kafkax.Permanent(err)
	}
	// Неклассифицированная ошибка (в т.ч. domain.ErrIndexUnavailable) —
	// retryable. Это симметрично дефолту самого kafkax (см. errors.go
	// пакета): лучше лишний повтор идемпотентной операции, чем невыясненная
	// ошибка молча уйдёт в DLQ, ни разу не попытавшись повториться.
	return kafkax.Retryable(err)
}

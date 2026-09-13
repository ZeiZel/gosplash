package app

import (
	"context"
	"time"

	"gosplash/services/catalog/internal/domain"
	"gosplash/services/catalog/internal/ports"
)

// ListingService — путь ЧТЕНИЯ каталога.
//
// PATTERN: cache-aside (pkg/redisx) на GetByID. Промах ленты (List) НЕ
// кэшируется — см. комментарий у List ниже.
type ListingService struct {
	repo  ports.ListingRepository
	cache ports.ListingCache
	views ports.ViewCounter
	pub   ports.ViewPublisher
}

func NewListingService(repo ports.ListingRepository, cache ports.ListingCache, views ports.ViewCounter, pub ports.ViewPublisher) *ListingService {
	return &ListingService{repo: repo, cache: cache, views: views, pub: pub}
}

// List — лента. ПРИНЦИПИАЛЬНО НЕ кэшируется, в отличие от отдельной
// карточки.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: у ленты нет ОДНОГО ключа, который можно инвалидировать
// по одному событию — она зависит от limit, cursor и набора tags, то есть
// комбинаций ключей столько же, сколько сочетаний параметров запроса.
// Инвалидировать "всё, что похоже на ленту" по SCAN на каждую публикацию
// карточки (а публикации происходят часто) дороже, чем просто не кэшировать
// вовсе. Второе: лента и так меняется на каждой загрузке новой карточки —
// то есть у неё нет "стабильного" состояния, которое имело бы смысл держать
// в кэше минутами, как у отдельной карточки. Если бы кэш всё же был нужен
// (например, при кратном росте RPS на GET /listings), правильный вариант —
// короткий TTL (секунды, не минуты) БЕЗ инвалидации по событиям вообще:
// пользователь просто увидит на несколько секунд более старую первую
// страницу, и это дешевле, чем городить точную инвалидацию по комбинации
// параметров.
func (s *ListingService) List(ctx context.Context, limit int, cursor string, tags []string) ([]domain.Listing, string, error) {
	return s.repo.List(ctx, limit, cursor, tags)
}

// Get — карточка по id, cache-aside, плюс счётчик просмотров и событие
// аналитики.
func (s *ListingService) Get(ctx context.Context, id string) (*domain.Listing, error) {
	listing, err := s.cache.GetOrLoad(ctx, id, func(ctx context.Context) (*domain.Listing, error) {
		return s.repo.GetByID(ctx, id)
	})
	if err != nil {
		return nil, err
	}

	s.recordView(ctx, listing, 0, "")
	return listing, nil
}

// recordView — см. подробный разбор решения "без outbox" в
// adapters/kafka/view_batcher.go. Здесь коротко: события просмотров на
// порядки многочисленнее любых других, обёртывать каждый в транзакцию
// (Claim + outbox.Write) означало бы транзакцию БД на каждый показ карточки
// — именно того, чего пакет outbox стоит избегать для событий такого объёма.
//
// viewerID и country сейчас всегда анонимны (0 и "") — GetListing публичный
// метод (см. main.go PublicMethods), интерсептор аутентификации для него не
// выполняется, и geoIP в проекте не реализован. Это сознательное упрощение
// этой фазы, а не забытая часть: сама возможность посчитать авторизованный
// просмотр требует опционального (не обязательного) JWT в grpcx, которого
// сейчас нет.
func (s *ListingService) recordView(ctx context.Context, listing *domain.Listing, viewerID int64, country string) {
	now := time.Now()
	if err := s.views.IncrView(ctx, listing.ID, now); err != nil {
		// Кэш и счётчики — не источник правды: провалить запрос карточки
		// из-за недоступного Redis было бы куда хуже, чем просто не
		// посчитать один просмотр.
		return
	}
	s.pub.Enqueue(listing.ID, listing.AuthorID, viewerID, country)
}

// Top — топ-100 просмотренных карточек за сутки (GET /catalog/top).
func (s *ListingService) Top(ctx context.Context, at time.Time, n int) ([]ports.TopEntry, error) {
	return s.views.Top(ctx, at, n)
}

// ReplicationLag — сколько строк на primary и сколько на реплике.
//
// Диагностика ради наглядности: сразу после загрузки числа могут отличаться,
// через мгновение сравняются. Это и есть отставание реплики — то самое
// «eventual consistency», о котором обычно читают в теории.
func (s *ListingService) ReplicationLag(ctx context.Context) (primary, replica int64, err error) {
	primary, err = s.repo.CountOn(ctx, "primary")
	if err != nil {
		return 0, 0, err
	}
	replica, err = s.repo.CountOn(ctx, "replica")
	if err != nil {
		return 0, 0, err
	}
	return primary, replica, nil
}

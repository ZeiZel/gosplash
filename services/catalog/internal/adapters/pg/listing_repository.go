// Package pg — адаптер каталога к PostgreSQL.
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"

	"gosplash/services/catalog/internal/domain"
)

// ListingRow — строка витрины. Отдельно от domain.Listing по той же причине,
// что и в media: схема таблицы меняется чаще предметной области.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ — как хранятся Tags и Thumbnails:
//
//	Tags       → нативный Postgres text[]. Список тегов — однородный набор
//	             скаляров, а не структура с собственными атрибутами, и
//	             ListListings обязана фильтровать по нему через "содержит
//	             любой из": оператор массива && (overlap) — ровно то, для
//	             чего массив и придуман, с GIN-индексом (см. auto.go —
//	             AutoMigrate не умеет GIN, индекс создаётся отдельным SQL).
//	             Отдельная таблица listing_tags(listing_id, tag) была бы
//	             "правильной" нормализацией, но она нужна, только если теги
//	             когда-нибудь станут отдельной сущностью со своими данными
//	             (счётчик карточек на тег, алиасы) — сейчас такого запроса
//	             в проекте нет, а джойн ради самого факта нормализации это
//	             цена без выгоды.
//	Thumbnails → jsonb. Это КАРТА размер→ключ (small/medium/large →
//	             storage_key), а не список — то есть ключи не однородны и
//	             оператор && тут не применим и не нужен: превью карточки
//	             всегда читаются и пишутся ЦЕЛИКОМ вместе с самой карточкой,
//	             отдельного запроса "найди карточки с превью size=large" в
//	             проекте нет. Тот же приём, что и у outbox.Headers: колонка
//	             типа jsonb, а в Go — []byte с уже сериализованным JSON,
//	             без промежуточного map[string]string на стороне GORM.
type ListingRow struct {
	ID       string `gorm:"type:uuid;primaryKey"`
	AuthorID int64  `gorm:"column:author_id;not null;index"`

	Title string   `gorm:"size:200"`
	Tags  []string `gorm:"type:text[]"`

	StorageKey string
	Thumbnails []byte `gorm:"type:jsonb;not null;default:'{}'"`

	PriceCents int64
	Currency   string `gorm:"size:10"`

	// draft → published → removed. Индекс по status: ListListings всегда
	// фильтрует "= published", без индекса это full scan таблицы карточек.
	Status      string `gorm:"size:20;not null;index"`
	PublishedAt *time.Time

	Width  int32
	Height int32
}

func (ListingRow) TableName() string { return "listings" }

// cursorKey — (published_at, id) строки для keyset-курсора. См. trimPage
// в cursor.go: вынесено методом, а не полем, ровно затем, чтобы trimPage
// работал и с ListingRow, и с listingWithAuthor (embedding), не зная про
// JOIN с authors_snapshot вовсе.
func (r ListingRow) cursorKey() (time.Time, string, bool) {
	if r.PublishedAt == nil {
		return time.Time{}, "", false
	}
	return *r.PublishedAt, r.ID, true
}

// AuthorSnapshotRow — денормализованный слепок автора: id → отображаемое имя.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ — почему ОТДЕЛЬНАЯ таблица, а не колонка author_name
// прямо в listings: имя автора меняется НЕЗАВИСИМО от карточек (человек
// поправил профиль), и при колонке в listings пришлось бы UPDATE-ить КАЖДУЮ
// карточку этого автора при каждом переименовании — а карточек у активного
// автора могут быть сотни, тогда как переименование JOIN'ом стоит один раз
// на каждое ЧТЕНИЕ, а не один раз на каждую карточку при каждой ЗАПИСИ имени.
// Денормализация ради скорости чтения не означает "продублировать одно и то
// же значение везде, где оно упомянуто" — здесь она означает "не ходить в
// другой сервис за именем при каждом GetListing", и с этим прекрасно
// справляется одна маленькая таблица и JOIN по author_id.
type AuthorSnapshotRow struct {
	ID        int64  `gorm:"primaryKey"`
	Name      string `gorm:"size:200;not null"`
	UpdatedAt time.Time
}

func (AuthorSnapshotRow) TableName() string { return "authors_snapshot" }

// listingWithAuthor — проекция JOIN listings ⋈ authors_snapshot. Отдельный
// тип (встраивание ListingRow), а не два отдельных Scan: GORM сам разложит
// колонки результата в поля по имени/тегу, включая "лишнее" author_name,
// которого нет в самой ListingRow.
type listingWithAuthor struct {
	ListingRow
	AuthorName string
}

func toDomain(r *ListingRow, authorName string) (*domain.Listing, error) {
	thumbnails, err := unmarshalThumbnails(r.Thumbnails)
	if err != nil {
		return nil, err
	}
	l := &domain.Listing{
		ID:         r.ID,
		AuthorID:   r.AuthorID,
		AuthorName: authorName,
		Title:      r.Title,
		Tags:       append([]string(nil), r.Tags...),
		StorageKey: r.StorageKey,
		Thumbnails: thumbnails,
		PriceCents: r.PriceCents,
		Currency:   r.Currency,
		Status:     r.Status,
		Width:      r.Width,
		Height:     r.Height,
	}
	if r.PublishedAt != nil {
		l.PublishedAt = *r.PublishedAt
	}
	return l, nil
}

func unmarshalThumbnails(raw []byte) (map[string]string, error) {
	if len(raw) == 0 {
		return map[string]string{}, nil
	}
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("разбор thumbnails: %w", err)
	}
	if m == nil {
		m = map[string]string{}
	}
	return m, nil
}

func marshalThumbnails(m map[string]string) ([]byte, error) {
	if m == nil {
		m = map[string]string{}
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("сериализация thumbnails: %w", err)
	}
	return data, nil
}

// ListingRepository — доступ к каталогу НА ЧТЕНИЕ (см. ports.ListingRepository
// про то, почему запись живёт отдельно, в UnitOfWork).
//
// Соединение здесь одно, но за ним стоят ДВА сервера: primary и реплика.
// Плагин dbresolver разводит запросы сам, по типу SQL:
//
//	INSERT / UPDATE / DELETE → primary
//	SELECT                   → реплика
//
// В коде репозитория это не видно вообще — и в этом весь смысл. Разделение
// чтения и записи не должно протекать в бизнес-логику.
type ListingRepository struct {
	db *gorm.DB
}

func NewListingRepository(db *gorm.DB) *ListingRepository {
	return &ListingRepository{db: db}
}

func (r *ListingRepository) GetByID(ctx context.Context, id string) (*domain.Listing, error) {
	var row listingWithAuthor
	err := r.db.WithContext(ctx).
		Table("listings").
		Select("listings.*, COALESCE(authors_snapshot.name, '') AS author_name").
		Joins("LEFT JOIN authors_snapshot ON authors_snapshot.id = listings.author_id").
		Where("listings.id = ?", id).
		First(&row).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("карточка: %w", err)
	}
	return toDomain(&row.ListingRow, row.AuthorName)
}

// List — лента. Читается с РЕПЛИКИ, только status=published, keyset-
// пагинация по (published_at, id) DESC — см. cursor.go про то, почему не
// OFFSET.
func (r *ListingRepository) List(ctx context.Context, limit int, cursor string, tags []string) ([]domain.Listing, string, error) {
	var cur *pageCursor
	if cursor != "" {
		c, err := decodeCursor(cursor)
		if err != nil {
			return nil, "", fmt.Errorf("курсор: %w", err)
		}
		cur = &c
	}

	q := r.db.WithContext(ctx).
		Table("listings").
		Select("listings.*, COALESCE(authors_snapshot.name, '') AS author_name").
		Joins("LEFT JOIN authors_snapshot ON authors_snapshot.id = listings.author_id").
		Where("listings.status = ?", domain.StatusPublished).
		Order("listings.published_at DESC, listings.id DESC").
		// +1 — способ узнать "есть ли следующая страница", не делая
		// отдельный COUNT(*): если приехало limit+1 строк, последняя лишняя,
		// её отрезаем и по НЕЙ строим next_cursor.
		Limit(limit + 1)

	if len(tags) > 0 {
		// Оператор && — "массивы пересекаются": true, если у карточки есть
		// ХОТЯ БЫ ОДИН из запрошенных тегов. GIN-индекс на listings.tags
		// (auto.go) делает это дешевле последовательного скана.
		q = q.Where("listings.tags && ?", tags)
	}
	if cur != nil {
		// Композитное сравнение кортежей — то, ради чего вообще существует
		// keyset-пагинация: "строго после точки, где остановились", без
		// пропуска и без дубликатов на границе страницы, даже если у двух
		// карточек совпал published_at до микросекунды (id как тай-брейк).
		q = q.Where("(listings.published_at, listings.id) < (?, ?)", cur.PublishedAt, cur.ID)
	}

	var fetched []listingWithAuthor
	if err := q.Find(&fetched).Error; err != nil {
		return nil, "", fmt.Errorf("лента: %w", err)
	}

	rows, nextCursor := trimPage(fetched, limit)

	listings := make([]domain.Listing, 0, len(rows))
	for i := range rows {
		l, err := toDomain(&rows[i].ListingRow, rows[i].AuthorName)
		if err != nil {
			return nil, "", err
		}
		listings = append(listings, *l)
	}
	return listings, nextCursor, nil
}

// CountOn — сколько строк на конкретном узле ("primary" | "replica").
//
// dbresolver.Read / dbresolver.Write заставляют запрос уйти на выбранный
// узел вопреки обычному правилу. Диагностика отставания реплики, унаследована
// из фазы 0 без изменений.
func (r *ListingRepository) CountOn(ctx context.Context, node string) (int64, error) {
	var n int64
	q := r.db.WithContext(ctx).Model(&ListingRow{})

	switch node {
	case "primary":
		q = q.Clauses(dbresolver.Write)
	case "replica":
		q = q.Clauses(dbresolver.Read)
	default:
		return 0, fmt.Errorf("неизвестный узел %q", node)
	}

	if err := q.Count(&n).Error; err != nil {
		return 0, err
	}
	return n, nil
}

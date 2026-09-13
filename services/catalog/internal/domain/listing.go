// Package domain — предметная область каталога.
//
// Тегов инфраструктуры здесь нет: строка таблицы живёт в
// internal/adapters/pg, представление для HTTP — в internal/adapters/http,
// представление для gRPC — в internal/adapters/grpc. Ошибки — переменные,
// сравниваемые через errors.Is; адаптеры переводят их в свой язык (HTTP —
// статус, gRPC — codes), обратного перевода нет: домен не обязан знать,
// что такое HTTP или gRPC.
package domain

import (
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("карточка не найдена")

	// ErrPhotoNotFound — media ответила NotFound на GetPhoto: фото удалено
	// или никогда не существовало под этим id. Отдельная переменная от
	// ErrNotFound: тот означает «нет строки в каталоге», этот — «нет фото
	// в media», и различие важно для классификации ошибок консьюмера
	// (см. internal/app/indexer.go): «карточки ещё нет» бывает временным
	// (событие thumbnail-ready обогнало uploaded), «фото не существует» —
	// постоянно, повторять бессмысленно.
	ErrPhotoNotFound = errors.New("фото не найдено в media")

	// ErrLicenseNotFound используется только внутри adapters/pg — наружу
	// (RevokeLicense) это НЕ ошибка, а revoked=false, см. license.go.
	ErrLicenseNotFound = errors.New("лицензия не найдена")
)

// Статусы жизненного цикла карточки.
const (
	StatusDraft     = "draft"
	StatusPublished = "published"
	StatusRemoved   = "removed"
)

// Listing — денормализованная витрина каталога.
//
// PATTERN: CQRS read-модель. В отличие от фазы 0 (урезанная копия фото),
// здесь она уже настоящая: в ней данные ТРЁХ источников — фото из media,
// имя автора из authors_snapshot, цена (пока не приходит ни от одного
// сервиса, см. комментарий у полей ниже). Смысл проекции тот же: один
// запрос к одной базе вместо походов в media и wallet на каждый показ.
// Плата — согласованность в конечном счёте: карточка может на секунды
// отставать от события, которое её породило.
type Listing struct {
	ID       string
	AuthorID int64
	// AuthorName сюда попадает ТОЛЬКО через JOIN с authors_snapshot в момент
	// чтения (adapters/pg) — в строке таблицы listings этого поля нет.
	// Причина в файле authors_snapshot: имя автора меняется независимо от
	// карточек (профиль отредактировали) и НЕ должно требовать обновления
	// каждой карточки, где он упомянут: денормализация ради скорости чтения
	// не означает "дублировать одно и то же значение в тысяче строк".
	AuthorName string

	Title string
	// Tags хранится в Postgres как text[] (нативный массив), не таблица и
	// не jsonb — см. обоснование в adapters/pg/listing_repository.go у
	// ListingRow.Tags.
	Tags []string

	StorageKey string
	// Thumbnails — размер превью → ключ в S3. jsonb в базе, не таблица —
	// см. обоснование там же, у ListingRow.Thumbnails.
	Thumbnails map[string]string

	// PriceCents и Currency обещаны контрактом (proto/gosplash/catalog/v1),
	// но ни один источник событий в этой фазе их не поставляет: media
	// не знает цену, а order/wallet ещё не публикуют событие «цена
	// назначена». Поэтому сейчас они всегда нулевые — это ЗАФИКСИРОВАННЫЙ
	// пробел, а не забытое поле, см. отчёт о работе.
	PriceCents int64
	Currency   string

	// Status: draft (создана) → published (превью готовы) → removed
	// (не реализовано в этой фазе — media.photo.deleted не входит в её объём).
	Status string
	// PublishedAt — нулевое время, пока карточка в статусе draft.
	PublishedAt time.Time

	Width  int32
	Height int32
}

// Package domain — предметная область аналитики: периоды отчётов, строки
// сводок и факты, которые сервис накапливает.
//
// Здесь сознательно нет ни строки ClickHouse (adapters/clickhouse), ни
// protobuf-типа gRPC-ответа (adapters/grpc) — оба выражают одно и то же
// понятие ("просмотр", "покупка", "период отчёта") на языке своей
// инфраструктуры, а домен не обязан о ней знать (docs/STYLE.md).
package domain

import (
	"errors"
	"fmt"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Ошибки. Сравниваются через errors.Is; адаптер (internal/adapters/grpc)
// переводит их в codes.InvalidArgument — сам домен не знает, что такое gRPC.
// ─────────────────────────────────────────────────────────────────────────────

var (
	ErrInvalidPeriod = errors.New("неизвестный период: допустимы day, week, month")
	ErrInvalidLimit  = errors.New("limit должен быть в диапазоне 1..1000")
	ErrEmptyPhotoID  = errors.New("photo_id обязателен")
	ErrEmptyOrderID  = errors.New("order_id обязателен")
	// ErrInvalidAuthorID — author_id <= 0. Ноль и отрицательные — это
	// «поля нет» (proto3 не отличает явный ноль от незаполненного), а не
	// «автор с таким номером»: сводка по несуществующему автору — пустой
	// ответ, а не ошибка, поэтому валидируется именно диапазон.
	ErrInvalidAuthorID = errors.New("author_id должен быть положительным")
)

// ─────────────────────────────────────────────────────────────────────────────
// Period — окно отчёта.
// ─────────────────────────────────────────────────────────────────────────────

type Period string

const (
	PeriodDay   Period = "day"
	PeriodWeek  Period = "week"
	PeriodMonth Period = "month"
)

// ParsePeriod разбирает строку из gRPC-запроса (proto: "day | week | month").
//
// Валидация живёт в домене, а не в адаптере: правило "период — одно из трёх
// слов" — часть смысла отчёта, а не деталь протокола. Адаптер (gRPC) знает
// только, в какой code превратить ErrInvalidPeriod.
func ParsePeriod(raw string) (Period, error) {
	switch Period(raw) {
	case PeriodDay, PeriodWeek, PeriodMonth:
		return Period(raw), nil
	default:
		return "", fmt.Errorf("%w: получено %q", ErrInvalidPeriod, raw)
	}
}

// Duration — длина окна отчёта в календарных сутках.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: месяц здесь — 30×24ч, а не «первое число текущего
// календарного месяца». Точный календарный месяц потребовал бы знать
// часовой пояс наблюдателя (у "начала месяца по Москве" и "по UTC" разные
// границы), а разница в один-два дня ни на что не влияет для отчёта вида
// «топ фото за месяц». Простое скользящее окно этой цены не имеет.
func (p Period) Duration() time.Duration {
	switch p {
	case PeriodWeek:
		return 7 * 24 * time.Hour
	case PeriodMonth:
		return 30 * 24 * time.Hour
	default:
		return 24 * time.Hour
	}
}

// Since возвращает начало окна периода относительно now.
func (p Period) Since(now time.Time) time.Time {
	return now.Add(-p.Duration())
}

// ─────────────────────────────────────────────────────────────────────────────
// Границы входных параметров.
// ─────────────────────────────────────────────────────────────────────────────

const (
	MinTopPhotosLimit     = 1
	MaxTopPhotosLimit     = 1000
	DefaultTopPhotosLimit = 100
)

// ValidateLimit проверяет limit из TopPhotosRequest.
//
// limit == 0 — не ошибка, а «клиент не указал», это частый случай для
// необязательного поля proto3 (там нет способа отличить «явный ноль» от
// «поле не заполнено»). DefaultTopPhotosLimit — то, что подставляется вместо
// него в internal/app, а не здесь: домен здесь только проверяет, ЧТО пришло,
// а решение о дефолте — уже сценарий (см. app/service.go).
func ValidateLimit(limit int32) error {
	if limit == 0 {
		return nil
	}
	if limit < MinTopPhotosLimit || limit > MaxTopPhotosLimit {
		return fmt.Errorf("%w: получено %d", ErrInvalidLimit, limit)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Строки сводок — то, что отдаёт наружу AnalyticsService.
// ─────────────────────────────────────────────────────────────────────────────

// PhotoRank — одна строка топа просмотров (proto: PhotoRank).
type PhotoRank struct {
	PhotoID   string
	AuthorID  int64
	Views     int64
	Purchases int64
}

// PhotoStats — сводка по одному фото за период (proto: PhotoStatsResponse).
type PhotoStats struct {
	PhotoID string
	Views   int64
	// UniqueViewersApprox — приблизительное число уникальных зрителей.
	// Откуда берётся приближение и почему не Redis HLL — см. комментарий
	// пакета internal/adapters/clickhouse/repository.go и ADR 0016.
	UniqueViewersApprox int64
	Purchases           int64
	RevenueCents        int64
}

// FunnelStats — воронка «загружено → превью готово» за период (proto:
// FunnelStatsResponse). Считается из тех же соображений, что и PhotoStats:
// сводные числа, а не сырые строки.
type FunnelStats struct {
	Uploads      int64
	Thumbnails   int64
	ReadyPercent float64
	LatencyP50Ms int64
	LatencyP95Ms int64
}

// AuthorStats — сводка по одному автору за период (proto: AuthorStatsResponse).
// Тот же состав чисел, что у PhotoStats, плюс Uploads: автор — единственная
// сущность, у которой «сколько загрузил» имеет смысл (у фото ровно одна
// загрузка по определению).
type AuthorStats struct {
	Views               int64
	UniqueViewersApprox int64
	Purchases           int64
	RevenueCents        int64
	Uploads             int64
}

// ─────────────────────────────────────────────────────────────────────────────
// Факты, приходящие из Kafka (analytics.photo.viewed, order.order.paid,
// media.photo.uploaded, media.photo.thumbnail-ready).
// Отдельные от protobuf-payload типы — как раз тот случай, когда домен и
// провод расходятся: PhotoViewed.viewer_id=0 в protobuf означает анонима
// (см. proto/gosplash/events/v1/events.proto), а в ClickHouse анонимный
// просмотр — это NULL в Nullable(Int64), а не число 0. Конвертация 0→nil
// сделана в internal/adapters/kafka (там, где Envelope превращается в
// доменный факт), а не здесь: домен просто описывает, что означает
// «зрителя не было» — указателем, а не магическим нулём.
// ─────────────────────────────────────────────────────────────────────────────

// PhotoView — один просмотр фото.
type PhotoView struct {
	PhotoID  string
	AuthorID int64
	// ViewerID == nil — анонимный просмотр.
	ViewerID   *int64
	Country    string
	OccurredAt time.Time
}

// Purchase — одна оплаченная покупка (order.order.paid).
type Purchase struct {
	OrderID    string
	PhotoID    string
	AuthorID   int64
	BuyerID    int64
	PriceCents int64
	OccurredAt time.Time
}

// Upload — «автор загрузил фото» (media.photo.uploaded). Отдельный факт, а не
// атрибут фото: воронке FunnelStats важен МОМЕНТ загрузки (лаг до
// thumbnail-ready считается между двумя событиями), поэтому это строка в
// таблице фактов uploads, а не колонка где-то ещё.
type Upload struct {
	PhotoID string
	// AuthorID — это PhotoUploaded.user_id: в media user_id — ключ
	// шардирования и одновременно владелец фото, то есть автор (см.
	// комментарий к полю в events.proto). Переименование на границе
	// Kafka→домен, а не в самом домене: аналитика говорит языком авторов.
	AuthorID   int64
	OccurredAt time.Time
}

// Thumbnail — «превью фото готовы» (media.photo.thumbnail-ready). Событие
// несёт и user_id, и список ключей превью (см. events.proto), но факт здесь
// ОДНОЗНАЧНО тоньше: воронке нужен только момент, когда фото стало готово
// к показу, — остальное потребители события (catalog) используют по месту.
type Thumbnail struct {
	PhotoID    string
	OccurredAt time.Time
}

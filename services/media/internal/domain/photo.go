// Package domain — предметная область media-сервиса: что такое фото и какие
// правила про него верны всегда, независимо от того, где оно хранится.
//
// Здесь НЕТ тегов gorm, json и protobuf, нет импортов инфраструктуры и
// сгенерированного кода. Это не догма ради догмы: как только в доменную
// структуру попадает `gorm:"primaryKey"`, изменение схемы таблицы начинает
// править доменную модель, а доменная модель — это то, что меняться должно
// реже всего. Строка таблицы живёт в internal/adapters/pg и отображается
// в этот тип вручную.
package domain

import (
	"errors"
	"time"
)

// Ошибки домена. Адаптеры переводят их в свой язык: HTTP-хендлер в 404,
// gRPC-сервер в codes.NotFound. Обратного перевода нет — домен не знает,
// что такое HTTP.
var (
	ErrNotFound      = errors.New("фото не найдено")
	ErrNoUserID      = errors.New("не указан user_id")
	ErrNotAnImage    = errors.New("файл не является изображением")
	ErrEmptyFileName = errors.New("не указано имя файла")
)

// Статусы жизненного цикла фото.
//
//	uploaded   — оригинал в хранилище, метаданные в базе;
//	processing — thumbnail-worker взял в работу;
//	ready      — превью готовы, фото можно показывать в каталоге;
//	deleted    — мягкое удаление: строка ОСТАЁТСЯ в базе, меняется только
//	             статус. Настоящее стирание файла из S3 и строки из базы —
//	             отдельная работа саги удаления (фаза 3, вне этого сервиса):
//	             пока событие media.photo.deleted не обработано подписчиками
//	             (catalog должен убрать карточку, thumbnail-worker — не
//	             начинать превью), удалять исходные данные преждевременно.
const (
	StatusUploaded   = "uploaded"
	StatusProcessing = "processing"
	StatusReady      = "ready"
	StatusDeleted    = "deleted"
)

// Photo — фотография, загруженная автором.
//
// UserID здесь не просто «чьё фото»: это ключ шардирования. Таблица photos
// разрезана по нему между несколькими базами, поэтому без user_id сервис
// физически не знает, где искать строку, — и это протекает наружу, вплоть до
// gRPC-контракта (см. docs/adr/0002-*).
type Photo struct {
	ID     string
	UserID int64

	Title      string
	StorageKey string
	Mime       string
	SizeBytes  int64
	Width      int
	Height     int

	Status string

	CreatedAt time.Time
	UpdatedAt time.Time
}

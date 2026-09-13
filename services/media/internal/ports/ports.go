// Package ports — интерфейсы, через которые прикладной слой разговаривает
// с внешним миром.
//
// PATTERN: hexagonal (ports & adapters). Интерфейс объявлен здесь, рядом
// с тем, кто им ПОЛЬЗУЕТСЯ, а не рядом с реализацией. Это разворачивает
// направление зависимости: app зависит от собственного интерфейса, а
// adapters/pg зависит от app — а не наоборот. Практическая польза ровно две:
// прикладной слой тестируется без базы и без S3, и замена GORM на что угодно
// другое не трогает ни строчки в app.
//
// Интерфейсы маленькие и по одному на роль. Один большой Repository со
// всеми методами сразу удобно писать и мучительно мокать.
package ports

import (
	"context"
	"io"
	"time"

	"gosplash/services/media/internal/domain"
)

// PhotoRepository — хранилище метаданных.
//
// user_id есть в сигнатуре ВСЕХ методов, даже там, где по смыслу хватило бы
// id. Это цена шардирования, и её платит каждый вызывающий.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: Create и Delete не просто пишут строку — они же
// отвечают за факт "фото загружено"/"фото удалено". Раньше (фаза 0) рядом
// был отдельный порт EventPublisher, и прикладной слой вызывал его ПОСЛЕ
// успешного Create; ADR 0008 объясняет, почему это в принципе не могло
// быть надёжным (окно между коммитом и публикацией). Мы не стали чинить
// EventPublisher переводом на outbox.Write, потому что тогда сценарию
// пришлось бы САМОМУ открывать транзакцию Postgres и передавать её в оба
// порта — то есть импортировать gorm, а это как раз то, что docs/STYLE.md
// запрещает internal/app. Вместо этого граница порта сдвинута: адаптер
// (adapters/pg.PhotoRepository), у которого и так есть *gorm.DB, открывает
// транзакцию сам и одним вызовом Create/Delete гарантирует, что строка
// photos и связанная с ней строка outbox либо обе попадут в базу, либо не
// попадёт ни одна. Прикладной слой при этом по-прежнему не знает, что такое
// Kafka, конверт или топик, — он просто вызывает Create(photo) и получает
// либо nil, либо ошибку "не получилось ничего из этого".
//
// Отдельного параметра "событие" у Create/Delete нет: и для photo.uploaded,
// и для photo.deleted всё содержимое события (photo_id, user_id) целиком
// выводится из уже переданных аргументов, поэтому провозить через порт
// ещё и protobuf-структуру payload'а — это тащить в ports (и, будь он там,
// в internal/app) знание о конкретном контракте события без всякой пользы:
// адаптер и так знает, какое событие соответствует какому методу.
type PhotoRepository interface {
	Create(ctx context.Context, photo *domain.Photo) error
	GetByID(ctx context.Context, userID int64, id string) (*domain.Photo, error)
	ListByUser(ctx context.Context, userID int64, limit int) ([]domain.Photo, error)
	// Delete — мягкое удаление: статус меняется на domain.StatusDeleted,
	// строка остаётся. domain.ErrNotFound, если фото нет или оно уже
	// удалено (повторный вызов не должен эмитировать событие ещё раз).
	Delete(ctx context.Context, userID int64, id string) error

	// ShardOf — номер шарда пользователя. Нужен только чтобы показать
	// шардирование в ответе API; бизнес-логика на него не опирается.
	ShardOf(userID int64) int
	// CountByShard — сколько строк на каждом шарде. Диагностика, пример
	// scatter-gather.
	CountByShard(ctx context.Context) ([]int64, error)
}

// ObjectStorage — хранилище байтов (локально MinIO).
//
// Сигнатуры совпадают с *s3x.Client, поэтому клиент удовлетворяет порту
// напрямую, без адаптера-прослойки. Так и должно быть: адаптер пишут тогда,
// когда формы не совпадают, а не «на всякий случай».
type ObjectStorage interface {
	Put(ctx context.Context, bucket, key string, reader io.Reader, size int64, contentType string) error
	PresignedURL(ctx context.Context, bucket, key string, ttl time.Duration) (string, error)
}

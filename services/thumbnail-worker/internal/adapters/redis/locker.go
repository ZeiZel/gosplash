// Package redis — адаптер распределённого лока thumbnail-worker'а поверх
// pkg/redisx.
//
// ПОЧЕМУ ЛОК ЗДЕСЬ ВООБЩЕ ЕСТЬ, при том что processed_events (идемпотентность,
// см. internal/adapters/pg.CommitReady) уже даёт настоящую гарантию "факт
// применится ровно один раз": лок — это ОПТИМИЗАЦИЯ, а не вторая гарантия
// того же самого. При ребалансе партиций media.photo.uploaded два инстанса
// воркера могут ненадолго ОБА решить, что они обрабатывают одно и то же
// фото (pkg/redisx.Lock честно объясняет в своём doc-комментарии, почему
// сам факт владения локом от этого не защищает — сетевое разделение и паузы
// GC ломают его в общем случае). Без лока оба инстанса дублируют самую
// дорогую часть работы — декодирование и ресайз изображения, три загрузки
// в S3 — прежде чем один из них (неважно, какой) первым дойдёт до
// processed_events и заберёт "победу"; второй просто не пройдёт Claim.
// Лок сокращает частоту этой гонки до редкой, а не устраняет её — итоговая
// корректность держится ИСКЛЮЧИТЕЛЬНО на CommitReady внутри одной
// транзакции, а не на факте владения локом.
package redis

import (
	"context"
	"time"

	"gosplash/pkg/redisx"
	"gosplash/services/thumbnail-worker/internal/ports"
)

// lockKind — первый сегмент ключа (см. pkg/redisx.Client.Key): заодно
// это label "kind" в метриках hit/miss пакета redisx, хотя лок метрики
// кэша не использует, единообразие ключа стоит того, чтобы не заводить
// исключение из общего правила "все ключи только через Key(...)".
const lockKind = "thumbnail-lock"

// Locker — реализация ports.Locker поверх *redisx.Client.
type Locker struct {
	client *redisx.Client
}

func NewLocker(client *redisx.Client) *Locker {
	return &Locker{client: client}
}

// AcquirePhotoLock прячет от прикладного слоя формат ключа: он строится
// ЕДИНСТВЕННЫМ разрешённым в проекте способом — client.Key(...) (namespace
// gosplash:<service>:..., см. package doc pkg/redisx/client.go).
func (l *Locker) AcquirePhotoLock(ctx context.Context, photoID string, ttl time.Duration) (ports.Lock, error) {
	key := l.client.Key(lockKind, photoID)

	lock, err := l.client.Acquire(ctx, key, ttl)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		// Лок занят — это НЕ ошибка (см. pkg/redisx.Client.Acquire), а
		// нормальный проигрыш в гонке. Явный ранний return вместо того,
		// чтобы просто вернуть lock как есть: *redisx.Lock(nil), завёрнутый
		// в интерфейс ports.Lock, был бы НЕ nil интерфейсом (классическая
		// ловушка typed nil) — вызывающий код сравнивал бы "lock == nil"
		// и получал бы false там, где на самом деле лока нет.
		return nil, nil
	}
	return lock, nil
}

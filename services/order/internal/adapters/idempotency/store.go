// Package idempotency реализует ports.IdempotencyStore: HTTP idempotency-key
// поверх Redis (быстрый путь) с fallback-таблицей idempotency_keys
// в PostgreSQL (источник правды). Полное обоснование — docs/adr/0018-*.md.
//
// PATTERN: idempotency-key с двухуровневым хранилищем.
//
// pkg/redisx.Client.Begin/Complete уже даёт SET NX EX ttl — атомарное
// «занять ключ или узнать его состояние» одной командой. Для большинства
// сервисов этого достаточно. Для order — нет: Begin/Complete в чистом Redis
// не переживает потерю ключа (вытеснение по памяти под давлением, рестарт
// Redis без AOF/RDB persistence — см. deploy/compose/redis.yml, где
// persistence сознательно не настроена ради простоты локальной разработки).
// Потерянный ключ на операции «списать деньги» означает, что повторный
// запрос с тем же Idempotency-Key увидит «свободно» и спишет деньги ВТОРОЙ
// раз — ровно то, что идемпотентность обязана предотвращать.
//
// Поэтому Redis здесь — ускоритель горячего пути (почти все повторы
// приходят, пока ключ ещё свежий в кэше), а не источник правды. PostgreSQL
// проверяется в ДВУХ случаях: Redis недоступен целиком, или Redis сказал
// «свободно» (что требует подтверждения, потому что «свободно» — это
// единственный исход Redis, который в принципе может быть ЛОЖНЫМ из-за
// потери ключа; IdempotencyInProgress/IdempotencyDone Redis не подделает
// сам по себе, если ключ найден). Платим за это лишним походом в PostgreSQL
// ровно в тех случаях, когда Redis не может дать однозначный ответ, — не
// на каждый запрос.
package idempotency

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"gosplash/pkg/redisx"
	"gosplash/services/order/internal/adapters/pg"
	"gosplash/services/order/internal/ports"
)

// Store — ports.IdempotencyStore.
type Store struct {
	redis *redisx.Client
	db    *gorm.DB
	ttl   time.Duration
}

func New(redis *redisx.Client, db *gorm.DB, ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = redisx.DefaultIdempotencyTTL
	}
	return &Store{redis: redis, db: db, ttl: ttl}
}

func (s *Store) Begin(ctx context.Context, key string) (ports.IdempotencyResult, error) {
	redisKey := s.redis.Key("idempotency", key)
	result, err := s.redis.Begin(ctx, redisKey, s.ttl)
	switch {
	case err != nil:
		// Redis недоступен целиком (не «ключ не найден» — это отдельная
		// ветка ниже, err==nil). Деградируем на PostgreSQL напрямую: он
		// источник правды и обязан уметь работать в одиночку, пусть
		// и медленнее.
		slog.WarnContext(ctx, "idempotency: redis недоступен, работаю через PostgreSQL", "error", err)
		return s.beginPG(ctx, key)
	case result.Status == redisx.IdempotencyInProgress:
		return ports.IdempotencyResult{Status: ports.IdempotencyInProgress}, nil
	case result.Status == redisx.IdempotencyDone:
		return ports.IdempotencyResult{Status: ports.IdempotencyDone, Response: result.Response}, nil
	default:
		// redisx.IdempotencyFree — ключ либо действительно новый, либо
		// Redis его потерял. Не доверяем этому исходу вслепую: пусть
		// PostgreSQL подтвердит.
		return s.beginPG(ctx, key)
	}
}

func (s *Store) Complete(ctx context.Context, key string, response []byte) error {
	// PostgreSQL — источник правды, пишем в него БЕЗУСЛОВНО и первым.
	err := s.db.WithContext(ctx).Model(&pg.IdempotencyKeyRow{}).Where("key = ?", key).
		Updates(map[string]any{
			"status":   pg.IdempotencyStatusDone,
			"response": response,
		}).Error
	if err != nil {
		return fmt.Errorf("idempotency: завершение в postgres: %w", err)
	}

	// Redis — не источник правды: провал записи в кэш не должен провалить
	// уже завершённую бизнес-операцию, только замедлить следующий повтор
	// (он упадёт в beginPG и получит тот же корректный ответ оттуда).
	if err := s.redis.Complete(ctx, s.redis.Key("idempotency", key), response, s.ttl); err != nil {
		slog.WarnContext(ctx, "idempotency: не удалось обновить redis", "error", err)
	}
	return nil
}

// beginPG — авторитетная проверка/занятие ключа в PostgreSQL.
func (s *Store) beginPG(ctx context.Context, key string) (ports.IdempotencyResult, error) {
	row := &pg.IdempotencyKeyRow{Key: key, Status: pg.IdempotencyStatusInProgress}

	// ON CONFLICT DO NOTHING — тот же приём, что и SET NX в Redis: атомарная
	// проверка «ключа ещё не было» и его занятие одним запросом. Отдельные
	// SELECT, затем INSERT впустили бы ту же гонку, от которой SET NX
	// защищает в Redis.
	res := s.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(row)
	if res.Error != nil {
		return ports.IdempotencyResult{}, fmt.Errorf("idempotency: занятие ключа в postgres: %w", res.Error)
	}
	if res.RowsAffected == 1 {
		return ports.IdempotencyResult{Status: ports.IdempotencyFree}, nil
	}

	var existing pg.IdempotencyKeyRow
	if err := s.db.WithContext(ctx).Where("key = ?", key).First(&existing).Error; err != nil {
		return ports.IdempotencyResult{}, fmt.Errorf("idempotency: чтение существующего ключа: %w", err)
	}

	if existing.Status == pg.IdempotencyStatusDone {
		// Redis только что сказал «свободно», а PostgreSQL помнит готовый
		// ответ — значит, Redis действительно потерял ключ. Прогреваем
		// кэш заново, чтобы следующий повтор не бил по базе снова.
		if err := s.redis.Complete(ctx, s.redis.Key("idempotency", key), existing.Response, s.ttl); err != nil {
			slog.WarnContext(ctx, "idempotency: не удалось прогреть redis после расхождения с postgres", "error", err)
		}
		return ports.IdempotencyResult{Status: ports.IdempotencyDone, Response: existing.Response}, nil
	}
	return ports.IdempotencyResult{Status: ports.IdempotencyInProgress}, nil
}

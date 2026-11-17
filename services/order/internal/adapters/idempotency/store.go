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
//
// Строки в PostgreSQL тоже подчиняются TTL: in_progress старше TTL считается
// протухшим (держатель погиб между Begin и Complete) и может быть перезанят,
// done-строки и совсем старые in_progress удаляются спустя TTL + день запаса
// (см. beginPG, cleanupExpired и «Обновление» в docs/adr/0018-*.md).
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

// expiredRetention — запас сверх TTL, в течение которого строка живёт
// в PostgreSQL ПОСЛЕ того, как её Redis-копия уже обязана была истечь.
// Повтор, опоздавший к Redis-ключу на секунды, ещё получает сохранённый
// ответ из источника правды; спустя TTL+retention строка удаляется —
// таблица не растёт бесконечно, а ключ для Redis и так уже «новый».
const expiredRetention = 24 * time.Hour

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

	// Чистка старых строк «на плечах» Complete, а не отдельным тикером:
	// Complete и так случается на каждый созданный заказ, лишний
	// планировщик ради одного DELETE был бы больше кода, чем сама чистка.
	// Провал чистки не проваливает Complete — операция уже завершена,
	// таблицу подметёт следующее завершение (см. cleanupExpired).
	if err := s.cleanupExpired(ctx); err != nil {
		slog.WarnContext(ctx, "idempotency: не удалось удалить старые ключи из postgres", "error", err)
	}
	return nil
}

// cleanupExpired удаляет строки старше TTL + expiredRetention: завершённые
// (done) — по updated_at (момент завершения), застрявшие (in_progress) —
// по created_at (момент занятия). Грань та же, что и у занятия в beginPG:
// дольше TTL ключ в Redis всё равно не живёт, значит повтор по прошествии
// TTL+retention — уже «новая операция» и с точки зрения быстрого пути, и
// с точки зрения источника правды; хранить её дольше — только расти
// таблице без пользы.
func (s *Store) cleanupExpired(ctx context.Context) error {
	cutoff := time.Now().Add(-(s.ttl + expiredRetention))
	err := s.db.WithContext(ctx).
		Where("status = ? AND updated_at < ?", pg.IdempotencyStatusDone, cutoff).
		Or("status = ? AND created_at < ?", pg.IdempotencyStatusInProgress, cutoff).
		Delete(&pg.IdempotencyKeyRow{}).Error
	if err != nil {
		return fmt.Errorf("idempotency: удаление старых ключей: %w", err)
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

	// in_progress: либо ЖИВОЙ параллельный вызов (ему — 409), либо след
	// процесса, упавшего между Begin и Complete: в Redis такой ключ истёк
	// бы по TTL, а авторитетная строка в PostgreSQL жила вечно (см. ADR
	// 0018, «известное ограничение»). Протухшим считаем in_progress, чья
	// эпоха занятия (created_at) старше того же TTL, что живёт Redis-копия.
	// Протухший ключ можно ПЕРЕЗАнять — атомарным UPDATE с тем же условием
	// в WHERE: из конкурентных повторов выиграет ровно один, остальные
	// увидят уже обновлённый created_at и получат RowsAffected=0 (тот же
	// приём «страж в WHERE», что ON CONFLICT у INSERT выше). Риск отнять
	// ключ у живого, но медленного запроса не выше, чем в Redis: TTL и там
	// не спрашивает держателя, завершился ли он.
	now := time.Now()
	claim := s.db.WithContext(ctx).Model(&pg.IdempotencyKeyRow{}).
		Where("key = ? AND status = ? AND created_at < ?",
			key, pg.IdempotencyStatusInProgress, now.Add(-s.ttl)).
		Updates(map[string]any{
			"status":     pg.IdempotencyStatusInProgress,
			"response":   nil,
			"created_at": now,
			"updated_at": now,
		})
	if claim.Error != nil {
		return ports.IdempotencyResult{}, fmt.Errorf("idempotency: перезанятие протухшего ключа в postgres: %w", claim.Error)
	}
	if claim.RowsAffected == 1 {
		// Ключ перезанят НАМИ: для вызывающего это обычный «свободно» —
		// прошлая попытка до Complete не дожила, операцию нужно выполнить
		// (и её Complete зафиксирует ответ уже для нас).
		return ports.IdempotencyResult{Status: ports.IdempotencyFree}, nil
	}
	return ports.IdempotencyResult{Status: ports.IdempotencyInProgress}, nil
}

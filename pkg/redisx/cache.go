package redisx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// PATTERN: cache-aside — читаем из Redis; промах — читаем из источника
// правды и кладём в Redis сами (в отличие от write-through, где запись в
// кэш делает сам источник правды на каждой записи). Разбор цены и почему
// это, а не write-through/refresh-ahead — в docs/adr/0013-cache-aside.md.
//
// Здесь же — вторая половина паттерна, без которой cache-aside в проде
// не переживает первый же наплыв трафика: защита от stampede (см. ниже).

// jitterFraction — амплитуда джиттера TTL, ±10% от заданного значения.
const jitterFraction = 0.10

// withJitter возвращает TTL, случайно сдвинутый в пределах ±10%.
//
// Зачем: тысяча карточек ленты, закэшированных в одну секунду с одним и тем
// же TTL=10м, протухнет ОДНОВРЕМЕННО. В этот момент придёт трафик — и тысяча
// запросов синхронно промахнутся мимо кэша и одновременно долбанут базу тем
// же залпом, каким когда-то был первый прогрев. Это и есть cache stampede.
// Джиттер размазывает истечение по времени: TTL из диапазона [0.9·ttl, 1.1·ttl]
// делает одновременное протухание тысяч ключей статистически невозможным —
// они протухают вразнобой, и база видит равномерный поток промахов, а не залп.
//
// Увеличение самого TTL (без джиттера) стемпед не лечит: оно только отодвигает
// момент синхронного протухания, не устраняя его синхронность — см. ADR 0013.
func withJitter(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	amplitude := float64(ttl) * jitterFraction
	// rand.Float64() ∈ [0,1) → delta ∈ [-amplitude, +amplitude).
	delta := (rand.Float64()*2 - 1) * amplitude
	return ttl + time.Duration(delta)
}

// GetOrLoadJSON — cache-aside для значений, которые сериализуются через
// encoding/json.
//
// Промах кэша схлопывается через singleflight: если на промах по ОДНОМУ
// ключу приходит N одновременных горутин в ЭТОМ процессе, load выполнится
// один раз, а N-1 горутин получат его результат без похода в базу.
//
// Честное ограничение: singleflight.Group живёт в памяти процесса и не видит
// другие реплики сервиса. При 5 репликах в момент протухания популярного
// ключа в базу уйдёт до 5 запросов (по одному на реплику), а не 5000
// (по одному на каждого одновременного пользователя) — то есть нагрузка
// падает в N_пользователей/N_реплик раз, а не до одного запроса. Для полного
// схлопывания между процессами нужен распределённый лок (см. lock.go) —
// здесь он не используется намеренно: лок на каждый промах кэша добавил бы
// сетевой round-trip к КАЖДОМУ читателю ради защиты, которая нужна только
// в момент реальной гонки, и сделал бы cache-aside дороже, чем поход в базу
// без кэша вовсе.
func GetOrLoadJSON[T any](ctx context.Context, c *Client, key string, ttl time.Duration, load func(context.Context) (T, error)) (T, error) {
	var zero T

	if v, ok, err := getJSON[T](ctx, c, key); err != nil {
		return zero, err
	} else if ok {
		c.recordHit(ctx, key)
		return v, nil
	}
	c.recordMiss(ctx, key)

	res, err, _ := c.sf.Do(key, func() (any, error) {
		v, err := load(ctx)
		if err != nil {
			return zero, err
		}
		data, err := json.Marshal(v)
		if err != nil {
			return zero, fmt.Errorf("redisx: json.Marshal: %w", err)
		}
		if err := c.rdb.Set(ctx, key, data, withJitter(ttl)).Err(); err != nil {
			// Не удалось положить в кэш — не повод проваливать запрос:
			// значение уже посчитано и валидно, просто следующий читатель
			// снова промахнётся. Логировать здесь нечем без контекста
			// вызывающего сервиса, поэтому ошибку возвращает только Set,
			// а не весь GetOrLoad.
			return v, nil
		}
		return v, nil
	})
	if err != nil {
		return zero, err
	}
	return res.(T), nil
}

func getJSON[T any](ctx context.Context, c *Client, key string) (T, bool, error) {
	var zero T
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, fmt.Errorf("redisx: get %q: %w", key, err)
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return zero, false, fmt.Errorf("redisx: json.Unmarshal %q: %w", key, err)
	}
	return v, true, nil
}

// protoPtr — ограничение для GetOrLoadProto: T — тип сообщения (структура,
// сгенерированная protoc), PT — указатель на T, реализующий proto.Message.
// Два параметра типа нужны ровно за тем, чтобы можно было материализовать
// новое пустое сообщение (var msg T; PT(&msg)) под proto.Unmarshal — при
// одном параметре T=proto.Message это было бы nil-интерфейсом, в который
// нечего разворачивать.
type protoPtr[T any] interface {
	*T
	proto.Message
}

// GetOrLoadProto — тот же cache-aside, что и GetOrLoadJSON, но для значений,
// которые сериализуются через protobuf.
//
// Выбор кодека — JSON или protobuf — сделан явно двумя разными функциями,
// а не одной с рефлексией внутри («если value — proto.Message, кодируем
// protobuf, иначе — JSON»). Неявный выбор по типу выглядит удобно ровно до
// первого случая, когда сообщение реализует MarshalJSON рядом с proto.Message
// (у сгенерированного кода такое бывает) — тогда магия начинает молча менять
// формат данных в Redis между релизами. Явные функции такого не допускают:
// вызывающий код сам решает и это видно в diff'е.
func GetOrLoadProto[T any, PT protoPtr[T]](ctx context.Context, c *Client, key string, ttl time.Duration, load func(context.Context) (PT, error)) (PT, error) {
	raw, err := c.rdb.Get(ctx, key).Bytes()
	switch {
	case err == nil:
		var msg T
		pt := PT(&msg)
		if uerr := proto.Unmarshal(raw, pt); uerr != nil {
			return nil, fmt.Errorf("redisx: proto.Unmarshal %q: %w", key, uerr)
		}
		c.recordHit(ctx, key)
		return pt, nil
	case errors.Is(err, redis.Nil):
		c.recordMiss(ctx, key)
	default:
		return nil, fmt.Errorf("redisx: get %q: %w", key, err)
	}

	res, err, _ := c.sf.Do(key, func() (any, error) {
		v, err := load(ctx)
		if err != nil {
			return nil, err
		}
		data, err := proto.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("redisx: proto.Marshal: %w", err)
		}
		if err := c.rdb.Set(ctx, key, data, withJitter(ttl)).Err(); err != nil {
			return v, nil
		}
		return v, nil
	})
	if err != nil {
		return nil, err
	}
	return res.(PT), nil
}

// Invalidate удаляет ключи через UNLINK, а не DEL.
//
// DEL освобождает память СИНХРОННО в главном потоке Redis: пока значение (а в
// кэше это может быть сериализованная страница листинга на сотни килобайт,
// или ключ, схлопнувший тысячи мелких через MSET) не будет полностью
// освобождено, Redis не обслужит ни одной другой команды — однопоточный
// сервер просто встанет на это время. UNLINK делает то же самое отличие
// в работе: сам ключ убирается из keyspace немедленно (последующий GET уже
// не увидит значения), а фактическое освобождение памяти уезжает в отдельный
// поток. Ровно поэтому в проекте (см. docs/STYLE.md) удаление — всегда
// UNLINK, а KEYS запрещён в принципе (обход — только SCAN).
func (c *Client) Invalidate(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := c.rdb.Unlink(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("redisx: unlink: %w", err)
	}
	return nil
}

package redisx

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// PATTERN: token bucket на Lua — атомарное пополнение по времени и попытка
// списать токен ЗА ОДНО обращение к Redis.
//
// Почему не INCR+EXPIRE (fixed window): INCR — это счётчик запросов в окне
// фиксированной длины (например, минуте), который сбрасывается в ноль по
// границе окна. Проблема в самой границе: клиент может выбрать 100 запросов
// в последнюю секунду окна N и ещё 100 — в первую секунду окна N+1. Redis
// увидит два разных счётчика, оба в пределах лимита, — а сервис за эти две
// секунды реально получит двойной лимит. Token bucket не имеет границ окон
// вообще: токены копятся и тратятся непрерывно.
//
// Почему не read-modify-write из Go (GET текущих токенов, посчитать,
// SET обратно): между GET и SET — сетевой round-trip, и если на лимит
// одновременно бьют два инстанса media (а в проде их несколько за одним
// load balancer'ом), оба прочитают одно и то же количество токенов, оба
// решат, что лимит не исчерпан, и оба спишут — итоговое состояние в Redis
// перезапишет более поздний SET, и один из запросов пройдёт «бесплатно».
// Это классическая гонка read-modify-write, и обычные примитивы Go (мьютекс,
// атомики) её не лечат — они защищают память ОДНОГО процесса, а гонка здесь
// между процессами. Lua-скрипт выполняется в Redis атомарно от начала до
// конца: другая команда не может вклиниться между чтением состояния бакета
// и записью нового — то есть Redis сам становится точкой сериализации,
// которой в этой схеме и не хватало.
const tokenBucketScript = `
local key           = KEYS[1]
local capacity      = tonumber(ARGV[1])
local refill_per_sec = tonumber(ARGV[2])
local requested     = tonumber(ARGV[3])
local ttl_sec       = tonumber(ARGV[4])

local time = redis.call('TIME')
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])

if tokens == nil then
	tokens = capacity
	ts = now_ms
end

local elapsed_sec = math.max(0, now_ms - ts) / 1000
tokens = math.min(capacity, tokens + elapsed_sec * refill_per_sec)

local allowed = 0
local retry_after_ms = 0

if tokens >= requested then
	tokens = tokens - requested
	allowed = 1
else
	local missing = requested - tokens
	if refill_per_sec > 0 then
		retry_after_ms = math.ceil((missing / refill_per_sec) * 1000)
	end
end

redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', tostring(now_ms))
redis.call('EXPIRE', key, ttl_sec)

return {allowed, retry_after_ms}
`

// tokenBucket — redis.NewScript считает SHA1 на клиенте один раз при старте
// процесса; Run(...) сперва пробует EVALSHA (скрипт передаётся по хэшу —
// один раз загружен на сервер, дальше по сети идёт 40 байт вместо всего
// текста), а при NOSCRIPT (сервер перезапустили или сделали SCRIPT FLUSH)
// прозрачно откатывается на полный EVAL, который заодно кладёт скрипт
// обратно в кэш сервера. Вызывающему коду эта деталь не видна.
var tokenBucket = redis.NewScript(tokenBucketScript)

// bucketTTL — TTL самого ключа бакета в Redis. Не совпадает с шириной окна
// лимита: это просто garbage collection на неактивных пользователях — если
// токенами не пользовались час, ключ не должен жить в памяти вечно, а при
// следующем обращении бакет честно пересоздастся полным (см. `if tokens ==
// nil then tokens = capacity`).
const bucketTTL = time.Hour

// Allow — попытка списать один токен из бакета key ёмкостью capacity,
// пополняемого со скоростью refillPerSec токенов в секунду.
//
// allowed=false означает, что вызывающий обязан ответить 429 с заголовком
// Retry-After: retryAfter — именно столько нужно подождать, чтобы бакет
// накопил недостающий токен.
func (c *Client) Allow(ctx context.Context, key string, capacity int, refillPerSec float64) (allowed bool, retryAfter time.Duration, err error) {
	if capacity <= 0 {
		return false, 0, fmt.Errorf("redisx: capacity должен быть положительным, получено %d", capacity)
	}

	res, err := tokenBucket.Run(ctx, c.rdb, []string{key},
		capacity, refillPerSec, 1, int(bucketTTL.Seconds()),
	).Int64Slice()
	if err != nil {
		return false, 0, fmt.Errorf("redisx: token bucket %q: %w", key, err)
	}
	if len(res) != 2 {
		return false, 0, fmt.Errorf("redisx: token bucket %q: неожиданный ответ скрипта: %v", key, res)
	}

	allowed = res[0] == 1
	retryAfter = time.Duration(res[1]) * time.Millisecond
	return allowed, retryAfter, nil
}

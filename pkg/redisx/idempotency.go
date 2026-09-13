package redisx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// PATTERN: idempotency-key — SET NX EX ttl со статусом in_progress, чтобы
// повторный запрос с тем же ключом (клиент не дождался ответа и повторил
// POST) не выполнил операцию дважды.
//
// Инфраструктура готовится здесь, в фазе 2; применяется в order в фазе 3
// (см. docs/PLAN.md, шаг 2.5) — HTTP-обработчик там сам решает, что делать
// с каждым из трёх исходов ниже.
const DefaultIdempotencyTTL = 24 * time.Hour

// IdempotencyStatus — три исхода Begin, и ровно три, а не булев
// «уже было/не было»: были бы два bool'а (found, done), пришлось бы
// перебирать четыре комбинации, одна из которых (found=false, done=true)
// не имеет смысла и обнаруживается только в рантайме. Явный enum делает
// невозможную комбинацию невыразимой на уровне типов.
type IdempotencyStatus int

const (
	// IdempotencyFree — ключ занят прямо сейчас этим вызовом: операция
	// не выполнялась раньше, можно и нужно её выполнять.
	IdempotencyFree IdempotencyStatus = iota
	// IdempotencyInProgress — тот же ключ уже обрабатывается ДРУГИМ
	// вызовом прямо сейчас. Вызывающий обязан ответить 409: результат
	// ещё не готов, и повторно выполнять операцию нельзя — это ровно тот
	// параллельный повтор, от которого и защищает идемпотентность.
	IdempotencyInProgress
	// IdempotencyDone — операция с этим ключом уже завершена; Response
	// содержит сохранённый в Complete результат. Вызывающий обязан
	// вернуть его повторно, не выполняя операцию заново.
	IdempotencyDone
)

// IdempotencyResult — исход Begin.
type IdempotencyResult struct {
	Status IdempotencyStatus
	// Response — сохранённый ответ. Валиден только при Status == IdempotencyDone;
	// в остальных случаях nil, и читать его не нужно — само наличие данных
	// без соответствующего статуса ничего не гарантирует.
	Response []byte
}

// idempotencyEnvelope — то, что реально лежит в Redis по ключу
// идемпотентности. Отдельная от IdempotencyResult структура: Result — это
// то, что видит вызывающий (три исхода), envelope — то, как это записано
// на диске (два состояния, in_progress и done, потому что «свободно» —
// это отсутствие ключа, а не значение в нём).
type idempotencyEnvelope struct {
	Status   string `json:"status"`
	Response []byte `json:"response,omitempty"`
}

const (
	envelopeInProgress = "in_progress"
	envelopeDone       = "done"
)

// Begin фиксирует попытку выполнить операцию с ключом key.
//
// Если ttl <= 0, используется DefaultIdempotencyTTL (24 часа — время,
// в течение которого клиент теоретически может повторить запрос с тем же
// Idempotency-Key после сетевого сбоя; дольше — не нужно, короче — риск
// пропустить повтор от медленного клиента).
func (c *Client) Begin(ctx context.Context, key string, ttl time.Duration) (IdempotencyResult, error) {
	if ttl <= 0 {
		ttl = DefaultIdempotencyTTL
	}

	data, err := json.Marshal(idempotencyEnvelope{Status: envelopeInProgress})
	if err != nil {
		return IdempotencyResult{}, fmt.Errorf("redisx: marshal envelope: %w", err)
	}

	// SET NX — атомарная проверка «ключа ещё не было» и его занятие ЗА ОДНО
	// обращение к Redis. Если бы вместо этого сначала делали GET, а по его
	// отсутствию — SET, между этими двумя командами мог бы вклиниться другой
	// параллельный повтор с тем же Idempotency-Key и тоже увидеть «ключа нет» —
	// именно та гонка, от которой идемпотентность должна защищать в первую
	// очередь.
	ok, err := c.rdb.SetNX(ctx, key, data, ttl).Result()
	if err != nil {
		return IdempotencyResult{}, fmt.Errorf("redisx: begin %q: %w", key, err)
	}
	if ok {
		return IdempotencyResult{Status: IdempotencyFree}, nil
	}

	raw, err := c.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		// Ключ протух между нашим SETNX и этим GET (окно в пару миллисекунд,
		// но при TTL в 24 часа это означает, что кто-то держал ключ до самого
		// конца — маловероятно, но не невозможно). Трактуем как обычный
		// первый запрос: SETNX уже гарантировал бы провал, если бы кто-то
		// снова успел занять ключ первым.
		return c.Begin(ctx, key, ttl)
	}
	if err != nil {
		return IdempotencyResult{}, fmt.Errorf("redisx: begin %q: get: %w", key, err)
	}

	var env idempotencyEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return IdempotencyResult{}, fmt.Errorf("redisx: begin %q: unmarshal: %w", key, err)
	}

	if env.Status == envelopeDone {
		return IdempotencyResult{Status: IdempotencyDone, Response: env.Response}, nil
	}
	return IdempotencyResult{Status: IdempotencyInProgress}, nil
}

// Complete сохраняет готовый ответ операции, начатой через Begin.
//
// Перезаписывает значение безусловно (SET, не SETNX): вызывающий владеет
// ключом, только если Begin вернул IdempotencyFree, и Complete — это его
// парная операция, а не независимая попытка занять ключ.
func (c *Client) Complete(ctx context.Context, key string, response []byte, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = DefaultIdempotencyTTL
	}

	data, err := json.Marshal(idempotencyEnvelope{Status: envelopeDone, Response: response})
	if err != nil {
		return fmt.Errorf("redisx: marshal envelope: %w", err)
	}
	if err := c.rdb.Set(ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("redisx: complete %q: %w", key, err)
	}
	return nil
}

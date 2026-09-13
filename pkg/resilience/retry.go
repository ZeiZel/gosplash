package resilience

import (
	"context"
	"math"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryConfig — параметры экспоненциального backoff с full jitter.
type RetryConfig struct {
	// MaxAttempts — общее число попыток, включая первую. 1 значит «без
	// ретраев вовсе».
	MaxAttempts int
	// BaseDelay — базовая пауза, от которой считается экспонента: попытка N
	// (нумеруя с нуля) ждёт СЛУЧАЙНОЕ время от 0 до BaseDelay*2^N.
	BaseDelay time.Duration
	// MaxDelay — потолок паузы. Без него экспонента на десятой попытке
	// улетает в минуты, и общий бюджет запроса кончится намного раньше,
	// чем ретраи — тратить его на всё удлиняющееся ожидание бессмысленно.
	MaxDelay time.Duration
}

// DefaultRetryConfig — разумные значения для межсервисного gRPC-вызова
// внутри одного дата-центра: суммарно попытки укладываются в единицы секунд,
// что обычно меньше HTTP-таймаута вызывающей стороны.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 4,
		BaseDelay:   50 * time.Millisecond,
		MaxDelay:    2 * time.Second,
	}
}

// Idempotency — явный признак идемпотентности операции. Не bool: голый
// true/false на месте вызова ничего не говорит читающему код, а
// resilience.Idempotent / resilience.NotIdempotent говорит.
type Idempotency bool

const (
	// Idempotent — повтор операции с теми же аргументами безопасен: он либо
	// ничего не меняет второй раз (запрос с idempotency_key), либо операция
	// по своей природе читающая (GetBalance, GetListing).
	Idempotent Idempotency = true
	// NotIdempotent — повтор МЕНЯЕТ результат: «списать деньги», «выдать
	// лицензию без idempotency_key». Для такой операции Retrier.Do не
	// делает ни одной дополнительной попытки, см. NewRetrier.
	NotIdempotent Idempotency = false
)

// Retrier выполняет операцию с ретраями по правилам пакета.
//
// PATTERN: retry с exponential backoff + full jitter.
//
// Почему full jitter, а не фиксированная пауза «подождать 200мс и
// повторить»: фиксированная пауза синхронизирует все клиенты, которые
// получили ошибку одновременно (типичная причина — сервис только что упал
// или передеплоился). Они все ждут одинаковые 200мс и все одновременно
// повторяют запрос — ровно в момент, когда сервис только-только поднялся,
// его кэши пусты, а пул соединений к базе ещё не прогрет. Результат — тот
// же сбой второй раз, thundering herd своими руками.
//
// Full jitter (AWS Architecture Blog, «Exponential Backoff And Jitter»)
// берёт не саму экспоненту, а СЛУЧАЙНОЕ число от 0 до неё. Попытки клиентов
// размазываются по всему окну задержки, и восстанавливающийся сервис видит
// растущую, а не мгновенно полную нагрузку.
//
// Цена: суммарное время ретраев становится случайной величиной, а не
// константой — тестировать конкретные тайминги нельзя, только свойства
// (растёт с номером попытки, укладывается в границы, действительно
// разбросано). См. retry_test.go.
type Retrier struct {
	cfg        RetryConfig
	idempotent bool
}

// NewRetrier — ЕДИНСТВЕННЫЙ конструктор Retrier, и он требует явно назвать
// признак идемпотентности при каждом вызове.
//
// Это не комментарий-предупреждение, а ограничение в рантайме: если
// вызывающий код передаёт resilience.NotIdempotent, Do() выполняет операцию
// РОВНО ОДИН РАЗ, что бы ни случилось, и ни при каких настройках cfg не
// сделает вторую попытку. Опечататься и «случайно» заретраить списание денег
// нельзя — единственный способ получить повторные попытки — явно написать
// в вызывающем коде resilience.Idempotent, а это ложь, которую видно при
// код-ревью, а не забытый комментарий, который никто не перечитывает.
func NewRetrier(cfg RetryConfig, idempotency Idempotency) *Retrier {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	return &Retrier{cfg: cfg, idempotent: bool(idempotency)}
}

// IsRetryable — правило безопасности ретраев, вынесенное в предикат, а не
// в комментарий: ретраить можно ТОЛЬКО codes.Unavailable (сервис временно
// не принимает соединения — упал под, не готов балансировщик) и
// codes.DeadlineExceeded (не успели — возможно, просто не повезло с
// задержкой сети). Любой другой код — это осмысленный ответ сервера
// (InvalidArgument, NotFound, PermissionDenied и т.д.), повторение того же
// запроса даст тот же результат, и тратить на это бюджет ретраев бессмысленно
// и вредно: пользователь ждёт лишние секунды ответа, который не изменится.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// Do выполняет op с ретраями (для идемпотентных Retrier) или ровно один раз
// (для неидемпотентных). method — имя операции, попадает в метрику
// retry_attempts_total{method,result} лейблом с конечным числом значений
// (полное имя gRPC-метода), поэтому не взрывает кардинальность.
func (r *Retrier) Do(ctx context.Context, method string, op func(ctx context.Context) error) error {
	if !r.idempotent {
		err := op(ctx)
		result := "success"
		if err != nil {
			result = "failure"
		}
		recordRetryAttempt(ctx, method, result)
		return err
	}

	var err error
	for attempt := 0; attempt < r.cfg.MaxAttempts; attempt++ {
		err = op(ctx)
		if err == nil {
			recordRetryAttempt(ctx, method, "success")
			return nil
		}
		if !IsRetryable(err) {
			recordRetryAttempt(ctx, method, "non_retryable")
			return err
		}
		if attempt == r.cfg.MaxAttempts-1 {
			break
		}

		delay := fullJitterDelay(r.cfg, attempt)

		// Уважение общего дедлайна: если до его истечения осталось меньше,
		// чем пауза перед следующей попыткой, начинать её незачем — ответ
		// всё равно не успеет дойти до того, как вызывающая сторона
		// перестанет ждать. Отдаём последнюю РЕАЛЬНУЮ ошибку сервера,
		// а не context.DeadlineExceeded — она информативнее для того, кто
		// будет разбирать инцидент.
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < delay {
			recordRetryAttempt(ctx, method, "deadline")
			return err
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			recordRetryAttempt(ctx, method, "ctx_done")
			return err
		case <-timer.C:
		}
	}
	recordRetryAttempt(ctx, method, "attempts_exhausted")
	return err
}

// fullJitterDelay — backoff = min(MaxDelay, BaseDelay*2^attempt), пауза —
// равномерно случайное число из [0, backoff). Вынесена отдельной функцией,
// чтобы тест мог проверить свойства распределения без накладных расходов
// на настоящий Do() с фейковой сетью.
func fullJitterDelay(cfg RetryConfig, attempt int) time.Duration {
	backoff := float64(cfg.BaseDelay) * math.Pow(2, float64(attempt))
	if maxDelay := float64(cfg.MaxDelay); cfg.MaxDelay > 0 && backoff > maxDelay {
		backoff = maxDelay
	}
	if backoff <= 0 {
		return 0
	}
	return time.Duration(rand.Float64() * backoff)
}

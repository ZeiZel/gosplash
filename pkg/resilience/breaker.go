package resilience

import (
	"errors"
	"log/slog"
	"time"

	"github.com/sony/gobreaker/v2"
)

// Ре-экспорт ошибок gobreaker, чтобы вызывающий код (в частности,
// pkg/grpcx) мог делать errors.Is(err, resilience.ErrBreakerOpen), не
// добавляя себе прямую зависимость от gobreaker ради двух сравнений ошибок.
var (
	// ErrBreakerOpen возвращается, когда breaker в состоянии Open: операция
	// не вызывается вовсе, отказ мгновенный.
	ErrBreakerOpen = gobreaker.ErrOpenState
	// ErrTooManyProbes возвращается в состоянии HalfOpen, когда пробных
	// запросов уже достаточно (см. BreakerConfig.MaxRequests) и новый
	// запрос дальше пробной проверки не пропускают.
	ErrTooManyProbes = gobreaker.ErrTooManyRequests
)

// BreakerConfig — настройки circuit breaker.
type BreakerConfig struct {
	// Name — попадает в метрики и в лог смены состояния. Как правило,
	// имя вызываемого сервиса ("wallet", "catalog").
	Name string
	// MaxRequests — сколько пробных запросов breaker пропускает в
	// состоянии HalfOpen, прежде чем решить, закрываться обратно или нет.
	MaxRequests uint32
	// Interval — период очистки счётчиков отказов, ПОКА breaker в Closed.
	// Без этого один всплеск отказов час назад продолжает учитываться в
	// пороге ReadyToTrip вечно, и breaker однажды откроется по накопленной
	// вчерашней статистике, а не по текущей ситуации.
	Interval time.Duration
	// Timeout — сколько breaker сидит в Open, прежде чем разрешить первый
	// пробный запрос (переход в HalfOpen).
	Timeout time.Duration
	// ReadyToTrip решает, пора ли из Closed переходить в Open, глядя на
	// накопленную с последнего Interval статистику.
	ReadyToTrip func(counts gobreaker.Counts) bool
}

// DefaultBreakerConfig — разумные значения для межсервисного gRPC-вызова.
//
// PATTERN: circuit breaker (Closed / Open / Half-Open).
//
//	Closed    — обычная работа, запросы идут в реальный сервис. Breaker
//	            считает отказы за скользящее окно Interval.
//	Open      — сервис признан мёртвым: ЛЮБОЙ вызов немедленно возвращает
//	            ErrBreakerOpen, без единого сетевого пакета. Именно это
//	            превращает 5-секундный таймаут в 503 за десятки миллисекунд —
//	            вызывающий код не ждёт TCP timeout, а получает ответ сразу.
//	Half-Open — по истечении Timeout breaker пропускает MaxRequests пробных
//	            запросов. Все успешны — Closed. Хоть один неудачен — снова
//	            Open, и Timeout начинается заново.
//
// Отличие от Retry в этом же пакете: Retry решает, что делать с ОДНИМ
// вызовом (мгновенный сбой, повторить сейчас же). Breaker решает, что
// делать со ВСЕМИ вызовами следующие Timeout секунд (затяжной сбой, не
// пытаться вовсе). Без breaker сервис с мёртвой зависимостью не падает
// сразу — он медленно вымирает: каждый запрос держит горутину и
// соединение на полный таймаут (retry ещё и умножает это на MaxAttempts),
// пока не кончится пул горутин или файловых дескрипторов, и тогда откажет
// уже сам вызывающий сервис, хотя проблема была только у одной зависимости.
func DefaultBreakerConfig(name string) BreakerConfig {
	return BreakerConfig{
		Name:        name,
		MaxRequests: 3,
		Interval:    30 * time.Second,
		Timeout:     10 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Порог намеренно двойной: и абсолютное число запросов (не
			// открываться от одного неудачного вызова при низком трафике),
			// и доля отказов (не терпеть до бесконечности при высоком).
			return counts.Requests >= 10 && float64(counts.TotalFailures)/float64(counts.Requests) >= 0.5
		},
	}
}

// Breaker — обёртка над gobreaker с метриками пакета resilience.
//
// Без дженерика T (в отличие от gobreaker.CircuitBreaker[T]) сознательно:
// единственный клиент этого типа в проекте — интерсептор gRPC-клиента
// (pkg/grpcx), а там результат вызова уже записан в reply по указателю,
// и Execute нужен только ради решения «звонить или нет» и ошибки. Добавлять
// дженерик ради типа, который никогда не используется, — сложность без
// пользы.
type Breaker struct {
	name string
	cb   *gobreaker.CircuitBreaker[struct{}]
}

// NewBreaker создаёт breaker и регистрирует его для метрики
// circuit_breaker_state.
func NewBreaker(cfg BreakerConfig) *Breaker {
	b := &Breaker{name: cfg.Name}
	b.cb = gobreaker.NewCircuitBreaker[struct{}](gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: cfg.MaxRequests,
		Interval:    cfg.Interval,
		Timeout:     cfg.Timeout,
		ReadyToTrip: cfg.ReadyToTrip,
		OnStateChange: func(name string, from, to gobreaker.State) {
			// Warn, а не Info: смена состояния breaker — это всегда сигнал
			// о проблеме (Open) или о её исчезновении (обратно в Closed),
			// то есть событие, интересное дежурному, а не рутинная запись.
			slog.Warn("resilience: circuit breaker сменил состояние",
				"name", name, "from", from.String(), "to", to.String())
		},
	})
	registerBreaker(b)
	return b
}

// Execute вызывает op под защитой breaker. Если breaker открыт (или в
// half-open исчерпаны пробные запросы), op НЕ вызывается вовсе, и
// возвращается ErrBreakerOpen / ErrTooManyProbes.
func (b *Breaker) Execute(op func() error) error {
	_, err := b.cb.Execute(func() (struct{}, error) {
		return struct{}{}, op()
	})

	// Сравнение через errors.Is, а не ==: gobreaker возвращает свои сентинелы
	// напрямую, но обёртка выше по стеку может завернуть их в %w, и тогда
	// прямое сравнение молча перестало бы срабатывать — метрика показывала бы
	// "failure" там, где на самом деле открыт предохранитель.
	result := "success"
	switch {
	case err == nil:
	case errors.Is(err, ErrBreakerOpen), errors.Is(err, ErrTooManyProbes):
		result = "open"
	default:
		result = "failure"
	}
	recordBreakerRequest(b.name, result)
	return err
}

// State — текущее состояние breaker. Используется в тестах и может
// использоваться в /readyz, если решат делать зависимость видимой в
// readiness (в проекте пока не делается, см. pkg/httpx.Health: readiness
// не должна знать о вещах глубже своей прямой зависимости).
func (b *Breaker) State() gobreaker.State { return b.cb.State() }

// Name — имя breaker, как задано в BreakerConfig.Name.
func (b *Breaker) Name() string { return b.name }

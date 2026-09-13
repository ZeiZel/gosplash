package kafkax

import "errors"

// Классификация ошибок обработчика.
//
// Handler не может просто вернуть err и рассчитывать, что консьюмер угадает,
// что с ним делать: сетевой сбой в S3 (повторить через секунду — скорее всего
// сработает) и «в payload лежит неизвестный тип события» (повторяй хоть сутки
// — не поможет) требуют противоположной реакции. Явная разметка через
// errors.Is снимает эту неопределённость и документируется прямо в самой
// ошибке, а не в комментарии, который никто не прочитает в 3 часа ночи.
//
// Неклассифицированная ошибка (не обёрнутая ни в Retryable, ни в Permanent)
// считается RETRYABLE. Это осознанный дефолт по умолчанию в пользу
// безопасности: разработчик, забывший классифицировать ошибку, получит
// лишние повторы идемпотентного обработчика, а не тихую потерю сообщения
// в DLQ без единой попытки его обработать.
var (
	ErrRetryable = errors.New("kafkax: временная ошибка обработки, повтор поможет")
	ErrPermanent = errors.New("kafkax: постоянная ошибка обработки, повтор не поможет")
)

// Retryable оборачивает err как временный: консьюмер повторит обработку
// на месте (с backoff+jitter), а если попытки кончатся — уйдёт в
// <topic>.retry и вернётся туда ещё раз позже.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{cause: err, sentinel: ErrRetryable}
}

// Permanent оборачивает err как постоянный: консьюмер сразу отправит
// сообщение в DLQ, не тратя окно ретраев на ошибку, которая от повторов
// не изменится (битый payload, неизвестный event_type и т. п.).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{cause: err, sentinel: ErrPermanent}
}

// classifiedError — обёртка, которая делает errors.Is(err, ErrRetryable)
// или errors.Is(err, ErrPermanent) истинным, сохраняя исходную ошибку
// доступной через errors.Unwrap (и, соответственно, через errors.Is/As
// дальше по цепочке — например, до доменной ошибки внутри cause).
type classifiedError struct {
	cause    error
	sentinel error
}

func (e *classifiedError) Error() string { return e.cause.Error() }
func (e *classifiedError) Unwrap() error { return e.cause }

// Is сравнивает с сигнальной ошибкой напрямую, а не через Unwrap: sentinel
// (ErrRetryable/ErrPermanent) — это ЯРЛЫК классификации, а не часть цепочки
// оборачивания cause, и через обычный Unwrap до него дойти нельзя.
func (e *classifiedError) Is(target error) bool { return target == e.sentinel }

// isPermanent решает, что делать с ошибкой обработчика: true — сразу в DLQ,
// false — retryable (в том числе неклассифицированная ошибка, см. комментарий
// к переменным выше).
func isPermanent(err error) bool { return errors.Is(err, ErrPermanent) }

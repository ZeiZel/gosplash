package es

import (
	"fmt"

	"gosplash/services/search/internal/domain"
)

// permanentErrorTypes — типы ошибок Elasticsearch, повтор которых
// заведомо бесполезен: документ структурно не подходит под маппинг.
// Список закрытый и короткий намеренно — если сюда попадёт лишний тип
// "на всякий случай", безобидная временная ошибка сервера может ошибочно
// улететь в DLQ без единой попытки повтора. Дефолт — retryable
// (см. classifyBulkItemErr ниже), поэтому цена ошибки в другую сторону:
// пропущенный здесь permanent-тип просто получит несколько лишних
// повторов перед тем, как всё равно попасть в DLQ по исчерпанию попыток.
var permanentErrorTypes = map[string]struct{}{
	"mapper_parsing_exception":         {},
	"strict_dynamic_mapping_exception": {},
	"illegal_argument_exception":       {},
}

// classifyBulkItemErr решает судьбу ОДНОГО элемента bulk-ответа.
//
// version_conflict_engine_exception — это НЕ ошибка в смысле "что-то
// сломалось": external_gte версия сработала штатно и отклонила запись,
// потому что в индексе уже лежит документ той же или более новой версии.
// Ровно это и есть цель версионирования (см. app/indexer.go) — событие,
// пришедшее не по порядку, должно быть тихо проигнорировано, а не считаться
// сбоем.
func classifyBulkItemErr(status int, errType, errReason string) error {
	if status < 300 {
		return nil
	}
	if errType == "version_conflict_engine_exception" {
		return nil
	}
	if _, permanent := permanentErrorTypes[errType]; permanent {
		return fmt.Errorf("%w: %s: %s", domain.ErrInvalidDocument, errType, errReason)
	}
	// 429 (too many requests) и 5xx — кластер перегружен или недоступен.
	// Всё остальное неизвестное тоже уходит сюда: retryable-дефолт
	// безопаснее, чем ошибочно классифицировать как permanent то, чего
	// не понял.
	return fmt.Errorf("%w: %s (%d): %s", domain.ErrIndexUnavailable, errType, status, errReason)
}

// classifyTransportErr — ошибка целого HTTP-запроса (не долетел, отказ
// соединения, таймаут) — Elasticsearch в принципе не ответил, это всегда
// временно.
func classifyTransportErr(op string, err error) error {
	return fmt.Errorf("%w: %s: %w", domain.ErrIndexUnavailable, op, err)
}

// classifyHTTPStatusErr — весь bulk-запрос целиком отклонён кластером
// (а не отдельный документ внутри него), например 429/503 на уровне всего
// _bulk. Тоже временно: сам факт, что запрос дошёл и получил статус, а не
// оборвался по сети, ничего не говорит про содержимое документов.
func classifyHTTPStatusErr(op, status string, body []byte) error {
	return fmt.Errorf("%w: %s: %s: %s", domain.ErrIndexUnavailable, op, status, body)
}

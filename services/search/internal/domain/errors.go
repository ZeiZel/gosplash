package domain

import "errors"

// Доменные ошибки индекса — переменные, сравниваемые через errors.Is
// (docs/STYLE.md). Оба конца знают о них: internal/adapters/es оборачивает
// в них низкоуровневые ошибки Elasticsearch, а internal/app.classifyESErr
// смотрит на них, чтобы решить kafkax.Retryable или kafkax.Permanent —
// сам app не должен знать, что такое "409 version_conflict" или
// "mapper_parsing_exception", это словарь Elasticsearch, а не поиска
// как предметной области.
var (
	// ErrIndexUnavailable — Elasticsearch недоступен или перегружен
	// (сеть, 503, 429). Временно по определению.
	ErrIndexUnavailable = errors.New("индекс поиска недоступен")

	// ErrInvalidDocument — документ не прошёл маппинг (dynamic: strict
	// отклонил поле, тип не совпал и т. п.). Постоянно: повтор с тем же
	// документом даст ту же ошибку.
	ErrInvalidDocument = errors.New("документ не прошёл маппинг индекса")
)

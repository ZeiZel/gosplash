// Package httpx — всё, что каждый HTTP-сервис проекта делает одинаково:
// разбор запроса, формат ответа, middleware, проверки живости и корректная
// остановка.
//
// Бизнес-логики здесь нет и быть не должно: пакет обязан подойти любому
// сервису, включая те, которых ещё не существует.
package httpx

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// JSON пишет данные как JSON с указанным кодом.
func JSON(w http.ResponseWriter, data any, statusCode int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	if data == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(data); err != nil {
		// Заголовки уже ушли клиенту, отправить ошибку нельзя — только записать.
		slog.Error("httpx: не смог закодировать ответ", "error", err)
	}
}

// Error — ошибка в том же формате, что и успешный ответ.
// Клиенту всегда приходит JSON, а не иногда JSON, а иногда текст от http.Error.
func Error(w http.ResponseWriter, message string, statusCode int) {
	JSON(w, map[string]string{"error": message}, statusCode)
}

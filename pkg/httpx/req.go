package httpx

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Body декодирует тело запроса в T.
//
// Отличие от прямого json.NewDecoder(r.Body).Decode(&v): DisallowUnknownFields
// превращает опечатку в имени поля из «поле молча осталось нулевым» в явную
// ошибку 400. На отладке это экономит часы.
//
// Хендлер сам решает, что делать с ошибкой, — функция ничего не пишет в
// ResponseWriter. Функция, которая и разбирает, и отвечает, неудобна ровно
// в тот момент, когда ответ нужен другой.
func Body[T any](r *http.Request) (*T, error) {
	var payload T

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("некорректный JSON: %w", err)
	}
	return &payload, nil
}

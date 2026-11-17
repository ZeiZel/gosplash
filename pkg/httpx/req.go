package httpx

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// DefaultMaxJSONBody — предел размера JSON-тела для Body (1 МиБ). JSON-ручки
// проекта принимают команды (place order и т.п.), а не файлы — гигабайтное
// «JSON-тело» возможно только от сломанного или злонамеренного клиента, и
// читать его целиком в декодер незачем.
const DefaultMaxJSONBody int64 = 1 << 20

// Body декодирует тело запроса в T.
//
// Отличие от прямого json.NewDecoder(r.Body).Decode(&v): DisallowUnknownFields
// превращает опечатку в имени поля из «поле молча осталось нулевым» в явную
// ошибку 400. На отладке это экономит часы.
//
// Тело оборачивается в http.MaxBytesReader с пределом DefaultMaxJSONBody:
// без него декодер честно тянет из соединения столько, сколько клиент
// прислал, и один запрос способен съесть память горутины целиком. nil вместо
// ResponseWriter — документированный вариант: он лишь отключает
// сопутствующее закрытие соединения, сам предел работает.
//
// Хендлер сам решает, что делать с ошибкой, — функция ничего не пишет в
// ResponseWriter. Функция, которая и разбирает, и отвечает, неудобна ровно
// в тот момент, когда ответ нужен другой.
func Body[T any](r *http.Request) (*T, error) {
	var payload T

	r.Body = http.MaxBytesReader(nil, r.Body, DefaultMaxJSONBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("некорректный JSON: %w", err)
	}
	return &payload, nil
}

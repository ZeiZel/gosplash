package req

import "net/http"

func HandleBody[T any](w *http.ResponseWriter, r *http.Request) (*T, error) {
}

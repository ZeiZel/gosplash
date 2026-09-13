// Package http — тонкий REST-фасад order-service поверх собственного
// *ordergrpc.Server, в процессе и без единого байта по сети — тот же приём
// и то же обоснование (buf.gen.yaml не настроен на grpc-gateway в этой
// фазе), что и в services/catalog/internal/adapters/http/handler.go.
package http

import (
	"net/http"

	"google.golang.org/grpc/status"

	orderv1 "gosplash/gen/go/gosplash/order/v1"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"
	ordergrpc "gosplash/services/order/internal/adapters/grpc"
)

type Handler struct {
	grpc *ordergrpc.Server
}

func Register(router *http.ServeMux, grpcServer *ordergrpc.Server) {
	h := &Handler{grpc: grpcServer}

	router.HandleFunc("POST /orders", h.place)
	router.HandleFunc("GET /orders/{id}", h.get)
}

type placeOrderBody struct {
	BuyerID   int64  `json:"buyer_id"`
	ListingID string `json:"listing_id"`
}

// POST /orders — заголовок Idempotency-Key, а не поле тела: PATTERN
// idempotency-key. Отдельный заголовок делает ключ видимым HTTP-
// инструментам (curl -H, Postman, прокси) независимо от бизнес-данных
// запроса — та же договорённость, на которой держится at-most-once
// у PUT/POST в любом REST API.
//
// Собственно проверка "пусто → 400" и "409 при параллельном повторе"
// НЕ дублируется здесь: заголовок просто копируется в поле
// PlaceOrderRequest.IdempotencyKey, а решение об исходе принимает
// ordergrpc.Server.PlaceOrder (см. его комментарий) — тем же кодом,
// которым ответил бы обычный gRPC-клиент, минуя HTTP вовсе.
func (h *Handler) place(w http.ResponseWriter, r *http.Request) {
	body, err := httpx.Body[placeOrderBody](r)
	if err != nil {
		httpx.Error(w, "некорректное тело запроса", http.StatusBadRequest)
		return
	}

	resp, err := h.grpc.PlaceOrder(r.Context(), &orderv1.PlaceOrderRequest{
		BuyerId:        body.BuyerID,
		ListingId:      body.ListingID,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	httpx.JSON(w, toOrderView(resp.GetOrder()), http.StatusOK)
}

// GET /orders/{id}
func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	resp, err := h.grpc.GetOrder(r.Context(), &orderv1.GetOrderRequest{Id: r.PathValue("id")})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	httpx.JSON(w, toOrderView(resp.GetOrder()), http.StatusOK)
}

func writeGRPCError(w http.ResponseWriter, err error) {
	st := status.Convert(err)
	httpx.Error(w, st.Message(), grpcx.CodeToHTTPStatus(st.Code()))
}

type orderView struct {
	ID            string `json:"id"`
	BuyerID       int64  `json:"buyer_id"`
	ListingID     string `json:"listing_id"`
	PriceCents    int64  `json:"price_cents"`
	Currency      string `json:"currency"`
	Status        string `json:"status"`
	FailureReason string `json:"failure_reason,omitempty"`
	LicenseID     string `json:"license_id,omitempty"`
}

func toOrderView(o *orderv1.Order) orderView {
	return orderView{
		ID:            o.GetId(),
		BuyerID:       o.GetBuyerId(),
		ListingID:     o.GetListingId(),
		PriceCents:    o.GetPriceCents(),
		Currency:      o.GetCurrency(),
		Status:        o.GetStatus(),
		FailureReason: o.GetFailureReason(),
		LicenseID:     o.GetLicenseId(),
	}
}

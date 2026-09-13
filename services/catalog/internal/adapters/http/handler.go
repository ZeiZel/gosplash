// Package http — публичный read-API каталога.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ — временный REST-фасад вместо grpc-gateway:
//
// В proto/buf.gen.yaml плагин protoc-gen-grpc-gateway ЗАКОММЕНТИРОВАН, а
// proto/** этому сервису трогать нельзя (контракт заморожен на уровне всей
// монорепы). Настоящий grpc-gateway потребовал бы двух шагов вне зоны
// ответственности catalog: добавить в catalog.proto аннотации
// google.api.http для каждого RPC и раскомментировать плагин в
// proto/buf.gen.yaml, после чего пересобрать gen/go. Ни один из этих шагов
// здесь не сделан НАМЕРЕННО — задача явно требует РЕАЛЬНЫЙ рабочий REST,
// а не заглушку, ожидающую будущей генерации.
//
// Поэтому вместо gateway — тонкий фасад: HTTP-хендлер вызывает МЕТОДЫ
// СВОЕГО ЖЕ *cataloggrpc.Server НАПРЯМУЮ, в процессе, без единого байта по
// сети (это обычный вызов Go-метода, просто у него есть параметр ctx и он
// возвращает gRPC-совместимую ошибку). Как только появятся аннотации и
// плагин включат, этот файл можно будет заменить сгенерированным раннером
// gateway почти без изменений публичного HTTP-контракта.
package http

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/status"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
	"gosplash/pkg/grpcx"
	"gosplash/pkg/httpx"
	cataloggrpc "gosplash/services/catalog/internal/adapters/grpc"
	"gosplash/services/catalog/internal/app"
)

type Handler struct {
	grpc    *cataloggrpc.Server
	service *app.ListingService
}

// Register регистрирует REST-фасад. grpcServer — тот же объект, что
// зарегистрирован в pkg/grpcx.NewServer в main.go: один сервис, два
// транспорта.
func Register(router *http.ServeMux, grpcServer *cataloggrpc.Server, service *app.ListingService) {
	h := &Handler{grpc: grpcServer, service: service}

	router.HandleFunc("GET /catalog/listings", h.list)
	router.HandleFunc("GET /catalog/listings/{id}", h.get)
	router.HandleFunc("GET /catalog/replication", h.replication)
	router.HandleFunc("GET /catalog/top", h.top)
}

// GET /catalog/listings?limit=20&cursor=...&tags=a,b — лента. Keyset-
// пагинация, см. adapters/pg/cursor.go.
func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	limit := int32(20)
	if v := r.URL.Query().Get("limit"); v != "" {
		// ParseInt с разрядностью 32, а не Atoi с приведением: так приведения
		// int→int32 нет вовсе, и вопрос «а влезет ли» не возникает. Значение
		// приходит из query-строки, то есть от кого угодно.
		if n, err := strconv.ParseInt(v, 10, 32); err == nil && n > 0 && n <= 100 {
			limit = int32(n)
		}
	}
	var tags []string
	if v := r.URL.Query().Get("tags"); v != "" {
		tags = strings.Split(v, ",")
	}

	resp, err := h.grpc.ListListings(r.Context(), &catalogv1.ListListingsRequest{
		Limit:  limit,
		Cursor: r.URL.Query().Get("cursor"),
		Tags:   tags,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	views := make([]listingView, 0, len(resp.GetListings()))
	for _, l := range resp.GetListings() {
		views = append(views, toListingView(l))
	}
	httpx.JSON(w, map[string]any{
		"count":       len(views),
		"listings":    views,
		"next_cursor": resp.GetNextCursor(),
	}, http.StatusOK)
}

// GET /catalog/listings/{id}
func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	resp, err := h.grpc.GetListing(r.Context(), &catalogv1.GetListingRequest{Id: r.PathValue("id")})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	httpx.JSON(w, toListingView(resp.GetListing()), http.StatusOK)
}

// GET /catalog/replication — отставание реплики в строках. Не часть
// catalog.proto (внутренняя диагностика фазы 0), поэтому идёт напрямую в
// app.ListingService, минуя gRPC-слой.
func (h *Handler) replication(w http.ResponseWriter, r *http.Request) {
	primary, replica, err := h.service.ReplicationLag(r.Context())
	if err != nil {
		httpx.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	httpx.JSON(w, map[string]any{
		"primary_rows": primary,
		"replica_rows": replica,
		"lag_rows":     primary - replica,
	}, http.StatusOK)
}

// GET /catalog/top — топ-100 просмотренных карточек за сутки. Тоже не часть
// catalog.proto, читает Redis напрямую через app.ListingService.Top.
func (h *Handler) top(w http.ResponseWriter, r *http.Request) {
	n := 100
	if v := r.URL.Query().Get("n"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 && parsed <= 100 {
			n = parsed
		}
	}

	entries, err := h.service.Top(r.Context(), time.Now(), n)
	if err != nil {
		httpx.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	httpx.JSON(w, map[string]any{"top": entries}, http.StatusOK)
}

// writeGRPCError переводит gRPC-код в HTTP-статус через
// grpcx.CodeToHTTPStatus — единственное место в проекте, которому положено
// это делать на границе REST-фасада поверх gRPC.
func writeGRPCError(w http.ResponseWriter, err error) {
	st := status.Convert(err)
	httpx.Error(w, st.Message(), grpcx.CodeToHTTPStatus(st.Code()))
}

type listingView struct {
	ID          string            `json:"id"`
	AuthorID    int64             `json:"author_id"`
	AuthorName  string            `json:"author_name"`
	Title       string            `json:"title"`
	Tags        []string          `json:"tags,omitempty"`
	StorageKey  string            `json:"storage_key"`
	Thumbnails  map[string]string `json:"thumbnails,omitempty"`
	PriceCents  int64             `json:"price_cents"`
	Currency    string            `json:"currency"`
	Status      string            `json:"status"`
	PublishedAt time.Time         `json:"published_at,omitempty"`
	Width       int32             `json:"width"`
	Height      int32             `json:"height"`
}

func toListingView(l *catalogv1.Listing) listingView {
	v := listingView{
		ID:         l.GetId(),
		AuthorID:   l.GetAuthorId(),
		AuthorName: l.GetAuthorName(),
		Title:      l.GetTitle(),
		Tags:       l.GetTags(),
		StorageKey: l.GetStorageKey(),
		Thumbnails: l.GetThumbnails(),
		PriceCents: l.GetPriceCents(),
		Currency:   l.GetCurrency(),
		Status:     l.GetStatus(),
		Width:      l.GetWidth(),
		Height:     l.GetHeight(),
	}
	if ts := l.GetPublishedAt(); ts != nil {
		v.PublishedAt = ts.AsTime()
	}
	return v
}

// Package http — публичный HTTP-адаптер media.
//
// Пакет называется http и при этом импортирует net/http — это законно:
// имя собственного пакета не является идентификатором внутри него самого,
// так что конфликта нет. Имена адаптеров выбраны по их роли (pg, kafka, grpc,
// http), и ради единообразия этот тоже назван по роли.
//
// Только загрузка (и мягкое удаление, которое с ней симметрично: то же
// разбирательство user_id + id, тот же outbox под капотом) остались «ручным»
// HTTP: файлы через gRPC-фасад передавать неудобно. Остальное в фазе 3
// переедет на grpc-gateway, который сгенерируется из того же media.proto.
package http

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"gosplash/pkg/httpx"
	"gosplash/services/media/internal/app"
	"gosplash/services/media/internal/domain"
)

// RateLimiter — то, что нужно HTTP-адаптеру от pkg/redisx.Client, объявлено
// рядом с потребителем (тот же приём, что и в internal/ports для прикладного
// слоя). Key и Allow совпадают по сигнатуре с одноимёнными методами
// *redisx.Client, поэтому клиент удовлетворяет интерфейсу напрямую, без
// обёртки — адаптер-прослойка нужна только там, где сигнатуры расходятся.
//
// Key — не украшение: pkg/redisx намеренно не даёт методов, принимающих
// голый ключ (см. заголовок пакета), и обход этого правила прямой склейкой
// строки "media:upload:"+userID в адаптере свёл бы гарантию namespace'а
// к соглашению, которое никто не проверяет.
type RateLimiter interface {
	Key(parts ...string) string
	Allow(ctx context.Context, key string, capacity int, refillPerSec float64) (allowed bool, retryAfter time.Duration, err error)
}

type Handler struct {
	service      *app.PhotoService
	maxBodyBytes int64
	presignedTTL time.Duration

	limiter          RateLimiter
	uploadCapacity   int
	uploadRefillRate float64
}

type Deps struct {
	Service      *app.PhotoService
	MaxBodyBytes int64
	PresignedTTL time.Duration

	// Limiter — nil допустим (например, в тестах, которым лимит не важен):
	// тогда ограничение просто не применяется, как будто бакет бесконечен.
	Limiter                RateLimiter
	UploadRateCapacity     int
	UploadRateRefillPerSec float64
}

// Register регистрирует маршруты в переданном роутере.
//
// Роутер приходит снаружи, а не создаётся внутри: так main остаётся
// единственным местом, где видно все маршруты сервиса целиком.
func Register(router *http.ServeMux, deps Deps) {
	h := &Handler{
		service:          deps.Service,
		maxBodyBytes:     deps.MaxBodyBytes,
		presignedTTL:     deps.PresignedTTL,
		limiter:          deps.Limiter,
		uploadCapacity:   deps.UploadRateCapacity,
		uploadRefillRate: deps.UploadRateRefillPerSec,
	}

	// Пути начинаются с /media, потому что NGINX проксирует по префиксу
	// и путь не срезает (см. deploy/nginx/gateway.conf).
	router.HandleFunc("POST /media/upload", h.upload)
	router.HandleFunc("GET /media/photos/{id}", h.get)
	router.HandleFunc("DELETE /media/photos/{id}", h.delete)
	router.HandleFunc("GET /media/photos", h.list)
	router.HandleFunc("GET /media/shards", h.shards)
}

// POST /media/upload
//
//	multipart/form-data: file=@photo.jpg, title=...
//	заголовок X-User-Id: 42
//
// Пользователь берётся из заголовка, потому что аутентификации ещё нет.
// Когда появится JWT (фаза 3), X-User-Id исчезнет — иначе любой сможет
// загрузить фото от чужого имени.
func (h *Handler) upload(w http.ResponseWriter, r *http.Request) {
	userID, err := userIDFrom(r)
	if err != nil {
		httpx.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if !h.allowUpload(r.Context(), w, userID) {
		return
	}

	// Жёсткий предел на размер тела. Без него клиент может прислать гигабайт,
	// и сервис будет честно его принимать.
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)

	// 32 МБ — сколько разрешено держать в памяти; остальное multipart-парсер
	// сбросит во временный файл. Сам файл мы отсюда не читаем целиком:
	// ниже он уезжает в хранилище как поток.
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		httpx.Error(w, "не удалось разобрать форму: "+err.Error(), http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		httpx.Error(w, "нужно поле file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Content-Type из формы клиента здесь НАМЕРЕННО не читается: сервис сам
	// определяет реальный тип по байтам файла (app.sniffImage) и не должен
	// получать снаружи значение, которому потом же не будет доверять —
	// оставлять его в вызове означало бы вводить в заблуждение читателя кода.
	photo, err := h.service.Upload(r.Context(), app.UploadInput{
		UserID:   userID,
		Title:    r.FormValue("title"),
		Filename: header.Filename,
		Size:     header.Size,
		Content:  file,
	})
	if errors.Is(err, domain.ErrNoUserID) || errors.Is(err, domain.ErrEmptyFileName) {
		httpx.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if errors.Is(err, domain.ErrNotAnImage) {
		httpx.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err != nil {
		slog.Error("media: загрузка не удалась", "error", err)
		httpx.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	httpx.JSON(w, h.view(r, photo), http.StatusCreated)
}

// allowUpload — рейт-лимит per-user на загрузку, token bucket в Redis
// (pkg/redisx.Allow). true — можно продолжать, false — ответ 429 уже
// отправлен вызывающему.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: почему лимит здесь, в HTTP-адаптере, а не в
// app.PhotoService.Upload. Rate limiting — это решение о ТРАФИКЕ (сколько
// запросов в секунду выдержит инфраструктура), а не о ПРЕДМЕТНОЙ ОБЛАСТИ
// (что значит "фото загружено"); прикладной сценарий одинаково корректен
// и с лимитом, и без него, и остаётся тестируемым фейками портов без
// единой мысли о Redis. Появись здесь ports.RateLimiter — app пришлось бы
// решать вопрос "а что если Redis недоступен", который к бизнес-логике
// загрузки фото не имеет отношения вообще. Ровно по этой же причине лимит
// не в gRPC-адаптере: GetPhoto — это чтение, для него другая политика
// (кэш, а не upload-квота), а не общий для всех транспортов лимитер.
//
// Redis недоступен → FAIL-OPEN: запрос пропускается, ошибка только
// логируется. Кэш и лимитер — это оптимизация и защита инфраструктуры,
// а не часть контракта "POST /media/upload должен принять файл". Если
// сделать наоборот (fail-closed — отбивать загрузку, когда Redis лежит),
// один упавший Redis положил бы всю функцию загрузки фото целиком, хотя
// PostgreSQL и S3 в этот момент прекрасно работают: инфраструктура защиты
// не должна быть более хрупкой, чем то, что она защищает.
func (h *Handler) allowUpload(ctx context.Context, w http.ResponseWriter, userID int64) bool {
	if h.limiter == nil {
		// Лимитер не сконфигурирован (например, в тестах, которым лимит не
		// важен) — веди себя так, будто бакет бесконечен, а не паникуй на nil.
		return true
	}

	key := h.limiter.Key("upload", strconv.FormatInt(userID, 10))
	allowed, retryAfter, err := h.limiter.Allow(ctx, key, h.uploadCapacity, h.uploadRefillRate)
	if err != nil {
		slog.Error("media: rate limiter недоступен, пропускаю запрос (fail-open)", "error", err)
		return true
	}
	if allowed {
		return true
	}

	// Retry-After — целые секунды, округление вверх: клиенту нужен ВЕРХНИЙ
	// предел ожидания ("подожди хотя бы столько"), округление вниз могло бы
	// отправить повтор на долю секунды раньше, чем бакет реально накопит
	// токен, и клиент получил бы ещё один 429 почти сразу же.
	seconds := int(math.Ceil(retryAfter.Seconds()))
	if seconds < 1 {
		seconds = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	httpx.Error(w, "слишком много запросов, попробуйте позже", http.StatusTooManyRequests)
	return false
}

// GET /media/photos/{id}?user_id=42
//
// user_id в query — не прихоть: без ключа шардирования сервис не знает,
// на какой из баз искать строку. Альтернатива — опросить все шарды,
// но это ровно то, чего шардирование должно избегать.
func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	userID, err := userIDFrom(r)
	if err != nil {
		httpx.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	photo, err := h.service.Get(r.Context(), userID, r.PathValue("id"))
	if errors.Is(err, domain.ErrNotFound) {
		httpx.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		httpx.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	httpx.JSON(w, h.view(r, photo), http.StatusOK)
}

// DELETE /media/photos/{id}?user_id=42 — мягкое удаление.
//
// 204 No Content на успех: тело ответа для DELETE ничего не добавляет —
// клиент и так знает id, который удалял, а сама операция уже необратима
// с его точки зрения (то, что физически строка осталась в базе со статусом
// deleted — деталь реализации, см. domain.StatusDeleted и app.Delete).
// 404, если фото нет или оно уже было удалено раньше: повторный DELETE —
// это не ошибка клиента, но и не повод притворяться, что что-то произошло
// СЕЙЧАС, поэтому не 204, а честный "такого фото (уже) нет".
func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	userID, err := userIDFrom(r)
	if err != nil {
		httpx.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.service.Delete(r.Context(), userID, r.PathValue("id"))
	if errors.Is(err, domain.ErrNotFound) {
		httpx.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		slog.Error("media: удаление не удалось", "error", err)
		httpx.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GET /media/photos?user_id=42&limit=20 — «мои фото».
func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	userID, err := userIDFrom(r)
	if err != nil {
		httpx.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			limit = n
		}
	}

	photos, err := h.service.ListByUser(r.Context(), userID, limit)
	if err != nil {
		httpx.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	views := make([]photoView, 0, len(photos))
	for i := range photos {
		views = append(views, h.view(r, &photos[i]))
	}

	httpx.JSON(w, map[string]any{
		"shard":  h.service.ShardOf(userID),
		"count":  len(views),
		"photos": views,
	}, http.StatusOK)
}

// GET /media/shards — сколько строк на каждом шарде.
// Ручка существует только ради наглядности: загрузи файлы от разных
// пользователей и посмотри, как они разъезжаются.
func (h *Handler) shards(w http.ResponseWriter, r *http.Request) {
	counts, err := h.service.CountByShard(r.Context())
	if err != nil {
		httpx.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	httpx.JSON(w, map[string]any{"photos_per_shard": counts}, http.StatusOK)
}

// photoView — то, что уходит наружу по HTTP. Отдельно от доменной модели
// по той же причине, что и protobuf-контракт: внутреннее представление
// меняется чаще, чем публичный API, и они не должны быть связаны.
type photoView struct {
	ID          string    `json:"id"`
	UserID      int64     `json:"user_id"`
	Title       string    `json:"title,omitempty"`
	Mime        string    `json:"mime"`
	SizeBytes   int64     `json:"size_bytes"`
	Width       int       `json:"width,omitempty"`
	Height      int       `json:"height,omitempty"`
	Status      string    `json:"status"`
	Shard       int       `json:"shard"`
	DownloadURL string    `json:"download_url,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

func (h *Handler) view(r *http.Request, p *domain.Photo) photoView {
	v := photoView{
		ID:        p.ID,
		UserID:    p.UserID,
		Title:     p.Title,
		Mime:      p.Mime,
		SizeBytes: p.SizeBytes,
		Width:     p.Width,
		Height:    p.Height,
		Status:    p.Status,
		Shard:     h.service.ShardOf(p.UserID),
		CreatedAt: p.CreatedAt,
	}
	// Ссылка временная и подписанная: бакет с оригиналами приватный.
	if url, err := h.service.DownloadURL(r.Context(), p, h.presignedTTL); err == nil {
		v.DownloadURL = url
	}
	return v
}

// userIDFrom достаёт пользователя из заголовка X-User-Id или из query.
func userIDFrom(r *http.Request) (int64, error) {
	raw := r.Header.Get("X-User-Id")
	if raw == "" {
		raw = r.URL.Query().Get("user_id")
	}
	if raw == "" {
		return 0, errors.New("нужен заголовок X-User-Id или параметр user_id")
	}

	userID, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || userID <= 0 {
		return 0, errors.New("user_id должен быть положительным числом")
	}
	return userID, nil
}

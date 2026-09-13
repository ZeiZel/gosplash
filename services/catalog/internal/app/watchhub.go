package app

import (
	"sync"
	"time"

	"gosplash/services/catalog/internal/domain"
)

// watchBufferSize — ёмкость канала одного подписчика WatchListing.
//
// Небольшая нарочно: это не очередь на потом, а буфер на "клиент читает чуть
// медленнее, чем происходят события прямо сейчас". Если он переполнился,
// клиент отстал не на секунду, а системно, и копить события дальше только
// отодвигает тот момент, когда это станет заметно.
const watchBufferSize = 8

// WatchEvent — то, что уходит подписчику WatchListing.
type WatchEvent struct {
	Listing    *domain.Listing
	Change     string // "published" | "updated"
	OccurredAt time.Time
}

// WatchHub — подписки на изменения карточек, PATTERN: fan-out в памяти
// процесса (а не через Kafka/Redis Pub/Sub).
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: WatchListing — это UX ОДНОГО активного соединения:
// клиент открыл карточку и держит стрим, пока не уйдёт со страницы. Гонять
// такое уведомление через брокер добавило бы сетевую задержку и
// инфраструктурную зависимость ради события, которое живёт секунды и нужно
// ровно одному текущему запросу. Цена этого выбора — при нескольких
// репликах catalog клиент увидит обновление, только если его стрим попал
// на ТУ реплику, что применила изменение; в проде для настоящего
// многорепличного WatchListing понадобился бы общий брокер (тот же
// catalog.listing.published, который уже публикуется в Kafka для search).
// Для учебного стрима это ограничение осознанное и не влияет на корректность
// в пределах одного процесса.
type WatchHub struct {
	mu   sync.Mutex
	subs map[string]map[chan WatchEvent]struct{}
}

func NewWatchHub() *WatchHub {
	return &WatchHub{subs: make(map[string]map[chan WatchEvent]struct{})}
}

// Subscribe возвращает канал обновлений конкретной карточки и функцию отмены
// подписки. Вызывающий (gRPC-адаптер) обязан вызвать cancel по выходу из
// WatchListing — по ctx.Done() в том числе, иначе подписка переживёт стрим.
func (h *WatchHub) Subscribe(listingID string) (<-chan WatchEvent, func()) {
	ch := make(chan WatchEvent, watchBufferSize)

	h.mu.Lock()
	if h.subs[listingID] == nil {
		h.subs[listingID] = make(map[chan WatchEvent]struct{})
	}
	h.subs[listingID][ch] = struct{}{}
	h.mu.Unlock()

	cancel := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if set, ok := h.subs[listingID]; ok {
			delete(set, ch)
			if len(set) == 0 {
				delete(h.subs, listingID)
			}
		}
	}
	return ch, cancel
}

// Notify рассылает изменение всем подписчикам карточки.
//
// PATTERN: неблокирующая доставка с отключением медленного клиента вместо
// его ожидания. Индексатор — это ОДИН консьюмер на партицию, и если Notify
// заблокируется на переполненном канале одного зависшего подписчика стрима,
// встанет обработка Kafka для ВСЕХ карточек, а не только для той, что
// смотрит медленный клиент. Поэтому при переполнении канал ЗАКРЫВАЕТСЯ и
// подписчик удаляется: WatchListing увидит закрытый канал (!ok) и завершит
// RPC ошибкой, вместо того чтобы либо тормозить индексатор, либо молчать
// вечно, изображая работающий стрим.
func (h *WatchHub) Notify(listingID string, listing *domain.Listing, change string, occurredAt time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()

	set := h.subs[listingID]
	for ch := range set {
		select {
		case ch <- WatchEvent{Listing: listing, Change: change, OccurredAt: occurredAt}:
		default:
			close(ch)
			delete(set, ch)
		}
	}
}

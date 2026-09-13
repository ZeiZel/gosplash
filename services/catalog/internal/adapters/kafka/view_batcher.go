// Package kafka — адаптер каталога к Kafka: пакетная публикация событий
// просмотра.
//
// PATTERN: асинхронный батчинг в памяти процесса вместо синхронной
// публикации на каждый вызов. Путь чтения (ListingService.Get) кладёт
// событие в буфер и продолжает работу немедленно; фоновая горутина сама
// решает, когда накопленное отправить в Kafka — по размеру буфера или по
// таймеру, смотря что наступит раньше. Цена паттерна: без outbox нет
// гарантии доставки — при падении процесса содержимое буфера теряется
// безвозвратно, и это осознанный компромисс, разобранный ниже.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ — почему просмотры публикуются НАПРЯМУЮ, без outbox
// (pkg/outbox), в отличие от catalog.listing.published:
//
//  1. Объём. Просмотров на порядки больше, чем всех остальных событий
//     каталога вместе взятых (это буквально прописано в комментарии к
//     PhotoViewed в proto/gosplash/events/v1/events.proto). Outbox.Write
//     пишет строку В ТОЙ ЖЕ транзакции, что и бизнес-изменение — то есть
//     "посчитать один просмотр" превратилось бы в "открыть транзакцию
//     Postgres на КАЖДЫЙ показ карточки". При текущей нагрузке фотостока
//     это означает транзакцию БД на каждый GET /listings/{id} — то есть
//     ровно та база, которую outbox для остальных событий защищает от
//     потери сообщений, здесь стала бы узким местом сама.
//
//  2. Цена ошибки другая. Потерять catalog.listing.published означает, что
//     search никогда не узнает о карточке — это заметно и требует
//     transactional outbox. Потерять НЕСКОЛЬКО событий PhotoViewed при
//     падении процесса или переполнении буфера означает, что счётчик
//     аналитики в ClickHouse окажется на десяток штук меньше правды —
//     полностью допустимая погрешность для метрики "сколько раз посмотрели",
//     которая и так приблизительна (без outbox уже нет ровно-однократной
//     доставки, но это тот же at-least-once/at-most-once компромисс, что и
//     у остального проекта, просто без страховки outbox).
//
//  3. Путь чтения не должен зависеть от Kafka. GET /listings/{id} — самый
//     частый запрос сервиса; если бы он делал синхронный Publish (или тем
//     более синхронную транзакцию outbox) на каждый вызов, недоступность
//     Kafka положила бы чтение карточек, хотя оно вообще не должно знать
//     о существовании брокера. Буфер в памяти с фоновым флашем полностью
//     развязывает путь чтения от Kafka: Enqueue — это select с default,
//     он никогда не блокируется и не возвращает ошибку.
//
// Итог: outbox — там, где нельзя терять и нужна консистентность с бизнес-
// изменением. Прямая пакетная публикация с осознанной потерей части
// событий — там, где объём на порядки выше, а цена потери на порядки ниже.
package kafka

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"gosplash/pkg/kafkax"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// EventPublisher — то немногое, что нужно ViewBatcher от kafkax.Producer.
// Узкий интерфейс объявлен здесь же (рядом с потребителем), а не в pkg/kafkax
// — исключительно ради тестируемости: *kafkax.Producer ему соответствует
// структурно, без единой правки в pkg/kafkax (который трогать нельзя).
type EventPublisher interface {
	Publish(ctx context.Context, topic string, env *eventsv1.Envelope) error
}

const (
	// defaultFlushSize и defaultFlushInterval — жёстко заданные константы,
	// а не значения из pkg/config: подходящих переменных окружения там нет,
	// а заводить их для этой фазы не входит в её объём (см. отчёт о работе).
	// Оба значения — компромисс "не слишком часто дёргать Kafka на почти
	// пустом буфере" (интервал) и "не копить слишком долго, если трафик
	// внезапно вырос" (размер).
	defaultFlushSize     = 200
	defaultFlushInterval = 2 * time.Second

	// bufferCapacity — ёмкость канала-буфера. При переполнении Enqueue
	// молча теряет событие (см. комментарий пакета выше про допустимость
	// потери) вместо того, чтобы блокировать вызывающего — вызывающий это
	// путь чтения карточки, и он не должен ждать Kafka ни при каких
	// обстоятельствах.
	bufferCapacity = 4096
)

// ViewBatcher — реализация ports.ViewPublisher: буферизует PhotoViewed в
// памяти и публикует накопленное по размеру буфера или по таймеру, а не по
// одному сообщению на вызов Enqueue.
type ViewBatcher struct {
	producer EventPublisher

	flushSize     int
	flushInterval time.Duration

	buf  chan *eventsv1.PhotoViewed
	done chan struct{}
	wg   sync.WaitGroup
}

// NewViewBatcher запускает фоновую горутину флаша. Close обязателен на
// graceful shutdown — иначе последний неполный батч (до flushInterval) будет
// потерян целиком, а не частично.
func NewViewBatcher(producer EventPublisher, flushSize int, flushInterval time.Duration) *ViewBatcher {
	if flushSize <= 0 {
		flushSize = defaultFlushSize
	}
	if flushInterval <= 0 {
		flushInterval = defaultFlushInterval
	}

	b := &ViewBatcher{
		producer:      producer,
		flushSize:     flushSize,
		flushInterval: flushInterval,
		buf:           make(chan *eventsv1.PhotoViewed, bufferCapacity),
		done:          make(chan struct{}),
	}
	b.wg.Add(1)
	go b.run()
	return b
}

// Enqueue никогда не блокируется и не возвращает ошибку — см. комментарий
// пакета: путь чтения карточки не должен зависеть от состояния Kafka или
// от заполненности буфера.
func (b *ViewBatcher) Enqueue(photoID string, authorID, viewerID int64, country string) {
	event := &eventsv1.PhotoViewed{
		PhotoId:  photoID,
		AuthorId: authorID,
		ViewerId: viewerID,
		Country:  country,
	}
	select {
	case b.buf <- event:
	default:
		slog.Warn("catalog: буфер просмотров переполнен, событие потеряно",
			"photo_id", photoID, "capacity", bufferCapacity)
	}
}

// run — единственный читатель b.buf: копит батч, флашит по размеру или по
// таймеру, что наступит раньше.
func (b *ViewBatcher) run() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	batch := make([]*eventsv1.PhotoViewed, 0, b.flushSize)
	for {
		select {
		case event := <-b.buf:
			batch = append(batch, event)
			if len(batch) >= b.flushSize {
				batch = b.flush(batch)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				batch = b.flush(batch)
			}
		case <-b.done:
			// Дренируем то, что успело накопиться в канале без ожидания
			// новых Enqueue — при shutdown их больше не будет.
			for {
				select {
				case event := <-b.buf:
					batch = append(batch, event)
				default:
					if len(batch) > 0 {
						b.flush(batch)
					}
					return
				}
			}
		}
	}
}

// flush публикует батч ПОСЛЕДОВАТЕЛЬНО, каждое событие — своим Envelope.
//
// kafkax.Producer.Publish синхронный (ждёт подтверждения брокера) — это
// осознанное свойство pkg/kafkax (его трогать нельзя), а не выбор этого
// файла. Настоящая сетевая пакетная отправка потребовала бы асинхронного
// Produce с общим ожиданием колбэков, которого Producer не предоставляет.
// Выигрыш от батчинга здесь в другом: горутина run — ЕДИНСТВЕННОЕ место,
// которое ходит в Kafka, и она делает это НЕ на пути чтения карточки —
// путь чтения (Enqueue) не ждёт ни одного из этих Publish ни секунды.
func (b *ViewBatcher) flush(batch []*eventsv1.PhotoViewed) []*eventsv1.PhotoViewed {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, event := range batch {
		env, err := kafkax.NewEnvelope(kafkax.EventPhotoViewed, event.GetPhotoId(), event)
		if err != nil {
			slog.Error("catalog: конверт analytics.photo.viewed", "error", err)
			continue
		}
		if err := b.producer.Publish(ctx, kafkax.TopicPhotoViewed, env); err != nil {
			// Ошибка публикации одного события аналитики не должна ронять
			// весь батч и уж тем более процесс — см. комментарий пакета
			// про допустимость потери части просмотров.
			slog.Warn("catalog: не смог опубликовать просмотр, событие потеряно",
				"photo_id", event.GetPhotoId(), "error", err)
		}
	}
	return batch[:0]
}

// Close останавливает фоновую горутину, дождавшись, пока она допишет то,
// что успело накопиться в буфере. Вызывается на graceful shutdown ДО
// закрытия самого kafkax.Producer.
func (b *ViewBatcher) Close() {
	close(b.done)
	b.wg.Wait()
}

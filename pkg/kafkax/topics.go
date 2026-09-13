package kafkax

// Имена топиков и типов событий — в одном месте, потому что опечатка в строке
// топика не ловится компилятором, а авто-создание топиков в проекте выключено
// (см. deploy/compose/kafka.yml). Продюсер с опечаткой получит ошибку от
// брокера; консьюмер с опечаткой будет молча слушать пустоту — и это куда
// неприятнее.
//
// ─────────────────────────────────────────────────────────────────────────────
// КОНВЕНЦИЯ ИМЁН: <домен>.<сущность>.<факт-в-прошедшем-времени>
//
//	media.photo.uploaded          ← кто (media) · что (photo) · что случилось
//	catalog.listing.published
//	order.order.paid
//
// Три части, а не две, потому что по имени должно быть видно ПРОДЮСЕРА.
// Топик image.uploaded не отвечает на вопрос «кто это пишет», и список топиков
// перестаёт читаться как карта системы ровно в тот момент, когда сервисов
// становится больше двух. Прошедшее время — потому что событие это факт,
// который уже произошёл; на него нельзя ответить «нет». Если тянет назвать
// топик в повелительном наклонении (photo.resize), то это не событие,
// а команда, и ей место в gRPC.
// ─────────────────────────────────────────────────────────────────────────────
const (
	TopicPhotoUploaded       = "media.photo.uploaded"
	TopicPhotoThumbnailReady = "media.photo.thumbnail-ready"
	TopicPhotoDeleted        = "media.photo.deleted"

	TopicListingPublished = "catalog.listing.published"

	TopicOrderPlaced = "order.order.placed"
	TopicOrderPaid   = "order.order.paid"

	TopicAccountDebited = "wallet.account.debited"

	TopicPhotoViewed = "analytics.photo.viewed"
)

// Типы событий для поля Envelope.event_type и заголовка Kafka event_type.
// Совпадают с именами топиков не случайно: пока в топике один тип события,
// дублирование выглядит лишним, но как только в order.order.* поедут placed,
// paid и completed, консьюмер начнёт разбирать сообщения именно по этому полю.
const (
	EventPhotoUploaded       = "media.photo.uploaded"
	EventPhotoThumbnailReady = "media.photo.thumbnail-ready"
	EventPhotoDeleted        = "media.photo.deleted"

	EventListingPublished = "catalog.listing.published"
	EventPhotoViewed      = "analytics.photo.viewed"
	EventOrderPlaced      = "order.order.placed"
	EventOrderPaid        = "order.order.paid"
	EventAccountDebited   = "wallet.account.debited"
)

// RetryTopic и DLQTopic — производные имена для схемы ретраев (фаза 1).
//
// Отдельные топики, а не «положим обратно в тот же»: повторная запись в
// исходный топик ломает порядок и превращает одно битое сообщение в
// бесконечный цикл, который вытесняет полезный трафик.
func RetryTopic(topic string) string { return topic + ".retry" }
func DLQTopic(topic string) string   { return topic + ".dlq" }

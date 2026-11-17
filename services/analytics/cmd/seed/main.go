// analytics-seed — засевает photo_views синтетическими просмотрами, а
// вместе с ними — таблицы воронки uploads/thumbnails (см. seed) для демо
// (mk/analytics.mk: analytics-seed, demo-clickhouse, demo-funnel).
//
// ПОЧЕМУ ОТДЕЛЬНАЯ УТИЛИТА, А НЕ "ПРОГНАТЬ N СОБЫТИЙ ЧЕРЕЗ KAFKA": цель —
// быстро наполнить ClickHouse миллионами строк для демонстрации разницы
// между колоночным и строчным хранением (README сервиса), а не проверить
// путь Kafka→ClickHouse (для этого есть internal/adapters/kafka и его
// тесты). Гонять 5 миллионов сообщений через Kafka ради заполнения таблицы
// заняло бы на порядки больше времени и нагрузило бы брокер без всякой
// пользы для демонстрации.
//
// ПОЧЕМУ ВСТАВКА БАТЧАМИ, А НЕ ПО ОДНОЙ СТРОКЕ ЗА РАЗ — ключевой момент
// самой утилиты, не только перформанса ради:
//
// Каждый INSERT в ClickHouse — это НОВЫЙ ПАРТ на диске (immutable-файл
// с отсортированными данными и их индексом), независимо от того, одна
// в нём строка или миллион. 5 миллионов INSERT по одной строке означали бы
// 5 миллионов файлов, которые ClickHouse обязан периодически СЛИТЬ фоновым
// процессом merge в более крупные парты (иначе SELECT читает и объединяет
// результат тысяч мелких файлов вместо одного большого — это и есть
// причина, по которой "too many parts" — одна из самых частых ошибок
// начинающих с ClickHouse, а не абстрактная страшилка). Даже если движок
// не откажет вставлять (лимит на количество активных партов конечен и
// настраиваем), фоновые мержи в таком темпе никогда не угонятся за темпом
// вставки, и часть I/O диска будет постоянно тратиться на попытки навёрстывать
// упущенное вместо полезной работы. Один INSERT на batchSize строк — это
// один парт на batchSize строк, и мержи успевают справляться.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"time"

	pkgconfig "gosplash/pkg/config"
	"gosplash/services/analytics/internal/adapters/clickhouse"
	"gosplash/services/analytics/migrations"
)

// run — вся утилита целиком: разбор флагов, подключение, миграции, засев.
// Возвращает ошибку вместо os.Exit, чтобы `defer client.Close()` успевал
// отработать на любом пути выхода — раньше os.Exit(1) после миграций или
// самого засева обрывал этот defer (gocritic: exitAfterDefer), и соединение
// с ClickHouse оставалось висеть до завершения процесса самой ОС.
//
// НЕ bootstrap.App: это не сервис с жизненным циклом (нечего Run и не с чем
// Close — один синхронный проход и выход), а разовая CLI-утилита. Тот же
// приём run()+main(), что и у сервисов, устраняет ровно ту же находку
// линтера безо всякой лишней структуры вокруг него — заводить App ради
// одной функции, которая не слушает сеть и не переживёт этот вызов, было бы
// решением ради единообразия, а не ради дела (см. задание, «делай осознанно,
// а не переделывай ради единообразия»).
func run() error {
	total := flag.Int("n", 5_000_000, "сколько строк просмотров засеять")
	batchSize := flag.Int("batch", 50_000, "размер одной batch-вставки")
	photos := flag.Int("photos", 5000, "сколько различных photo_id использовать")
	windowDays := flag.Int("window-days", 30, "просмотры равномерно разбросаны по последним N дням")
	flag.Parse()

	local := pkgconfig.Load().Analytics

	client, err := clickhouse.New(clickhouse.Config{
		Addr:        local.ClickHouse.Addr,
		Database:    local.ClickHouse.Database,
		User:        local.ClickHouse.User,
		Password:    local.ClickHouse.Password,
		DialTimeout: local.ClickHouse.DialTimeout,
		ReadTimeout: local.ClickHouse.ReadTimeout,
	})
	if err != nil {
		return fmt.Errorf("analytics-seed: clickhouse: %w", err)
	}
	defer client.Close()

	ctx := context.Background()
	if err := migrations.Migrate(ctx, client.Conn); err != nil {
		return fmt.Errorf("analytics-seed: миграции: %w", err)
	}

	if err := seed(ctx, client, *total, *batchSize, *photos, *windowDays); err != nil {
		return fmt.Errorf("analytics-seed: засев: %w", err)
	}
	return nil
}

// main — три строки: вызвать run(), при ошибке залогировать и os.Exit(1).
// os.Exit здесь безопасен: run() уже вернула управление, и её defer
// (закрытие клиента ClickHouse) успел отработать до этой строки.
func main() {
	if err := run(); err != nil {
		slog.Error("analytics-seed: остановлено с ошибкой", "error", err.Error())
		os.Exit(1)
	}
}

var countries = []string{"RU", "US", "DE", "FR", "KZ", "BY", "TR", "IN", "BR", "CN"}

// photoPool — фиксированный набор photo_id/author_id: 5000 "фотографий",
// у каждой свой автор. Реалистичнее случайного UUID на КАЖДЫЙ просмотр —
// у настоящей платформы просмотры концентрируются на конечном наборе
// опубликованных карточек, а не размазаны по бесконечному множеству id
// (иначе TopPhotos и PhotoStats демонстрировать было бы не на чем: у
// каждого фото была бы ровно пара просмотров).
type photoPool struct {
	photoIDs  []string
	authorIDs []int64
}

func newPhotoPool(n int) photoPool {
	p := photoPool{photoIDs: make([]string, n), authorIDs: make([]int64, n)}
	for i := range n {
		// Валидный UUID (8-4-4-4-12 hex), а не случайная строка с префиксом
		// "seed-": колонка photo_id типизирована как UUID (см.
		// migrations/auto.go), и clickhouse-go парсит параметр через
		// uuid.Parse — невалидная строка провалит вставку.
		p.photoIDs[i] = fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i)
		// Авторов на порядок меньше, чем фото: у одного автора обычно
		// несколько карточек, это же предположение сделано в остальных
		// сервисах монорепы (services/catalog: author_id != listing_id).
		p.authorIDs[i] = int64(1000 + i%500)
	}
	return p
}

// seed вставляет total строк пачками по batchSize через clickhouse-go —
// см. комментарий пакета про то, почему не по одной строке.
//
// Вместе с просмотрами засевается воронка «загружено → превью готово»:
// на каждый ~10-й просмотр — загрузка того же фото тем же автором, и на
// ~95% этих загрузок — готовность превью со случайным лагом 200мс..30с.
// Оставшиеся ~5% загрузок «застревают в генерации» НАМЕРЕННО: дроп-офф
// воронки — это не артефакт сида, а ровно то состояние конвейера, которое
// FunnelStats.ready_percent < 100 и должен показывать (фото без превью
// не доезжает до каталога — так же, как в живой системе при падении
// thumbnail-worker).
func seed(ctx context.Context, client *clickhouse.Client, total, batchSize, photoCount, windowDays int) error {
	pool := newPhotoPool(photoCount)
	// rand/v2 не требует ручного сида: пакетовые функции сеются сами
	// (в отличие от math/rand, где фиксированный сид по умолчанию делал бы
	// каждый запуск детерминированным), а локальный источник ради одной
	// горутины здесь больше не нужен.
	now := time.Now()
	window := time.Duration(windowDays) * 24 * time.Hour

	started := time.Now()
	inserted := 0
	uploadsInserted := 0
	thumbnailsInserted := 0
	for inserted < total {
		n := batchSize
		if remaining := total - inserted; remaining < n {
			n = remaining
		}

		batch, err := client.Conn.PrepareBatch(ctx, "INSERT INTO photo_views (photo_id, author_id, viewer_id, ts, country)")
		if err != nil {
			return fmt.Errorf("prepare batch: %w", err)
		}
		uploadsBatch, err := client.Conn.PrepareBatch(ctx, "INSERT INTO uploads (photo_id, author_id, ts)")
		if err != nil {
			return fmt.Errorf("prepare batch uploads: %w", err)
		}
		thumbnailsBatch, err := client.Conn.PrepareBatch(ctx, "INSERT INTO thumbnails (photo_id, ts)")
		if err != nil {
			return fmt.Errorf("prepare batch thumbnails: %w", err)
		}

		batchUploads, batchThumbnails := 0, 0
		for range n {
			idx := rand.IntN(photoCount)
			ts := now.Add(-rand.N(window))
			country := countries[rand.IntN(len(countries))]

			// ~30% просмотров анонимные — viewer_id остаётся nil (NULL),
			// см. комментарий domain.PhotoView про то, почему это НЕ ноль.
			var viewerID *int64
			if rand.IntN(10) >= 3 {
				v := int64(rand.IntN(2_000_000))
				viewerID = &v
			}

			if err := batch.Append(pool.photoIDs[idx], pool.authorIDs[idx], viewerID, ts, country); err != nil {
				return fmt.Errorf("append: %w", err)
			}

			// Каждый ~10-й просмотр дополняется загрузкой этого фото —
			// пропорция условная: демо важен порядок величин и ненулевой
			// хвост лагов, а не точная копия продакшен-конверсии.
			if rand.IntN(10) == 0 {
				if err := uploadsBatch.Append(pool.photoIDs[idx], pool.authorIDs[idx], ts); err != nil {
					return fmt.Errorf("append uploads: %w", err)
				}
				batchUploads++

				// ~95% загрузок получают превью; лаг 200мс..30s — типичный
				// коридор ресайза из thumbnail-worker плюс очередь.
				if rand.IntN(100) < 95 {
					latency := 200*time.Millisecond + time.Duration(rand.Int64N(int64(30*time.Second-200*time.Millisecond)))
					if err := thumbnailsBatch.Append(pool.photoIDs[idx], ts.Add(latency)); err != nil {
						return fmt.Errorf("append thumbnails: %w", err)
					}
					batchThumbnails++
				}
			}
		}

		if err := batch.Send(); err != nil {
			return fmt.Errorf("send batch (%d строк): %w", n, err)
		}
		// Пустой батч не отправляем: последних строк окна может не хватить
		// ни на одну загрузку, и Send без строк — это лишний round-trip
		// (а для каких-то версий драйвера и ошибка).
		if batchUploads > 0 {
			if err := uploadsBatch.Send(); err != nil {
				return fmt.Errorf("send uploads batch (%d строк): %w", batchUploads, err)
			}
		}
		if batchThumbnails > 0 {
			if err := thumbnailsBatch.Send(); err != nil {
				return fmt.Errorf("send thumbnails batch (%d строк): %w", batchThumbnails, err)
			}
		}

		inserted += n
		uploadsInserted += batchUploads
		thumbnailsInserted += batchThumbnails
		slog.Info("analytics-seed: вставлено",
			"inserted", inserted, "total", total,
			"uploads", uploadsInserted, "thumbnails", thumbnailsInserted,
			"elapsed", time.Since(started).Round(time.Millisecond))
	}

	slog.Info("analytics-seed: готово",
		"rows", inserted, "uploads", uploadsInserted, "thumbnails", thumbnailsInserted,
		"elapsed", time.Since(started).Round(time.Millisecond))
	return nil
}

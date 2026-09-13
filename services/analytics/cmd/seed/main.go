// analytics-seed — засевает photo_views синтетическими просмотрами для
// демо (mk/analytics.mk: analytics-seed, demo-clickhouse).
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
	"math/rand"
	"os"
	"time"

	pkgconfig "gosplash/pkg/config"
	"gosplash/services/analytics/internal/adapters/clickhouse"
	"gosplash/services/analytics/migrations"
)

func main() {
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
		slog.Error("analytics-seed: clickhouse", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	ctx := context.Background()
	if err := migrations.Migrate(ctx, client.Conn); err != nil {
		slog.Error("analytics-seed: миграции", "error", err)
		os.Exit(1)
	}

	if err := seed(ctx, client, *total, *batchSize, *photos, *windowDays); err != nil {
		slog.Error("analytics-seed: засев", "error", err)
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
	for i := 0; i < n; i++ {
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
func seed(ctx context.Context, client *clickhouse.Client, total, batchSize, photoCount, windowDays int) error {
	pool := newPhotoPool(photoCount)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	now := time.Now()
	window := time.Duration(windowDays) * 24 * time.Hour

	started := time.Now()
	inserted := 0
	for inserted < total {
		n := batchSize
		if remaining := total - inserted; remaining < n {
			n = remaining
		}

		batch, err := client.Conn.PrepareBatch(ctx, "INSERT INTO photo_views (photo_id, author_id, viewer_id, ts, country)")
		if err != nil {
			return fmt.Errorf("prepare batch: %w", err)
		}

		for i := 0; i < n; i++ {
			idx := rng.Intn(photoCount)
			ts := now.Add(-time.Duration(rng.Int63n(int64(window))))
			country := countries[rng.Intn(len(countries))]

			// ~30% просмотров анонимные — viewer_id остаётся nil (NULL),
			// см. комментарий domain.PhotoView про то, почему это НЕ ноль.
			var viewerID *int64
			if rng.Intn(10) >= 3 {
				v := int64(rng.Intn(2_000_000))
				viewerID = &v
			}

			if err := batch.Append(pool.photoIDs[idx], pool.authorIDs[idx], viewerID, ts, country); err != nil {
				return fmt.Errorf("append: %w", err)
			}
		}

		if err := batch.Send(); err != nil {
			return fmt.Errorf("send batch (%d строк): %w", n, err)
		}

		inserted += n
		slog.Info("analytics-seed: вставлено",
			"inserted", inserted, "total", total,
			"elapsed", time.Since(started).Round(time.Millisecond))
	}

	slog.Info("analytics-seed: готово",
		"rows", inserted, "elapsed", time.Since(started).Round(time.Millisecond))
	return nil
}

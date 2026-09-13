//go:build integration

// Интеграционные тесты пакета redisx — требуют Docker (testcontainers-go
// поднимает redis:8-alpine, тот же образ, что и deploy/compose/redis.yml).
// Запуск: go test -tags=integration ./pkg/redisx/...
//
// Без тега пакет собирается и тестируется (см. остальные *_test.go) без
// какой-либо инфраструктуры — этот файл не участвует в обычном `go test ./...`.
package redisx

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
)

// newTestClient поднимает отдельный контейнер Redis на тест.
//
// Отдельный контейнер, а не один общий на пакет: тесты создают, инвалидируют
// и просрачивают ключи в одном и том же namespace сервиса, и общее состояние
// между тестами сделало бы падения недетерминированными (тест Б видит ключ,
// оставленный тестом А). Контейнер поднимается за секунды, а тесты и так
// изолированы от обычного прогона тегом integration.
func newTestClient(t *testing.T) *Client {
	t.Helper()
	ctx := context.Background()

	container, err := testcontainers.Run(ctx, "redis:8-alpine",
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("6379/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, container.Terminate(context.Background()))
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err)

	c, err := New(Config{
		Addr:        fmt.Sprintf("%s:%s", host, port.Port()),
		PoolSize:    10,
		MaxRetries:  3,
		DialTimeout: 5 * time.Second,
		ReadTimeout: 5 * time.Second,
		ServiceName: "redisx-test",
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, c.Close()) })

	return c
}

func TestClient_PingUspeshenNaZhivomRedis(t *testing.T) {
	c := newTestClient(t)
	assert.NoError(t, c.Ping(context.Background()))
}

// ─────────────────────────────────────────────────────────────────────────────
// cache-aside
// ─────────────────────────────────────────────────────────────────────────────

type cachedListing struct {
	ID    string
	Title string
}

func TestGetOrLoadJSON_PromahPotomPopadanie(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	key := c.Key("listing", "1")

	var loads int32
	load := func(context.Context) (cachedListing, error) {
		atomic.AddInt32(&loads, 1)
		return cachedListing{ID: "1", Title: "Закат"}, nil
	}

	v1, err := GetOrLoadJSON(ctx, c, key, time.Minute, load)
	require.NoError(t, err)
	assert.Equal(t, "Закат", v1.Title)
	assert.EqualValues(t, 1, atomic.LoadInt32(&loads), "первый вызов — обязан промахнуться и загрузить")

	v2, err := GetOrLoadJSON(ctx, c, key, time.Minute, load)
	require.NoError(t, err)
	assert.Equal(t, v1, v2)
	assert.EqualValues(t, 1, atomic.LoadInt32(&loads), "второй вызов — обязан попасть в кэш, load больше не звать")
}

func TestGetOrLoadJSON_SingleflightSхлопываетOdnovremennyePromahi(t *testing.T) {
	// Честное ограничение, зафиксированное doc-комментарием в cache.go:
	// singleflight схлопывает промахи только в ОДНОМ процессе. Здесь у нас
	// как раз один процесс (один *Client) с N горутинами — ровно тот случай,
	// для которого singleflight и добавлен.
	c := newTestClient(t)
	ctx := context.Background()
	key := c.Key("listing", "stampede")

	var loads int32
	const concurrent = 50
	release := make(chan struct{})
	load := func(context.Context) (cachedListing, error) {
		atomic.AddInt32(&loads, 1)
		<-release // держим load "в полёте", пока не соберутся все горутины
		return cachedListing{ID: "stampede", Title: "толпа"}, nil
	}

	var wg sync.WaitGroup
	results := make([]cachedListing, concurrent)
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := GetOrLoadJSON(ctx, c, key, time.Minute, load)
			assert.NoError(t, err)
			results[i] = v
		}(i)
	}

	// Даём горутинам время дойти до load и встать на <-release.
	time.Sleep(200 * time.Millisecond)
	close(release)
	wg.Wait()

	assert.EqualValues(t, 1, atomic.LoadInt32(&loads),
		"N одновременных промахов по одному ключу обязаны вызвать load РОВНО один раз")
	for _, v := range results {
		assert.Equal(t, "толпа", v.Title)
	}
}

func TestGetOrLoadProto_RoundTrip(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	key := c.Key("listing-proto", "1")

	var loads int32
	load := func(context.Context) (*catalogv1.Listing, error) {
		atomic.AddInt32(&loads, 1)
		return &catalogv1.Listing{Id: "1", Title: "Горы", Tags: []string{"nature", "hdr"}}, nil
	}

	v1, err := GetOrLoadProto[catalogv1.Listing](ctx, c, key, time.Minute, load)
	require.NoError(t, err)
	assert.Equal(t, "Горы", v1.GetTitle())
	assert.EqualValues(t, 1, loads)

	v2, err := GetOrLoadProto[catalogv1.Listing](ctx, c, key, time.Minute, load)
	require.NoError(t, err)
	assert.Equal(t, v1.GetId(), v2.GetId())
	assert.Equal(t, v1.GetTags(), v2.GetTags())
	assert.EqualValues(t, 1, loads, "второй вызов обязан попасть в кэш")
}

func TestInvalidate_UnlinkUdalyaetKlyuch(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	key := c.Key("listing", "to-invalidate")

	_, err := GetOrLoadJSON(ctx, c, key, time.Minute, func(context.Context) (cachedListing, error) {
		return cachedListing{ID: "x"}, nil
	})
	require.NoError(t, err)

	require.NoError(t, c.Invalidate(ctx, key))

	var loads int32
	_, err = GetOrLoadJSON(ctx, c, key, time.Minute, func(context.Context) (cachedListing, error) {
		atomic.AddInt32(&loads, 1)
		return cachedListing{ID: "x"}, nil
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, loads, "после Invalidate ключ обязан отсутствовать — новый вызов снова промахивается")
}

// ─────────────────────────────────────────────────────────────────────────────
// rate limiter
// ─────────────────────────────────────────────────────────────────────────────

func TestAllow_RazreshaetVPredelahEmkostiIOtkazyvaetPosle(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	key := c.Key("rl", "user-1")

	for i := 0; i < 3; i++ {
		allowed, _, err := c.Allow(ctx, key, 3, 1)
		require.NoError(t, err)
		assert.Truef(t, allowed, "запрос %d обязан пройти — бакет ещё не пуст", i+1)
	}

	allowed, retryAfter, err := c.Allow(ctx, key, 3, 1)
	require.NoError(t, err)
	assert.False(t, allowed, "четвёртый запрос подряд обязан быть отклонён")
	assert.Positive(t, retryAfter, "отказ обязан сопровождаться Retry-After")
}

func TestAllow_PopolnyaetsyaSoVremenem(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	key := c.Key("rl", "user-2")

	allowed, _, err := c.Allow(ctx, key, 1, 5) // ёмкость 1, пополнение 5 токенов/сек
	require.NoError(t, err)
	require.True(t, allowed)

	allowed, retryAfter, err := c.Allow(ctx, key, 1, 5)
	require.NoError(t, err)
	require.False(t, allowed)

	time.Sleep(retryAfter + 50*time.Millisecond)

	allowed, _, err = c.Allow(ctx, key, 1, 5)
	require.NoError(t, err)
	assert.True(t, allowed, "после ожидания Retry-After бакет обязан пополниться")
}

func TestAllow_NevalidnayaEmkost(t *testing.T) {
	c := newTestClient(t)
	_, _, err := c.Allow(context.Background(), c.Key("rl", "bad"), 0, 1)
	assert.Error(t, err)
}

// ─────────────────────────────────────────────────────────────────────────────
// distributed lock
// ─────────────────────────────────────────────────────────────────────────────

func TestAcquire_VzaimnoeIsklyuchenieIOsvobozhdenie(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	key := c.Key("lock", "thumbnail-42")

	l1, err := c.Acquire(ctx, key, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, l1, "первый Acquire обязан взять свободный лок")

	l2, err := c.Acquire(ctx, key, 5*time.Second)
	require.NoError(t, err)
	assert.Nil(t, l2, "второй Acquire на занятый ключ обязан вернуть (nil, nil), а не ошибку")

	require.NoError(t, l1.Unlock(ctx))

	l3, err := c.Acquire(ctx, key, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, l3, "после Unlock ключ обязан снова стать свободным")
	require.NoError(t, l3.Unlock(ctx))
}

func TestUnlock_ChuzhoyTokenNeSnimaetsya(t *testing.T) {
	// Симулируем ровно ту ситуацию, ради которой нужна сверка токена: лок
	// протух (или мы считаем, что владеем им), но ключ в Redis уже принадлежит
	// другому владельцу. Заменяем значение напрямую через Raw(), не трогая
	// сам объект Lock, — так же, как если бы TTL истёк и другой процесс
	// перехватил ключ, пока watchdog не успел продлить.
	c := newTestClient(t)
	ctx := context.Background()
	key := c.Key("lock", "stolen")

	l, err := c.Acquire(ctx, key, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, l)

	require.NoError(t, c.rdb.Set(ctx, key, "чужой-токен", 5*time.Second).Err())

	err = l.Unlock(ctx)
	assert.Error(t, err, "Unlock обязан отказаться снимать чужой лок")

	got, err := c.rdb.Get(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, "чужой-токен", got, "чужой лок обязан остаться нетронутым")
}

func TestAcquire_WatchdogProdlevaetLokDolzheEgoIznachalnogoTTL(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	key := c.Key("lock", "long-job")

	ttl := 300 * time.Millisecond
	l, err := c.Acquire(ctx, key, ttl)
	require.NoError(t, err)
	require.NotNil(t, l)

	// Работа длится дольше исходного TTL. Без watchdog лок протух бы
	// на середине, и следующий Acquire отдал бы его конкуренту.
	time.Sleep(ttl * 3)

	l2, err := c.Acquire(ctx, key, ttl)
	require.NoError(t, err)
	assert.Nil(t, l2, "watchdog обязан был продлить лок — он всё ещё занят")

	require.NoError(t, l.Unlock(ctx))
}

// ─────────────────────────────────────────────────────────────────────────────
// idempotency-key
// ─────────────────────────────────────────────────────────────────────────────

func TestBeginComplete_TriIshoda(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	key := c.Key("idem", "order-1")

	first, err := c.Begin(ctx, key, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, IdempotencyFree, first.Status, "первый вызов — путь свободен")

	second, err := c.Begin(ctx, key, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, IdempotencyInProgress, second.Status,
		"параллельный повтор до Complete обязан получить конфликт (409 на стороне HTTP)")

	response := []byte(`{"order_id":"1","status":"paid"}`)
	require.NoError(t, c.Complete(ctx, key, response, time.Minute))

	third, err := c.Begin(ctx, key, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, IdempotencyDone, third.Status)
	assert.Equal(t, response, third.Response, "повтор после Complete обязан вернуть ТОТ ЖЕ ответ")
}

// ─────────────────────────────────────────────────────────────────────────────
// Sorted Set — топ-N
// ─────────────────────────────────────────────────────────────────────────────

func TestIncrViewTop_PoryadokPoUbyvaniyu(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	now := time.Now()

	views := map[string]int{"photo-a": 5, "photo-b": 10, "photo-c": 1}
	for item, n := range views {
		for i := 0; i < n; i++ {
			require.NoError(t, c.IncrView(ctx, item, now))
		}
	}

	top, err := c.Top(ctx, now, 2)
	require.NoError(t, err)
	require.Len(t, top, 2)
	assert.Equal(t, "photo-b", top[0].Item)
	assert.Equal(t, float64(10), top[0].Score)
	assert.Equal(t, "photo-a", top[1].Item)
}

func TestIncrView_RazniyeSutkiNeSmeshivayutsya(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	today := time.Now()
	yesterday := today.Add(-24 * time.Hour)

	require.NoError(t, c.IncrView(ctx, "photo-x", yesterday))

	top, err := c.Top(ctx, today, 10)
	require.NoError(t, err)
	for _, e := range top {
		assert.NotEqual(t, "photo-x", e.Item, "просмотр вчерашнего дня не должен попадать в топ сегодняшнего")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HyperLogLog
// ─────────────────────────────────────────────────────────────────────────────

func TestTrackUniqueView_PriblizitelnyySchetIDeduplikaciya(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	const n = 2000
	for i := 0; i < n; i++ {
		require.NoError(t, c.TrackUniqueView(ctx, "photo-popular", fmt.Sprintf("viewer-%d", i)))
	}
	// Повторный просмотр теми же зрителями не должен ничего добавить —
	// в этом и разница PFADD с обычным INCR.
	for i := 0; i < n; i++ {
		require.NoError(t, c.TrackUniqueView(ctx, "photo-popular", fmt.Sprintf("viewer-%d", i)))
	}

	got, err := c.UniqueViewCount(ctx, "photo-popular")
	require.NoError(t, err)

	// Погрешность HyperLogLog ~0.81%, берём запас 5%, чтобы тест не был
	// хрупким к конкретной реализации хэша внутри Redis.
	assert.InDeltaf(t, n, got, float64(n)*0.05,
		"PFCOUNT обязан быть БЛИЗОК к реальному числу уникальных, не точным: got=%d, want≈%d", got, n)
}

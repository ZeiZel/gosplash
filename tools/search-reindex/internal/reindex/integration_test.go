//go:build integration

package reindex

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tces "github.com/testcontainers/testcontainers-go/modules/elasticsearch"
)

// testESImage — см. объяснение выбора версии в
// services/search/internal/adapters/es/integration_test.go: любой
// 8.x/9.x-совместимый образ подходит, взят уже закэшированный локально.
const testESImage = "docker.elastic.co/elasticsearch/elasticsearch:9.5.2"

func startES(t *testing.T) *ESAdapter {
	t.Helper()
	ctx := context.Background()

	container, err := tces.Run(ctx, testESImage,
		testcontainers.WithEnv(map[string]string{"xpack.security.enabled": "false"}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	adapter, err := NewESAdapter([]string{container.Settings.Address})
	require.NoError(t, err)

	// Ждём готовности кластера тем же способом, что и у search-сервиса:
	// первый CreateIndex после старта контейнера иначе может улететь в
	// "connection refused", пока Elasticsearch ещё поднимается изнутри.
	require.Eventually(t, func() bool {
		_, _, err := adapter.CurrentIndex(ctx, "healthcheck-probe")
		return err == nil
	}, 60*time.Second, 500*time.Millisecond, "кластер не поднялся за отведённое время")

	return adapter
}

// TestReindex_PereklyucheniyeAlias — главный сценарий, ради которого
// существует этот инструмент: индекс с новым маппингом собирается ЗАРАНЕЕ
// (пока alias всё ещё смотрит на старый), и только после полного наполнения
// alias переключается ОДНИМ атомарным запросом. Тест проверяет именно это
// атомарное поведение на настоящем Elasticsearch, а не на подделке.
func TestReindex_PereklyucheniyeAlias(t *testing.T) {
	es := startES(t)
	ctx := context.Background()
	alias := fmt.Sprintf("listings-reindex-test-%d", time.Now().UnixNano())

	oldIndex := alias + "_old"
	require.NoError(t, es.CreateIndex(ctx, oldIndex))
	require.NoError(t, es.BulkIndex(ctx, oldIndex, []CatalogListing{
		{ID: "old-1", Title: "Старая карточка", Status: "published", PublishedAt: time.Now()},
	}))
	require.NoError(t, es.SwitchAlias(ctx, alias, oldIndex, ""))

	// alias уже смотрит на старый индекс — ровно та ситуация, в которой
	// Reindexer.Run обнаружит hadOld=true при следующем запуске.
	current, exists, err := es.CurrentIndex(ctx, alias)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, oldIndex, current)

	// Наполняем НОВЫЙ индекс, пока alias ещё указывает на старый — это и
	// есть суть "без даунтайма": читатели через alias всё это время видят
	// старые, но полностью рабочие данные.
	newIndex := alias + "_new"
	require.NoError(t, es.CreateIndex(ctx, newIndex))
	require.NoError(t, es.BulkIndex(ctx, newIndex, []CatalogListing{
		{ID: "new-1", Title: "Новая карточка", Status: "published", PublishedAt: time.Now()},
	}))

	require.NoError(t, es.SwitchAlias(ctx, alias, newIndex, oldIndex))

	current, exists, err = es.CurrentIndex(ctx, alias)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, newIndex, current, "alias обязан указывать на НОВЫЙ индекс сразу после переключения")

	require.NoError(t, es.DeleteIndex(ctx, oldIndex))

	// Старого индекса больше нет — второй DeleteIndex того же имени обязан
	// вернуть ошибку, а не тихо succeed.
	err = es.DeleteIndex(ctx, oldIndex)
	assert.Error(t, err)
}

// TestReindex_CurrentIndex_AliasNeSuschestvuyet — самый первый запуск:
// alias ещё ни разу не создавался.
func TestReindex_CurrentIndex_AliasNeSuschestvuyet(t *testing.T) {
	es := startES(t)
	ctx := context.Background()

	_, exists, err := es.CurrentIndex(ctx, fmt.Sprintf("listings-nikogda-ne-suschestvoval-%d", time.Now().UnixNano()))
	require.NoError(t, err)
	assert.False(t, exists)
}

// TestReindex_SkvoznoyProgon — Reindexer целиком (не отдельные методы
// ESAdapter) поверх настоящего Elasticsearch и подделки каталога:
// gRPC-клиент catalog здесь намеренно НЕ поднимается — для сквозной
// проверки alias-механики достаточно фиксированных страниц, а поднимать
// ради этого ещё и весь catalog-сервис means testing чужую фазу, а не свою.
func TestReindex_SkvoznoyProgon(t *testing.T) {
	es := startES(t)
	ctx := context.Background()
	alias := fmt.Sprintf("listings-e2e-%d", time.Now().UnixNano())

	catalog := &fakeCatalog{
		pages: [][]CatalogListing{
			{{ID: "e1", Title: "Закат", Status: "published", PublishedAt: time.Now()}},
		},
		nextCursors: []string{""},
	}

	r := New(catalog, es, time.Now)
	result, err := r.Run(ctx, Options{Alias: alias, BatchSize: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.DocsIndexed)
	assert.Empty(t, result.OldIndex, "первый запуск — старого индекса не было")
	assert.False(t, result.OldIndexDeleted)

	current, exists, err := es.CurrentIndex(ctx, alias)
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, result.NewIndex, current)

	// Пауза нужна ТОЛЬКО тесту: имя нового индекса строится по unix-секунде
	// (reindex.go), и без паузы два прогона внутри одной секунды попытались
	// бы создать индекс с одинаковым именем.
	time.Sleep(1100 * time.Millisecond)

	// Второй прогон — уже со старым индексом, который должен быть заменён
	// и удалён.
	result2, err := r.Run(ctx, Options{Alias: alias, BatchSize: 10})
	require.NoError(t, err)
	assert.Equal(t, current, result2.OldIndex)
	assert.True(t, result2.OldIndexDeleted)
	assert.NotEqual(t, result.NewIndex, result2.NewIndex)
}

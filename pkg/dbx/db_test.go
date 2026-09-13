package dbx

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Тест на ShardIndex проверяет не арифметику, а СВОЙСТВА, на которые
// опирается весь сервис. Проверять «hash(42) == 1» бессмысленно: это
// закрепило бы конкретное значение fnv, а не то, что от него требуется.

func TestShardIndex_Detereminirovan(t *testing.T) {
	// Главное свойство: одно и то же значение при каждом вызове, в каждом
	// процессе, после каждого перезапуска. Именно поэтому в коде fnv, а не
	// hash/maphash — тот рандомизирован при старте программы, и фотографии
	// после рестарта «переезжали» бы на другой шард.
	for userID := int64(1); userID <= 100; userID++ {
		first := ShardIndex(userID, 2)
		for range 10 {
			require.Equal(t, first, ShardIndex(userID, 2),
				"шард пользователя %d изменился между вызовами", userID)
		}
	}
}

func TestShardIndex_VDiapazone(t *testing.T) {
	tests := []struct {
		name       string
		shardCount int
	}{
		{"один шард", 1},
		{"два шарда", 2},
		{"три шарда", 3},
		{"шестнадцать шардов", 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for userID := int64(-50); userID <= 1000; userID++ {
				idx := ShardIndex(userID, tt.shardCount)
				require.GreaterOrEqual(t, idx, 0, "user %d", userID)
				require.Less(t, idx, tt.shardCount, "user %d", userID)
			}
		})
	}
}

func TestShardIndex_NolShardovNePanikuet(t *testing.T) {
	// Защита от деления на ноль: Shards.NewShards не даст создать пустой
	// набор, но функция публичная, и вызвать её могут откуда угодно.
	assert.Equal(t, 0, ShardIndex(42, 0))
	assert.Equal(t, 0, ShardIndex(42, -1))
}

func TestShardIndex_RaspredelenieRovnoe(t *testing.T) {
	// Хэш не обязан раскладывать идеально поровну, но перекос в разы означал
	// бы, что половина нагрузки уедет на один сервер. Допускаем отклонение
	// в 20 % от идеала на выборке в 10 000 пользователей.
	const users = 10_000
	const shards = 4

	counts := make([]int, shards)
	for userID := int64(1); userID <= users; userID++ {
		counts[ShardIndex(userID, shards)]++
	}

	ideal := users / shards
	for i, got := range counts {
		assert.InDelta(t, ideal, got, float64(ideal)*0.2,
			"шард %d получил %d из %d пользователей", i, got, users)
	}
}

func TestShardIndex_DobavlenieShardaPeremeshivaetKlyuchi(t *testing.T) {
	// Тест фиксирует ИЗВЕСТНУЮ СЛАБОСТЬ, а не желаемое поведение: при
	// переходе с 2 шардов на 3 больше половины пользователей меняют шард.
	// Если однажды кто-то заменит формулу на consistent hashing, этот тест
	// упадёт — и это будет правильным поводом переписать его вместе с ADR.
	moved := 0
	const users = 1000
	for userID := int64(1); userID <= users; userID++ {
		if ShardIndex(userID, 2) != ShardIndex(userID, 3) {
			moved++
		}
	}
	assert.Greater(t, moved, users/2,
		"ожидалось, что %% переезжающих ключей при 2→3 будет больше половины")
}

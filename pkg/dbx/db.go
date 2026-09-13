// Package dbx — подключение к PostgreSQL через GORM.
//
// Здесь живут два разных ответа на вопрос «база не справляется»:
//
//	Shards  — данные РАЗРЕЗАНЫ по нескольким серверам. Каждая строка лежит
//	          ровно на одном. Лечит объём и нагрузку на запись. Цена: запрос
//	          без ключа шардирования становится дорогим, а транзакция
//	          между шардами — невозможной.
//	Replica — данные СКОПИРОВАНЫ на второй сервер. Лечит нагрузку на чтение
//	          и потерю сервера. Цена: реплика отстаёт, и запись, сделанную
//	          миллисекунду назад, можно там не увидеть.
//
// Их постоянно путают. Правило: «много пишем» → шарды, «много читаем» → реплики.
// В проекте media шардирован, catalog реплицирован.
package dbx

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/plugin/dbresolver"
	"gorm.io/plugin/opentelemetry/tracing"
)

// Open — одно обычное соединение. Пул настроен явно: значения по умолчанию
// у database/sql (неограниченное число открытых соединений) быстро упирают
// Postgres в max_connections, когда сервис запущен в нескольких репликах.
func Open(dsn string) (*gorm.DB, error) {
	gdb, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		return nil, fmt.Errorf("подключение к postgres: %w", err)
	}

	// Плагин добавляет спан на каждый запрос. Без него трейс обрывается на
	// входе в репозиторий, и на вопрос «медленный запрос или медленный код»
	// приходится отвечать гаданием.
	//
	// WithoutMetrics: плагин умеет снимать метрики пула, но делает это своим
	// способом и мимо нашего meter'а — метрики пула снимаются отдельно.
	// WithoutQueryVariables: значения параметров в спан не попадают, в них
	// бывают персональные данные, и место им в логе базы, а не в трассировке.
	if err := gdb.Use(tracing.NewPlugin(
		tracing.WithoutMetrics(),
		tracing.WithoutQueryVariables(),
	)); err != nil {
		return nil, fmt.Errorf("подключение трассировки gorm: %w", err)
	}

	sqlDB, err := gdb.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(20)
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetConnMaxLifetime(time.Hour)

	return gdb, nil
}

// Ping — проверка живости для /readyz.
//
// PingContext, а не «выполним SELECT 1»: пинг берёт соединение из пула и
// проверяет именно его, не создавая нагрузки на планировщик запросов.
func Ping(ctx context.Context, gdb *gorm.DB) error {
	sqlDB, err := gdb.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

// ─────────────────────────────────────────────────────────────────────────────
// РЕПЛИКАЦИЯ
// ─────────────────────────────────────────────────────────────────────────────

// OpenWithReplica возвращает соединение, которое само разводит запросы:
// SELECT уходит на реплику, INSERT/UPDATE/DELETE — на primary.
//
// Делает это плагин dbresolver. Он смотрит на тип запроса, а не на то, что
// ты имел в виду, поэтому есть два важных исключения:
//
//   - внутри db.Transaction(...) ВСЁ идёт в primary, включая чтения.
//     Это правильно: иначе транзакция читала бы отставшие данные;
//   - принудительно отправить чтение в primary можно так:
//     db.Clauses(dbresolver.Write).First(&x)
//     Нужно, когда только что записал и обязан увидеть свою запись
//     (read-your-writes).
//
// Если replicaDSN пустой, вернётся обычное соединение с primary. Так сервис
// работает и без реплики — удобно на старте и в тестах.
func OpenWithReplica(primaryDSN, replicaDSN string) (*gorm.DB, error) {
	gdb, err := Open(primaryDSN)
	if err != nil {
		return nil, err
	}
	if replicaDSN == "" {
		return gdb, nil
	}

	err = gdb.Use(dbresolver.Register(dbresolver.Config{
		Replicas: []gorm.Dialector{postgres.Open(replicaDSN)},
		// При нескольких репликах запросы раскидываются по кругу.
		Policy: dbresolver.RandomPolicy{},
		// TraceResolverMode: true в логе покажет, какое соединение выбрано, —
		// включи, когда будешь разбираться, почему чтение попало не туда.
	}))
	if err != nil {
		return nil, fmt.Errorf("настройка реплики: %w", err)
	}
	return gdb, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// ШАРДИРОВАНИЕ
// ─────────────────────────────────────────────────────────────────────────────

// Shards — набор одинаковых по схеме, но разных по содержимому баз.
//
// Postgres про шардирование не знает: для него это просто два независимых
// сервера. Всё решение «в какой писать» принимает Go-код, вот этот файл.
type Shards struct {
	conns []*gorm.DB
}

func NewShards(dsns []string) (*Shards, error) {
	if len(dsns) == 0 {
		return nil, fmt.Errorf("не задан ни один шард (MEDIA_SHARD_0_DSN)")
	}
	s := &Shards{conns: make([]*gorm.DB, 0, len(dsns))}
	for i, dsn := range dsns {
		conn, err := Open(dsn)
		if err != nil {
			return nil, fmt.Errorf("шард %d: %w", i, err)
		}
		s.conns = append(s.conns, conn)
	}
	return s, nil
}

// Index — где живут данные пользователя.
//
// Хэш обязан быть СТАБИЛЬНЫМ между запусками и между процессами: media-сервис
// и tools/automigrate должны считать одинаково. Поэтому fnv, а не встроенный
// хэш map'ы: hash/maphash специально рандомизирован при каждом старте
// программы, и с ним фотографии после рестарта «переезжали» бы на другой шард.
func (s *Shards) Index(userID int64) int {
	return ShardIndex(userID, len(s.conns))
}

// ShardIndex — та же формула отдельной функцией, без соединений.
//
// Вынесена из метода по двум причинам: её можно протестировать, не поднимая
// ни одной базы, и её можно вызвать из скрипта миграции данных, который
// считает «куда строка переедет», не открывая целевой шард.
//
// Здесь же видно главную слабость схемы: делитель — это КОЛИЧЕСТВО шардов.
// Добавишь третий — hash % 3 отличается от hash % 2 почти для всех ключей,
// и почти все данные окажутся «не там». Лечится это consistent hashing или
// виртуальными шардами (много логических шардов на малом числе физических),
// и именно поэтому в проде число шардов выбирают заранее с запасом.
func ShardIndex(userID int64, shardCount int) int {
	if shardCount <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(strconv.FormatInt(userID, 10)))
	return int(h.Sum32() % uint32(shardCount))
}

// For — соединение с шардом, где лежат данные пользователя.
// Все фото одного автора всегда на одном шарде: значит, «покажи мои фото»
// — это один обычный запрос, а не опрос всех серверов.
func (s *Shards) For(userID int64) *gorm.DB {
	return s.conns[s.Index(userID)]
}

// All — все шарды. Нужен для запросов БЕЗ ключа шардирования: «последние
// загрузки по всему сайту» приходится собирать со всех серверов и склеивать
// в памяти. Это называется scatter-gather, и это дорого: время ответа равно
// времени самого медленного шарда, а сортировка и пагинация ломаются.
//
// Именно поэтому в проекте есть catalog: он держит денормализованную копию
// в одной базе, чтобы публичные списки не ходили по шардам.
func (s *Shards) All() []*gorm.DB { return s.conns }

func (s *Shards) Count() int { return len(s.conns) }

// Close закрывает пулы. Вызывается при graceful shutdown.
func (s *Shards) Close() {
	for _, c := range s.conns {
		if sqlDB, err := c.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
}

// Ping проверяет ВСЕ шарды. Один недоступный шард означает, что сервис не
// готов: он не сможет обслужить пользователей, чьи данные лежат именно там,
// а по номеру шарда заранее не угадаешь, кто придёт следующим.
func (s *Shards) Ping(ctx context.Context) error {
	for i, conn := range s.conns {
		if err := Ping(ctx, conn); err != nil {
			return fmt.Errorf("шард %d: %w", i, err)
		}
	}
	return nil
}

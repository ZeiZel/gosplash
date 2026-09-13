// Package reindex — переиндексация каталога в НОВЫЙ индекс Elasticsearch
// без даунтайма, через alias.
//
// PATTERN: alias indirection. Elasticsearch не умеет менять маппинг уже
// созданного индекса — поля, однажды типизированные (services/search
// объясняет почему в internal/adapters/es/mapping.go), неизменяемы навсегда.
// Единственный способ сменить схему — создать НОВЫЙ индекс с новым
// маппингом и переключить на него указатель. Alias и есть этот указатель:
// клиенты (search-сервис, его консьюмер) всегда обращаются по имени alias
// ("listings"), никогда — по физическому имени индекса ("listings_1699..").
// Переключение — это ОДИН атомарный запрос _aliases (add новый + remove
// старый), поэтому снаружи оно не видно вообще: до запроса alias указывал
// на старый индекс, после — на новый, промежуточного состояния "alias не
// указывает никуда" не существует.
//
// Оркестрация (этот файл) не знает, что за ней Elasticsearch и gRPC — она
// работает через два узких интерфейса (CatalogClient, ESClient), что и
// делает её тестируемой подделками без docker (reindex_test.go). Реальные
// реализации — catalog_adapter.go и es_adapter.go.
package reindex

import (
	"context"
	"fmt"
	"time"
)

// CatalogListing — карточка каталога в объёме, нужном индексу поиска.
// Отдельный тип от catalogv1.Listing (а не прямое использование
// сгенерированного protobuf-типа за пределами catalog_adapter.go) — по той
// же причине, что и domain.ListingDoc в services/search: оркестрации не
// нужно знать про protobuf-теги и Any, только про поля документа.
type CatalogListing struct {
	ID          string
	AuthorID    int64
	AuthorName  string
	Title       string
	Tags        []string
	PriceCents  int64
	Currency    string
	Status      string
	PublishedAt time.Time
}

// CatalogClient — то, что нужно Reindexer от каталога: постранично отдать
// ВСЕ карточки. Keyset-пагинация (cursor), а не offset — тот же контракт,
// что и у catalog.v1.CatalogService.ListListings (proto/gosplash/catalog/v1),
// который эта переиндексация и вызывает.
type CatalogClient interface {
	ListListings(ctx context.Context, limit int32, cursor string) (listings []CatalogListing, nextCursor string, err error)
}

// ESClient — то немногое от Elasticsearch, что нужно переиндексации.
// Не ports.IndexWriter/SearchIndex из services/search: те работают через
// alias и не умеют управлять физическими индексами и alias'ами напрямую —
// а это ровно то, для чего существует данный инструмент. Дублирование
// части кода между services/search/internal/adapters/es и
// internal/reindex/es_adapter.go (в частности, маппинг) осознанное —
// это отдельные Go-модули (services/search и search-reindex), и
// internal-пакет одного не виден другому в принципе, даже в рамках
// одного go.work (правило internal смотрит на путь импорта, а не на
// workspace).
type ESClient interface {
	// CreateIndex создаёт физический индекс с явным маппингом.
	CreateIndex(ctx context.Context, name string) error
	// BulkIndex пишет пачку документов В УКАЗАННЫЙ индекс (по имени, не по
	// alias — во время заполнения alias на него ещё не указывает).
	BulkIndex(ctx context.Context, indexName string, docs []CatalogListing) error
	// CurrentIndex — на какой физический индекс сейчас указывает alias.
	// exists=false — alias ещё не существует (самый первый запуск).
	CurrentIndex(ctx context.Context, alias string) (indexName string, exists bool, err error)
	// SwitchAlias переключает alias на newIndex ОДНИМ атомарным запросом
	// _aliases. oldIndex == "" — переключать не с чего (alias создаётся
	// впервые), тогда запрос содержит только действие add.
	SwitchAlias(ctx context.Context, alias, newIndex, oldIndex string) error
	// DeleteIndex удаляет физический индекс безвозвратно.
	DeleteIndex(ctx context.Context, name string) error
}

// Options — параметры одного запуска, один в один флаги cmd/search-reindex.
type Options struct {
	Alias     string
	BatchSize int32
	KeepOld   bool
	DryRun    bool
}

// Result — что произошло, для вывода в лог/консоль.
type Result struct {
	OldIndex        string
	NewIndex        string
	DocsIndexed     int64
	DryRun          bool
	OldIndexDeleted bool
}

// Reindexer — сама оркестрация.
type Reindexer struct {
	catalog CatalogClient
	es      ESClient
	// now — источник времени для имени нового индекса (listings_<unix>).
	// Отдельное поле, а не прямой time.Now() в коде, — ЕДИНСТВЕННОЕ, что
	// делает Run детерминированным в тестах (reindex_test.go подставляет
	// фиксированное время).
	now func() time.Time
}

func New(catalog CatalogClient, es ESClient, now func() time.Time) *Reindexer {
	if now == nil {
		now = time.Now
	}
	return &Reindexer{catalog: catalog, es: es, now: now}
}

// Run выполняет переиндексацию целиком.
//
// -dry-run обрывает работу ДО единого обращения к ES на запись (даже до
// CreateIndex): он показывает, сколько документов будет перенесено, читая
// каталог по факту, но не имея ни малейшего наблюдаемого побочного эффекта
// на кластер поиска — специально для того, чтобы можно было проверить
// объём переноса перед боевым запуском, не рискуя случайно создать индекс
// или (что хуже) не суметь корректно остановиться на середине.
func (r *Reindexer) Run(ctx context.Context, opts Options) (Result, error) {
	oldIndex, hadOld, err := r.es.CurrentIndex(ctx, opts.Alias)
	if err != nil {
		return Result{}, fmt.Errorf("текущий индекс alias %s: %w", opts.Alias, err)
	}

	newIndex := fmt.Sprintf("%s_%d", opts.Alias, r.now().Unix())

	if opts.DryRun {
		count, err := r.countAll(ctx, opts.BatchSize)
		return Result{OldIndex: oldIndex, NewIndex: newIndex, DocsIndexed: count, DryRun: true}, err
	}

	if err := r.es.CreateIndex(ctx, newIndex); err != nil {
		return Result{}, fmt.Errorf("создание индекса %s: %w", newIndex, err)
	}

	total, err := r.fill(ctx, newIndex, opts.BatchSize)
	if err != nil {
		return Result{NewIndex: newIndex, OldIndex: oldIndex}, fmt.Errorf("наполнение индекса %s: %w", newIndex, err)
	}

	switchFrom := ""
	if hadOld {
		switchFrom = oldIndex
	}
	if err := r.es.SwitchAlias(ctx, opts.Alias, newIndex, switchFrom); err != nil {
		return Result{NewIndex: newIndex, OldIndex: oldIndex, DocsIndexed: total},
			fmt.Errorf("переключение alias %s → %s: %w", opts.Alias, newIndex, err)
	}

	result := Result{OldIndex: oldIndex, NewIndex: newIndex, DocsIndexed: total}

	if hadOld && !opts.KeepOld {
		if err := r.es.DeleteIndex(ctx, oldIndex); err != nil {
			// Alias уже переключён и рабочий — старый индекс, оставшийся
			// физически висеть, не мешает поиску прямо сейчас. Это не
			// повод откатывать успешную переиндексацию: сообщаем об
			// ошибке отдельно, старый индекс можно убрать вручную позже.
			return result, fmt.Errorf("удаление старого индекса %s: %w", oldIndex, err)
		}
		result.OldIndexDeleted = true
	}

	return result, nil
}

// fill постранично читает каталог и пишет каждую пачку В НОВЫЙ индекс.
func (r *Reindexer) fill(ctx context.Context, indexName string, batchSize int32) (int64, error) {
	var total int64
	cursor := ""
	for {
		listings, next, err := r.catalog.ListListings(ctx, batchSize, cursor)
		if err != nil {
			return total, fmt.Errorf("чтение каталога: %w", err)
		}
		if len(listings) > 0 {
			if err := r.es.BulkIndex(ctx, indexName, listings); err != nil {
				return total, err
			}
			total += int64(len(listings))
		}
		if next == "" {
			return total, nil
		}
		cursor = next
	}
}

// countAll — то же постраничное чтение, что и fill, но без единой записи —
// используется только для -dry-run.
func (r *Reindexer) countAll(ctx context.Context, batchSize int32) (int64, error) {
	var total int64
	cursor := ""
	for {
		listings, next, err := r.catalog.ListListings(ctx, batchSize, cursor)
		if err != nil {
			return total, fmt.Errorf("чтение каталога: %w", err)
		}
		total += int64(len(listings))
		if next == "" {
			return total, nil
		}
		cursor = next
	}
}

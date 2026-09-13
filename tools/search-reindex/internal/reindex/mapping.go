package reindex

// indexMapping — маппинг НОВОГО индекса listings, дословно совпадающий с
// services/search/internal/adapters/es/mapping.go.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ (дублирование): search-reindex — отдельный Go-модуль
// (go.mod: gosplash/search-reindex), а internal-пакеты видны только внутри
// дерева импорта, начинающегося с родителя internal/, — правило Go смотрит
// на путь импорта, а не на go.work. gosplash/services/search/internal/es и
// gosplash/search-reindex/internal/reindex не делят общего "родителя" в
// этом смысле, поэтому даже в одном workspace один не может импортировать
// internal-пакет другого. Маппинг индекса — это, по сути, часть КОНТРАКТА
// между продюсером схемы (кто создаёт индекс) и её потребителями, и в этом
// проекте у контракта ровно два места создания: первый запуск search-сервиса
// (bootstrap) и каждый запуск этого инструмента (смена схемы). Дублирование
// короткой строки с явным комментарием здесь честнее, чем городить третий
// модуль или паблик-пакет ради одной константы: если однажды маппинг
// разъедется между двумя копиями, это будет ЗАМЕТНАЯ ошибка (документ,
// проходящий в одном индексе и падающий в другом), а не тихая деградация.
const indexMapping = `{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "listing_id": {"type": "keyword"},
      "title": {
        "type": "text",
        "fields": {
          "russian": {"type": "text", "analyzer": "russian"},
          "english": {"type": "text", "analyzer": "english"}
        }
      },
      "tags": {"type": "keyword"},
      "author_id": {"type": "long"},
      "author_name": {
        "type": "text",
        "fields": {
          "keyword": {"type": "keyword"}
        }
      },
      "price_cents": {"type": "integer"},
      "currency": {"type": "keyword"},
      "status": {"type": "keyword"},
      "published_at": {"type": "date"}
    }
  }
}`

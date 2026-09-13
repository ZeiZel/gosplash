package es

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

// Client — тонкая обёртка над esapi: адрес(а) кластера и имя alias'а, за
// которым живёт реальный индекс. Всё остальное (BulkIndexer, поиск)
// использует raw-клиент напрямую, а не прячет его ещё глубже — обёртка
// нужна только там, где есть решение, которое стоит задокументировать
// (EnsureIndex ниже), а не ради обёртки самой по себе.
type Client struct {
	raw   *elasticsearch.Client
	Alias string
}

// New поднимает клиента ES. Соединение НЕ проверяется здесь — как и у
// kafkax.NewProducer, конструктор может успешно вернуть клиента, даже если
// кластер сейчас недоступен: недоступность проверяется отдельно, через
// Ping (для /readyz) и обычные ошибки вызовов.
func New(addrs []string, alias string) (*Client, error) {
	raw, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: addrs})
	if err != nil {
		return nil, fmt.Errorf("elasticsearch client: %w", err)
	}
	return &Client{raw: raw, Alias: alias}, nil
}

// Ping — проверка живости кластера для /readyz, тот же смысл, что и
// kafkax.Producer.Ping или dbx.Ping.
func (c *Client) Ping(ctx context.Context) error {
	res, err := c.raw.Info(c.raw.Info.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("elasticsearch ping: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("elasticsearch ping: %s", res.Status())
	}
	return nil
}

// EnsureIndex проверяет, что Alias уже указывает на реальный индекс, и если
// нет — создаёт новый индекс с явным маппингом (mapping.go) и заводит alias
// на него.
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: сервис поиска работает ТОЛЬКО с именем alias
// (Client.Alias), никогда — с физическим именем индекса вроде
// "listings_1699999999". Физическое имя знает только тот, кто индекс
// создал: при самом первом запуске — этот метод, при последующих сменах
// схемы — tools/search-reindex. Маппинг Elasticsearch нельзя изменить
// после создания индекса (поля, однажды заданные, неизменяемы — см.
// mapping.go), поэтому единственный способ поменять схему — создать НОВЫЙ
// индекс и переключить alias на него ОДНИМ атомарным запросом _aliases
// (tools/search-reindex/internal/reindex). Если бы консьюмер или Search
// ходили в индекс по прямому имени, такое переключение было бы видно
// клиентам как провал (запросы к старому имени, которого больше нет),
// а не мгновенной и незаметной подменой.
//
// Идемпотентно: повторный вызов при уже существующем alias — no-op.
func (c *Client) EnsureIndex(ctx context.Context) error {
	exists, err := c.aliasExists(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	name := fmt.Sprintf("%s_%d", c.Alias, time.Now().Unix())
	if err := c.createIndex(ctx, name); err != nil {
		return err
	}
	return c.bindAlias(ctx, name)
}

func (c *Client) aliasExists(ctx context.Context) (bool, error) {
	res, err := c.raw.Indices.GetAlias(
		c.raw.Indices.GetAlias.WithContext(ctx),
		c.raw.Indices.GetAlias.WithName(c.Alias),
	)
	if err != nil {
		return false, fmt.Errorf("проверка alias %s: %w", c.Alias, err)
	}
	defer res.Body.Close()

	// 404 здесь — штатный ответ "alias не существует", а не ошибка сети:
	// GetAlias отвечает так же, как и обычный GET по несуществующему
	// ресурсу.
	if res.StatusCode == 404 {
		return false, nil
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return false, fmt.Errorf("проверка alias %s: %s: %s", c.Alias, res.Status(), body)
	}
	return true, nil
}

func (c *Client) createIndex(ctx context.Context, name string) error {
	res, err := c.raw.Indices.Create(
		name,
		c.raw.Indices.Create.WithContext(ctx),
		c.raw.Indices.Create.WithBody(strings.NewReader(indexMapping)),
	)
	if err != nil {
		return fmt.Errorf("создание индекса %s: %w", name, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("создание индекса %s: %s: %s", name, res.Status(), body)
	}
	return nil
}

// Refresh делает только что записанные документы видимыми для поиска
// немедленно, не дожидаясь refresh_interval (по умолчанию 1с).
//
// НЕОЧЕВИДНОЕ РЕШЕНИЕ: в проде этот метод НЕ вызывается ни из consumer'а,
// ни из Search — форсированный refresh на каждую запись убивает главную
// причину, ради которой Elasticsearch вообще откладывает видимость новых
// документов (частый refresh резко увеличивает нагрузку на сегменты
// Lucene). Здесь он нужен ТОЛЬКО тестам (integration_test.go): без него
// тест "проиндексировал → тут же поискал" был бы недетерминированным —
// иногда документ уже виден, иногда ещё нет, в зависимости от того, успел
// ли пройти автоматический refresh за время между двумя вызовами.
func (c *Client) Refresh(ctx context.Context) error {
	res, err := c.raw.Indices.Refresh(
		c.raw.Indices.Refresh.WithContext(ctx),
		c.raw.Indices.Refresh.WithIndex(c.Alias),
	)
	if err != nil {
		return fmt.Errorf("refresh индекса %s: %w", c.Alias, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("refresh индекса %s: %s: %s", c.Alias, res.Status(), body)
	}
	return nil
}

func (c *Client) bindAlias(ctx context.Context, indexName string) error {
	body := fmt.Sprintf(`{"actions":[{"add":{"index":%q,"alias":%q}}]}`, indexName, c.Alias)
	res, err := c.raw.Indices.UpdateAliases(
		strings.NewReader(body),
		c.raw.Indices.UpdateAliases.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("привязка alias %s → %s: %w", c.Alias, indexName, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		respBody, _ := io.ReadAll(res.Body)
		return fmt.Errorf("привязка alias %s → %s: %s: %s", c.Alias, indexName, res.Status(), respBody)
	}
	return nil
}

package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Config — параметры подключения. Дублирует внутренний вид
// internal/config.ClickHouseConfig намеренно: этот пакет не должен знать
// о том, откуда взялись значения (env, флаги, тесты) — только сами значения.
type Config struct {
	Addr        string
	Database    string
	User        string
	Password    string
	DialTimeout time.Duration
	ReadTimeout time.Duration
}

// Client — тонкая обёртка над chdriver.Conn: только то, что нужно
// analytics-сервису (Ping для /readyz, доступ к самому соединению для
// адаптеров writer.go/repository.go/migrations).
type Client struct {
	Conn chdriver.Conn
}

// New открывает соединение по native-протоколу (порт 9000/59440 с хоста —
// см. deploy/compose/clickhouse.yml). Native, а не HTTP: он быстрее и
// поддерживает PrepareBatch, на котором строится весь батчинг вставок
// (batch.go, writer.go) — через HTTP пришлось бы собирать INSERT вручную.
func New(cfg Config) (*Client, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		DialTimeout: cfg.DialTimeout,
		ReadTimeout: cfg.ReadTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	return &Client{Conn: conn}, nil
}

// Ping — проверка живости для /readyz, тем же именем и смыслом, что и
// Producer.Ping/Consumer.Ping в pkg/kafkax.
func (c *Client) Ping(ctx context.Context) error {
	if err := c.Conn.Ping(ctx); err != nil {
		return fmt.Errorf("clickhouse: ping: %w", err)
	}
	return nil
}

func (c *Client) Close() error {
	return c.Conn.Close()
}

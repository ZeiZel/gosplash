// search-reindex — переиндексация каталога в НОВЫЙ индекс Elasticsearch
// без даунтайма поиска, через atomic-переключение alias.
//
// Зачем вообще нужен отдельный инструмент, а не "просто поправить маппинг":
// Elasticsearch не позволяет менять маппинг уже созданного индекса — поля,
// однажды типизированные, неизменяемы навсегда (см.
// services/search/internal/adapters/es/mapping.go). Единственный способ
// сменить схему — создать НОВЫЙ индекс с новым маппингом, заполнить его
// заново из источника правды (catalog, а не из старого индекса поиска — тот
// может не содержать полей, которые появились только в новой схеме) и
// переключить alias на него ОДНИМ атомарным запросом _aliases. Пока новый
// индекс наполняется, alias всё ещё указывает на старый — читатели (сам
// search-сервис) не видят ни пустоты, ни рывка, только мгновенную подмену
// в момент переключения. Разбор решения целиком — docs/adr/0017.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	catalogv1 "gosplash/gen/go/gosplash/catalog/v1"
	"gosplash/search-reindex/internal/reindex"
)

func main() {
	var (
		catalogAddr = flag.String("catalog", "localhost:9102", "адрес gRPC catalog.v1.CatalogService")
		addrsFlag   = flag.String("addrs", "http://localhost:59200", "адреса узлов Elasticsearch через запятую")
		alias       = flag.String("alias", "listings", "имя alias'а индекса поиска")
		batchSize   = flag.Int("batch", 200, "сколько карточек забирать за одну страницу ListListings / писать за один _bulk")
		keepOld     = flag.Bool("keep-old", false, "не удалять старый физический индекс после переключения alias")
		dryRun      = flag.Bool("dry-run", false, "посчитать документы, ничего не создавая и не переключая")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	catalogConn, err := grpc.NewClient(
		*catalogAddr,
		// Без TLS — внутренняя сеть локальной разработки, тот же выбор,
		// что и у остальных внутренних gRPC-клиентов проекта (см.
		// services/catalog/cmd/catalog/main.go, gRPC-клиент к media).
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		slog.Error("search-reindex: gRPC к catalog", "error", err)
		os.Exit(1)
	}
	defer catalogConn.Close()

	esAdapter, err := reindex.NewESAdapter(splitAddrs(*addrsFlag))
	if err != nil {
		slog.Error("search-reindex: elasticsearch client", "error", err)
		os.Exit(1)
	}

	catalogAdapter := reindex.NewCatalogAdapter(catalogv1.NewCatalogServiceClient(catalogConn))
	reindexer := reindex.New(catalogAdapter, esAdapter, nil)

	result, err := reindexer.Run(ctx, reindex.Options{
		Alias:     *alias,
		BatchSize: int32(*batchSize),
		KeepOld:   *keepOld,
		DryRun:    *dryRun,
	})
	if err != nil {
		slog.Error("search-reindex: переиндексация прервана", "error", err,
			"old_index", result.OldIndex, "new_index", result.NewIndex, "docs_indexed", result.DocsIndexed)
		os.Exit(1)
	}

	if result.DryRun {
		fmt.Printf("dry-run: перенос затронул бы %d документов (%s → %s), ничего не изменено\n",
			result.DocsIndexed, orNone(result.OldIndex), result.NewIndex)
		return
	}

	fmt.Printf("готово: %d документов перенесено в %s, alias %q переключён (было: %s)\n",
		result.DocsIndexed, result.NewIndex, *alias, orNone(result.OldIndex))
	if result.OldIndex != "" {
		if result.OldIndexDeleted {
			fmt.Printf("старый индекс %s удалён\n", result.OldIndex)
		} else {
			fmt.Printf("старый индекс %s сохранён (-keep-old)\n", result.OldIndex)
		}
	}
}

func orNone(s string) string {
	if s == "" {
		return "не было"
	}
	return s
}

func splitAddrs(v string) []string {
	parts := strings.Split(v, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			result = append(result, p)
		}
	}
	return result
}

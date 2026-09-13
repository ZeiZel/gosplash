package httpx

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // регистрирует /debug/pprof/* в DefaultServeMux
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServerConfig — таймауты HTTP-сервера.
//
// Значения по умолчанию у net/http — ноль, то есть «ждать вечно». Это не
// теоретическая проблема: одно зависшее соединение занимает горутину и
// файловый дескриптор, а тысяча таких соединений кладёт сервис без единого
// байта полезного трафика (slowloris).
type ServerConfig struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

// DefaultServerConfig — разумные значения для обычного API-сервиса.
func DefaultServerConfig(addr string) ServerConfig {
	return ServerConfig{
		Addr:              addr,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// NewServer собирает *http.Server с проставленными таймаутами.
func NewServer(cfg ServerConfig, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}
}

// Serve запускает сервер и логирует всё, кроме штатной остановки.
// Блокирует вызывающую горутину.
func Serve(server *http.Server, name string) {
	slog.Info("http: слушаю", "server", name, "addr", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("http: сервер остановлен с ошибкой", "server", name, "error", err)
	}
}

// ServeMetricsAndPprof поднимает служебный сервер на отдельном порту.
//
// Отдельный порт, а не путь на основном, по трём причинам:
//
//  1. /metrics и /debug/pprof не должны быть доступны снаружи — на служебный
//     порт просто не выдают ingress;
//  2. профилировать нужно ровно тогда, когда основной сервер захлёбывается,
//     и очередь его хендлеров не должна мешать;
//  3. на /metrics не нужны ни аутентификация, ни rate limit, ни трассировка —
//     то есть весь middleware основного сервера здесь лишний.
//
// pprof регистрируется в DefaultServeMux самим фактом импорта net/http/pprof —
// поэтому здесь http.DefaultServeMux, а не свой роутер. Это же причина, по
// которой основной сервер проекта НИКОГДА не использует DefaultServeMux:
// иначе профилировщик оказался бы в публичном API.
func ServeMetricsAndPprof(addr string) *http.Server {
	http.DefaultServeMux.Handle("GET /metrics", promhttp.Handler())

	server := &http.Server{
		Addr:              addr,
		Handler:           http.DefaultServeMux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go Serve(server, "metrics+pprof")
	return server
}

// Shutdown останавливает серверы, давая доработать текущим запросам.
//
// Порядок при получении SIGTERM:
//
//  1. health.NotReady() — балансировщик перестаёт слать новый трафик;
//  2. пауза (drainDelay) — балансировщику нужно время узнать об этом.
//     Без паузы часть запросов прилетит уже в закрывающийся сервер и получит
//     connection reset. Это самая частая причина 502 при выкатке;
//  3. Shutdown — сервер перестаёт принимать соединения и ждёт активные.
func Shutdown(ctx context.Context, timeout time.Duration, servers ...*http.Server) {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	for _, server := range servers {
		if server == nil {
			continue
		}
		if err := server.Shutdown(shutdownCtx); err != nil {
			// Таймаут означает, что какой-то запрос не уложился в бюджет.
			// Соединение будет оборвано — это плохо, но лучше, чем зависшая
			// навсегда выкатка.
			slog.Error("http: shutdown не уложился в таймаут", "addr", server.Addr, "error", err)
		}
	}
}

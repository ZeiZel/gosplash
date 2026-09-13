package httpx

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// PATTERN: liveness/readiness — две РАЗНЫЕ проверки, и путать их дорого.
//
//	/healthz (liveness)  — «процесс жив». Отвечает всегда, пока не завис
//	                       намертво. Провал → Kubernetes ПЕРЕЗАПУСКАЕТ под.
//	/readyz  (readiness) — «готов принимать трафик»: есть база, Kafka, Redis.
//	                       Провал → под УБИРАЮТ ИЗ БАЛАНСИРОВКИ, но не трогают.
//
// Классическая авария: в liveness засунули проверку базы. База моргнула на
// полминуты — Kubernetes перезапустил все поды разом, они одновременно полезли
// в базу за соединениями и добили её. Правило: в liveness не проверяют ничего,
// что находится за пределами процесса.
type Health struct {
	mu     sync.RWMutex
	checks map[string]Check

	// ready снимается вручную при получении SIGTERM: под должен перестать
	// получать НОВЫЙ трафик до того, как начнёт закрывать соединения.
	// Между «перестал быть ready» и «начал останавливаться» нужен зазор —
	// балансировщику нужно время узнать об изменении.
	ready bool
}

// Check — проверка одной внешней зависимости. Обязана уважать ctx: readiness
// с зависшей проверкой хуже, чем readiness без проверки.
type Check func(ctx context.Context) error

func NewHealth() *Health {
	return &Health{checks: make(map[string]Check), ready: true}
}

// Register добавляет проверку готовности.
func (h *Health) Register(name string, check Check) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks[name] = check
}

// NotReady переводит сервис в «не готов», не останавливая его.
// Вызывается первым делом при SIGTERM.
func (h *Health) NotReady() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ready = false
}

// Handle регистрирует /healthz и /readyz в роутере.
func (h *Health) Handle(router *http.ServeMux, serviceName string) {
	// Liveness: ничего не проверяет намеренно. Если этот хендлер выполнился,
	// значит, процесс жив и планировщик Go работает — больше liveness ничего
	// и не должен означать.
	router.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		JSON(w, map[string]string{"status": "ok", "service": serviceName}, http.StatusOK)
	})

	router.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		h.mu.RLock()
		ready := h.ready
		checks := make(map[string]Check, len(h.checks))
		for name, check := range h.checks {
			checks[name] = check
		}
		h.mu.RUnlock()

		// Общий бюджет на все проверки. Без него readiness-пробу можно
		// «подвесить» одной медленной зависимостью, и kubelet будет считать
		// под неготовым по таймауту — с непонятной причиной в логах.
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		results := make(map[string]string, len(checks)+1)
		healthy := ready
		if !ready {
			results["shutdown"] = "сервис останавливается"
		}

		for name, check := range checks {
			if err := check(ctx); err != nil {
				results[name] = err.Error()
				healthy = false
				continue
			}
			results[name] = "ok"
		}

		status := http.StatusOK
		if !healthy {
			status = http.StatusServiceUnavailable
		}
		JSON(w, map[string]any{
			"service": serviceName,
			"ready":   healthy,
			"checks":  results,
		}, status)
	})
}

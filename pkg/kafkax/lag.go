package kafkax

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// lagPollInterval — как часто опрашиваем брокер за лагом группы.
//
// Не на каждое сообщение и не через хук клиента: лаг — это отставание ГРУППЫ
// от конца партиции, а группа — это несколько процессов, часть из которых
// может работать не в этом инстансе вовсе (партиции раскиданы ребалансом).
// Посчитать лаг из processOne нельзя даже в принципе — там виден только
// закоммиченный ЭТИМ процессом offset, а не последний закоммиченный группой
// и не текущий конец партиции. Это отдельный административный запрос
// к брокеру (kadm), и раз в 15 секунд he нагружает кластер, но и не даёт
// дашборду отставать от реальности больше чем на этот интервал.
const lagPollInterval = 15 * time.Second

// startLagMetrics запускает фоновую горутину, публикующую метрику
// kafka_consumer_lag{group,topic,partition}.
//
// Имя метрики зафиксировано ТОЧНО таким — на него ссылаются дашборд Grafana
// и алерт «консьюмер не успевает» (docs/runbooks). Переименование метрики —
// это переименование обоих без права на ошибку, поэтому здесь оно
// не параметризовано и не может быть переопределено вызывающим кодом.
//
// Возвращает функцию остановки: вызывающий обязан её вызвать (обычно
// через defer) и дождаться завершения горутины, иначе метрика продолжит
// писаться после Close() клиента и словит гонку в franz-go.
func startLagMetrics(ctx context.Context, client *kgo.Client, group string, meter metric.Meter) func() {
	gauge, err := meter.Int64Gauge(
		"kafka_consumer_lag",
		metric.WithDescription("Отставание consumer group от конца партиции (сообщений)"),
	)
	if err != nil {
		slog.Error("kafkax: метрика kafka_consumer_lag", "error", err)
		return func() {}
	}

	// kadm поверх УЖЕ существующего клиента, а не отдельное TCP-соединение:
	// Lag() — это обычный administrative-запрос (DescribeGroups + ListOffsets),
	// ему всё равно, состоит ли клиент в группе сам, и плодить второй пул
	// соединений ради него нет смысла.
	admin := kadm.NewClient(client)

	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(lagPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reportLag(ctx, admin, gauge, group)
			}
		}
	}()
	return func() { <-done }
}

func reportLag(ctx context.Context, admin *kadm.Client, gauge metric.Int64Gauge, group string) {
	lctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	lags, err := admin.Lag(lctx, group)
	if err != nil {
		slog.Error("kafkax: kadm lag", "group", group, "error", err)
		return
	}

	described, ok := lags[group]
	if !ok {
		return
	}
	if err := described.Error(); err != nil {
		// Группа временно не отвечает (например, идёт ребаланс) — не страшно,
		// следующий тик через 15с попробует снова. Пишем в лог на Warn,
		// а не Error: это ожидаемое переходное состояние, а не авария.
		slog.Warn("kafkax: лаг группы недоступен", "group", group, "error", err)
		return
	}

	for topic, partitions := range described.Lag {
		for partition, memberLag := range partitions {
			if memberLag.Lag < 0 {
				// Отрицательный Lag — сигнал ошибки вычисления по конкретной
				// партиции (не удалось прочитать commit или list offset).
				// Пропускаем: лучше отсутствующая точка на графике, чем
				// лживый ноль или минус единица, которые легко принять
				// за «лага нет» или «обогнали конец партиции».
				continue
			}
			gauge.Record(ctx, memberLag.Lag, metric.WithAttributes(
				attribute.String("group", group),
				attribute.String("topic", topic),
				attribute.Int("partition", int(partition)),
			))
		}
	}
}

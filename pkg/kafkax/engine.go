package kafkax

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// PATTERN: goroutine-per-partition consuming (образец —
// franz-go/examples/goroutine_per_partition_consuming).
//
// Наивный консьюмер обрабатывает записи одну за другой в единственном
// цикле, и тогда партиции топика с 6 партициями делят одну очередь: пока
// зависла обработка события из партиции 3, партиции 0-2 и 4-5 тоже ждут,
// хотя относятся к другим агрегатам и друг другу ничем не мешают. Партиция
// в Kafka и так уже единица параллелизма — этот пакет просто доводит её
// до кода: одна горутина на партицию, у каждой свой канал входящих батчей.
//
// Цена паттерна: жизненным циклом этих горутин теперь надо управлять руками
// при ребалансе. kgo.BlockRebalanceOnPoll() придерживает вызов
// OnPartitionsRevoked/Lost до явного client.AllowRebalance() (мы вызываем
// его в конце каждой итерации Run, сразу после раздачи записей по
// каналам) — а сам callback Revoked/Lost блокируется, пока не остановит
// и не дождётся ИМЕННО тех горутин, чьи партиции отбирают. Без этого
// ожидания старая горутина могла бы закоммитить offset уже ПОСЛЕ того,
// как партицию отдали другому консьюмеру группы — потерянный или задвоенный
// коммит, который проявляется через раз и только под нагрузкой.
type tp struct {
	topic     string
	partition int32
}

// partitionWorker — одна горутина на одну партицию.
type partitionWorker struct {
	recs chan []*kgo.Record
	quit chan struct{}
	done chan struct{}
}

func newPartitionWorker(ctx context.Context, process func(context.Context, *kgo.Record)) *partitionWorker {
	w := &partitionWorker{
		// Буфер 4: PollFetches может вернуть несколько батчей подряд быстрее,
		// чем горутина успевает их разбирать (например, пока идёт backoff
		// внутри processOne у предыдущей записи). Без буфера канал без
		// буфера заблокировал бы dispatch() при первом же батче, работающем
		// дольше одного тика Poll — а вместе с ним и раздачу записей ВСЕМ
		// остальным партициям в этой же итерации Run.
		recs: make(chan []*kgo.Record, 4),
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
	go w.run(ctx, process)
	return w
}

func (w *partitionWorker) run(ctx context.Context, process func(context.Context, *kgo.Record)) {
	defer close(w.done)
	for {
		select {
		case <-w.quit:
			return
		case records := <-w.recs:
			for _, record := range records {
				select {
				case <-w.quit:
					// Партицию уже отозвали: остаток батча достанется тому,
					// кому её переназначат, — он начнёт с последнего
					// закоммиченного offset'а и получит эти записи заново.
					return
				default:
				}
				process(ctx, record)
			}
		}
	}
}

// partitionEngine — общий диспетчер «горутина на партицию» для Consumer
// и RetryConsumer: оба гоняют один и тот же протокол ребалансировки,
// различается только функция process.
type partitionEngine struct {
	ctx     context.Context
	process func(context.Context, *kgo.Record)

	mu      sync.Mutex
	workers map[tp]*partitionWorker
}

func newPartitionEngine(ctx context.Context, process func(context.Context, *kgo.Record)) *partitionEngine {
	return &partitionEngine{ctx: ctx, process: process, workers: make(map[tp]*partitionWorker)}
}

// assigned — вызывается из kgo.OnPartitionsAssigned. Заводит по горутине
// на каждую новую партицию.
func (e *partitionEngine) assigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			e.workers[tp{topic, partition}] = newPartitionWorker(e.ctx, e.process)
		}
	}
}

// revoked — партиции забирают, но группа продолжает работать (обычный
// ребаланс: добавили/убрали инстанс). Дожидаемся, пока горутины домолотят
// уже полученные записи, прежде чем вернуть управление kgo и разрешить
// ребалансу завершиться, — иначе новый владелец партиции и наша угасающая
// горутина могли бы одновременно коммитить в одну и ту же партицию.
func (e *partitionEngine) revoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	e.stop(revoked)
}

// lost — партиции отобрали без штатного протокола (например, сессия
// с координатором истекла раньше, чем мы ответили). Коммитить уже поздно —
// брокер считает нас выбывшими, — но горутины всё равно надо остановить,
// иначе они допишут в уже нам не принадлежащую партицию.
func (e *partitionEngine) lost(ctx context.Context, cl *kgo.Client, lost map[string][]int32) {
	e.stop(lost)
}

func (e *partitionEngine) stop(tps map[string][]int32) {
	e.mu.Lock()
	var stopped []*partitionWorker
	for topic, partitions := range tps {
		for _, partition := range partitions {
			key := tp{topic, partition}
			if w, ok := e.workers[key]; ok {
				close(w.quit)
				delete(e.workers, key)
				stopped = append(stopped, w)
			}
		}
	}
	e.mu.Unlock()

	for _, w := range stopped {
		<-w.done
	}
}

// dispatch раздаёт батч из PollFetches по каналам партиций. Вызывается
// из главного цикла Run ПЕРЕД AllowRebalance — если партицию отзовут
// на середине dispatch, воркер для нехватающей записи просто не найдётся
// (см. комментарий ниже), и запись достанется новому владельцу партиции.
func (e *partitionEngine) dispatch(fetches kgo.Fetches) {
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if len(p.Records) == 0 {
			return
		}
		e.mu.Lock()
		w, ok := e.workers[tp{p.Topic, p.Partition}]
		e.mu.Unlock()
		if !ok {
			// Между PollFetches и dispatch партицию уже успели отозвать —
			// бывает при быстром повторном ребалансе. Запись не теряется:
			// offset для неё не закоммичен, и её получит новый владелец.
			return
		}
		w.recs <- p.Records
	})
}

// stopAll останавливает все горутины движка — используется при выходе
// из Run (ctx отменён), чтобы не оставлять висящие горутины между
// перезапусками консьюмера в рамках одного процесса (в первую очередь это
// сценарий тестов).
func (e *partitionEngine) stopAll() {
	e.mu.Lock()
	all := make(map[string][]int32)
	for key := range e.workers {
		all[key.topic] = append(all[key.topic], key.partition)
	}
	e.mu.Unlock()
	e.stop(all)
}

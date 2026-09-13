package kafkax

import (
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

// PATTERN: dead letter queue — сообщения, которые обработать НЕЛЬЗЯ
// в принципе (битый payload, неизвестный event_type, доменная ошибка
// валидации), уезжают в отдельный топик вместо того, чтобы либо блокировать
// партицию навсегда, либо молча теряться.
//
// Цена паттерна: DLQ никто не читает автоматически. Сообщение там будет
// лежать, пока человек не придёт руками (tools/kafka-reprocess) — то есть
// DLQ превращает «тихую потерю данных» в «видимую, но требующую внимания
// очередь», а не решает проблему за оператора.

// dlqHeaders собирает заголовки DLQ-сообщения.
//
// original* заголовки принимаются параметрами, а не читаются из record
// напрямую: когда в DLQ отправляет RetryConsumer (retry.go), record.Topic —
// это <topic>.retry, а не исходный топик, и «оригиналом» надо считать то,
// что уже записано в заголовках original_* от предыдущего раунда, а не сам
// retry-топик. Выделение в отдельную функцию с явными параметрами исключает
// эту ошибку по построению и заодно тестируется без Kafka.
func dlqHeaders(base []kgo.RecordHeader, cause error, origTopic string, origPartition int32, origOffset int64) []kgo.RecordHeader {
	headers := append([]kgo.RecordHeader{}, base...)
	headers = setHeader(headers, HeaderError, cause.Error())
	headers = setHeader(headers, HeaderOriginalTopic, origTopic)
	headers = setHeader(headers, HeaderOriginalPartition, strconv.Itoa(int(origPartition)))
	headers = setHeader(headers, HeaderOriginalOffset, strconv.FormatInt(origOffset, 10))
	return headers
}

// originalTopic, originalPartition, originalOffset — читают original_*
// заголовки с осмысленным дефолтом «сам record», на случай если сообщение
// попадает в DLQ прямо из основного консьюмера и original_* заголовков
// у него ещё нет (тогда сам record и есть оригинал).
func originalTopic(record *kgo.Record) string {
	if v, ok := headerValue(record.Headers, HeaderOriginalTopic); ok {
		return v
	}
	return record.Topic
}

func originalPartition(record *kgo.Record) int32 {
	if v, ok := headerValue(record.Headers, HeaderOriginalPartition); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return int32(n)
		}
	}
	return record.Partition
}

func originalOffset(record *kgo.Record) int64 {
	if v, ok := headerValue(record.Headers, HeaderOriginalOffset); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return record.Offset
}

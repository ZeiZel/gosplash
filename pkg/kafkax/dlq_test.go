package kafkax

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDlqHeaders_DobavlyaetVseChetyre(t *testing.T) {
	base := []kgo.RecordHeader{{Key: HeaderEventID, Value: []byte("e-1")}}
	headers := dlqHeaders(base, errors.New("разбор конверта: unexpected EOF"), "media.photo.uploaded", 3, 42)

	get := func(key string) string {
		v, _ := headerValue(headers, key)
		return v
	}

	assert.Equal(t, "e-1", get(HeaderEventID), "исходные заголовки сохраняются")
	assert.Contains(t, get(HeaderError), "unexpected EOF")
	assert.Equal(t, "media.photo.uploaded", get(HeaderOriginalTopic))
	assert.Equal(t, "3", get(HeaderOriginalPartition))
	assert.Equal(t, "42", get(HeaderOriginalOffset))
}

func TestDlqHeaders_NeMenyaetBazu(t *testing.T) {
	// dlqHeaders не должна мутировать переданный слайс — иначе повторный
	// вызов (например, из RetryConsumer после ещё одного раунда) увидел бы
	// заголовки предыдущего вызова как "исходные".
	base := []kgo.RecordHeader{{Key: HeaderEventID, Value: []byte("e-1")}}
	_ = dlqHeaders(base, errors.New("x"), "t", 0, 0)
	assert.Len(t, base, 1, "исходный слайс не должен вырасти")
}

func TestOriginalXxx_PadaetNaSamZapisPriOtsutstviiZagolovkov(t *testing.T) {
	record := &kgo.Record{Topic: "media.photo.uploaded", Partition: 2, Offset: 17}

	assert.Equal(t, "media.photo.uploaded", originalTopic(record))
	assert.Equal(t, int32(2), originalPartition(record))
	assert.Equal(t, int64(17), originalOffset(record))
}

func TestOriginalXxx_ChitaetZagolovkiKogdaEstMomen(t *testing.T) {
	record := &kgo.Record{
		Topic:     "media.photo.uploaded.retry",
		Partition: 5,
		Offset:    99,
		Headers: []kgo.RecordHeader{
			{Key: HeaderOriginalTopic, Value: []byte("media.photo.uploaded")},
			{Key: HeaderOriginalPartition, Value: []byte("2")},
			{Key: HeaderOriginalOffset, Value: []byte("17")},
		},
	}

	assert.Equal(t, "media.photo.uploaded", originalTopic(record))
	assert.Equal(t, int32(2), originalPartition(record))
	assert.Equal(t, int64(17), originalOffset(record))
}

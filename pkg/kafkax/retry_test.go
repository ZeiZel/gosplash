package kafkax

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestSetHeader_DobavlyaetNovyy(t *testing.T) {
	headers := []kgo.RecordHeader{{Key: HeaderEventID, Value: []byte("e-1")}}
	headers = setHeader(headers, HeaderRetryCount, "1")

	assert.Len(t, headers, 2)
	v, ok := headerValue(headers, HeaderRetryCount)
	assert.True(t, ok)
	assert.Equal(t, "1", v)
}

func TestSetHeader_ZamenyaetSushestvuyushiy(t *testing.T) {
	// Критично для retry → retry (повторный раунд): если бы Set добавлял,
	// а не заменял, у записи оказалось бы два retry_count с разными
	// значениями, и непонятно, какому верить.
	headers := []kgo.RecordHeader{{Key: HeaderRetryCount, Value: []byte("1")}}
	headers = setHeader(headers, HeaderRetryCount, "2")

	assert.Len(t, headers, 1)
	v, _ := headerValue(headers, HeaderRetryCount)
	assert.Equal(t, "2", v)
}

func TestParseRetryAt_ChitaetUnixMs(t *testing.T) {
	want := time.UnixMilli(1_700_000_000_123)
	record := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: HeaderRetryAt, Value: []byte("1700000000123")},
	}}

	assert.True(t, parseRetryAt(record).Equal(want))
}

func TestParseRetryAt_OtsutstvuetIliBitOznachaetSeychas(t *testing.T) {
	// "уже пора" — безопасный дефолт: лучше обработать немедленно,
	// чем ждать неопределённо долго из-за битого заголовка.
	noHeader := &kgo.Record{}
	assert.True(t, parseRetryAt(noHeader).IsZero())

	badHeader := &kgo.Record{Headers: []kgo.RecordHeader{{Key: HeaderRetryAt, Value: []byte("не число")}}}
	assert.True(t, parseRetryAt(badHeader).IsZero())

	// time.Time{} (нулевое значение) — заведомо в прошлом относительно
	// time.Now(), поэтому time.Until(...) для него всегда отрицательно,
	// и вызывающий код (RetryConsumer.processOne) трактует это как «пора».
	assert.True(t, time.Until(parseRetryAt(noHeader)) < 0)
}

func TestRetryRound_NolPoUmolchaniyu(t *testing.T) {
	assert.Equal(t, 0, retryRound(&kgo.Record{}))

	record := &kgo.Record{Headers: []kgo.RecordHeader{{Key: HeaderRetryCount, Value: []byte("2")}}}
	assert.Equal(t, 2, retryRound(record))

	bad := &kgo.Record{Headers: []kgo.RecordHeader{{Key: HeaderRetryCount, Value: []byte("х")}}}
	assert.Equal(t, 0, retryRound(bad))
}

package kafkax

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	eventsv1 "gosplash/gen/go/gosplash/events/v1"
)

// Регрессия на «паника в Handler убивает процесс»: handleSafely обязан
// превращать панику в permanent-ошибку — именно по этому признаку processOne
// (основной консьюмер и RetryConsumer) отправляет запись в DLQ по общему
// пути (dlq.go), вместо того чтобы ронять весь сервис из-за одной битой
// записи.
func TestHandleSafely_PanikaStanovitsyaPermanentnoyOshibkoy(t *testing.T) {
	record := &kgo.Record{Topic: "media.photo.uploaded", Partition: 2, Offset: 17}

	err := handleSafely(context.Background(), record,
		func(context.Context, *eventsv1.Envelope) error { panic("index out of range") },
		&eventsv1.Envelope{})

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPermanent, "паника — это permanent: повтор с теми же байтами упадёт так же")
	assert.NotErrorIs(t, err, ErrRetryable)
	assert.Contains(t, err.Error(), "паника в обработчике")
	assert.Contains(t, err.Error(), "index out of range")
}

// Штатно вернувшийся Handler обёртку замечать не должен: ни recover,
// ни классификация не вмешиваются — это защищает существующий retry-путь
// от случайной переклассификации нормальных ошибок.
func TestHandleSafely_ShtatnyjVozvratNeMenyaetsya(t *testing.T) {
	record := &kgo.Record{Topic: "t", Partition: 0, Offset: 1}
	ctx := context.Background()

	sentinel := errors.New("сеть легла")
	err := handleSafely(ctx, record,
		func(context.Context, *eventsv1.Envelope) error { return Retryable(sentinel) },
		&eventsv1.Envelope{})
	assert.ErrorIs(t, err, sentinel, "исходная ошибка доступна через Unwrap")
	assert.ErrorIs(t, err, ErrRetryable, "классификация вызывающего сохраняется")
	assert.NotErrorIs(t, err, ErrPermanent)

	assert.NoError(t, handleSafely(ctx, record, noopHandler, &eventsv1.Envelope{}))
}

// Паника значением-ошибкой (errors.New как panic value) — частный случай:
// текст должен содержать её значение, а классификация остаться permanent.
func TestHandleSafely_PanikaOshibkoyTakzheVKachestvePermanentnoy(t *testing.T) {
	record := &kgo.Record{Topic: "t", Partition: 0, Offset: 1}
	boom := errors.New("битый payload")

	err := handleSafely(context.Background(), record,
		func(context.Context, *eventsv1.Envelope) error { panic(boom) },
		&eventsv1.Envelope{})

	assert.ErrorIs(t, err, ErrPermanent)
	assert.Contains(t, err.Error(), boom.Error())
}

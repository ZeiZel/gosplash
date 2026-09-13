package kafka

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"gosplash/pkg/kafkax"
	"gosplash/services/thumbnail-worker/internal/domain"
)

func TestClassify_DomennyeOshibkiPostoyannye(t *testing.T) {
	tests := []error{
		domain.ErrNotAnImage,
		domain.ErrOriginalMissing,
		domain.ErrPhotoNotFound,
	}

	for _, base := range tests {
		wrapped := errWrap(base)
		got := classify(wrapped)
		assert.ErrorIs(t, got, kafkax.ErrPermanent, "%v обязана классифицироваться как permanent", base)
	}
}

func TestClassify_NeklassificirovannayaOshibkaRetryable(t *testing.T) {
	got := classify(errors.New("s3: connection refused"))

	assert.ErrorIs(t, got, kafkax.ErrRetryable)
	assert.NotErrorIs(t, got, kafkax.ErrPermanent)
}

func TestClassify_NilOshibkaOstayotsyaNil(t *testing.T) {
	assert.NoError(t, classify(nil))
}

func errWrap(err error) error {
	return &wrapper{err: err}
}

type wrapper struct{ err error }

func (w *wrapper) Error() string { return "обёртка: " + w.err.Error() }
func (w *wrapper) Unwrap() error { return w.err }

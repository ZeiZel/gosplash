package outbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeadersToRecordHeaders_ParsitJSON(t *testing.T) {
	headers, err := headersToRecordHeaders([]byte(`{"event_id":"e-1","event_type":"media.photo.uploaded"}`))
	require.NoError(t, err)
	require.Len(t, headers, 2)

	get := func(key string) (string, bool) {
		for _, h := range headers {
			if h.Key == key {
				return string(h.Value), true
			}
		}
		return "", false
	}

	v, ok := get("event_id")
	assert.True(t, ok)
	assert.Equal(t, "e-1", v)
}

func TestHeadersToRecordHeaders_PustyeVhodnyeDannye(t *testing.T) {
	headers, err := headersToRecordHeaders(nil)
	require.NoError(t, err)
	assert.Empty(t, headers)

	headers, err = headersToRecordHeaders([]byte(`{}`))
	require.NoError(t, err)
	assert.Empty(t, headers)
}

func TestHeadersToRecordHeaders_BitiyJSONVozvrashaetOshibku(t *testing.T) {
	_, err := headersToRecordHeaders([]byte(`не json`))
	assert.Error(t, err)
}

package lb

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestStatusWriterExposesFlusher: the status-capturing wrapper must not hide
// the underlying Flusher, or streaming/SSE responses proxied through hoplb
// would be buffered. http.ResponseController reaches it via Unwrap.
func TestStatusWriterExposesFlusher(t *testing.T) {
	rec := httptest.NewRecorder() // ResponseRecorder implements http.Flusher
	sw := &statusWriter{ResponseWriter: rec, statusCode: http.StatusOK}

	if err := http.NewResponseController(sw).Flush(); err != nil {
		t.Fatalf("Flush through the wrapped writer must work (SSE), got %v", err)
	}
}

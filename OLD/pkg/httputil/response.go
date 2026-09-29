package httputil

import (
	"encoding/json"

	"github.com/xinix00/lean/leanhttp"
)

// WriteJSON writes a JSON response with the given status code
func WriteJSON(w leanhttp.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

// WriteError writes a JSON error response
func WriteError(w leanhttp.ResponseWriter, status int, message string) {
	WriteJSON(w, status, map[string]string{"error": message})
}

// SSEWriter prepares w for Server-Sent Events streaming. It never returns nil:
// every leanhttp.ResponseWriter can flush, which is the point of having Flush on
// the interface instead of behind a type assertion — an assertion that can fail
// invites a stream that silently buffers, and a stream has no end at which a
// buffer would be released.
func SSEWriter(w leanhttp.ResponseWriter) *SSE {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	return &SSE{w: w}
}

// SSE writes Server-Sent Events to a ResponseWriter
type SSE struct {
	w leanhttp.ResponseWriter
}

// WriteEvent writes a named SSE event with data and flushes
func (s *SSE) WriteEvent(event, data string) {
	_, _ = s.w.Write([]byte("event: " + event + "\ndata: " + data + "\n\n"))
	s.Flush()
}

// WriteData writes an SSE data-only event and flushes
func (s *SSE) WriteData(data string) {
	_, _ = s.w.Write([]byte("data: " + data + "\n\n"))
	s.Flush()
}

// Flush pushes what is buffered to the client. The error is dropped on purpose:
// a stream whose reader is gone fails on the next write anyway, and there is
// nothing an event loop can do about it that it would not already do.
func (s *SSE) Flush() { _ = s.w.Flush() }

package httputil

// server_test.go — de stop-lifecycle: starten, bedienen, en bij Close alles
// loslaten (listener én lopende verbindingen), met ErrServerClosed als enige
// einde van een gesloten server.

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xinix00/lean/leanhttp"
)

// startServer draait een Server op een vrije poort en geeft adres + exit-kanaal.
func startServer(t *testing.T, h leanhttp.Handler) (*Server, string, chan error) {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close() // alleen de vrije poort was nodig; de Server luistert zelf
	s := NewServer(addr, h)
	done := make(chan error, 1)
	go func() { done <- s.ListenAndServe() }()
	for i := 0; i < 100; i++ {
		c, err := net.Dial("tcp4", addr)
		if err == nil {
			c.Close()
			return s, addr, done
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server op %s kwam niet op", addr)
	return nil, "", nil
}

func rawRoundTrip(t *testing.T, addr, request string) string {
	t.Helper()
	c, err := net.Dial("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(c)
	return string(out)
}

func TestServerServesEnSluitMetErrServerClosed(t *testing.T) {
	s, addr, done := startServer(t, func(w leanhttp.ResponseWriter, r *leanhttp.Request) {
		w.Header().Set("Content-Length", "2")
		io.WriteString(w, "ok")
	})
	if got := rawRoundTrip(t, addr, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"); !strings.Contains(got, "ok") {
		t.Fatalf("antwoord %q, wil ok", got)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if err != ErrServerClosed {
			t.Fatalf("ListenAndServe gaf %v, wil ErrServerClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ListenAndServe keerde niet terug na Close")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("tweede Close hoort een no-op te zijn: %v", err)
	}
}

// Close hoort ook een LOPENDE stream-verbinding te sluiten: de handler hangt
// op de request-lifetime, en die eindigt doordat Close de verbinding sluit.
func TestServerCloseSluitLopendeVerbindingen(t *testing.T) {
	entered := make(chan struct{})
	exited := make(chan struct{})
	s, addr, done := startServer(t, func(w leanhttp.ResponseWriter, r *leanhttp.Request) {
		ctx := r.Context() // claimt de lifetime vóór de eerste Flush
		w.Flush()
		close(entered)
		<-ctx.Done()
		close(exited)
	})
	c, err := net.Dial("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("GET /stream HTTP/1.1\r\nHost: x\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("de stream-handler zag Close niet als einde van zijn lifetime")
	}
	<-done
}

func TestServerCloseVoorListenAndServe(t *testing.T) {
	s := NewServer("127.0.0.1:0", func(w leanhttp.ResponseWriter, r *leanhttp.Request) {})
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.ListenAndServe(); err != ErrServerClosed {
		t.Fatalf("ListenAndServe na Close gaf %v, wil ErrServerClosed", err)
	}
}

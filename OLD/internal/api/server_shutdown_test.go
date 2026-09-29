package api

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/leader"
)

// TestServerPortNotReleasedWithoutStop proves that cancelling the context
// does not synchronously release the port. This is the bug: becomeLeader
// fires srv.Run in a goroutine and leaderCancel() doesn't wait for port release.
func TestServerPortNotReleasedWithoutStop(t *testing.T) {
	t.Skip("Race condition demo — proves the bug exists, not a regression test")
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go l.Run(ctx)
	defer cancel()
	time.Sleep(10 * time.Millisecond)

	// Start server in goroutine (like becomeLeader does)
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	srv := NewServer(l, addr, "", "test")
	_ = srv
	go func() { _ = srv.Run(leaderCtx) }()
	waitForPort(t, addr)

	// Cancel context (like "Lost leadership") — port release is async
	leaderCancel()

	// Immediately try to bind again — no waiting!
	// This simulates becomeLeader being called right after losing leadership.
	srv2 := NewServer(l, addr, "", "test")
	leaderCtx2, leaderCancel2 := context.WithCancel(ctx)
	defer leaderCancel2()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv2.Run(leaderCtx2)
	}()

	// The new server must be reachable within 1 second
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("Port %s not reusable after context cancel (this is the bug): %v", addr, err)
	}
	conn.Close()
}

// TestServerPortReleasedWithExplicitStop proves that calling srv.Stop()
// synchronously releases the port so a new server can bind immediately.
func TestServerPortReleasedWithExplicitStop(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go l.Run(ctx)
	defer cancel()
	time.Sleep(10 * time.Millisecond)

	// Start server in goroutine (like becomeLeader does)
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	srv := NewServer(l, addr, "", "test")
	go func() { _ = srv.Run(leaderCtx) }()
	waitForPort(t, addr)

	// Explicit Stop (synchronous) + cancel context
	srv.Stop()
	leaderCancel()

	// New server should bind immediately
	srv2 := NewServer(l, addr, "", "test")
	leaderCtx2, leaderCancel2 := context.WithCancel(ctx)
	defer leaderCancel2()

	go func() { _ = srv2.Run(leaderCtx2) }()
	waitForPort(t, addr)

	// Verify new server is serving
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("New server failed to bind to %s after explicit Stop: %v", addr, err)
	}
	conn.Close()
}

// TestServerStopClosesActiveEventStream guards the leadership-churn leak: an
// idle SSE request has no natural return point, so Stop must cancel its handler
// before waiting for graceful HTTP shutdown.
func TestServerStopClosesActiveEventStream(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()
	go l.Run(leaderCtx)
	time.Sleep(10 * time.Millisecond)

	srv := NewServer(l, addr, "", "test")
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(context.Background()) }()
	waitForPort(t, addr)

	resp, err := http.Get("http://" + addr + "/v1/events")
	if err != nil {
		t.Fatalf("open event stream: %v", err)
	}
	defer resp.Body.Close()

	// The initial event proves the handler is subscribed and actively streaming.
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		t.Fatalf("read initial event: %v", err)
	}

	stopped := make(chan struct{})
	go func() {
		srv.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop waited on an idle event stream")
	}

	bodyDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, resp.Body)
		bodyDone <- err
	}()
	select {
	case <-bodyDone:
	case <-time.After(2 * time.Second):
		t.Fatal("event response body stayed open after Stop")
	}

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run stayed alive after Stop")
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func waitForPort(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Server never started listening on %s", addr)
}

package discovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xinix00/hoplock"
	"github.com/xinix00/hoplock/mem"
)

// errBackend is a hoplock.Backend that is always unreachable (transport
// error, never ErrLeaseHeld) — models an isolated node during a lock-store
// outage.
type errBackend struct{}

func (errBackend) Read(context.Context) (*hoplock.State, string, error) {
	return nil, "", errors.New("unreachable")
}
func (errBackend) Write(context.Context, string, *hoplock.State) (string, error) {
	return "", errors.New("unreachable")
}
func (errBackend) Delete(context.Context, string) error { return errors.New("unreachable") }

// TestRenewLeaseDistinguishesDisplacedFromUnreachable is the split-brain
// guard: a renew that fails because another node holds the lease must report
// displaced=true (step down), while an unreachable store must report
// displaced=false (connectivity blip — safe to keep leading).
func TestRenewLeaseDistinguishesDisplacedFromUnreachable(t *testing.T) {
	backend := mem.New()
	a := New(backend, "10.0.0.1", 8080, 30*time.Second)
	b := New(backend, "10.0.0.2", 8080, 30*time.Second)

	if !a.TryBecomeLeader() {
		t.Fatal("a should acquire the lease")
	}

	// b cannot renew — a holds a live lease → genuinely displaced.
	if renewed, displaced := b.RenewLease(); renewed || !displaced {
		t.Fatalf("b: renewed=%v displaced=%v, want false/true (displaced)", renewed, displaced)
	}
	// a still renews fine.
	if renewed, displaced := a.RenewLease(); !renewed || displaced {
		t.Fatalf("a: renewed=%v displaced=%v, want true/false", renewed, displaced)
	}

	// Unreachable store → renew fails but NOT displaced (blip, keep leading).
	c := New(errBackend{}, "10.0.0.3", 8080, 30*time.Second)
	if renewed, displaced := c.RenewLease(); renewed || displaced {
		t.Fatalf("c: renewed=%v displaced=%v, want false/false (unreachable)", renewed, displaced)
	}
}

func TestDiscoveryNew(t *testing.T) {
	d := New(mem.New(), "192.168.1.10", 8080, 30*time.Second)
	if got := d.NodeAddr(); got != "192.168.1.10:8080" {
		t.Errorf("NodeAddr() = %q, want %q", got, "192.168.1.10:8080")
	}
}

func TestNilBackendIsHarmless(t *testing.T) {
	d := New(nil, "10.0.0.1", 9080, time.Second)
	if d.GetLeader() != "" {
		t.Error("GetLeader() with nil backend should return empty")
	}
	if d.TryBecomeLeader() {
		t.Error("TryBecomeLeader() with nil backend should return false")
	}
	d.ReleaseLeadership() // must not panic
}

func TestTryBecomeLeaderCreates(t *testing.T) {
	d := New(mem.New(), "192.168.1.10", 8080, 30*time.Second)
	if !d.TryBecomeLeader() {
		t.Fatal("expected to claim empty lease")
	}
	if got := d.GetLeader(); got != "192.168.1.10:8080" {
		t.Errorf("GetLeader() = %q, want %q", got, "192.168.1.10:8080")
	}
	if !d.IsLeader() {
		t.Error("IsLeader() should be true")
	}
}

func TestTryBecomeLeaderDeniedWhenHeldByOther(t *testing.T) {
	backend := mem.New()
	other := New(backend, "192.168.1.20", 8080, 30*time.Second)
	if !other.TryBecomeLeader() {
		t.Fatal("other node failed to acquire")
	}

	mine := New(backend, "192.168.1.10", 8080, 30*time.Second)
	if mine.TryBecomeLeader() {
		t.Error("should not be able to take over a non-expired lease")
	}
	if mine.GetLeader() != "192.168.1.20:8080" {
		t.Errorf("expected leader to be other node, got %q", mine.GetLeader())
	}
}

func TestTryBecomeLeaderTakesOverExpired(t *testing.T) {
	backend := mem.New()
	now := time.Unix(1_000_000, 0)
	clock := &fakeClock{now: now}

	other := New(backend, "192.168.1.20", 8080, 10*time.Second)
	other.now = clock.Now
	if !other.TryBecomeLeader() {
		t.Fatal("other node failed to acquire")
	}

	// Past expiry from another candidate's perspective.
	clock.now = now.Add(time.Hour)

	mine := New(backend, "192.168.1.10", 8080, 10*time.Second)
	mine.now = clock.Now
	if !mine.TryBecomeLeader() {
		t.Fatal("expected to take over expired lease")
	}
	if got := mine.GetLeader(); got != "192.168.1.10:8080" {
		t.Errorf("after takeover GetLeader() = %q, want %q", got, "192.168.1.10:8080")
	}
}

func TestRenewKeepsHandle(t *testing.T) {
	d := New(mem.New(), "192.168.1.10", 8080, 30*time.Second)
	if !d.TryBecomeLeader() {
		t.Fatal("acquire")
	}
	for i := 0; i < 3; i++ {
		if renewed, _ := d.RenewLease(); !renewed {
			t.Fatalf("renew %d failed", i)
		}
	}
}

func TestReleaseAllowsTakeover(t *testing.T) {
	backend := mem.New()
	a := New(backend, "192.168.1.10", 8080, 30*time.Second)
	if !a.TryBecomeLeader() {
		t.Fatal("a acquire")
	}
	a.ReleaseLeadership()

	b := New(backend, "192.168.1.20", 8080, 30*time.Second)
	if !b.TryBecomeLeader() {
		t.Fatal("b should be able to acquire immediately after release")
	}
	if b.GetLeader() != "192.168.1.20:8080" {
		t.Errorf("after takeover GetLeader() = %q", b.GetLeader())
	}
}

func TestGenerationMonotonic(t *testing.T) {
	backend := mem.New()
	now := time.Unix(1_000_000, 0)
	clock := &fakeClock{now: now}

	a := New(backend, "a:1", 0, 10*time.Second)
	a.now = clock.Now
	if !a.TryBecomeLeader() {
		t.Fatal("a acquire")
	}
	state, _, err := backend.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 1 {
		t.Errorf("first generation = %d, want 1", state.Generation)
	}

	// a renews — generation stays the same.
	if renewed, _ := a.RenewLease(); !renewed {
		t.Fatal("renew")
	}
	state, _, _ = backend.Read(t.Context())
	if state.Generation != 1 {
		t.Errorf("after renew generation = %d, want 1", state.Generation)
	}

	// b takes over after expiry — generation bumps.
	clock.now = now.Add(time.Hour)
	b := New(backend, "b:1", 0, 10*time.Second)
	b.now = clock.Now
	if !b.TryBecomeLeader() {
		t.Fatal("b takeover")
	}
	state, _, _ = backend.Read(t.Context())
	if state.Generation != 2 {
		t.Errorf("after takeover generation = %d, want 2", state.Generation)
	}
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

// Ensure compile-time that hoplock errors are still in scope.
var _ = hoplock.ErrNoLease

func TestBackendTimeoutScalesWithLease(t *testing.T) {
	cases := []struct {
		ttl, want time.Duration
	}{
		{30 * time.Second, 10 * time.Second},  // default lease: a third
		{15 * time.Second, 5 * time.Second},   // floor, never below 5s
		{120 * time.Second, 40 * time.Second}, // slow store: a third of the lease
	}
	for _, c := range cases {
		if got := backendTimeoutFor(c.ttl); got != c.want {
			t.Errorf("backendTimeoutFor(%v) = %v, want %v", c.ttl, got, c.want)
		}
	}
	if d := New(mem.New(), "10.0.0.1", 8080, 120*time.Second); d.timeout != 40*time.Second {
		t.Errorf("New: timeout = %v, want 40s", d.timeout)
	}
}

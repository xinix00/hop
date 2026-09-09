package agentloop

import (
	"sync"
	"testing"
	"time"
)

// gate is a store call that blocks until the test releases it.
type gate struct {
	release chan struct{}
	calls   int
	mu      sync.Mutex
}

func newGate() *gate { return &gate{release: make(chan struct{}, 8)} }

func (g *gate) wait() {
	g.mu.Lock()
	g.calls++
	g.mu.Unlock()
	<-g.release
}

func (g *gate) count() int { g.mu.Lock(); defer g.mu.Unlock(); return g.calls }

// slowDiscoverer is the store as the loop must survive it: every call waits
// for the test to let it through, and answers what the test set.
type slowDiscoverer struct {
	read, claim, renew, rel *gate
	leader                  string
	storeOK                 bool
	claimOK                 bool
	renewOK, displaced      bool
}

func (d *slowDiscoverer) LeaderState() (string, bool) { d.read.wait(); return d.leader, d.storeOK }

func newSlowDiscoverer() *slowDiscoverer {
	return &slowDiscoverer{read: newGate(), claim: newGate(), renew: newGate(), rel: newGate(), storeOK: true}
}

func (d *slowDiscoverer) GetLeader() string        { d.read.wait(); return d.leader }
func (d *slowDiscoverer) TryBecomeLeader() bool    { d.claim.wait(); return d.claimOK }
func (d *slowDiscoverer) RenewLease() (bool, bool) { d.renew.wait(); return d.renewOK, d.displaced }
func (d *slowDiscoverer) ReleaseLeadership()       { d.rel.wait() }

// eventually polls f until it holds or a second passed.
func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: did not happen in time", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// newAsync starts an AsyncDiscoverer with a fake clock for the lease
// arithmetic and a real (fast) renew cadence for Run's ticker.
func newAsync(t *testing.T, inner Discoverer, ttl, renew time.Duration) (*AsyncDiscoverer, *time.Time) {
	t.Helper()
	a := NewAsyncDiscoverer(inner, ttl)
	a.renew = renew
	now := time.Date(2026, 9, 8, 17, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return now }
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go a.Run(done)
	return a, &now
}

// A slow read never blocks the caller: the first call answers "" and starts
// one read; the answer arrives for a later call; a second read is not started
// while one is in flight.
func TestAsyncGetLeaderNeverBlocks(t *testing.T) {
	d := newSlowDiscoverer()
	d.leader = "10.0.0.2:9080"
	a, now := newAsync(t, d, 30*time.Second, time.Hour)

	finished := make(chan string, 1)
	go func() { finished <- a.GetLeader() }()
	select {
	case got := <-finished:
		if got != "" {
			t.Fatalf("first GetLeader() = %q, want empty (read in flight)", got)
		}
	case <-time.After(time.Second):
		t.Fatal("GetLeader blocked on the store")
	}
	a.GetLeader() // still in flight: no second read
	eventually(t, "one read started", func() bool { return d.read.count() == 1 })
	if d.read.count() != 1 {
		t.Fatalf("reads started = %d, want 1", d.read.count())
	}

	d.read.release <- struct{}{}
	eventually(t, "answer lands", func() bool { return a.GetLeader() == "10.0.0.2:9080" })

	// Fresh for a lease: no new read within ttl, one after it.
	*now = now.Add(29 * time.Second)
	a.GetLeader()
	if d.read.count() != 1 {
		t.Fatalf("read again within ttl: %d", d.read.count())
	}
	*now = now.Add(2 * time.Second)
	if got := a.GetLeader(); got != "" {
		t.Fatalf("stale answer served: %q", got)
	}
	eventually(t, "second read", func() bool { return d.read.count() == 2 })
	d.read.release <- struct{}{}
}

// A claim is reported once, on the tick after it completed, and the lease
// timer starts from the store's answer.
func TestAsyncTryBecomeLeaderReportsOnce(t *testing.T) {
	d := newSlowDiscoverer()
	d.claimOK = true
	a, now := newAsync(t, d, 30*time.Second, time.Hour)

	if a.TryBecomeLeader() {
		t.Fatal("claim reported before the store answered")
	}
	a.TryBecomeLeader() // in flight: no second claim
	eventually(t, "claim started", func() bool { return d.claim.count() == 1 })
	d.claim.release <- struct{}{}
	eventually(t, "claim reported", func() bool { return a.TryBecomeLeader() })
	if a.TryBecomeLeader() {
		t.Fatal("claim reported twice")
	}
	if renewed, displaced := a.RenewLease(); !renewed || displaced {
		t.Fatalf("RenewLease right after claim = (%v,%v), want (true,false)", renewed, displaced)
	}
	if exp := a.LeaseExpiresAt(); exp != now.Add(30*time.Second) {
		t.Fatalf("LeaseExpiresAt = %v, want now+30s", exp)
	}
}

// While a renew hangs in the store, RenewLease keeps answering from the lease
// timer: mine until ExpiresAt, then "not renewed" (transient), never blocking.
func TestAsyncRenewAnswersFromTheLeaseTimer(t *testing.T) {
	d := newSlowDiscoverer()
	d.claimOK, d.renewOK = true, true
	a, now := newAsync(t, d, 30*time.Second, 20*time.Millisecond)
	// Boot path: TryBecomeLeaderSync blocks on the store gate; run it in a
	// goroutine and let it through.
	done := make(chan bool, 1)
	go func() { done <- a.TryBecomeLeaderSync() }()
	d.claim.release <- struct{}{}
	<-done

	*now = now.Add(20 * time.Second)
	if renewed, _ := a.RenewLease(); !renewed {
		t.Fatalf("lease lapsed before ttl while a renew is pending: now=%v expires=%v", a.now(), a.LeaseExpiresAt())
	}
	*now = now.Add(11 * time.Second)
	if renewed, displaced := a.RenewLease(); renewed || displaced {
		t.Fatalf("after ttl without a renew: (%v,%v), want (false,false)", renewed, displaced)
	}
	// The renew finally gets through: the timer restarts.
	eventually(t, "renew started by Run", func() bool { return d.renew.count() >= 1 })
	d.renew.release <- struct{}{}
	eventually(t, "renew extends the lease", func() bool { r, _ := a.RenewLease(); return r })
}

// A renew that meets another owner is reported exactly once, as displaced.
func TestAsyncDisplacedReportedOnce(t *testing.T) {
	d := newSlowDiscoverer()
	d.claimOK = true
	d.displaced = true
	a, _ := newAsync(t, d, 30*time.Second, 20*time.Millisecond) // fast renew cadence for the test
	done := make(chan bool, 1)
	go func() { done <- a.TryBecomeLeaderSync() }()
	d.claim.release <- struct{}{}
	<-done

	eventually(t, "renew started", func() bool { return d.renew.count() >= 1 })
	d.renew.release <- struct{}{}
	eventually(t, "displaced reported", func() bool { _, disp := a.RenewLease(); return disp })
	if _, disp := a.RenewLease(); disp {
		t.Fatal("displaced reported twice")
	}
	if a.LeaseExpiresAt() != (time.Time{}) {
		t.Fatal("still holding after being displaced")
	}
}

// Release forgets the lease at once; the store delete runs in the background.
func TestAsyncReleaseIsImmediate(t *testing.T) {
	d := newSlowDiscoverer()
	d.claimOK = true
	a, _ := newAsync(t, d, 30*time.Second, time.Hour)
	done := make(chan bool, 1)
	go func() { done <- a.TryBecomeLeaderSync() }()
	d.claim.release <- struct{}{}
	<-done

	finished := make(chan struct{})
	go func() { a.ReleaseLeadership(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("ReleaseLeadership blocked on the store")
	}
	if r, _ := a.RenewLease(); r {
		t.Fatal("still renewed after release")
	}
	eventually(t, "delete started", func() bool { return d.rel.count() == 1 })
	d.rel.release <- struct{}{}
}

// Invalidate makes the next GetLeader ask the store again within the ttl.
func TestAsyncInvalidateForcesARead(t *testing.T) {
	d := newSlowDiscoverer()
	d.leader = "10.0.0.2:9080"
	a, _ := newAsync(t, d, 30*time.Second, time.Hour)
	a.GetLeader()
	d.read.release <- struct{}{}
	eventually(t, "first answer", func() bool { return a.GetLeader() == "10.0.0.2:9080" })

	d.leader = "" // the lease lapsed
	a.Invalidate()
	if got := a.GetLeader(); got != "" {
		t.Fatalf("cached answer served after Invalidate: %q", got)
	}
	eventually(t, "second read", func() bool { return d.read.count() == 2 })
	d.read.release <- struct{}{}
	eventually(t, "fresh answer", func() bool { a.GetLeader(); return d.read.count() == 2 && a.GetLeader() == "" })
}

// StoreReachable follows the last read: false before any read, false when
// the store did not answer, true when it did (even with no leader).
func TestAsyncStoreReachableFollowsTheRead(t *testing.T) {
	d := newSlowDiscoverer()
	d.leader, d.storeOK = "", false
	a, _ := newAsync(t, d, 30*time.Second, time.Hour)
	if a.StoreReachable() {
		t.Fatal("reachable before any read")
	}
	a.GetLeader()
	d.read.release <- struct{}{}
	eventually(t, "read done", func() bool { return d.read.count() == 1 && !a.reading() })
	if a.StoreReachable() {
		t.Fatal("reachable although the store did not answer")
	}
	a.Invalidate()
	d.storeOK = true
	a.GetLeader()
	d.read.release <- struct{}{}
	eventually(t, "store answers", func() bool { return a.StoreReachable() })
}

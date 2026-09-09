package agentloop

import (
	"log"
	"time"
)

// AsyncDiscoverer is a Discoverer whose store calls never block the caller.
// The tick loop asks the same four questions as before — who leads, may I
// lead, is my lease still mine, let go — but every answer comes from the
// last completed store call, and the call itself runs in its own goroutine.
//
// Why: the tick loop also carries registration and heartbeats. When the lock
// store is slow (Bunny Storage: 4–20 s per PUT, measured 2026-09-08) a
// synchronous renew inside the tick delayed the leader's own heartbeat past
// the agent-dead threshold; the config had to be widened to cover it. Here
// the lease is the timer: a renew that completes extends ExpiresAt, and
// RenewLease answers "still mine" from that timestamp, whatever the store is
// doing right now.
//
// State lives in one goroutine (Run) reached through ops, like the agent and
// the leader; store workers report back through the same channel.
type AsyncDiscoverer struct {
	inner Discoverer
	ttl   time.Duration // lease validity as configured (timeouts.leader_lease)
	renew time.Duration // renew cadence while holding: ttl/3
	ops   chan func(*electState)
	now   func() time.Time
}

// electState is owned by Run.
type electState struct {
	// What the store said the last time we asked, and when. A leader
	// answer older than ttl is stale: the lease it described has expired.
	leader   string
	leaderAt time.Time
	storeOK  bool // the last read got an answer from the store
	reading  bool

	// Our own lease.
	holding   bool
	expiresAt time.Time
	renewing  bool
	claiming  bool
	claimed   bool // a background claim succeeded and nobody consumed it yet
	displaced bool // a renew met another owner; reported once, then cleared
}

// NewAsyncDiscoverer wraps inner. ttl is the lease validity (0 = 30 s).
// Call Run before use.
func NewAsyncDiscoverer(inner Discoverer, ttl time.Duration) *AsyncDiscoverer {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &AsyncDiscoverer{
		inner: inner,
		ttl:   ttl,
		renew: ttl / 3,
		ops:   make(chan func(*electState), 64),
		now:   time.Now,
	}
}

// Run owns the state until done closes. It also drives the renew cadence:
// while holding, one renew at a time, every ttl/3.
func (a *AsyncDiscoverer) Run(done <-chan struct{}) {
	var st electState
	ticker := time.NewTicker(a.renew)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case op := <-a.ops:
			op(&st)
		case <-ticker.C:
			if st.holding && !st.renewing {
				st.renewing = true
				go a.renewWorker()
			}
		}
	}
}

func (a *AsyncDiscoverer) do(op func(*electState)) { a.ops <- op }

func (a *AsyncDiscoverer) query(op func(*electState)) {
	wait := make(chan struct{})
	a.ops <- func(st *electState) { op(st); close(wait) }
	<-wait
}

// GetLeader returns the leader the store reported most recently, or "" when
// none is known or the last answer is older than the lease. An unknown or
// stale answer starts one background read; the caller asks again next tick.
func (a *AsyncDiscoverer) GetLeader() string {
	var leader string
	a.query(func(st *electState) {
		fresh := !st.leaderAt.IsZero() && a.now().Sub(st.leaderAt) < a.ttl
		if fresh {
			leader = st.leader
			return
		}
		if !st.reading {
			st.reading = true
			go a.readWorker()
		}
	})
	return leader
}

// Invalidate drops the cached leader answer so the next GetLeader asks the
// store again — used when the loop lost contact with the leader it knew.
func (a *AsyncDiscoverer) Invalidate() {
	a.query(func(st *electState) { st.leaderAt = time.Time{} })
}

func (a *AsyncDiscoverer) readWorker() {
	leader, storeOK := "", true
	if ls, ok := a.inner.(interface{ LeaderState() (string, bool) }); ok {
		leader, storeOK = ls.LeaderState()
	} else {
		leader = a.inner.GetLeader()
	}
	a.do(func(st *electState) {
		st.reading = false
		st.leader, st.leaderAt, st.storeOK = leader, a.now(), storeOK
	})
}

// reading reports whether a leader read is in flight (tests).
func (a *AsyncDiscoverer) reading() bool {
	var r bool
	a.query(func(st *electState) { r = st.reading })
	return r
}

// StoreReachable reports whether the last leader read got an answer from
// the store. With GetLeader() == "" it separates "nobody leads" (true) from
// "we cannot tell" (false) — the fail-safe in the loop hangs on that.
func (a *AsyncDiscoverer) StoreReachable() bool {
	var ok bool
	a.query(func(st *electState) { ok = !st.leaderAt.IsZero() && st.storeOK })
	return ok
}

// TryBecomeLeader reports true once for every background claim that
// succeeded; otherwise it starts one claim (if none is running) and returns
// false. The loop calls it every tick while it has no leader, so a claim
// that takes a slow store 20 s is picked up two ticks later — the lease is
// ours from the moment the store said so, and Run keeps renewing it.
func (a *AsyncDiscoverer) TryBecomeLeader() bool {
	var ok bool
	a.query(func(st *electState) {
		if st.claimed {
			st.claimed = false
			ok = true
			return
		}
		if st.holding || st.claiming {
			return
		}
		st.claiming = true
		go a.claimWorker()
	})
	return ok
}

func (a *AsyncDiscoverer) claimWorker() {
	got := a.inner.TryBecomeLeader()
	a.do(func(st *electState) {
		st.claiming = false
		if got {
			st.holding, st.claimed = true, true
			st.expiresAt = a.now().Add(a.ttl)
			st.leader, st.leaderAt = "", time.Time{} // we lead; the loop knows its own address
		}
	})
}

// TryBecomeLeaderSync is the boot-time claim: one synchronous attempt so a
// fresh cluster (or standalone) has a leader before the first tick. Loop.
// BecomeLeaderNow prefers it when available.
func (a *AsyncDiscoverer) TryBecomeLeaderSync() bool {
	got := a.inner.TryBecomeLeader()
	if got {
		a.query(func(st *electState) {
			st.holding = true
			st.expiresAt = a.now().Add(a.ttl)
		})
	}
	return got
}

// RenewLease answers from the lease timer: renewed while the last completed
// renew (or claim) is younger than the lease; displaced once when a renew
// met another owner; neither while the lease has lapsed without a renew
// getting through — the loop then keeps leading only while it still sees
// agents, exactly as before.
func (a *AsyncDiscoverer) RenewLease() (renewed, displaced bool) {
	a.query(func(st *electState) {
		if st.displaced {
			st.displaced = false
			displaced = true
			return
		}
		renewed = st.holding && a.now().Before(st.expiresAt)
	})
	return renewed, displaced
}

func (a *AsyncDiscoverer) renewWorker() {
	renewed, displaced := a.inner.RenewLease()
	a.do(func(st *electState) {
		st.renewing = false
		if !st.holding {
			return // released while the renew was in flight
		}
		switch {
		case renewed:
			st.expiresAt = a.now().Add(a.ttl)
		case displaced:
			st.holding, st.displaced = false, true
			log.Printf("lease renew: another owner holds the lease, stepping down")
		default:
			log.Printf("lease renew failed; lease valid for another %v", st.expiresAt.Sub(a.now()).Round(time.Second))
		}
	})
}

// ReleaseLeadership forgets the lease at once and deletes it in the
// background; a renew still in flight is ignored when it returns.
func (a *AsyncDiscoverer) ReleaseLeadership() {
	a.query(func(st *electState) {
		st.holding, st.claimed, st.displaced = false, false, false
		st.expiresAt = time.Time{}
	})
	go a.inner.ReleaseLeadership()
}

// LeaseExpiresAt is when our lease lapses unless renewed (zero when we do
// not hold one). Published for status and metrics.
func (a *AsyncDiscoverer) LeaseExpiresAt() time.Time {
	var t time.Time
	a.query(func(st *electState) {
		if st.holding {
			t = st.expiresAt
		}
	})
	return t
}

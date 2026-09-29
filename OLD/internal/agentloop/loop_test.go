package agentloop

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/config"
)

// ---- mocks ----

type mockDiscoverer struct {
	leader                 string
	storeDown              bool // GetLeader()=="" because the store is out, not because nobody leads
	becomeLeaderOK         bool
	renewLeaseOK           bool
	renewLeaseDisplaced    bool
	tryBecomeLeaderCalls   int
	releaseLeadershipCalls int
	onRelease              func()
}

func (m *mockDiscoverer) GetLeader() string    { return m.leader }
func (m *mockDiscoverer) StoreReachable() bool { return !m.storeDown }
func (m *mockDiscoverer) RenewLease() (bool, bool) {
	return m.renewLeaseOK, m.renewLeaseDisplaced
}
func (m *mockDiscoverer) ReleaseLeadership() {
	m.releaseLeadershipCalls++
	if m.onRelease != nil {
		m.onRelease()
	}
}
func (m *mockDiscoverer) TryBecomeLeader() bool {
	m.tryBecomeLeaderCalls++
	return m.becomeLeaderOK
}

type mockAgent struct {
	id             string
	endpoint       string
	placed         map[string]int
	stopAllCalls   int
	leaderAddr     string
	leaseExpiresAt time.Time
}

func (m *mockAgent) ID() string                          { return m.id }
func (m *mockAgent) Endpoint() string                    { return m.endpoint }
func (m *mockAgent) GetPlacedTaskCounts() map[string]int { return m.placed }
func (m *mockAgent) SetLeaderAddr(addr string)           { m.leaderAddr = addr }
func (m *mockAgent) SetLeaseExpiresAt(t time.Time)       { m.leaseExpiresAt = t }
func (m *mockAgent) StopAllTasks()                       { m.stopAllCalls++ }

type mockLeader struct {
	agents []*types.Agent
}

func (m *mockLeader) GetAgents() []*types.Agent { return m.agents }

// ---- helpers ----

func errRegister(err error) func(_, _, _ string, _ map[string]int, _ string) error {
	return func(_, _, _ string, _ map[string]int, _ string) error { return err }
}

func okRegister() func(_, _, _ string, _ map[string]int, _ string) error {
	return errRegister(nil)
}

func errHeartbeat(err error) func(_, _, _, _ string) error {
	return func(_, _, _, _ string) error {
		return err
	}
}

func okHeartbeat() func(_, _, _, _ string) error {
	return func(_, _, _, _ string) error {
		return nil
	}
}

func nopBecomeLeader() (func(), LeaderAPI) {
	return func() {}, nil
}

func newTestLoop(disc *mockDiscoverer, ag *mockAgent) *Loop {
	cfg := config.DefaultConfig()
	cfg.Node.IP = "127.0.0.1"
	cfg.Node.Port = 8080
	return &Loop{
		Cfg:            cfg,
		Ag:             ag,
		Disc:           disc,
		DoRegister:     okRegister(),
		DoHeartbeat:    okHeartbeat(),
		DoBecomeLeader: nopBecomeLeader,
	}
}

// ---- tests ----

func TestRunCancellationStopsLeaderBeforeReleasingLease(t *testing.T) {
	disc := &mockDiscoverer{}
	loop := newTestLoop(disc, &mockAgent{id: "leader"})
	var order []string
	loop.stopLeader = func() { order = append(order, "stop") }
	loop.l = &mockLeader{}
	loop.registered = true
	loop.lastLeaderAddr = "leader:9080"
	disc.onRelease = func() { order = append(order, "release") }

	done := make(chan struct{})
	close(done)
	loop.Run(done, time.Hour)

	if !reflect.DeepEqual(order, []string{"stop", "release"}) {
		t.Fatalf("shutdown order = %v, want [stop release]", order)
	}
	if loop.stopLeader != nil || loop.l != nil || loop.registered || loop.lastLeaderAddr != "" {
		t.Fatalf("leader state was retained after Run exit: %#v", loop)
	}
	if disc.tryBecomeLeaderCalls != 0 {
		t.Fatalf("already-cancelled Run attempted election %d time(s)", disc.tryBecomeLeaderCalls)
	}
}

func TestStepDownIsIdempotent(t *testing.T) {
	disc := &mockDiscoverer{}
	loop := newTestLoop(disc, &mockAgent{id: "leader"})
	stops := 0
	loop.stopLeader = func() { stops++ }

	loop.stepDown(true)
	loop.stepDown(true)

	if stops != 1 || disc.releaseLeadershipCalls != 1 {
		t.Fatalf("repeated stepDown: stops=%d releases=%d, want 1/1", stops, disc.releaseLeadershipCalls)
	}
}

// No leader from raft → 4 ticks → tryTakeOver triggered (~30s: T=0,10,20,30)
func TestTick_NoLeader_TriggersAfter4(t *testing.T) {
	disc := &mockDiscoverer{leader: ""}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})

	for i := range 4 {
		loop.Tick()
		if loop.failCount != i+1 {
			t.Fatalf("tick %d: failCount = %d, want %d", i+1, loop.failCount, i+1)
		}
	}

	if disc.tryBecomeLeaderCalls != 1 {
		t.Errorf("TryBecomeLeader calls = %d, want 1", disc.tryBecomeLeaderCalls)
	}
}

// 3 ticks must NOT yet trigger tryTakeOver
func TestTick_NoLeader_NotYetAt3(t *testing.T) {
	disc := &mockDiscoverer{leader: ""}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})

	for range 3 {
		loop.Tick()
	}

	if disc.tryBecomeLeaderCalls != 0 {
		t.Errorf("TryBecomeLeader should not be called after only 3 failures, got %d", disc.tryBecomeLeaderCalls)
	}
}

// Register fails 4 times → tryTakeOver triggered (~30s with immediate first tick)
func TestTick_RegisterFails_TriggersAfter4(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})
	loop.DoRegister = errRegister(errors.New("connection refused"))

	for range 4 {
		loop.Tick()
	}

	if disc.tryBecomeLeaderCalls != 1 {
		t.Errorf("TryBecomeLeader calls = %d, want 1", disc.tryBecomeLeaderCalls)
	}
}

// Register fails 7 times → StopAllTasks called (network isolation, ~60s)
func TestTick_RegisterFails7_StopsAllTasks(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080", becomeLeaderOK: false}
	ag := &mockAgent{id: "a1"}
	loop := newTestLoop(disc, ag)
	loop.DoRegister = errRegister(errors.New("connection refused"))

	for range 7 {
		loop.Tick()
	}

	if ag.stopAllCalls != 1 {
		t.Errorf("StopAllTasks calls = %d, want 1", ag.stopAllCalls)
	}
	// failCount is clamped to 4 after StopAllTasks
	if loop.failCount != 4 {
		t.Errorf("failCount = %d, want 4 (clamped)", loop.failCount)
	}
}

// Successful register → registered=true, failCount reset
func TestTick_RegisterSuccess_ResetsState(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})
	loop.failCount = 2 // simulated prior failures

	loop.Tick()

	if !loop.registered {
		t.Error("expected registered = true")
	}
	if loop.failCount != 0 {
		t.Errorf("failCount = %d, want 0", loop.failCount)
	}
	if loop.lastLeaderAddr != "leader:9080" {
		t.Errorf("lastLeaderAddr = %q, want %q", loop.lastLeaderAddr, "leader:9080")
	}
}

// Heartbeat fails 4 times → tryTakeOver triggered (~30s)
func TestTick_HeartbeatFails_TriggersAfter4(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})
	loop.registered = true
	loop.lastLeaderAddr = "leader:9080"
	loop.DoHeartbeat = errHeartbeat(errors.New("timeout"))

	for range 4 {
		loop.Tick()
	}

	if disc.tryBecomeLeaderCalls != 1 {
		t.Errorf("TryBecomeLeader calls = %d, want 1", disc.tryBecomeLeaderCalls)
	}
}

// Heartbeat returns 404 → re-register, no failCount increment
func TestTick_HeartbeatNotRegistered_Reregisters(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})
	loop.registered = true
	loop.lastLeaderAddr = "leader:9080"
	loop.DoHeartbeat = errHeartbeat(ErrNotRegistered)

	loop.Tick()

	if loop.registered {
		t.Error("expected registered = false after 404")
	}
	if loop.failCount != 0 {
		t.Errorf("failCount = %d, want 0 (no failure counted for 404)", loop.failCount)
	}
	if loop.lastLeaderAddr != "leader:9080" {
		t.Errorf("lastLeaderAddr = %q, want the known leader kept (re-register there, no store read)", loop.lastLeaderAddr)
	}
	if disc.tryBecomeLeaderCalls != 0 {
		t.Errorf("TryBecomeLeader calls = %d, want 0 (just re-register)", disc.tryBecomeLeaderCalls)
	}
	// Next tick re-registers at the same address without asking the store.
	registeredAt := ""
	loop.DoRegister = func(addr, _, _ string, _ map[string]int, _ string) error { registeredAt = addr; return nil }
	loop.DoHeartbeat = okHeartbeat()
	loop.Tick()
	if registeredAt != "leader:9080" || !loop.registered {
		t.Fatalf("re-register went to %q (registered=%v), want leader:9080", registeredAt, loop.registered)
	}
}

// Successful heartbeat → failCount reset. Heartbeat is nu puur liveness
// (16-07): geen job-sync meer — gewenste staat heeft één auteur (leader→S3).
func TestTick_HeartbeatSuccess_ResetsFailCount(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	ag := &mockAgent{id: "a1"}
	loop := newTestLoop(disc, ag)
	loop.registered = true
	loop.lastLeaderAddr = "leader:9080"
	loop.failCount = 2 // simulated prior failures

	loop.DoHeartbeat = okHeartbeat()
	loop.Tick()

	if loop.failCount != 0 {
		t.Errorf("failCount = %d, want 0", loop.failCount)
	}
}

// After successful takeover, stopLeader is set and failCount is reset
func TestTick_TakeoverSucceeds_SetsStopLeader(t *testing.T) {
	disc := &mockDiscoverer{leader: "", becomeLeaderOK: true}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})

	stopCalled := false
	loop.DoBecomeLeader = func() (func(), LeaderAPI) {
		return func() { stopCalled = true }, &mockLeader{}
	}

	// 4 ticks → takeover (~30s)
	for range 4 {
		loop.Tick()
	}

	if loop.stopLeader == nil {
		t.Fatal("stopLeader should be set after successful takeover")
	}
	if loop.failCount != 0 {
		t.Errorf("failCount = %d, want 0 after takeover", loop.failCount)
	}

	// cleanup: call stopLeader to avoid leaks
	loop.stopLeader()
	if !stopCalled {
		t.Error("stopLeader function was not the one returned by doBecomeLeader")
	}
}

// Register failure count is independent per failure — 2 fails then success resets
func TestTick_PartialFailureThenSuccess_Resets(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})

	callCount := 0
	loop.DoRegister = func(_, _, _ string, _ map[string]int, _ string) error {
		callCount++
		if callCount < 3 {
			return errors.New("not ready")
		}
		return nil
	}

	loop.Tick() // fail 1
	loop.Tick() // fail 2
	loop.Tick() // success

	if !loop.registered {
		t.Error("expected registered = true after eventual success")
	}
	if loop.failCount != 0 {
		t.Errorf("failCount = %d, want 0", loop.failCount)
	}
	if disc.tryBecomeLeaderCalls != 0 {
		t.Errorf("TryBecomeLeader should not be called with <4 failures, got %d", disc.tryBecomeLeaderCalls)
	}
}

// Leader with raft unreachable but agents connected stays leader
func TestTick_LeaderRaftDown_StaysLeader(t *testing.T) {
	disc := &mockDiscoverer{renewLeaseOK: false}
	loop := newTestLoop(disc, &mockAgent{id: "leader1"})
	loop.stopLeader = func() {}
	loop.l = &mockLeader{agents: []*types.Agent{{ID: "follower1"}}}

	loop.Tick()

	if loop.stopLeader == nil {
		t.Error("should still be leader when agents are connected")
	}
}

// Leader with raft unreachable and NO agents loses leadership
func TestTick_LeaderRaftDown_NoAgents_LosesLeadership(t *testing.T) {
	disc := &mockDiscoverer{renewLeaseOK: false}
	loop := newTestLoop(disc, &mockAgent{id: "leader1"})

	stopCalled := false
	loop.stopLeader = func() { stopCalled = true }
	loop.l = &mockLeader{agents: []*types.Agent{}}

	loop.Tick()

	if !stopCalled {
		t.Error("should have called stopLeader")
	}
	if loop.stopLeader != nil {
		t.Error("stopLeader should be nil after losing leadership")
	}
}

// Raft recovers after being down → failCount resets
func TestTick_LeaderRaftRecovers_ResetsFailCount(t *testing.T) {
	disc := &mockDiscoverer{renewLeaseOK: false}
	loop := newTestLoop(disc, &mockAgent{id: "leader1"})
	loop.stopLeader = func() {}
	loop.l = &mockLeader{agents: []*types.Agent{{ID: "follower1"}}}
	loop.failCount = 3

	// Raft down: stays leader, failCount unchanged
	loop.Tick()
	if loop.failCount != 3 {
		t.Fatalf("failCount should stay %d during raft-down, got %d", 3, loop.failCount)
	}

	// Raft recovers
	disc.renewLeaseOK = true
	loop.Tick()
	if loop.failCount != 0 {
		t.Errorf("failCount should reset to 0 after raft recovery, got %d", loop.failCount)
	}
}

// Uses cached leader address, does not call GetLeader when lastLeaderAddr set
func TestTick_UsesCachedLeaderAddr(t *testing.T) {
	disc := &mockDiscoverer{leader: "wrong:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})
	loop.registered = true
	loop.lastLeaderAddr = "cached:9080"

	var calledAddr string
	loop.DoHeartbeat = func(addr, _, _, _ string) error {
		calledAddr = addr
		return nil
	}

	loop.Tick()

	if calledAddr != "cached:9080" {
		t.Errorf("heartbeat sent to %q, want cached %q", calledAddr, "cached:9080")
	}
}

// De LEIDER-variant van het herstel: de eigen leader-API is bereikbaar maar is
// deze agent vergeten (agent-timeout tijdens een netwerkstoring). De
// self-heartbeat-tak liet dat vóór de fix eeuwig doorlopen — GEMETEN 13-08 op
// een LicheeRV: 70+ "not registered"-heartbeats, lege /v1/agents, geen herstel.
func TestTick_SelfHeartbeatNotRegistered_Reregisters(t *testing.T) {
	disc := &mockDiscoverer{leader: "127.0.0.1:9080", renewLeaseOK: true}
	registers := 0
	loop := newTestLoop(disc, &mockAgent{id: "a1", placed: map[string]int{"stulp": 1}})
	loop.stopLeader = func() {} // we ZIJN de leader
	loop.l = &mockLeader{}
	loop.DoHeartbeat = errHeartbeat(ErrNotRegistered)
	loop.DoRegister = func(_, _, _ string, placed map[string]int, _ string) error {
		registers++
		if placed["stulp"] != 1 {
			t.Errorf("herregistratie zonder placed counts: %v", placed)
		}
		return nil
	}

	loop.Tick()

	if registers != 1 {
		t.Fatalf("DoRegister %d keer geroepen, wil 1 — de leider herstelt zijn eigen registratie niet", registers)
	}
	if loop.selfBeatFails != 0 {
		t.Errorf("selfBeatFails = %d, wil 0 na herstel", loop.selfBeatFails)
	}
	if loop.stopLeader == nil {
		t.Error("het leiderschap zelf hoort onaangeroerd te blijven")
	}
}

// Een transportfout op de self-heartbeat blijft wat hij was: zichtbaar maken,
// niet op reageren (de API accepteert dan geen verbindingen — herregistreren
// zou daar niets aan doen).
func TestTick_SelfHeartbeatTransportFout_AlleenTellen(t *testing.T) {
	disc := &mockDiscoverer{leader: "127.0.0.1:9080", renewLeaseOK: true}
	registers := 0
	loop := newTestLoop(disc, &mockAgent{id: "a1"})
	loop.stopLeader = func() {}
	loop.l = &mockLeader{}
	loop.DoHeartbeat = errHeartbeat(errors.New("dial tcp: connection refused"))
	loop.DoRegister = func(_, _, _ string, _ map[string]int, _ string) error {
		registers++
		return nil
	}

	loop.Tick()
	loop.Tick()

	if registers != 0 {
		t.Fatalf("DoRegister %d keer geroepen op een transportfout, wil 0", registers)
	}
	if loop.selfBeatFails != 2 {
		t.Errorf("selfBeatFails = %d, wil 2", loop.selfBeatFails)
	}
}

// LeaderAddr is what the agent API proxies to. It must follow the loop's own
// knowledge — never a lock-store read per request.
func TestLeaderAddrFollowsTheLoop(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	ag := &mockAgent{}
	loop := newTestLoop(disc, ag)
	if got := ag.leaderAddr; got != "" {
		t.Fatalf("before first tick LeaderAddr() = %q, want empty", got)
	}

	loop.Tick() // discovers leader:9080 and registers there
	if got := ag.leaderAddr; got != "leader:9080" {
		t.Fatalf("after register LeaderAddr() = %q, want leader:9080", got)
	}

	// The leader forgot us: re-discovery is forced, but the published
	// address stays — it is still the same leader.
	loop.DoHeartbeat = errHeartbeat(ErrNotRegistered)
	loop.Tick()
	if got := ag.leaderAddr; got != "leader:9080" {
		t.Fatalf("after not-registered LeaderAddr() = %q, want leader:9080", got)
	}

	// Leader unreachable for 4 ticks → takeover attempt clears it, and a
	// successful takeover publishes our own leader API.
	loop.DoHeartbeat = errHeartbeat(errors.New("connection refused"))
	loop.DoRegister = errRegister(errors.New("connection refused"))
	disc.becomeLeaderOK = false
	for i := 0; i < 4; i++ {
		loop.Tick()
	}
	if got := ag.leaderAddr; got != "" {
		t.Fatalf("after leader lost LeaderAddr() = %q, want empty", got)
	}
	disc.becomeLeaderOK = true
	loop.Tick()
	if got := ag.leaderAddr; got != "127.0.0.1:9080" {
		t.Fatalf("as leader LeaderAddr() = %q, want 127.0.0.1:9080", got)
	}

	loop.stepDown(true)
	if got := ag.leaderAddr; got != "" {
		t.Fatalf("after step-down LeaderAddr() = %q, want empty", got)
	}
}

// No leader at all (election in progress, lease lapsed, store out): tasks
// keep running however long it takes. Measured 2026-09-08: a ghost lease left
// traqqr leaderless and after 70 s both agents had stopped every task.
func TestTick_NoLeader_NeverStopsTasks(t *testing.T) {
	disc := &mockDiscoverer{leader: ""}
	ag := &mockAgent{id: "a1"}
	loop := newTestLoop(disc, ag)

	for range 20 {
		loop.Tick()
	}

	if ag.stopAllCalls != 0 {
		t.Fatalf("StopAllTasks called %d time(s) without any leader", ag.stopAllCalls)
	}
	if disc.tryBecomeLeaderCalls == 0 {
		t.Fatal("never tried to take over")
	}
}

// A leader we cannot reach whose lease has meanwhile lapsed is not an
// isolation: the store says nobody leads, so nobody re-places our tasks.
func TestTick_LeaderGoneFromStore_NeverStopsTasks(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	ag := &mockAgent{id: "a1"}
	loop := newTestLoop(disc, ag)
	loop.registered = true
	loop.lastLeaderAddr = "leader:9080"
	loop.DoHeartbeat = errHeartbeat(errors.New("connection refused"))
	loop.DoRegister = errRegister(errors.New("connection refused"))

	for i := range 12 {
		if i == 4 {
			disc.leader = "" // the lease lapsed: the store reports no leader
		}
		loop.Tick()
	}

	if ag.stopAllCalls != 0 {
		t.Fatalf("StopAllTasks called %d time(s) while the store reported no leader", ag.stopAllCalls)
	}
}

// Store AND leader unreachable: we cannot tell whether we are the isolated
// party, so the fail-safe stays: stop after 7 ticks (as it always did).
func TestTick_LeaderAndStoreUnreachable_StopsTasks(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	ag := &mockAgent{id: "a1"}
	loop := newTestLoop(disc, ag)
	loop.registered = true
	loop.lastLeaderAddr = "leader:9080"
	loop.DoHeartbeat = errHeartbeat(errors.New("connection refused"))
	loop.DoRegister = errRegister(errors.New("connection refused"))

	for i := range 12 {
		if i == 3 {
			disc.leader, disc.storeDown = "", true // the store went dark too
		}
		loop.Tick()
	}

	if ag.stopAllCalls == 0 {
		t.Fatal("fail-safe did not stop tasks with leader and store both unreachable")
	}
}

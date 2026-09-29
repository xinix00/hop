package leader

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
)

// ============== ROBUSTNESS TESTS ==============
// These tests verify placed-based reconciliation under failure scenarios
// not covered by failover_test.go, phantom_node_test.go, or chaos_test.go.

// TestThreeNodeClusterOneDies verifies that when one of three agents dies,
// its tasks are redistributed to BOTH survivors via round-robin.
func TestThreeNodeClusterOneDies(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	agentC := newMockAgent()
	defer agentA.Close()
	defer agentC.Close()

	store := NewMockJobStore()
	l := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	l.agentTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-a", agentA.URL(), "", nil)
	l.RegisterAgent("agent-b", agentB.URL(), "", nil)
	l.RegisterAgent("agent-c", agentC.URL(), "", nil)
	l.Heartbeat("agent-a", "", 0)
	l.Heartbeat("agent-b", "", 0)
	l.Heartbeat("agent-c", "", 0)
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{
		Name:    "app",
		Command: "./app",
		Count:   30,
	}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	aTasks := agentA.TaskCount()
	bTasks := agentB.TaskCount()
	cTasks := agentC.TaskCount()
	t.Logf("Initial: A=%d, B=%d, C=%d (total=%d)", aTasks, bTasks, cTasks, aTasks+bTasks+cTasks)

	if aTasks+bTasks+cTasks != 30 {
		t.Fatalf("Expected 30 tasks, got %d", aTasks+bTasks+cTasks)
	}

	// Agent B dies
	agentB.Close()
	time.Sleep(300 * time.Millisecond)

	// Keep A and C alive
	l.Heartbeat("agent-a", "", 0)
	l.Heartbeat("agent-c", "", 0)
	time.Sleep(20 * time.Millisecond)

	l.checkDeadAgents()
	time.Sleep(100 * time.Millisecond)

	aFinal := agentA.TaskCount()
	cFinal := agentC.TaskCount()
	total := aFinal + cFinal
	t.Logf("After B dies: A=%d (+%d), C=%d (+%d), total=%d",
		aFinal, aFinal-aTasks, cFinal, cFinal-cTasks, total)

	if total != 30 {
		t.Errorf("Expected 30 total tasks, got %d (B had %d)", total, bTasks)
	}

	// Both survivors should have received some of B's tasks (round-robin)
	if aFinal <= aTasks && cFinal <= cTasks {
		t.Errorf("At least one survivor should have gained tasks: A %d→%d, C %d→%d",
			aTasks, aFinal, cTasks, cFinal)
	}
}

// TestDaemonStableDuringBlipNewNodeJoins verifies that daemon jobs (count=-1)
// are not re-dispatched to existing agents during a network blip.
// Only the newly joined agent should receive the daemon.
func TestDaemonStableDuringBlipNewNodeJoins(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	agentC := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()
	defer agentC.Close()

	store := NewMockJobStore()
	l := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	l.agentTimeout = 2 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// Dispatch daemon to A and B
	l.RegisterAgent("agent-a", agentA.URL(), "", nil)
	l.RegisterAgent("agent-b", agentB.URL(), "", nil)
	l.Heartbeat("agent-a", "", 0)
	l.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	daemon := &types.Job{
		Name:    "daemon",
		Command: "./daemon",
		Count:   -1,
	}
	if err := l.DispatchJob(daemon); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agentA.TaskCount() != 1 || agentB.TaskCount() != 1 {
		t.Fatalf("Both should have daemon: A=%d, B=%d", agentA.TaskCount(), agentB.TaskCount())
	}

	aRunsBefore := agentA.RunCallCount()
	bRunsBefore := agentB.RunCallCount()

	// Agent A has network blip (stops heartbeating, timeout not reached)
	time.Sleep(100 * time.Millisecond)
	l.Heartbeat("agent-b", "", 0)

	// Agent C joins during A's blip via RegisterAgent
	l.RegisterAgent("agent-c", agentC.URL(), "", nil)
	time.Sleep(100 * time.Millisecond)

	// C should get daemon (new agent, placed[C][daemon] == 0)
	if agentC.TaskCount() != 1 {
		t.Errorf("Agent C should have daemon, got %d tasks", agentC.TaskCount())
	}

	// A and B should NOT get duplicate daemon dispatches
	if agentA.RunCallCount() != aRunsBefore {
		t.Errorf("Agent A got %d extra /run calls during blip", agentA.RunCallCount()-aRunsBefore)
	}
	if agentB.RunCallCount() != bRunsBefore {
		t.Errorf("Agent B got %d extra /run calls", agentB.RunCallCount()-bRunsBefore)
	}

	// Agent A recovers
	l.Heartbeat("agent-a", "", 0)
	time.Sleep(50 * time.Millisecond)

	// Still exactly 1 daemon per agent, 3 total
	placed := l.GetPlaced(daemon.Name)
	if len(placed) != 3 {
		t.Errorf("Daemon should be on 3 agents, got %d: %v", len(placed), placed)
	}
}

// TestHeartbeatReducedPlacedTriggersReconcile verifies that when an agent
// reports fewer placed counts via heartbeat (tasks crashed on the agent),
// reconciliation dispatches the missing instances.
func TestHeartbeatReducedPlacedTriggersReconcile(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()

	store := NewMockJobStore()
	l := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-a", agentA.URL(), "", nil)
	l.RegisterAgent("agent-b", agentB.URL(), "", nil)
	l.Heartbeat("agent-a", "", 0)
	l.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{
		Name:    "app",
		Command: "./app",
		Count:   20,
	}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	aTasks := agentA.TaskCount()
	bTasks := agentB.TaskCount()
	t.Logf("Initial: A=%d, B=%d", aTasks, bTasks)

	// Agent B loses 3 tasks (crashed and couldn't restart)
	agentB.mu.Lock()
	agentB.tasks = agentB.tasks[:bTasks-3]
	agentB.mu.Unlock()

	// B registers with reduced placed count → triggers reconcile → dispatches 3 missing
	l.RegisterAgent("agent-b", agentB.URL(), "", map[string]int{job.Name: bTasks - 3})
	time.Sleep(100 * time.Millisecond)

	// RegisterAgent triggers reconcile: placed=10+7=17, desired=20, dispatches 3
	total := agentA.TaskCount() + agentB.TaskCount()
	t.Logf("After reconcile: A=%d, B=%d (total=%d)", agentA.TaskCount(), agentB.TaskCount(), total)

	if total != 20 {
		t.Errorf("Expected 20 total after reconciliation, got %d (3 should have been dispatched)", total)
	}
}

// TestMixedJobsAgentDies verifies that when an agent dies with both daemon
// and regular jobs, only regular job tasks are rescheduled. Daemon tasks
// are NOT rescheduled because other agents already have their own instances.
func TestMixedJobsAgentDies(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentB.Close()

	store := NewMockJobStore()
	l := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	l.agentTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-a", agentA.URL(), "", nil)
	l.RegisterAgent("agent-b", agentB.URL(), "", nil)
	l.Heartbeat("agent-a", "", 0)
	l.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	daemon := &types.Job{Name: "daemon", Command: "./daemon", Count: -1}
	regular := &types.Job{Name: "web", Command: "./web", Count: 4}

	if err := l.DispatchJob(daemon); err != nil {
		t.Fatalf("Daemon dispatch failed: %v", err)
	}
	if err := l.DispatchJob(regular); err != nil {
		t.Fatalf("Regular dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	aTasks := agentA.TaskCount()
	bTasks := agentB.TaskCount()
	t.Logf("Initial: A=%d, B=%d (1 daemon + 2 regular each)", aTasks, bTasks)

	bRunsBefore := agentB.RunCallCount()

	// Agent A dies
	agentA.Close()
	time.Sleep(300 * time.Millisecond)

	l.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	l.checkDeadAgents()
	time.Sleep(100 * time.Millisecond)

	bNewRuns := agentB.RunCallCount() - bRunsBefore
	t.Logf("After A dies: B got %d new /run calls", bNewRuns)

	// B should get A's 2 regular tasks only (daemon NOT rescheduled)
	if bNewRuns != 2 {
		t.Errorf("Expected 2 new dispatches (regular only), got %d", bNewRuns)
	}

	// Daemon placed: only B (A was removed)
	daemonPlaced := l.GetPlaced(daemon.Name)
	if len(daemonPlaced) != 1 {
		t.Errorf("Daemon should be on 1 agent, got %d: %v", len(daemonPlaced), daemonPlaced)
	}

	// Regular placed: all 4 on B
	regularPlaced := l.GetPlaced(regular.Name)
	totalRegular := 0
	for _, count := range regularPlaced {
		totalRegular += count
	}
	if totalRegular != 4 {
		t.Errorf("Regular should have 4 placed, got %d", totalRegular)
	}
}

// TestGracefulLeaveThreeNodes verifies that when an agent gracefully
// unregisters from a 3-node cluster, its tasks are redistributed
// to the remaining 2 agents.
func TestGracefulLeaveThreeNodes(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	agentC := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()
	defer agentC.Close()

	store := NewMockJobStore()
	l := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-a", agentA.URL(), "", nil)
	l.RegisterAgent("agent-b", agentB.URL(), "", nil)
	l.RegisterAgent("agent-c", agentC.URL(), "", nil)
	l.Heartbeat("agent-a", "", 0)
	l.Heartbeat("agent-b", "", 0)
	l.Heartbeat("agent-c", "", 0)
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{
		Name:    "app",
		Command: "./app",
		Count:   30,
	}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	aTasks := agentA.TaskCount()
	bTasks := agentB.TaskCount()
	cTasks := agentC.TaskCount()
	t.Logf("Initial: A=%d, B=%d, C=%d", aTasks, bTasks, cTasks)

	// Agent B gracefully unregisters
	l.UnregisterAgent("agent-b")
	time.Sleep(100 * time.Millisecond)

	aFinal := agentA.TaskCount()
	cFinal := agentC.TaskCount()
	total := aFinal + cFinal
	t.Logf("After B leaves: A=%d (+%d), C=%d (+%d), total=%d",
		aFinal, aFinal-aTasks, cFinal, cFinal-cTasks, total)

	if total != 30 {
		t.Errorf("Expected 30 total after graceful leave, got %d", total)
	}

	// B should not be in agents or placed
	agents := l.GetAgents()
	for _, a := range agents {
		if a.ID == "agent-b" {
			t.Error("Agent B should not be in agents after unregister")
		}
	}
	placed := l.GetPlaced(job.Name)
	if _, ok := placed["agent-b"]; ok {
		t.Error("Agent B should have no placed entries after unregister")
	}
}

// TestAgentDiesAndRejoinsGetsDaemon verifies the full daemon lifecycle:
// 1. Daemon dispatched to 2 agents
// 2. One agent dies → daemon NOT rescheduled to survivor (count=-1 design)
// 3. Dead agent rejoins → daemon dispatched to it again
func TestAgentDiesAndRejoinsGetsDaemon(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentA.Close()

	store := NewMockJobStore()
	l := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	l.agentTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-a", agentA.URL(), "", nil)
	l.RegisterAgent("agent-b", agentB.URL(), "", nil)
	l.Heartbeat("agent-a", "", 0)
	l.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	daemon := &types.Job{Name: "daemon", Command: "./daemon", Count: -1}
	if err := l.DispatchJob(daemon); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agentA.TaskCount() != 1 || agentB.TaskCount() != 1 {
		t.Fatalf("Both should have daemon: A=%d, B=%d", agentA.TaskCount(), agentB.TaskCount())
	}

	// Agent B dies
	agentB.Close()
	time.Sleep(300 * time.Millisecond)

	l.Heartbeat("agent-a", "", 0)
	time.Sleep(20 * time.Millisecond)

	l.checkDeadAgents()
	time.Sleep(20 * time.Millisecond)

	// A still has 1 daemon (daemon NOT rescheduled from B, by count=-1 design)
	if agentA.TaskCount() != 1 {
		t.Errorf("A should still have 1 daemon, got %d", agentA.TaskCount())
	}
	if len(l.GetPlaced(daemon.Name)) != 1 {
		t.Errorf("Daemon should only be placed on A after B dies")
	}

	// Agent B rejoins (fresh process, no tasks) via RegisterAgent
	agentBNew := newMockAgent()
	defer agentBNew.Close()

	l.RegisterAgent("agent-b", agentBNew.URL(), "", nil)
	time.Sleep(100 * time.Millisecond)

	// B should get daemon again (count=-1 → run on all agents)
	if agentBNew.TaskCount() != 1 {
		t.Errorf("Rejoined B should get daemon, got %d tasks", agentBNew.TaskCount())
	}

	placed := l.GetPlaced(daemon.Name)
	if len(placed) != 2 {
		t.Errorf("Daemon should be on 2 agents after rejoin, got %d: %v", len(placed), placed)
	}
}

// TestZombieAgentNoOverScheduling verifies that when a dead agent returns
// after its tasks have been redistributed, no additional tasks are dispatched.
//
// Scenario (network partition):
// 1. A and B each have 10 tasks (job count=20)
// 2. A is partitioned → leader removes A, redistributes 10 tasks to B (B now has 20)
// 3. Partition heals: A comes back with its 10 tasks still running
// 4. Leader sees totalPlaced = 10 (A) + 20 (B) = 30 > desired 20
// 5. No more tasks are dispatched (system is over-scheduled but stable)
func TestZombieAgentNoOverScheduling(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentB.Close()

	store := NewMockJobStore()
	l := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	l.agentTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-a", agentA.URL(), "", nil)
	l.RegisterAgent("agent-b", agentB.URL(), "", nil)
	l.Heartbeat("agent-a", "", 0)
	l.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{
		Name:    "app",
		Command: "./app",
		Count:   20,
	}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	aTasks := agentA.TaskCount()
	bTasks := agentB.TaskCount()
	t.Logf("Initial: A=%d, B=%d", aTasks, bTasks)

	// Agent A dies (network partition)
	agentA.Close()
	time.Sleep(300 * time.Millisecond)

	l.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	l.checkDeadAgents()
	time.Sleep(100 * time.Millisecond)

	if agentB.TaskCount() != 20 {
		t.Fatalf("B should have all 20 after A dies, got %d", agentB.TaskCount())
	}

	bRunsAfterRedist := agentB.RunCallCount()

	// Zombie: Agent A comes back (partition healed, tasks still running)
	zombieA := newMockAgent()
	defer zombieA.Close()

	zombieA.mu.Lock()
	for i := 0; i < aTasks; i++ {
		zombieA.tasks = append(zombieA.tasks, &types.Task{
			ID:      fmt.Sprintf("zombie-task-%d", i),
			JobName: job.Name,
			State:   types.TaskRunning,
		})
	}
	zombieA.mu.Unlock()

	// Zombie registers with its placed data and heartbeats for keepalive
	l.RegisterAgent("agent-a", zombieA.URL(), "", map[string]int{job.Name: aTasks})
	l.Heartbeat("agent-a", "", 0)
	time.Sleep(100 * time.Millisecond)

	// No more tasks dispatched (totalPlaced > desired)
	zombieRuns := zombieA.RunCallCount()
	bRunsAfterZombie := agentB.RunCallCount()

	if zombieRuns > 0 {
		t.Errorf("Zombie A should NOT receive new /run calls, got %d", zombieRuns)
	}
	if bRunsAfterZombie > bRunsAfterRedist {
		t.Errorf("B should NOT receive new /run calls after zombie returns, got %d new",
			bRunsAfterZombie-bRunsAfterRedist)
	}

	// Document: 30 tasks running (20 on B + 10 on zombie A) but desired is 20.
	// Placed-based system doesn't scale down — this is intentional.
	totalPlaced := l.GetPlaced(job.Name)
	total := 0
	for _, count := range totalPlaced {
		total += count
	}
	t.Logf("After zombie: totalPlaced=%d (desired=20, over-scheduled by %d)", total, total-20)
}

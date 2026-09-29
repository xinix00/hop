package leader

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
)

// TestConcurrentDispatchNewAgentJoinOverScheduling reproduces the over-scheduling bug:
// DispatchJob(count=20) dispatches one-by-one to agent-1. While dispatching (slowly),
// agent-2 joins via heartbeat → isNew → reconcileJobs sees partial placed → dispatches
// more instances. Total ends up > 20.
//
// Fix: dispatching flag. dispatchInstances sets dispatching[jobID]=true, reconcileJob skips it.
func TestConcurrentDispatchNewAgentJoinOverScheduling(t *testing.T) {
	agent1 := newMockAgent()
	agent1.SetRunDelay(50 * time.Millisecond) // Each dispatch takes ~60ms
	defer agent1.Close()

	agent2 := newMockAgent()
	defer agent2.Close()

	store := NewMockJobStore()
	ldr := New("leader", store, &httputil.Client{Timeout: 200 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ldr.stateLoop(ctx)

	// Register agent-1
	ldr.RegisterAgent("agent-1", agent1.URL(), "", nil)
	ldr.Heartbeat("agent-1", "", 0)
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{Name: "app", Command: "./app", Count: 20}
	// Pre-store job so reconcileJobs can find it during dispatch
	store.StoreJob(job)

	// Start dispatching in background (~60ms per dispatch = ~1.2s for 20)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = ldr.DispatchJob(job)
	}()

	// Wait for a few dispatches to complete (~5 at 60ms each)
	time.Sleep(350 * time.Millisecond)

	// Agent-2 joins mid-dispatch → RegisterAgent → reconcileJobs
	// With fix: reconcileJob sees dispatching[app-id]=true → skips
	// Without fix: reconcileJob dispatches 15 more → total ≈ 35
	ldr.RegisterAgent("agent-2", agent2.URL(), "", nil)

	// Wait for all dispatches to finish
	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	total := agent1.TaskCount() + agent2.TaskCount()
	t.Logf("Tasks: agent-1=%d, agent-2=%d, total=%d (desired=20)",
		agent1.TaskCount(), agent2.TaskCount(), total)

	if total != 20 {
		t.Errorf("Over-scheduling bug! Got %d total tasks, expected exactly 20", total)
	}
}

// TestLeaderCrashConcurrentHeartbeatsOverScheduling reproduces the bug during failover:
// New leader has persisted job (count=20). Two agents each had 10 tasks running.
// Without settle: agent-1 heartbeats first → reconcileJobs dispatches 10 extra.
//
// Fix: settleDelay. Leader waits for agents to report before reconciling.
// After settle, totalPlaced=10+10=20 → no new dispatches.
func TestLeaderCrashConcurrentHeartbeatsOverScheduling(t *testing.T) {
	agent1 := newMockAgent()
	defer agent1.Close()

	agent2 := newMockAgent()
	defer agent2.Close()

	// Pre-populate agents with existing tasks from old leader era
	agent1.mu.Lock()
	for i := 0; i < 10; i++ {
		agent1.tasks = append(agent1.tasks, &types.Task{
			ID:      fmt.Sprintf("old-task-a%d", i),
			JobName: "app",
			State:   types.TaskRunning,
		})
	}
	agent1.mu.Unlock()

	agent2.mu.Lock()
	for i := 0; i < 10; i++ {
		agent2.tasks = append(agent2.tasks, &types.Task{
			ID:      fmt.Sprintf("old-task-b%d", i),
			JobName: "app",
			State:   types.TaskRunning,
		})
	}
	agent2.mu.Unlock()

	// New leader with persisted job from before crash
	store := NewMockJobStore()
	job := &types.Job{Name: "app", Command: "./app", Count: 20}
	store.StoreJob(job)
	store.stateTime = time.Now().Add(-1 * time.Minute)

	ldr := New("leader", store, nil)
	ldr.settleDelay = 300 * time.Millisecond // Wait for agents before reconciling

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ldr.stateLoop(ctx)

	// Both agents register with placed counts during settle period (placed learned, no dispatch)
	ldr.RegisterAgent("agent-1", agent1.URL(), "", map[string]int{"app": 10})
	ldr.RegisterAgent("agent-2", agent2.URL(), "", map[string]int{"app": 10})
	// Heartbeat for keepalive
	ldr.Heartbeat("agent-1", "", 0)
	ldr.Heartbeat("agent-2", "", 0)

	// Wait for settle timer + reconciliation
	time.Sleep(500 * time.Millisecond)

	// After settle: totalPlaced=10+10=20 → no new dispatches needed
	total := agent1.TaskCount() + agent2.TaskCount()
	t.Logf("Total tasks: agent-1=%d (10 old + %d new), agent-2=%d (10 old + %d new)",
		agent1.TaskCount(), agent1.TaskCount()-10,
		agent2.TaskCount(), agent2.TaskCount()-10)
	t.Logf("Total=%d, desired=20", total)

	if total > 20 {
		t.Errorf("Over-scheduling! Got %d total tasks, expected at most 20 "+
			"(agents already had 10+10=20 from old leader)", total)
	}
}

// TestAgentRestartHeartbeatRaceOverScheduling reproduces the 11/10 over-scheduling
// bug when turning an agent off and on.
//
// Race condition:
//  1. 3 agents, job count=10: placed a1=4, a2=3, a3=3
//  2. Agent-2 goes down → leader dispatches 3 replacements to a1 and a3
//     → trackPlacement updates: placed a1=6, a3=4 (total=10)
//  3. Agent-1 sends heartbeat with placed={4} — stale because the newly
//     dispatched tasks haven't appeared in s.tasks yet (handleRun returns
//     202 Accepted, task starts in background goroutine)
//     → leader REPLACES placed[a1] from 6 to 4 (2 dispatches lost!)
//     → placed: a1=4, a3=4 (total=8)
//  4. Agent-2 comes back → RegisterAgent → reconcileJobs sees total=8
//     → dispatches 2 more → 12 total tasks dispatched (expected 10)
func TestAgentRestartHeartbeatRaceOverScheduling(t *testing.T) {
	agents := make([]*mockAgent, 3)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	store := NewMockJobStore()
	ldr := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ldr.stateLoop(ctx)

	// Register all 3 agents
	for i := range agents {
		agentID := fmt.Sprintf("agent-%d", i+1)
		ldr.RegisterAgent(agentID, agents[i].URL(), "", nil)
	}
	time.Sleep(20 * time.Millisecond)

	// Dispatch job count=10 → round-robin: a1=4, a2=3, a3=3
	job := &types.Job{Name: "app", Command: "./app", Count: 10}
	if err := ldr.DispatchJob(job); err != nil {
		t.Fatalf("DispatchJob failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Verify initial placement
	a1Initial := agents[0].TaskCount()
	a2Initial := agents[1].TaskCount()
	a3Initial := agents[2].TaskCount()
	total := a1Initial + a2Initial + a3Initial
	t.Logf("Initial: a1=%d, a2=%d, a3=%d (total=%d)", a1Initial, a2Initial, a3Initial, total)
	if total != 10 {
		t.Fatalf("Initial dispatch should be 10, got %d", total)
	}

	// ---- Agent-2 goes down ----
	agents[1].ClearTasks()
	ldr.UnregisterAgent("agent-2")
	time.Sleep(50 * time.Millisecond) // let trackPlacement ops settle

	t.Logf("After a2 down: a1=%d (+%d), a3=%d (+%d)",
		agents[0].TaskCount(), agents[0].TaskCount()-a1Initial,
		agents[2].TaskCount(), agents[2].TaskCount()-a3Initial)

	// ---- Stale heartbeat from agent-1 ----
	// In production: agent-1 received /run (202 Accepted) but task hasn't
	// appeared in s.tasks yet → GetPlacedTaskCounts() returns OLD count.
	// This heartbeat REPLACES the leader's placed[a1] with stale data.
	ldr.Heartbeat("agent-1", "", 0)
	time.Sleep(20 * time.Millisecond)

	// ---- Agent-2 comes back ----
	ldr.RegisterAgent("agent-2", agents[1].URL(), "", nil)
	time.Sleep(100 * time.Millisecond)

	// Count total tasks across all agents
	finalTotal := agents[0].TaskCount() + agents[1].TaskCount() + agents[2].TaskCount()
	t.Logf("Final: a1=%d, a2=%d, a3=%d (total=%d, desired=10)",
		agents[0].TaskCount(), agents[1].TaskCount(), agents[2].TaskCount(), finalTotal)

	if finalTotal != 10 {
		t.Errorf("Over-scheduling! Got %d total tasks, expected 10.\n"+
			"Heartbeat from agent-1 replaced placed count from %d to %d (stale),\n"+
			"causing reconcile to dispatch %d extra tasks.",
			finalTotal, agents[0].TaskCount(), a1Initial, finalTotal-10)
	}
}

// TestAgentCrashRejoinWithinTimeout reproduces the under-scheduling bug:
// 3 agents running 20 tasks (7+7+6). Agent-3 crashes and rejoins within 30s
// (before dead agent timeout). Leader sees it as existing agent → no reconciliation.
// Agent-3's placed counts get updated to 0, but nobody triggers reconcileJobs.
// Result: 14/20 tasks running (agent-3's 6 tasks are lost).
//
// Fix: agent sends "fresh" flag → leader treats it as rejoin → reconcile.
func TestAgentCrashRejoinWithinTimeout(t *testing.T) {
	agent1 := newMockAgent()
	defer agent1.Close()

	agent2 := newMockAgent()
	defer agent2.Close()

	agent3 := newMockAgent()
	defer agent3.Close()

	store := NewMockJobStore()
	ldr := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ldr.stateLoop(ctx)

	// Register all 3 agents
	ldr.RegisterAgent("agent-1", agent1.URL(), "", nil)
	ldr.RegisterAgent("agent-2", agent2.URL(), "", nil)
	ldr.RegisterAgent("agent-3", agent3.URL(), "", nil)
	ldr.Heartbeat("agent-1", "", 0)
	ldr.Heartbeat("agent-2", "", 0)
	ldr.Heartbeat("agent-3", "", 0)
	time.Sleep(20 * time.Millisecond)

	// Dispatch 20 tasks (round-robin across 3 agents → 7+7+6)
	job := &types.Job{Name: "app", Command: "./app", Count: 20}
	if err := ldr.DispatchJob(job); err != nil {
		t.Fatalf("DispatchJob failed: %v", err)
	}

	a1Before := agent1.TaskCount()
	a2Before := agent2.TaskCount()
	a3Before := agent3.TaskCount()
	totalBefore := a1Before + a2Before + a3Before
	t.Logf("Before crash: agent-1=%d, agent-2=%d, agent-3=%d, total=%d",
		a1Before, a2Before, a3Before, totalBefore)

	if totalBefore != 20 {
		t.Fatalf("Expected 20 tasks before crash, got %d", totalBefore)
	}

	// Agent-3 crashes: loses all tasks
	agent3.mu.Lock()
	agent3.tasks = nil
	agent3.mu.Unlock()

	// Agent-3 comes back within timeout (< 30s), registers as fresh
	// RegisterAgent clears old placed, treats as NEW → reconciles
	ldr.RegisterAgent("agent-3", agent3.URL(), "", nil)

	// Wait for any reconciliation to happen
	time.Sleep(500 * time.Millisecond)

	a1After := agent1.TaskCount()
	a2After := agent2.TaskCount()
	a3After := agent3.TaskCount()
	total := a1After + a2After + a3After
	t.Logf("After rejoin: agent-1=%d, agent-2=%d, agent-3=%d, total=%d (desired=20)",
		a1After, a2After, a3After, total)

	if total != 20 {
		t.Errorf("Under-scheduling bug! Got %d total tasks, expected 20 "+
			"(agent-3 crashed and rejoined but lost %d tasks)", total, a3Before)
	}
}

package leader

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// TestLeaderRestart_RejectUnknownHeartbeat verifies:
// Heartbeat REJECTS unknown agents (returns known=false).
// Unknown agents must re-register via RegisterAgent with placed counts.
// After settle, reconcileJobs has correct placed → no over-dispatch.
func TestLeaderRestart_RejectUnknownHeartbeat(t *testing.T) {
	agents := make([]*mockAgent, 4)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	// === PHASE 1: Dispatch 20 tasks ===
	store := NewMockJobStore()
	leader1 := New("agent-0", store, nil)

	ctx1, cancel1 := context.WithCancel(context.Background())
	go leader1.stateLoop(ctx1)

	for i, agent := range agents {
		leader1.RegisterAgent(fmt.Sprintf("agent-%d", i), agent.URL(), "", nil)
	}
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{
		Name:    "my-job",
		Command: "./my-app",
		Count:   20,
	}
	if err := leader1.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	agentTaskCounts := make([]int, 4)
	for i, agent := range agents {
		agentTaskCounts[i] = agent.TaskCount()
	}
	t.Logf("Phase 1: dispatched %v", agentTaskCounts)

	// === PHASE 2: Leader crashes ===
	cancel1()
	time.Sleep(10 * time.Millisecond)

	// === PHASE 3: New leader with settle ===
	leader2 := New("agent-0", store, nil)
	leader2.SetSettleDelay(300 * time.Millisecond)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go leader2.stateLoop(ctx2)

	// Leader registers itself
	leader2.RegisterAgent("agent-0", agents[0].URL(), "", map[string]int{job.Name: agentTaskCounts[0]})

	// Other agents heartbeat → should be REJECTED (unknown)
	for i := 1; i < 4; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		known := leader2.Heartbeat(agentID, "", 0)
		if known {
			t.Errorf("Agent %s heartbeat should be rejected (unknown), but was accepted", agentID)
		}
	}

	// Unknown agents should NOT be in agents map
	knownAgents := leader2.GetAgents()
	if len(knownAgents) != 1 {
		t.Errorf("Only agent-0 should be known, got %d agents", len(knownAgents))
	}

	// Agents get 404, re-register with placed counts
	for i := 1; i < 4; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		placed := map[string]int{job.Name: agentTaskCounts[i]}
		leader2.RegisterAgent(agentID, agents[i].URL(), "", placed)
	}

	// Wait for settle → reconcileJobs with correct placed data
	time.Sleep(500 * time.Millisecond)

	// === VERIFY: no over-scheduling ===
	finalTotal := 0
	for i, agent := range agents {
		count := agent.TaskCount()
		t.Logf("Agent %d: was=%d, now=%d", i, agentTaskCounts[i], count)
		finalTotal += count
	}

	if finalTotal != 20 {
		t.Errorf("Total should be 20, got %d", finalTotal)
	}
}

// TestAgentRestart_WithinTimeout_PlacedStale reproduces the bug:
// Agent restarts (clean state, 0 tasks) but comes back before leader times it out.
// Agent sends heartbeat → accepted (still known) → placed map stays stale.
// Leader thinks tasks still exist → doesn't re-dispatch → tasks lost.
//
// Fix: Agent should always RegisterAgent on startup (not just heartbeat).
// RegisterAgent clears+updates placed counts → reconcileJobs dispatches missing.
func TestAgentRestart_WithinTimeout_PlacedStale(t *testing.T) {
	agents := make([]*mockAgent, 4)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	store := NewMockJobStore()
	l := New("leader", store, nil)
	l.agentTimeout = 30 * time.Second // Long timeout — agent won't be detected as dead

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)

	// Register all agents and dispatch 20 tasks
	for i, agent := range agents {
		l.RegisterAgent(fmt.Sprintf("agent-%d", i), agent.URL(), "", nil)
	}
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{
		Name:    "my-job",
		Command: "./my-app",
		Count:   20,
	}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Verify 20 tasks
	initialCounts := make([]int, 4)
	totalBefore := 0
	for i, agent := range agents {
		initialCounts[i] = agent.TaskCount()
		totalBefore += initialCounts[i]
	}
	if totalBefore != 20 {
		t.Fatalf("Expected 20 tasks initially, got %d", totalBefore)
	}
	t.Logf("Initial: %v (total=%d)", initialCounts, totalBefore)

	// === Agent-1 restarts (process dies, tasks die, comes back with clean state) ===
	// Simulate: clear all tasks on agent-1
	agents[1].ClearTasks()
	lostTasks := initialCounts[1]
	t.Logf("Agent-1 restarted: lost %d tasks, now has %d", lostTasks, agents[1].TaskCount())

	// FIX: Agent always registers on startup (not just heartbeat).
	// RegisterAgent clears old placed data and sets current counts (0 after restart).
	// This lets reconcileJobs detect missing tasks and re-dispatch.
	l.RegisterAgent("agent-1", agents[1].URL(), "", map[string]int{}) // empty = 0 tasks running

	// Give reconcile time to run (it runs on dead agent check interval)
	time.Sleep(200 * time.Millisecond)

	// Count total tasks — should be 20 if leader detected the lost tasks
	finalTotal := 0
	for i, agent := range agents {
		count := agent.TaskCount()
		t.Logf("Agent %d: initial=%d, final=%d", i, initialCounts[i], count)
		finalTotal += count
	}

	// BUG: Leader's placed map is stale → thinks 20 tasks exist → dispatches 0
	// With fix (agent registers on startup): placed updated → reconcile dispatches missing
	if finalTotal != 20 {
		t.Errorf("Expected 20 tasks after agent restart, got %d (lost %d tasks)", finalTotal, 20-finalTotal)
	}
}

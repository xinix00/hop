package leader

import (
	"context"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

func TestDispatch406SkipsToNextAgent(t *testing.T) {
	// When agent A rejects with 406 (affinity mismatch), leader should
	// try agent B and succeed there.
	agentA := newMockAgent()
	defer agentA.Close()
	agentA.SetRejectAffinity(true) // A rejects all

	agentB := newMockAgent()
	defer agentB.Close()
	// B accepts (default)

	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("agent-a", agentA.URL(), "", nil)
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)
	time.Sleep(50 * time.Millisecond)

	job := &types.Job{
		Name:     "web",
		Command:  "./web",
		Count:    1,
		Affinity: map[string]string{"node.os": "linux"},
	}

	err := leader.DispatchJob(job)
	time.Sleep(100 * time.Millisecond)

	if err != nil {
		t.Fatalf("DispatchJob failed: %v", err)
	}

	// Agent B should have received the job (A rejected with 406)
	if agentB.RunCallCount() != 1 {
		t.Errorf("Agent B run calls = %d, want 1 (should receive job after A rejects)", agentB.RunCallCount())
	}
}

func TestDispatchAllAgentsReject406(t *testing.T) {
	// When ALL agents reject with 406, dispatch should fail with error.
	agentA := newMockAgent()
	defer agentA.Close()
	agentA.SetRejectAffinity(true)

	agentB := newMockAgent()
	defer agentB.Close()
	agentB.SetRejectAffinity(true)

	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("agent-a", agentA.URL(), "", nil)
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)
	time.Sleep(50 * time.Millisecond)

	job := &types.Job{
		Name:     "gpu-job",
		Command:  "./train",
		Count:    1,
		Affinity: map[string]string{"gpu": "true"},
	}

	err := leader.DispatchJob(job)

	if err == nil {
		t.Fatal("Expected dispatch error when all agents reject with 406")
	}

	// Job should still be stored (reconciliation will retry later when matching agent joins)
	stored := store.GetJobs()
	found := false
	for _, j := range stored {
		if j.Name == "gpu-job" {
			found = true
		}
	}
	if !found {
		t.Error("Job should still be stored even after dispatch failure")
	}
}

func TestDaemonWithAffinityMixedAgents(t *testing.T) {
	// Daemon job (count=-1) with affinity: some agents accept, some reject.
	// Leader dispatches to all, only accepting agents get placement.
	darwinAgent := newMockAgent()
	defer darwinAgent.Close()
	// darwin agent accepts (simulates matching node.os=darwin)

	linuxAgent := newMockAgent()
	defer linuxAgent.Close()
	linuxAgent.SetRejectAffinity(true) // linux agent rejects (affinity mismatch)

	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("darwin-node", darwinAgent.URL(), "", nil)
	leader.RegisterAgent("linux-node", linuxAgent.URL(), "", nil)
	time.Sleep(50 * time.Millisecond)

	daemon := &types.Job{
		Name:     "mac-monitor",
		Command:  "./monitor",
		Count:    -1, // all agents
		Affinity: map[string]string{"node.os": "darwin"},
	}

	err := leader.DispatchJob(daemon)
	time.Sleep(100 * time.Millisecond)

	if err != nil {
		t.Fatalf("DispatchJob failed: %v (should succeed if at least one agent accepts)", err)
	}

	// Darwin agent should have received the job
	if darwinAgent.RunCallCount() != 1 {
		t.Errorf("Darwin agent run calls = %d, want 1", darwinAgent.RunCallCount())
	}

	// Linux agent was tried but rejected — that's fine for daemon reconciliation
	// The leader still dispatched to it (agent-side rejection)
}

func TestDaemonAllAgentsReject406(t *testing.T) {
	// Daemon job where ALL agents reject — should return error
	agentA := newMockAgent()
	defer agentA.Close()
	agentA.SetRejectAffinity(true)

	agentB := newMockAgent()
	defer agentB.Close()
	agentB.SetRejectAffinity(true)

	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("agent-a", agentA.URL(), "", nil)
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)
	time.Sleep(50 * time.Millisecond)

	daemon := &types.Job{
		Name:     "special-daemon",
		Command:  "./special",
		Count:    -1,
		Affinity: map[string]string{"node.os": "windows"},
	}

	err := leader.DispatchJob(daemon)

	if err == nil {
		t.Fatal("Expected error when all agents reject daemon job")
	}

	// Job should still be stored for future reconciliation
	stored := store.GetJobs()
	found := false
	for _, j := range stored {
		if j.Name == "special-daemon" {
			found = true
		}
	}
	if !found {
		t.Error("Daemon job should still be stored even after all agents reject")
	}
}

func TestNewAgentJoinsDaemonWithAffinity(t *testing.T) {
	// Daemon job exists. New agent joins that matches affinity → gets the job.
	// Existing agent doesn't match → doesn't get it.
	existingAgent := newMockAgent()
	defer existingAgent.Close()
	existingAgent.SetRejectAffinity(true) // doesn't match

	store := NewMockJobStore()
	daemon := &types.Job{
		Name:     "darwin-only",
		Command:  "./darwin-tool",
		Count:    -1,
		Affinity: map[string]string{"node.os": "darwin"},
	}
	store.StoreJob(daemon)

	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Existing agent registers — daemon dispatched but rejected
	leader.RegisterAgent("linux-node", existingAgent.URL(), "", nil)
	time.Sleep(100 * time.Millisecond)

	// New agent joins that accepts (simulates matching darwin)
	newAgent := newMockAgent()
	defer newAgent.Close()
	// newAgent accepts (default = no affinity rejection)

	leader.RegisterAgent("darwin-node", newAgent.URL(), "", nil)
	time.Sleep(100 * time.Millisecond)

	// New agent should receive the daemon (reconciliation on registration)
	if newAgent.RunCallCount() < 1 {
		t.Errorf("New darwin agent run calls = %d, want >= 1 (should get daemon on join)", newAgent.RunCallCount())
	}
}

func TestCountedJobWithAffinityMixedAgents(t *testing.T) {
	// Regular job (count=2) with affinity. 3 agents, only 2 match.
	// Leader should successfully place 2 instances (on the accepting agents).
	matchA := newMockAgent()
	defer matchA.Close()

	matchB := newMockAgent()
	defer matchB.Close()

	noMatch := newMockAgent()
	defer noMatch.Close()
	noMatch.SetRejectAffinity(true)

	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("match-a", matchA.URL(), "", nil)
	leader.RegisterAgent("match-b", matchB.URL(), "", nil)
	leader.RegisterAgent("no-match", noMatch.URL(), "", nil)
	time.Sleep(50 * time.Millisecond)

	job := &types.Job{
		Name:     "api",
		Command:  "./api",
		Count:    2,
		Affinity: map[string]string{"node.arch": "arm64"},
	}

	err := leader.DispatchJob(job)
	time.Sleep(100 * time.Millisecond)

	if err != nil {
		t.Fatalf("DispatchJob failed: %v", err)
	}

	totalRuns := matchA.RunCallCount() + matchB.RunCallCount()
	if totalRuns != 2 {
		t.Errorf("Total runs on matching agents = %d, want 2", totalRuns)
	}
}

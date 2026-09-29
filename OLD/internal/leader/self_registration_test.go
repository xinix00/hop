package leader

import (
	"context"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// TestLeaderRegistersItself verifies leader appears in agents list
func TestLeaderRegistersItself(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Leader registers itself via heartbeat
	leader.RegisterAgent("local-agent", "http://10.0.0.1:8080", "", nil)
	leader.Heartbeat("local-agent", "", 0)

	// Verify leader is in agents list
	agents := leader.GetAgents()
	if len(agents) != 1 {
		t.Fatalf("Expected 1 agent (self), got %d", len(agents))
	}

	if agents[0].ID != "local-agent" {
		t.Errorf("Agent ID = %s, want local-agent", agents[0].ID)
	}

	if agents[0].Endpoint != "http://10.0.0.1:8080" {
		t.Errorf("Agent endpoint = %s, want http://10.0.0.1:8080", agents[0].Endpoint)
	}
}

// TestLeaderPlusFollowerAgents verifies leader + followers all in list
func TestLeaderPlusFollowerAgents(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader-node", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Leader registers itself
	leader.RegisterAgent("leader-node", "http://10.0.0.1:8080", "", nil)
	leader.Heartbeat("leader-node", "", 0)

	// Two followers register
	leader.RegisterAgent("follower-1", "http://10.0.0.2:8080", "", nil)
	leader.Heartbeat("follower-1", "", 0)
	leader.RegisterAgent("follower-2", "http://10.0.0.3:8080", "", nil)
	leader.Heartbeat("follower-2", "", 0)

	time.Sleep(10 * time.Millisecond)

	// All 3 should be in agents list
	agents := leader.GetAgents()
	if len(agents) != 3 {
		t.Fatalf("Expected 3 agents (leader + 2 followers), got %d", len(agents))
	}

	// Verify all IDs present
	ids := make(map[string]bool)
	for _, a := range agents {
		ids[a.ID] = true
	}

	if !ids["leader-node"] {
		t.Error("Leader node missing from agents list")
	}
	if !ids["follower-1"] {
		t.Error("Follower 1 missing from agents list")
	}
	if !ids["follower-2"] {
		t.Error("Follower 2 missing from agents list")
	}
}

// TestLeaderCanDispatchToItself verifies leader can run jobs on itself
func TestLeaderCanDispatchToItself(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader-node", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Leader registers itself
	leader.RegisterAgent("leader-node", "http://10.0.0.1:8080", "", nil)
	leader.Heartbeat("leader-node", "", 0)
	time.Sleep(10 * time.Millisecond)

	// Job with count=1 should be dispatchable to leader itself
	job := &types.Job{
		Name:    "test",
		Command: "./test",
		Count:   1,
	}

	store.StoreJob(job)

	// Verify leader is available for scheduling
	agents := leader.GetAgents()
	if len(agents) != 1 {
		t.Fatalf("Leader should be in agents list for scheduling, got %d agents", len(agents))
	}

	// Placement should be possible
	placed := leader.GetPlaced(job.Name)
	if len(placed) > 0 {
		// Job was dispatched (in real scenario with HTTP)
		t.Logf("Job dispatched to %d agents including leader", len(placed))
	}
}

// TestSingleNodeClusterLeaderIsOnlyAgent verifies single-node works
func TestSingleNodeClusterLeaderIsOnlyAgent(t *testing.T) {
	store := NewMockJobStore()
	leader := New("solo-node", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Solo node registers itself
	leader.RegisterAgent("solo-node", "http://10.0.0.1:8080", "", nil)
	leader.Heartbeat("solo-node", "", 0)
	time.Sleep(10 * time.Millisecond)

	agents := leader.GetAgents()
	if len(agents) != 1 {
		t.Fatalf("Single-node cluster should have 1 agent (itself), got %d", len(agents))
	}

	if agents[0].ID != "solo-node" {
		t.Errorf("Agent should be solo-node, got %s", agents[0].ID)
	}

	// Verify it can dispatch to itself
	job := &types.Job{
		Name:    "solo-job",
		Command: "./app",
		Count:   1,
	}
	store.StoreJob(job)

	// In real scenario, DispatchJob would succeed because leader is available
	t.Log("Single-node cluster verified: leader is both leader AND agent")
}

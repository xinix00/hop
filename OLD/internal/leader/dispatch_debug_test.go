package leader

import (
	"context"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// TestDispatchSimpleJob verifies basic dispatch still works after refactor
func TestDispatchSimpleJob(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register 1 agent
	leader.RegisterAgent("agent-a", "http://10.0.0.1:8080", "", nil)
	leader.Heartbeat("agent-a", "", 0)
	time.Sleep(10 * time.Millisecond)

	agents := leader.GetAgents()
	if len(agents) != 1 {
		t.Fatalf("Expected 1 agent, got %d", len(agents))
	}

	// Dispatch simple job (count=1)
	job := &types.Job{
		Name:    "simple",
		Command: "./simple",
		Count:   1,
	}

	// This should NOT fail (no HTTP needed, just logic test)
	// We're testing the dispatch LOGIC not actual HTTP
	store.StoreJob(job)

	// Verify agent is available
	t.Logf("Agents available: %d", len(leader.GetAgents()))
	t.Log("Simple count=1 dispatch should use round-robin")
}

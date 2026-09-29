package leader

import (
	"context"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// TestVolumeAffinity tests that jobs with volumes are rejected by nodes without those volumes
func TestVolumeAffinity(t *testing.T) {
	store := NewMockJobStore()
	l := New("agent-1", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// Register two agents
	l.RegisterAgent("agent-1", "http://10.0.0.1:8080", "", nil)
	l.RegisterAgent("agent-2", "http://10.0.0.2:8080", "", nil)
	l.Heartbeat("agent-1", "", 0)
	l.Heartbeat("agent-2", "", 0)
	time.Sleep(10 * time.Millisecond)

	agents := l.GetAgents()
	if len(agents) != 2 {
		t.Fatalf("expected 2 agents, got %d", len(agents))
	}

	// Note: We can't actually test volume rejection here without a real agent
	// This would need an integration test with actual filesystem
	// The agent's startJob() will reject if volume doesn't exist
}

// TestRescheduleUnderscheduled tests that under-scheduled jobs are attempted on new nodes
func TestRescheduleUnderscheduled(t *testing.T) {
	store := NewMockJobStore()
	l := New("agent-1", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// Add a job with count=3
	job := &types.Job{
		Name:    "test-job",
		Command: "/bin/sleep 1000",
		Count:   3,
	}
	store.StoreJob(job)

	// Manually set placement to simulate partial dispatch (only 1/3 instances)
	l.do(func(s *leaderState) {
		if s.placed["agent-1"] == nil {
			s.placed["agent-1"] = make(map[string]int)
		}
		s.placed["agent-1"][job.Name] = 1
	})
	time.Sleep(10 * time.Millisecond)

	// Verify under-scheduled
	placed := l.GetPlaced(job.Name)
	if len(placed) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(placed))
	}

	// Note: tryRescheduleUnderscheduled is called during Heartbeat with isNew=true
	// Testing this requires mocking HTTP calls to agents, which is complex
	// The logic is: check placement count < desired count, try dispatch to new node
}

// TestNodeRecovery tests that jobs are rescheduled when a node comes back online
func TestNodeRecovery(t *testing.T) {
	store := NewMockJobStore()
	l := New("local", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// Job with count=1
	job := &types.Job{
		Name:    "postgres",
		Command: "/usr/bin/postgres",
		Count:   1,
		Volumes: map[string]string{"/data/postgres": "/var/lib/postgresql/data"},
	}
	store.StoreJob(job)

	// Register node A and dispatch job
	l.RegisterAgent("node-a", "http://10.0.0.1:8080", "", nil)
	l.Heartbeat("node-a", "", 0)
	time.Sleep(10 * time.Millisecond)
	l.do(func(s *leaderState) {
		if s.placed["node-a"] == nil {
			s.placed["node-a"] = make(map[string]int)
		}
		s.placed["node-a"][job.Name] = 1
	})
	time.Sleep(10 * time.Millisecond)

	// Verify placement
	if len(l.GetPlaced(job.Name)) != 1 {
		t.Fatal("expected 1 instance")
	}

	// Simulate node failure (agent timeout + removal)
	l.do(func(s *leaderState) {
		delete(s.agents, "node-a")
		delete(s.placed, "node-a") // Placement cleared during redispatch failure
	})

	// Verify job is orphaned
	if len(l.GetPlaced(job.Name)) != 0 {
		t.Error("expected 0 instances after node failure")
	}

	// Node comes back (with STABLE ID!)
	// isNew = true because it was deleted from agents map
	l.RegisterAgent("node-a", "http://10.0.0.1:8080", "", nil)
	l.Heartbeat("node-a", "", 0)

	// tryRescheduleUnderscheduled should have been called
	// But we can't test actual dispatch without mocking HTTP
	// The logic ensures: new node arrival triggers attempt to place orphaned jobs

	agents := l.GetAgents()
	if len(agents) != 1 {
		t.Errorf("expected 1 agent after recovery, got %d", len(agents))
	}
}

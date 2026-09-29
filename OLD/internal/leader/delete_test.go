package leader

import (
	"context"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// TestDeleteJobRemovesFromStore verifies job is removed from store, not just placement
func TestDeleteJobRemovesFromStore(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Create job (Name is the unique key)
	job := &types.Job{
		Name:    "to-delete",
		Command: "./app",
		Count:   1,
	}
	store.StoreJob(job)

	// Verify it exists
	jobs := store.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("Expected 1 job before delete, got %d", len(jobs))
	}

	// Delete job by name
	leader.DeleteJobByName("to-delete")
	time.Sleep(10 * time.Millisecond)

	// Verify removed from store
	jobs = store.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("Expected 0 jobs after delete, got %d", len(jobs))
	}

	// Verify can't find by name
	found := store.GetJob("to-delete")
	if found != nil {
		t.Error("Job should not be findable after delete")
	}
}

// TestDeleteJobWithNoPlacementStillRemovesFromStore verifies job deleted even with no agents
func TestDeleteJobWithNoPlacementStillRemovesFromStore(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Create job (no placement - never dispatched)
	job := &types.Job{
		Name:    "orphan",
		Command: "./orphan",
		Count:   1,
	}
	store.StoreJob(job)

	// Delete (no agents to cleanup, just remove from store)
	leader.DeleteJobByName("orphan")
	time.Sleep(10 * time.Millisecond)

	// Should be gone from store
	jobs := store.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("Orphan job should be deleted from store, got %d jobs", len(jobs))
	}
}

// TestDeleteJobTwiceDoesNotError verifies deleting same job twice is safe
func TestDeleteJobTwiceDoesNotError(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	job := &types.Job{
		Name:    "test",
		Command: "./test",
		Count:   1,
	}
	store.StoreJob(job)

	// Delete once
	leader.DeleteJobByName("test")
	time.Sleep(5 * time.Millisecond)

	// Delete again (should be safe, just log "not found")
	leader.DeleteJobByName("test")
	time.Sleep(5 * time.Millisecond)

	// Should still be empty
	jobs := store.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("Expected 0 jobs after double delete, got %d", len(jobs))
	}
}

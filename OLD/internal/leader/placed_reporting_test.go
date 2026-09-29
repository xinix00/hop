package leader

import (
	"context"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// TestGetPlacedCounts_PreExistingJobs verifies placed counts work for jobs
// loaded from disk (already in store when leader starts).
// Agents register with placed counts keyed by job name.
func TestGetPlacedCounts_PreExistingJobs(t *testing.T) {
	store := NewMockJobStore()

	// Pre-existing jobs in store (leader loaded from disk)
	jobs := []*types.Job{
		{Name: "webapp", Command: "./webapp", Count: 3},
		{Name: "api", Command: "./api", Count: 2},
		{Name: "worker", Command: "./worker", Count: 1},
	}
	for _, j := range jobs {
		store.StoreJob(j)
	}
	store.stateTime = time.Now() // leader has current state

	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// Agents register with placed counts (ground truth from their running tasks)
	l.RegisterAgent("agent-1", "http://10.0.0.1:8080", "", map[string]int{
		"webapp": 2,
		"api":    1,
		"worker": 1,
	})
	l.RegisterAgent("agent-2", "http://10.0.0.2:8080", "", map[string]int{
		"webapp": 1,
		"api":    1,
	})
	time.Sleep(20 * time.Millisecond)

	// Heartbeat with same stateTime (NOT newer → no sync)
	l.Heartbeat("agent-1", "", 0)
	l.Heartbeat("agent-2", "", 0)
	time.Sleep(20 * time.Millisecond)

	placed := l.GetPlacedCounts()

	if placed["webapp"] != 3 {
		t.Errorf("webapp placed = %d, want 3", placed["webapp"])
	}
	if placed["api"] != 2 {
		t.Errorf("api placed = %d, want 2", placed["api"])
	}
	if placed["worker"] != 1 {
		t.Errorf("worker placed = %d, want 1", placed["worker"])
	}

	// Also verify total
	total := 0
	for _, count := range placed {
		total += count
	}
	if total != 6 {
		t.Errorf("total placed = %d, want 6", total)
	}
}

// TestGetPlacedCounts_AfterDispatch verifies placed counts work for newly dispatched jobs.
func TestGetPlacedCounts_AfterDispatch(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{
		Name:    "myapp",
		Command: "./myapp",
		Count:   3,
	}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	placed := l.GetPlacedCounts()
	if placed["myapp"] != 3 {
		t.Errorf("myapp placed = %d, want 3", placed["myapp"])
	}
}

// TestGetPlacedCounts_MixedPreExistingAndNew verifies placed counts
// work correctly when some jobs are pre-existing and some are newly dispatched.
func TestGetPlacedCounts_MixedPreExistingAndNew(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()

	// Pre-existing job
	existingJob := &types.Job{Name: "existing", Command: "./existing", Count: 2}
	store.StoreJob(existingJob)
	store.stateTime = time.Now()

	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// Agent registers with placed count for existing job
	l.RegisterAgent("agent-1", agent.URL(), "", map[string]int{"existing": 2})
	time.Sleep(20 * time.Millisecond)

	// Dispatch a NEW job
	newJob := &types.Job{
		Name:    "new-service",
		Command: "./new",
		Count:   1,
	}
	if err := l.DispatchJob(newJob); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	placed := l.GetPlacedCounts()
	if placed["existing"] != 2 {
		t.Errorf("existing placed = %d, want 2", placed["existing"])
	}
	if placed["new-service"] != 1 {
		t.Errorf("new-service placed = %d, want 1", placed["new-service"])
	}
}

// TestGetPlacedCounts_DaemonJob verifies placed counts for count=-1 (daemon) jobs.
func TestGetPlacedCounts_DaemonJob(t *testing.T) {
	store := NewMockJobStore()

	daemonJob := &types.Job{Name: "hopdns", Command: "./hopdns", Count: -1}
	store.StoreJob(daemonJob)
	store.stateTime = time.Now()

	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// 3 agents each running the daemon
	l.RegisterAgent("agent-1", "http://10.0.0.1:8080", "", map[string]int{"hopdns": 1})
	l.RegisterAgent("agent-2", "http://10.0.0.2:8080", "", map[string]int{"hopdns": 1})
	l.RegisterAgent("agent-3", "http://10.0.0.3:8080", "", map[string]int{"hopdns": 1})
	time.Sleep(20 * time.Millisecond)

	placed := l.GetPlacedCounts()
	if placed["hopdns"] != 3 {
		t.Errorf("hopdns placed = %d, want 3", placed["hopdns"])
	}
}

// TestGetPlacedCounts_UpdatedJob verifies that after a rolling update
// (same job name, new command), placed counts still work correctly.
func TestGetPlacedCounts_UpdatedJob(t *testing.T) {
	agent1 := newMockAgent()
	agent2 := newMockAgent()
	defer agent1.Close()
	defer agent2.Close()

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// Register agents and dispatch v1
	l.RegisterAgent("agent-1", agent1.URL(), "", nil)
	l.RegisterAgent("agent-2", agent2.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	jobV1 := &types.Job{Name: "myapp", Command: "./myapp-v1", Count: 2}
	if err := l.DispatchJob(jobV1); err != nil {
		t.Fatalf("DispatchJob v1 failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	placed := l.GetPlacedCounts()
	if placed["myapp"] != 2 {
		t.Errorf("before update: myapp placed = %d, want 2", placed["myapp"])
	}

	// Rolling update: v1 → v2 (same name, new command)
	jobV2 := &types.Job{Name: "myapp", Command: "./myapp-v2", Count: 2}
	if err := l.UpdateJob(jobV2); err != nil {
		t.Fatalf("UpdateJob v2 failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	placed = l.GetPlacedCounts()
	if placed["myapp"] != 2 {
		t.Errorf("after update: myapp placed = %d, want 2", placed["myapp"])
	}
}

// TestGetPlacedCounts_LeaderWithSettlePeriod verifies placed counts
// work during and after the settle period.
func TestGetPlacedCounts_LeaderWithSettlePeriod(t *testing.T) {
	store := NewMockJobStore()

	jobs := []*types.Job{
		{Name: "job-a", Command: "./a", Count: 5},
		{Name: "job-b", Command: "./b", Count: 3},
	}
	for _, j := range jobs {
		store.StoreJob(j)
	}
	store.stateTime = time.Now()

	l := New("leader", store, nil)
	l.SetSettleDelay(200 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// Agents register during settle period
	l.RegisterAgent("agent-1", "http://10.0.0.1:8080", "", map[string]int{
		"job-a": 3,
		"job-b": 2,
	})
	l.RegisterAgent("agent-2", "http://10.0.0.2:8080", "", map[string]int{
		"job-a": 2,
		"job-b": 1,
	})
	time.Sleep(20 * time.Millisecond)

	// Even DURING settle, placed counts should be accurate
	placed := l.GetPlacedCounts()
	if placed["job-a"] != 5 {
		t.Errorf("during settle: job-a placed = %d, want 5", placed["job-a"])
	}
	if placed["job-b"] != 3 {
		t.Errorf("during settle: job-b placed = %d, want 3", placed["job-b"])
	}

	// After settle, counts should still be correct
	time.Sleep(300 * time.Millisecond)

	placed = l.GetPlacedCounts()
	if placed["job-a"] != 5 {
		t.Errorf("after settle: job-a placed = %d, want 5", placed["job-a"])
	}
	if placed["job-b"] != 3 {
		t.Errorf("after settle: job-b placed = %d, want 3", placed["job-b"])
	}
}

// TestJobStore_SeededAtInit verifies that jobs loaded from the store at startup
// are immediately available via store.GetJob(name).
func TestJobStore_SeededAtInit(t *testing.T) {
	store := NewMockJobStore()

	jobs := []*types.Job{
		{Name: "svc-a", Command: "./a", Count: 1},
		{Name: "svc-b", Command: "./b", Count: 1},
		{Name: "svc-c", Command: "./c", Count: 1},
	}
	for _, j := range jobs {
		store.StoreJob(j)
	}

	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Jobs should be accessible via the store by name
	for _, j := range jobs {
		found := store.GetJob(j.Name)
		if found == nil {
			t.Errorf("store.GetJob(%q) returned nil", j.Name)
		} else if found.Name != j.Name {
			t.Errorf("store.GetJob(%q).Name = %q, want %q", j.Name, found.Name, j.Name)
		}
	}

	_ = l // leader started successfully
}

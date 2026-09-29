package leader

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// ============== PRIORITY & PREEMPTION TESTS ==============

func prio(n int) *int { return &n }

// TestEffectivePriority verifies the priority ordering semantics.
// nil = not set = sorts last (math.MaxInt). 0 = top (most important).
func TestEffectivePriority(t *testing.T) {
	cases := []struct {
		p    *int
		want int
	}{
		{prio(0), 0},
		{prio(1), 1},
		{prio(5), 5},
		{prio(100), 100},
		{nil, math.MaxInt},
	}
	for _, c := range cases {
		if got := effectivePriority(c.p); got != c.want {
			t.Errorf("effectivePriority(%v) = %d, want %d", c.p, got, c.want)
		}
	}
}

// TestPreemptionHighPriorityEvictsLow verifies that a high-priority job preempts
// a low-priority job when an agent is at capacity.
func TestPreemptionHighPriorityEvictsLow(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()
	agent.SetMaxCapacity(1)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	lowPrio := &types.Job{Name: "batch", Command: "./batch", Count: 1, Priority: prio(10)}
	if err := l.DispatchJob(lowPrio); err != nil {
		t.Fatalf("Dispatch low-prio job failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	if agent.TaskCount() != 1 {
		t.Fatalf("Expected 1 task on agent, got %d", agent.TaskCount())
	}

	highPrio := &types.Job{Name: "critical", Command: "./critical", Count: 1, Priority: prio(1)}
	if err := l.DispatchJob(highPrio); err != nil {
		t.Fatalf("Dispatch high-prio job failed (preemption should have succeeded): %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agent.TaskCount() != 1 {
		t.Errorf("Expected 1 task after preemption, got %d", agent.TaskCount())
	}
	jobs := agent.GetJobs()
	found := false
	for _, j := range jobs {
		if j.Name == "critical" {
			found = true
		}
	}
	if !found {
		t.Error("High-priority job 'critical' should be running after preemption")
	}
}

// TestPreemptionLowPriorityCannotEvictHigh verifies that a low-priority job
// does NOT evict a higher-priority job when an agent is at capacity.
func TestPreemptionLowPriorityCannotEvictHigh(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()
	agent.SetMaxCapacity(1)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	highPrio := &types.Job{Name: "critical", Command: "./critical", Count: 1, Priority: prio(1)}
	if err := l.DispatchJob(highPrio); err != nil {
		t.Fatalf("Dispatch high-prio job failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	lowPrio := &types.Job{Name: "batch", Command: "./batch", Count: 1, Priority: prio(10)}
	if err := l.DispatchJob(lowPrio); err == nil {
		t.Error("Low-priority job should not preempt a higher-priority job")
	}

	for _, j := range agent.GetJobs() {
		if j.Name == "batch" {
			t.Error("Low-priority job 'batch' should not have been placed via preemption")
		}
	}
}

// TestPreemptionPrio0IsUnevictable verifies that a job with priority=0
// (most important) cannot be evicted by any other job.
func TestPreemptionPrio0IsUnevictable(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()
	agent.SetMaxCapacity(1)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	first := &types.Job{Name: "first", Command: "./first", Count: 1, Priority: prio(0)}
	if err := l.DispatchJob(first); err != nil {
		t.Fatalf("Dispatch prio-0 job failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	later := &types.Job{Name: "later", Command: "./later", Count: 1, Priority: prio(5)}
	if err := l.DispatchJob(later); err == nil {
		t.Error("Later job (prio=5) should not preempt prio=0 job")
	}
	for _, j := range agent.GetJobs() {
		if j.Name == "later" {
			t.Error("'later' (prio=5) should not have displaced 'first' (prio=0)")
		}
	}
}

// TestPreemptionNilPriorityIsLowest verifies that a job with nil priority
// (not set) sorts last and can be evicted by any job with explicit priority.
func TestPreemptionNilPriorityIsLowest(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()
	agent.SetMaxCapacity(1)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	// Job with nil priority = lowest (auto/unset).
	bg := &types.Job{Name: "background", Command: "./bg", Count: 1} // Priority: nil
	if err := l.DispatchJob(bg); err != nil {
		t.Fatalf("Dispatch nil-prio job failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	// Any explicit priority (even the highest number) should preempt nil.
	fg := &types.Job{Name: "foreground", Command: "./fg", Count: 1, Priority: prio(99)}
	if err := l.DispatchJob(fg); err != nil {
		t.Fatalf("Explicit-prio job should preempt nil-prio job: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	found := false
	for _, j := range agent.GetJobs() {
		if j.Name == "foreground" {
			found = true
		}
	}
	if !found {
		t.Error("'foreground' should be running after preempting nil-priority job")
	}
}

// TestPreemptionAffinityAgentNeverEvicts verifies that an agent that returned 406
// (affinity mismatch) is never a candidate for preemption.
func TestPreemptionAffinityAgentNeverEvicts(t *testing.T) {
	agentA := newMockAgent()
	defer agentA.Close()
	agentA.SetRejectAffinity(true)

	agentB := newMockAgent()
	defer agentB.Close()
	agentB.SetFailRuns(true)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-a", agentA.URL(), "", nil)
	l.RegisterAgent("agent-b", agentB.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{Name: "critical", Command: "./critical", Count: 1, Priority: prio(1)}
	if err := l.DispatchJob(job); err == nil {
		t.Error("Dispatch should fail: agent-a rejects via affinity, agent-b always 503 with no placed jobs")
	}
}

// TestPreemptionChoosesLowestPriority verifies that among multiple placed jobs,
// the one with the highest priority number (least important) is evicted first.
func TestPreemptionChoosesLowestPriority(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()
	agent.SetMaxCapacity(2)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	medPrio := &types.Job{Name: "medium", Command: "./med", Count: 1, Priority: prio(5)}
	lowPrio := &types.Job{Name: "batch", Command: "./batch", Count: 1, Priority: prio(20)}
	if err := l.DispatchJob(medPrio); err != nil {
		t.Fatalf("Dispatch medium-prio job failed: %v", err)
	}
	if err := l.DispatchJob(lowPrio); err != nil {
		t.Fatalf("Dispatch low-prio job failed: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	if agent.TaskCount() != 2 {
		t.Fatalf("Expected 2 tasks on agent, got %d", agent.TaskCount())
	}

	highPrio := &types.Job{Name: "critical", Command: "./critical", Count: 1, Priority: prio(1)}
	if err := l.DispatchJob(highPrio); err != nil {
		t.Fatalf("High-priority dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agent.TasksForJob("batch") > 0 {
		t.Error("'batch' (prio=20) should have been evicted, not 'medium' (prio=5)")
	}
	if agent.TasksForJob("critical") == 0 {
		t.Error("High-priority job 'critical' should be running")
	}
}

// TestPatchPriorityTriggersPreemption verifies that patching a job's priority
// triggers reconciliation and preempts lower-priority jobs to free capacity.
func TestPatchPriorityTriggersPreemption(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()
	agent.SetMaxCapacity(1)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	// counter dispatched first, fills the only slot (prio=10, low importance)
	counter := &types.Job{Name: "counter", Command: "./counter", Count: 1, Priority: prio(10)}
	if err := l.DispatchJob(counter); err != nil {
		t.Fatalf("Dispatch counter failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	if agent.TaskCount() != 1 {
		t.Fatalf("Expected 1 task, got %d", agent.TaskCount())
	}

	// counter2 dispatched second — no capacity, stays pending (prio=20, even lower)
	counter2 := &types.Job{Name: "counter2", Command: "./counter2", Count: 1, Priority: prio(20)}
	_ = l.DispatchJob(counter2) // expected to fail (no capacity)
	time.Sleep(20 * time.Millisecond)

	// Patch counter2 to priority 0 (most important) — should trigger reconcile+preempt
	if err := l.PatchJobPriority(counter2.Name, 0); err != nil {
		t.Fatalf("PatchJobPriority failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// counter2 should now be running, counter tasks should have been evicted
	if agent.TasksForJob("counter2") == 0 {
		t.Error("'counter2' should be running after priority patch triggered preemption")
	}
	if agent.TasksForJob("counter") > 0 {
		t.Error("'counter' (prio=10) should have been evicted after counter2 was patched to prio=0")
	}
}

// TestMultiAgentPreemptionFillsAllAgents verifies that when a high-priority job
// preempts a low-priority job, it fills ALL agents — not just the first one.
func TestMultiAgentPreemptionFillsAllAgents(t *testing.T) {
	const numAgents = 4
	const cap = 4 // capacity per agent = 16 total slots

	agents := make([]*mockAgent, numAgents)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
		agents[i].SetMaxCapacity(cap)
	}

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	for i, a := range agents {
		l.RegisterAgent(fmt.Sprintf("agent-%d", i), a.URL(), "", nil)
	}
	time.Sleep(30 * time.Millisecond)

	// Low-priority job fills all 16 slots
	low := &types.Job{Name: "low", Command: "./low", Count: numAgents * cap, Priority: prio(10)}
	if err := l.DispatchJob(low); err != nil {
		t.Fatalf("dispatch low failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	total := 0
	for _, a := range agents {
		total += a.TaskCount()
	}
	if total != numAgents*cap {
		t.Fatalf("expected %d low tasks before preemption, got %d", numAgents*cap, total)
	}

	// High-priority job dispatched — should preempt low across ALL agents
	high := &types.Job{Name: "high", Command: "./high", Count: numAgents * cap, Priority: prio(1)}
	_ = l.DispatchJob(high) // may fail initially; preemption fills capacity
	time.Sleep(200 * time.Millisecond)

	// Every agent should have exactly cap high-priority tasks, none low
	for i, a := range agents {
		if a.TaskCount() != cap {
			t.Errorf("agent-%d: expected %d tasks, got %d", i, cap, a.TaskCount())
		}
		if n := a.TasksForJob("low"); n > 0 {
			t.Errorf("agent-%d still has %d 'low' tasks (should have been evicted)", i, n)
		}
	}
}

// TestMultiAgentPatchPreemption verifies that dragging a job to position 0
// (one PATCH) causes ALL agents to evict the now-lower-priority job.
func TestMultiAgentPatchPreemption(t *testing.T) {
	const numAgents = 4
	const cap = 4

	agents := make([]*mockAgent, numAgents)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
		agents[i].SetMaxCapacity(cap)
	}

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	for i, a := range agents {
		l.RegisterAgent(fmt.Sprintf("agent-%d", i), a.URL(), "", nil)
	}
	time.Sleep(30 * time.Millisecond)

	// counter2 fills all slots at prio=0 (top)
	counter2 := &types.Job{Name: "counter2", Command: "./c2", Count: numAgents * cap, Priority: prio(0)}
	if err := l.DispatchJob(counter2); err != nil {
		t.Fatalf("dispatch counter2 failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// counter has prio=1 (lower), stays pending (no capacity)
	counter := &types.Job{Name: "counter", Command: "./c", Count: numAgents * cap, Priority: prio(1)}
	_ = l.DispatchJob(counter) // expected to fail

	// GUI: drag counter above counter2 → ONE PATCH (insert counter at position 0)
	// Server renumbers: counter(0), counter2(1) atomically, then ONE reconcileJobs.
	if err := l.PatchJobPriority(counter.Name, 0); err != nil {
		t.Fatalf("PatchJobPriority counter failed: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// All agents should have counter tasks, none should have counter2 tasks
	for i, a := range agents {
		if a.TaskCount() != cap {
			t.Errorf("agent-%d: expected %d tasks, got %d", i, cap, a.TaskCount())
		}
		if n := a.TasksForJob("counter2"); n > 0 {
			t.Errorf("agent-%d still has %d 'counter2' tasks (should have been evicted by counter)", i, n)
		}
	}
}

// TestDragAboveOversizedCount simulates the real-world scenario:
// 4 agents × 14 capacity = 56 total slots. Both jobs want 100 instances.
// job_b fills all 56. Dragging job_a above job_b (one PATCH) should give
// ALL 56 slots to job_a — even though neither job can run all 100 instances.
func TestDragAboveOversizedCount(t *testing.T) {
	const numAgents = 4
	const coresPerAgent = 14
	total := numAgents * coresPerAgent // 56

	agents := make([]*mockAgent, numAgents)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
		agents[i].SetMaxCapacity(coresPerAgent)
	}

	store := NewMockJobStore()
	l := New("leader", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	for i, a := range agents {
		l.RegisterAgent(fmt.Sprintf("agent-%d", i), a.URL(), "", nil)
	}
	time.Sleep(30 * time.Millisecond)

	// jobB fills all 56 slots (count=100, capacity=56), priority=0
	jobB := &types.Job{Name: "jobB", Command: "./b", Count: 100, Priority: prio(0)}
	_ = l.DispatchJob(jobB) // partially succeeds: 56/100
	time.Sleep(100 * time.Millisecond)

	placed := 0
	for _, a := range agents {
		placed += a.TaskCount()
	}
	if placed != total {
		t.Fatalf("setup: expected %d jobB instances, got %d", total, placed)
	}

	// jobA wants 100, gets 0 initially (no capacity), priority=1
	jobA := &types.Job{Name: "jobA", Command: "./a", Count: 100, Priority: prio(1)}
	_ = l.DispatchJob(jobA) // expected to fail
	time.Sleep(30 * time.Millisecond)

	// GUI: drag jobA to position 0 (above jobB) — one PATCH
	if err := l.PatchJobPriority(jobA.Name, 0); err != nil {
		t.Fatalf("PatchJobPriority: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// jobA should have all 56 slots across all agents, jobB tasks should be gone
	for i, a := range agents {
		if a.TaskCount() != coresPerAgent {
			t.Errorf("agent-%d: expected %d tasks, got %d", i, coresPerAgent, a.TaskCount())
		}
		if jobBTasks := a.TasksForJob("jobB"); jobBTasks > 0 {
			t.Errorf("agent-%d still has %d jobB tasks (should have been evicted)", i, jobBTasks)
		}
	}
}

package leader

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
)

// ============== SCENARIO TESTS ==============
//
// Comprehensive tests for real-world operations:
// - Updates with failed/mixed-state tasks
// - Scale up/down
// - Node add/remove with tasks in various states
// - Priority changes during partial dispatch
// - Deploy/delete/redeploy lifecycle
//
// These tests use the mockAgent with full stop-task support.

// ---------- HELPERS ----------

// setupLeader creates a leader with state loop running, no settle delay.
func setupLeader(t *testing.T) (*Leader, *MockJobStore, context.CancelFunc) {
	t.Helper()
	store := NewMockJobStore()
	l := New("local", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go l.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)
	return l, store, cancel
}

// registerAgent registers an agent and sends an initial heartbeat.
func registerAgent(l *Leader, id string, agent *mockAgent) {
	l.RegisterAgent(id, agent.URL(), "", nil)
	l.Heartbeat(id, "", 0)
}

// totalTasks returns the sum of task counts across all agents.
func totalTasks(agents ...*mockAgent) int {
	n := 0
	for _, a := range agents {
		n += a.TaskCount()
	}
	return n
}

// totalRunning returns the sum of running (non-failed) task counts.
func totalRunning(agents ...*mockAgent) int {
	n := 0
	for _, a := range agents {
		n += a.RunningTaskCount()
	}
	return n
}

// totalPlacedForJob sums placed counts across all agents for a job.
func totalPlacedForJob(l *Leader, jobName string) int {
	placed := l.GetPlaced(jobName)
	total := 0
	for _, count := range placed {
		total += count
	}
	return total
}

// ============================================================
//  A. UPDATES WITH FAILED TASKS
// ============================================================

// TestUpdateRolling_OneTaskFailed verifies rolling update when one of three
// tasks is in "failed" state. The update should:
// 1. Snapshot all 3 tasks (including the failed one)
// 2. Dispatch 3 new instances
// 3. Stop all 3 old instances (including the failed one)
// 4. Result: 3 running tasks, placed=3
func TestUpdateRolling_OneTaskFailed(t *testing.T) {
	agent1 := newMockAgent()
	agent2 := newMockAgent()
	agent3 := newMockAgent()
	defer agent1.Close()
	defer agent2.Close()
	defer agent3.Close()

	l, _, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent1)
	registerAgent(l, "a2", agent2)
	registerAgent(l, "a3", agent3)
	time.Sleep(10 * time.Millisecond)

	// Deploy v1 with count=3
	job := &types.Job{Name: "api", Command: "./api-v1", Count: 3}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if total := totalTasks(agent1, agent2, agent3); total != 3 {
		t.Fatalf("Expected 3 tasks before update, got %d", total)
	}

	// Mark one task as failed (simulate health check failure + max restarts exceeded)
	ids := agent1.TaskIDs("api")
	if len(ids) > 0 {
		agent1.MarkTaskState(ids[0], types.TaskFailed)
	} else {
		ids = agent2.TaskIDs("api")
		if len(ids) > 0 {
			agent2.MarkTaskState(ids[0], types.TaskFailed)
		}
	}

	// Verify: 3 tasks total, 2 running + 1 failed
	if running := totalRunning(agent1, agent2, agent3); running != 2 {
		t.Fatalf("Expected 2 running tasks after marking one failed, got %d", running)
	}

	// Rolling update to v2
	newJob := &types.Job{
		Name:         "api",
		Command:      "./api-v2",
		Count:        3,
		UpdatePolicy: types.UpdateRolling,
	}
	if err := l.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// After rolling update: all old tasks (including failed) should be stopped,
	// 3 new tasks should be running
	placed := totalPlacedForJob(l, "api")
	running := totalRunning(agent1, agent2, agent3)

	t.Logf("After update: placed=%d, running=%d, total_tasks=%d",
		placed, running, totalTasks(agent1, agent2, agent3))

	if running != 3 {
		t.Errorf("Expected 3 running tasks after rolling update, got %d", running)
	}
	if placed != 3 {
		t.Errorf("Expected placed=3 after rolling update, got %d", placed)
	}
}

// TestUpdateRolling_AllTasksFailed verifies rolling update when ALL tasks
// are in "failed" state. The update should still dispatch new instances.
func TestUpdateRolling_AllTasksFailed(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	l, _, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	// Deploy with count=2
	job := &types.Job{Name: "worker", Command: "./worker-v1", Count: 2}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agent.TaskCount() != 2 {
		t.Fatalf("Expected 2 tasks, got %d", agent.TaskCount())
	}

	// Mark ALL tasks as failed
	agent.MarkJobTasksState("worker", types.TaskFailed)
	if agent.RunningTaskCount() != 0 {
		t.Fatalf("Expected 0 running after marking all failed")
	}

	// Rolling update to v2
	newJob := &types.Job{
		Name:         "worker",
		Command:      "./worker-v2",
		Count:        2,
		UpdatePolicy: types.UpdateRolling,
	}
	if err := l.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	running := agent.RunningTaskCount()
	placed := totalPlacedForJob(l, "worker")

	t.Logf("After update: placed=%d, running=%d, total=%d", placed, running, agent.TaskCount())

	if running != 2 {
		t.Errorf("Expected 2 running after update, got %d", running)
	}
	if placed != 2 {
		t.Errorf("Expected placed=2 after update, got %d", placed)
	}
}

// TestUpdateRecreate_WithFailedTask verifies recreate update with a failed task.
// Recreate stops all → dispatches fresh. Failed tasks should also be stopped.
func TestUpdateRecreate_WithFailedTask(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	l, _, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "web", Command: "./web-v1", Count: 3}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Mark one as failed
	ids := agent.TaskIDs("web")
	if len(ids) > 0 {
		agent.MarkTaskState(ids[0], types.TaskFailed)
	}

	// Recreate update
	newJob := &types.Job{
		Name:         "web",
		Command:      "./web-v2",
		Count:        3,
		UpdatePolicy: types.UpdateRecreate,
	}
	if err := l.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob recreate failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	running := agent.RunningTaskCount()
	placed := totalPlacedForJob(l, "web")

	t.Logf("After recreate: placed=%d, running=%d, total=%d", placed, running, agent.TaskCount())

	if running != 3 {
		t.Errorf("Expected 3 running after recreate, got %d", running)
	}
}

// TestUpdateBlueGreen_WithFailedTask verifies blue-green update with a failed task.
func TestUpdateBlueGreen_WithFailedTask(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	l, _, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "app", Command: "./app-v1", Count: 2}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Mark one as failed
	ids := agent.TaskIDs("app")
	if len(ids) > 0 {
		agent.MarkTaskState(ids[0], types.TaskFailed)
	}

	// Blue-green update
	newJob := &types.Job{
		Name:         "app",
		Command:      "./app-v2",
		Count:        2,
		UpdatePolicy: types.UpdateBlueGreen,
	}
	if err := l.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob blue-green failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	running := agent.RunningTaskCount()
	placed := totalPlacedForJob(l, "app")

	t.Logf("After blue-green: placed=%d, running=%d, total=%d", placed, running, agent.TaskCount())

	// Blue-green dispatches new (2) then stops old (2 including failed)
	if running != 2 {
		t.Errorf("Expected 2 running after blue-green, got %d", running)
	}
	if placed != 2 {
		t.Errorf("Expected placed=2 after blue-green, got %d", placed)
	}
}

// ============================================================
//  B. SCALE UP / SCALE DOWN DURING UPDATES
// ============================================================

// TestUpdateRolling_ScaleUp verifies that rolling update from count=2 to count=5
// replaces old tasks AND dispatches additional instances for the increased count.
func TestUpdateRolling_ScaleUp(t *testing.T) {
	agents := make([]*mockAgent, 3)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	l, _, cancel := setupLeader(t)
	defer cancel()

	for i, a := range agents {
		registerAgent(l, fmt.Sprintf("a%d", i), a)
	}
	time.Sleep(10 * time.Millisecond)

	// Deploy with count=2
	job := &types.Job{Name: "api", Command: "./api-v1", Count: 2}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if total := totalTasks(agents...); total != 2 {
		t.Fatalf("Expected 2 initial tasks, got %d", total)
	}

	// Rolling update: scale from 2 → 5
	newJob := &types.Job{
		Name:         "api",
		Command:      "./api-v2",
		Count:        5,
		UpdatePolicy: types.UpdateRolling,
	}
	if err := l.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	placed := totalPlacedForJob(l, "api")
	total := totalTasks(agents...)

	t.Logf("After scale-up rolling update: placed=%d, total_tasks=%d", placed, total)

	// Rolling update replaces 2 old + reconcile dispatches 3 extra = 5 total
	if placed != 5 {
		t.Errorf("Expected placed=5 after rolling scale-up, got %d", placed)
	}
	if total != 5 {
		t.Errorf("Expected 5 total tasks after rolling scale-up, got %d", total)
	}
}

// TestUpdateRecreate_ScaleUp verifies recreate update with increased count.
// Recreate stops all → dispatches fresh with new count.
func TestUpdateRecreate_ScaleUp(t *testing.T) {
	agents := make([]*mockAgent, 3)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	l, _, cancel := setupLeader(t)
	defer cancel()

	for i, a := range agents {
		registerAgent(l, fmt.Sprintf("a%d", i), a)
	}
	time.Sleep(10 * time.Millisecond)

	// Deploy with count=2
	job := &types.Job{Name: "api", Command: "./api-v1", Count: 2}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Recreate update: scale 2 → 5
	newJob := &types.Job{
		Name:         "api",
		Command:      "./api-v2",
		Count:        5,
		UpdatePolicy: types.UpdateRecreate,
	}
	if err := l.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	placed := totalPlacedForJob(l, "api")
	total := totalTasks(agents...)

	t.Logf("After recreate scale-up: placed=%d, total=%d", placed, total)

	// Recreate stops all, then DispatchJob(count=5) dispatches 5 new
	if placed != 5 {
		t.Errorf("Expected placed=5 after recreate scale-up, got %d", placed)
	}
	if total != 5 {
		t.Errorf("Expected 5 total tasks after recreate scale-up, got %d", total)
	}
}

// TestUpdateRolling_ScaleDown verifies rolling update from count=5 to count=3.
// Rolling update only replaces up to new count, leaving extras running.
func TestUpdateRolling_ScaleDown(t *testing.T) {
	agents := make([]*mockAgent, 3)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	l, _, cancel := setupLeader(t)
	defer cancel()

	for i, a := range agents {
		registerAgent(l, fmt.Sprintf("a%d", i), a)
	}
	time.Sleep(10 * time.Millisecond)

	// Deploy with count=5
	job := &types.Job{Name: "api", Command: "./api-v1", Count: 5}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if total := totalTasks(agents...); total != 5 {
		t.Fatalf("Expected 5 initial tasks, got %d", total)
	}

	// Rolling update: scale down 5 → 3
	newJob := &types.Job{
		Name:         "api",
		Command:      "./api-v2",
		Count:        3,
		UpdatePolicy: types.UpdateRolling,
	}
	if err := l.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	placed := totalPlacedForJob(l, "api")
	total := totalTasks(agents...)

	t.Logf("After scale-down rolling: placed=%d, total=%d", placed, total)

	// Rolling update replaces 3 + stops 2 excess = exactly 3 remaining
	if placed != 3 {
		t.Errorf("Expected placed=3 after rolling scale-down, got %d", placed)
	}
	if total != 3 {
		t.Errorf("Expected 3 total tasks after rolling scale-down, got %d", total)
	}
}

// TestUpdateRecreate_ScaleDown verifies recreate with decreased count.
func TestUpdateRecreate_ScaleDown(t *testing.T) {
	agents := make([]*mockAgent, 3)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	l, _, cancel := setupLeader(t)
	defer cancel()

	for i, a := range agents {
		registerAgent(l, fmt.Sprintf("a%d", i), a)
	}
	time.Sleep(10 * time.Millisecond)

	// Deploy with count=5
	job := &types.Job{Name: "api", Command: "./api-v1", Count: 5}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Recreate update: scale down 5 → 3
	newJob := &types.Job{
		Name:         "api",
		Command:      "./api-v2",
		Count:        3,
		UpdatePolicy: types.UpdateRecreate,
	}
	if err := l.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	placed := totalPlacedForJob(l, "api")
	total := totalTasks(agents...)

	t.Logf("After recreate scale-down: placed=%d, total=%d", placed, total)

	// Recreate stops ALL 5, then dispatches 3 new
	if placed != 3 {
		t.Errorf("Expected placed=3 after recreate scale-down, got %d", placed)
	}
	if total != 3 {
		t.Errorf("Expected 3 total tasks after recreate scale-down, got %d", total)
	}
}

// ============================================================
//  C. PLACED COUNT ACCURACY
// ============================================================

// TestPlacedCount_NotUpdatedOnTaskFailure verifies that when a task fails
// on an agent, the leader's placed count is NOT automatically decremented.
// This documents current behavior: placed tracks dispatches, not actual running state.
func TestPlacedCount_NotUpdatedOnTaskFailure(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	l, _, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "app", Command: "./app", Count: 3}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agent.TaskCount() != 3 {
		t.Fatalf("Expected 3 tasks, got %d", agent.TaskCount())
	}

	// Simulate: 1 task fails permanently (max restarts exceeded)
	ids := agent.TaskIDs("app")
	agent.MarkTaskState(ids[0], types.TaskFailed)

	// Leader's placed count is still 3 (doesn't track task state)
	placed := totalPlacedForJob(l, "app")
	if placed != 3 {
		t.Errorf("Expected placed=3 (unchanged after failure), got %d", placed)
	}

	// But only 2 are actually running
	if agent.RunningTaskCount() != 2 {
		t.Errorf("Expected 2 running, got %d", agent.RunningTaskCount())
	}

	// Reconcile sees placed=3 == desired=3 → no action
	l.reconcileJobs()
	time.Sleep(50 * time.Millisecond)

	// Still no new dispatch (reconcile doesn't know about failures)
	if agent.RunCallCount() != 3 {
		t.Errorf("Reconcile should NOT dispatch more (placed matches desired), run_calls=%d", agent.RunCallCount())
	}
}

// TestPlacedCount_FixedByReRegistration verifies that when an agent
// re-registers with actual placed counts, the leader corrects its state.
func TestPlacedCount_FixedByReRegistration(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	l, _, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "app", Command: "./app", Count: 5}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agent.TaskCount() != 5 {
		t.Fatalf("Expected 5 tasks, got %d", agent.TaskCount())
	}

	// Simulate: 2 tasks fail permanently and get removed from agent
	ids := agent.TaskIDs("app")
	// Remove 2 tasks (simulating agent cleanup after permanent failure)
	agent.mu.Lock()
	filtered := make([]*types.Task, 0)
	removed := 0
	for _, task := range agent.tasks {
		if task.JobName == "app" && removed < 2 {
			removed++
			continue
		}
		filtered = append(filtered, task)
	}
	agent.tasks = filtered
	agent.mu.Unlock()
	_ = ids

	if agent.TaskCount() != 3 {
		t.Fatalf("Expected 3 tasks after simulated failures, got %d", agent.TaskCount())
	}

	// Leader still thinks placed=5
	if placed := totalPlacedForJob(l, "app"); placed != 5 {
		t.Fatalf("Leader should still think placed=5, got %d", placed)
	}

	// Agent re-registers with actual placed count → triggers reconcile
	l.RegisterAgent("a1", agent.URL(), "", map[string]int{"app": 3})
	time.Sleep(100 * time.Millisecond)

	// Now leader knows placed=3, desired=5 → dispatches 2 more
	if agent.TaskCount() != 5 {
		t.Errorf("Expected 5 tasks after re-registration + reconcile, got %d", agent.TaskCount())
	}

	placed := totalPlacedForJob(l, "app")
	if placed != 5 {
		t.Errorf("Expected placed=5 after reconcile, got %d", placed)
	}
}

// ============================================================
//  D. NODE ADD/REMOVE WITH MIXED TASK STATES
// ============================================================

// TestNodeLeave_WithMixedTaskStates verifies that when a node dies
// that had both running and failed tasks, the correct number of tasks
// is redistributed to survivors.
func TestNodeLeave_WithMixedTaskStates(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()

	store := NewMockJobStore()
	l := New("local", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	l.agentTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	registerAgent(l, "a", agentA)
	registerAgent(l, "b", agentB)
	time.Sleep(10 * time.Millisecond)

	// Deploy count=6 → 3 per agent
	job := &types.Job{Name: "api", Command: "./api", Count: 6}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	aTasks := agentA.TaskCount()
	bTasks := agentB.TaskCount()
	t.Logf("Initial: A=%d, B=%d", aTasks, bTasks)

	if aTasks+bTasks != 6 {
		t.Fatalf("Expected 6 total, got %d", aTasks+bTasks)
	}

	// Mark 1 task on agent A as failed
	ids := agentA.TaskIDs("api")
	if len(ids) > 0 {
		agentA.MarkTaskState(ids[0], types.TaskFailed)
	}

	aRunning := agentA.RunningTaskCount()
	t.Logf("A has %d running, %d failed", aRunning, aTasks-aRunning)

	// Agent A dies (both running and failed tasks lost)
	agentA.Close()
	time.Sleep(300 * time.Millisecond)

	l.Heartbeat("b", "", 0)
	time.Sleep(20 * time.Millisecond)

	l.checkDeadAgents()
	time.Sleep(100 * time.Millisecond)

	// All of A's placed count (3) should be removed.
	// Reconcile: desired=6, placed on B=3, missing=3 → dispatch 3 to B
	bFinal := agentB.TaskCount()
	t.Logf("After A dies: B=%d (was %d)", bFinal, bTasks)

	if bFinal != 6 {
		t.Errorf("Expected B to have 6 tasks after A dies, got %d", bFinal)
	}

	// A should not be in placed
	placed := l.GetPlaced("api")
	if _, ok := placed["a"]; ok {
		t.Error("Dead agent A should not be in placed map")
	}
}

// TestNodeJoin_GetsDaemonAndFilledRegularJobs verifies that when a new
// node joins: (1) daemon jobs are dispatched to it, (2) under-scheduled
// regular jobs get additional instances on the new node.
func TestNodeJoin_GetsDaemonAndFilledRegularJobs(t *testing.T) {
	agent1 := newMockAgent()
	agent2 := newMockAgent()
	defer agent1.Close()
	defer agent2.Close()

	l, _, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent1)
	time.Sleep(10 * time.Millisecond)

	// Deploy daemon (count=-1) and regular job (count=4)
	daemon := &types.Job{Name: "monitor", Command: "./monitor", Count: -1}
	regular := &types.Job{Name: "web", Command: "./web", Count: 4}

	if err := l.DispatchJob(daemon); err != nil {
		t.Fatalf("Daemon dispatch failed: %v", err)
	}
	if err := l.DispatchJob(regular); err != nil {
		t.Fatalf("Regular dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// agent1 should have: 1 daemon + 4 regular = 5 tasks
	if agent1.TasksForJob("monitor") != 1 {
		t.Errorf("Expected 1 daemon on a1, got %d", agent1.TasksForJob("monitor"))
	}
	if agent1.TasksForJob("web") != 4 {
		t.Errorf("Expected 4 web on a1, got %d", agent1.TasksForJob("web"))
	}

	// New node joins → should get daemon
	registerAgent(l, "a2", agent2)
	time.Sleep(200 * time.Millisecond)

	if agent2.TasksForJob("monitor") != 1 {
		t.Errorf("New agent should get daemon, got %d", agent2.TasksForJob("monitor"))
	}

	// Regular job already fully placed (4/4 on a1), should NOT get more on a2
	totalWeb := agent1.TasksForJob("web") + agent2.TasksForJob("web")
	if totalWeb != 4 {
		t.Errorf("Regular job should stay at 4 total, got %d", totalWeb)
	}
}

// TestNodeJoin_UnderScheduledJobGetsInstances verifies that when a new node
// joins and a job is under-scheduled (placed < desired), the new node
// gets some of the missing instances.
func TestNodeJoin_UnderScheduledJobGetsInstances(t *testing.T) {
	agent1 := newMockAgent()
	agent1.SetMaxCapacity(3) // Can only hold 3 tasks
	defer agent1.Close()

	l, _, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent1)
	time.Sleep(10 * time.Millisecond)

	// Deploy count=5 → agent1 can only hold 3
	job := &types.Job{Name: "api", Command: "./api", Count: 5}
	_ = l.DispatchJob(job) // partial failure expected
	time.Sleep(50 * time.Millisecond)

	a1Tasks := agent1.TaskCount()
	t.Logf("After dispatch to a1 (cap=3): %d tasks", a1Tasks)

	if a1Tasks > 3 {
		t.Fatalf("Agent1 should be at capacity (3), got %d", a1Tasks)
	}

	placed := totalPlacedForJob(l, "api")
	t.Logf("Placed=%d (desired=5)", placed)

	// New node joins with more capacity → should get remaining instances
	agent2 := newMockAgent()
	defer agent2.Close()

	registerAgent(l, "a2", agent2)
	time.Sleep(200 * time.Millisecond)

	total := agent1.TaskCount() + agent2.TaskCount()
	t.Logf("After a2 joins: a1=%d, a2=%d, total=%d", agent1.TaskCount(), agent2.TaskCount(), total)

	if total != 5 {
		t.Errorf("Expected 5 total tasks after second agent joins, got %d", total)
	}
}

// TestNodeLeave_DuringRollingUpdate verifies that if a node dies while
// a rolling update is in progress, the system eventually converges.
func TestNodeLeave_DuringRollingUpdate(t *testing.T) {
	agent1 := newMockAgent()
	agent2 := newMockAgent()
	defer agent1.Close()
	defer agent2.Close()

	store := NewMockJobStore()
	l := New("local", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	l.agentTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	registerAgent(l, "a1", agent1)
	registerAgent(l, "a2", agent2)
	time.Sleep(10 * time.Millisecond)

	// Deploy count=6 → 3 per agent
	job := &types.Job{Name: "api", Command: "./api-v1", Count: 6}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	total := totalTasks(agent1, agent2)
	t.Logf("Initial: a1=%d, a2=%d", agent1.TaskCount(), agent2.TaskCount())
	if total != 6 {
		t.Fatalf("Expected 6 tasks, got %d", total)
	}

	// Start rolling update in background
	done := make(chan error, 1)
	go func() {
		done <- l.UpdateJob(&types.Job{
			Name:         "api",
			Command:      "./api-v2",
			Count:        6,
			UpdatePolicy: types.UpdateRolling,
		})
	}()

	// While rolling update is happening, agent2 dies
	time.Sleep(10 * time.Millisecond)
	agent2.Close()

	// Wait for update to finish (may partially fail)
	select {
	case err := <-done:
		t.Logf("Rolling update result: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Rolling update timed out")
	}

	// Wait for agent2 timeout to expire
	time.Sleep(300 * time.Millisecond)

	// Keep agent1 alive (heartbeat JUST before dead agent check)
	l.Heartbeat("a1", "", 0)
	time.Sleep(10 * time.Millisecond)

	l.checkDeadAgents()
	time.Sleep(200 * time.Millisecond)

	// After dead agent check + reconcile, agent1 should eventually have 6 tasks
	a1Final := agent1.TaskCount()
	t.Logf("After node death + reconcile: a1=%d tasks", a1Final)

	if a1Final < 6 {
		t.Errorf("Expected at least 6 tasks on surviving agent after reconcile, got %d", a1Final)
	}
}

// ============================================================
//  E. PRIORITY CHANGES DURING PARTIAL DISPATCH
// ============================================================

// TestPriorityPatch_DuringActiveDispatch verifies that patching priority
// while a job is being dispatched doesn't cause over-scheduling.
// The dispatching flag should prevent reconcileJobs from double-dispatching.
func TestPriorityPatch_DuringActiveDispatch(t *testing.T) {
	agent := newMockAgent()
	agent.SetRunDelay(20 * time.Millisecond) // Slow dispatch
	defer agent.Close()

	store := NewMockJobStore()
	l := New("local", store, &httputil.Client{Timeout: 200 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	// Start dispatching a big job
	job := &types.Job{Name: "big", Command: "./big", Count: 10, Priority: prio(5)}
	store.StoreJob(job)

	done := make(chan error, 1)
	go func() {
		done <- l.DispatchJob(job)
	}()

	// While dispatching, patch priority (triggers reconcileJobs)
	time.Sleep(50 * time.Millisecond) // Let some dispatches happen
	_ = l.PatchJobPriority("big", 0)

	// Wait for original dispatch to complete
	select {
	case err := <-done:
		if err != nil {
			t.Logf("Dispatch partially failed (expected if reconcile interfered): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Dispatch timed out")
	}

	time.Sleep(200 * time.Millisecond)

	total := agent.TaskCount()
	t.Logf("Total tasks after dispatch + priority patch: %d (desired=10)", total)

	if total > 10 {
		t.Errorf("Over-scheduling! Got %d tasks, expected at most 10", total)
	}
}

// TestPriorityPatch_UnscheduledJobGetsCapacity verifies that a job that
// couldn't be scheduled (no capacity) gets dispatched after priority
// patch causes preemption of lower-priority jobs.
func TestPriorityPatch_UnscheduledJobGetsCapacity(t *testing.T) {
	agent := newMockAgent()
	agent.SetMaxCapacity(5)
	defer agent.Close()

	l, _, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	// Fill capacity with low-priority job
	lowJob := &types.Job{Name: "batch", Command: "./batch", Count: 5, Priority: prio(10)}
	if err := l.DispatchJob(lowJob); err != nil {
		t.Fatalf("Dispatch low failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agent.TaskCount() != 5 {
		t.Fatalf("Expected 5 tasks, got %d", agent.TaskCount())
	}

	// Try to dispatch high-count job with low priority → fails (no capacity)
	highJob := &types.Job{Name: "critical", Command: "./critical", Count: 5, Priority: prio(20)}
	_ = l.DispatchJob(highJob) // expected to fail
	time.Sleep(50 * time.Millisecond)

	if agent.TasksForJob("critical") > 0 {
		t.Fatal("Critical job should not be running (lower priority, no capacity)")
	}

	// Patch critical to top priority → preemption should kick in
	if err := l.PatchJobPriority("critical", 0); err != nil {
		t.Fatalf("PatchJobPriority failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	criticalTasks := agent.TasksForJob("critical")
	batchTasks := agent.TasksForJob("batch")

	t.Logf("After priority patch: critical=%d, batch=%d", criticalTasks, batchTasks)

	if criticalTasks == 0 {
		t.Error("Critical job should be running after priority patch (preemption)")
	}
	if batchTasks > 0 && criticalTasks < 5 {
		t.Logf("Note: batch not fully evicted (critical=%d, batch=%d)", criticalTasks, batchTasks)
	}
}

// TestNormalizePriorities_AfterDeletion verifies that after deleting a job,
// priorities are renormalized to 0..N-1 without gaps.
func TestNormalizePriorities_AfterDeletion(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	l, store, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	// Create 3 jobs with priorities 0, 1, 2
	for i, name := range []string{"job-a", "job-b", "job-c"} {
		job := &types.Job{Name: name, Command: "./" + name, Count: 1, Priority: prio(i)}
		if err := l.DispatchJob(job); err != nil {
			t.Fatalf("Dispatch %s failed: %v", name, err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	// Delete middle job (priority=1)
	l.DeleteJobByName("job-b")
	time.Sleep(100 * time.Millisecond)

	// Remaining jobs should be renormalized to 0, 1 (no gap)
	jobA := store.GetJob("job-a")
	jobC := store.GetJob("job-c")

	if jobA == nil || jobC == nil {
		t.Fatal("Remaining jobs should exist in store")
	}

	if jobA.Priority == nil || *jobA.Priority != 0 {
		t.Errorf("job-a priority should be 0, got %v", jobA.Priority)
	}
	if jobC.Priority == nil || *jobC.Priority != 1 {
		t.Errorf("job-c priority should be 1 (renormalized), got %v", jobC.Priority)
	}
}

// TestPriorityReorder_ThreeJobs verifies reordering 3 jobs via priority patches.
func TestPriorityReorder_ThreeJobs(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	l, store, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	// Create jobs: A(0), B(1), C(2)
	for i, name := range []string{"A", "B", "C"} {
		job := &types.Job{Name: name, Command: "./" + name, Count: 1, Priority: prio(i)}
		if err := l.DispatchJob(job); err != nil {
			t.Fatalf("Dispatch %s failed: %v", name, err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	// Move C to top (position 0): C(0), A(1), B(2)
	if err := l.PatchJobPriority("C", 0); err != nil {
		t.Fatalf("PatchJobPriority failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	jobA := store.GetJob("A")
	jobB := store.GetJob("B")
	jobC := store.GetJob("C")

	if jobC.Priority == nil || *jobC.Priority != 0 {
		t.Errorf("C should be prio 0, got %v", jobC.Priority)
	}
	if jobA.Priority == nil || *jobA.Priority != 1 {
		t.Errorf("A should be prio 1, got %v", jobA.Priority)
	}
	if jobB.Priority == nil || *jobB.Priority != 2 {
		t.Errorf("B should be prio 2, got %v", jobB.Priority)
	}
}

// TestUpdateJob_PreservesPriority verifies that updating a job without
// explicitly setting priority preserves the old job's relative position.
func TestUpdateJob_PreservesPriority(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	l, store, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	// Deploy 3 jobs: A(0), B(1), C(2)
	for i, name := range []string{"A", "B", "C"} {
		if err := l.DispatchJob(&types.Job{
			Name: name, Command: "./" + name, Count: 1, Priority: prio(i),
		}); err != nil {
			t.Fatalf("Dispatch %s failed: %v", name, err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	// Update B without setting priority (nil) → should keep position 1
	if err := l.UpdateJob(&types.Job{
		Name:    "B",
		Command: "./B-v2",
		Count:   1,
	}); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// After normalization: A(0), B(1), C(2) — B keeps its position
	jobA := store.GetJob("A")
	jobB := store.GetJob("B")
	jobC := store.GetJob("C")

	if jobB.Priority == nil {
		t.Fatal("B priority should be preserved, got nil")
	}

	// B should still be between A and C
	if *jobA.Priority >= *jobB.Priority {
		t.Errorf("A(%d) should be before B(%d)", *jobA.Priority, *jobB.Priority)
	}
	if *jobB.Priority >= *jobC.Priority {
		t.Errorf("B(%d) should be before C(%d)", *jobB.Priority, *jobC.Priority)
	}

	// Command should be updated
	if jobB.Command != "./B-v2" {
		t.Errorf("B command should be ./B-v2, got %s", jobB.Command)
	}
}

// ============================================================
//  F. DEPLOY / DELETE / REDEPLOY LIFECYCLE
// ============================================================

// TestDeploy_Delete_Redeploy verifies that deleting a job and redeploying
// with the same name works correctly. No ghost state from the old deployment.
func TestDeploy_Delete_Redeploy(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	l, store, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	// Deploy v1
	job := &types.Job{Name: "api", Command: "./api-v1", Count: 3}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch v1 failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agent.TaskCount() != 3 {
		t.Fatalf("Expected 3 tasks after v1 deploy, got %d", agent.TaskCount())
	}

	// Delete job
	l.DeleteJobByName("api")
	time.Sleep(100 * time.Millisecond)

	if store.GetJob("api") != nil {
		t.Error("Job should be deleted from store")
	}
	if agent.TasksForJob("api") != 0 {
		t.Errorf("Agent should have 0 api tasks after delete, got %d", agent.TasksForJob("api"))
	}
	if placed := totalPlacedForJob(l, "api"); placed != 0 {
		t.Errorf("Placed should be 0 after delete, got %d", placed)
	}

	// Redeploy with same name, different config
	job2 := &types.Job{Name: "api", Command: "./api-v2", Count: 2}
	if err := l.DispatchJob(job2); err != nil {
		t.Fatalf("Dispatch v2 failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agent.TasksForJob("api") != 2 {
		t.Errorf("Expected 2 api tasks after redeploy, got %d", agent.TasksForJob("api"))
	}
	if placed := totalPlacedForJob(l, "api"); placed != 2 {
		t.Errorf("Expected placed=2 after redeploy, got %d", placed)
	}

	// Verify job definition is v2
	storedJob := store.GetJob("api")
	if storedJob == nil || storedJob.Command != "./api-v2" {
		t.Error("Stored job should have v2 command")
	}
}

// TestDeleteJob_WithMixedTaskStates verifies that deleting a job removes
// all tasks regardless of their state (running, failed, etc.)
func TestDeleteJob_WithMixedTaskStates(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	l, _, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "app", Command: "./app", Count: 4}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Mark 2 tasks as failed
	ids := agent.TaskIDs("app")
	if len(ids) >= 2 {
		agent.MarkTaskState(ids[0], types.TaskFailed)
		agent.MarkTaskState(ids[1], types.TaskFailed)
	}

	if agent.RunningTaskCount() != 2 {
		t.Fatalf("Expected 2 running, got %d", agent.RunningTaskCount())
	}

	// Delete job — should remove ALL tasks (running + failed)
	l.DeleteJobByName("app")
	time.Sleep(100 * time.Millisecond)

	if agent.TasksForJob("app") != 0 {
		t.Errorf("All tasks should be deleted, got %d", agent.TasksForJob("app"))
	}
	if placed := totalPlacedForJob(l, "app"); placed != 0 {
		t.Errorf("Placed should be 0 after delete, got %d", placed)
	}
}

// TestMultipleJobs_IndependentLifecycle verifies that multiple jobs can be
// deployed, updated, and deleted independently without interference.
func TestMultipleJobs_IndependentLifecycle(t *testing.T) {
	agents := make([]*mockAgent, 2)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	l, store, cancel := setupLeader(t)
	defer cancel()

	for i, a := range agents {
		registerAgent(l, fmt.Sprintf("a%d", i), a)
	}
	time.Sleep(10 * time.Millisecond)

	// Deploy 3 independent jobs
	jobs := []*types.Job{
		{Name: "api", Command: "./api", Count: 4, Priority: prio(0)},
		{Name: "worker", Command: "./worker", Count: 2, Priority: prio(1)},
		{Name: "cron", Command: "./cron", Count: 1, Priority: prio(2)},
	}

	for _, job := range jobs {
		if err := l.DispatchJob(job); err != nil {
			t.Fatalf("Dispatch %s failed: %v", job.Name, err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	totalAll := totalTasks(agents...)
	if totalAll != 7 {
		t.Fatalf("Expected 7 total tasks (4+2+1), got %d", totalAll)
	}

	// Update worker (rolling)
	if err := l.UpdateJob(&types.Job{
		Name:         "worker",
		Command:      "./worker-v2",
		Count:        2,
		UpdatePolicy: types.UpdateRolling,
	}); err != nil {
		t.Fatalf("Update worker failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// api and cron should be unaffected
	apiPlaced := totalPlacedForJob(l, "api")
	cronPlaced := totalPlacedForJob(l, "cron")

	if apiPlaced != 4 {
		t.Errorf("api should still have 4 placed, got %d", apiPlaced)
	}
	if cronPlaced != 1 {
		t.Errorf("cron should still have 1 placed, got %d", cronPlaced)
	}

	// Verify worker was updated
	workerJob := store.GetJob("worker")
	if workerJob.Command != "./worker-v2" {
		t.Errorf("worker should be v2, got %s", workerJob.Command)
	}

	// Delete cron — api and worker unaffected
	l.DeleteJobByName("cron")
	time.Sleep(100 * time.Millisecond)

	if store.GetJob("cron") != nil {
		t.Error("cron should be deleted from store")
	}
	if apiPlaced := totalPlacedForJob(l, "api"); apiPlaced != 4 {
		t.Errorf("api should still have 4 placed after cron delete, got %d", apiPlaced)
	}
}

// ============================================================
//  G. EDGE CASES
// ============================================================

// TestUpdateRolling_NoOldTasks verifies rolling update when the job exists
// in the store but has no running tasks (all crashed and were removed).
func TestUpdateRolling_NoOldTasks(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	l, store, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	// Store job definition but don't dispatch (simulates all tasks gone)
	store.StoreJob(&types.Job{Name: "ghost", Command: "./ghost-v1", Count: 2})
	l.do(func(s *leaderState) {
		if s.placed["a1"] == nil {
			s.placed["a1"] = make(map[string]int)
		}
		s.placed["a1"]["ghost"] = 2
	})
	time.Sleep(10 * time.Millisecond)

	// Agent has no tasks for this job (they all crashed and got cleaned up)
	// Rolling update should see 0 old tasks → replace 0 → placed unchanged
	newJob := &types.Job{
		Name:         "ghost",
		Command:      "./ghost-v2",
		Count:        2,
		UpdatePolicy: types.UpdateRolling,
	}
	if err := l.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Rolling update found 0 old tasks → replaced 0.
	// Job definition updated but no new dispatches from the update itself.
	// Placed still shows 2 (stale from manual setup).
	storedJob := store.GetJob("ghost")
	if storedJob.Command != "./ghost-v2" {
		t.Errorf("Job should be updated to v2, got %s", storedJob.Command)
	}
}

// TestUpdateRolling_CountOne_SingleAgent verifies the critical scenario:
// rolling update of a count=1 job on a single agent. New task dispatched,
// old task stopped by ID (not by name, which would kill both).
func TestUpdateRolling_CountOne_SingleAgent(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	l, _, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	// Deploy v1 count=1
	job := &types.Job{Name: "api", Command: "./api-v1", Count: 1}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agent.TaskCount() != 1 {
		t.Fatalf("Expected 1 task, got %d", agent.TaskCount())
	}

	// Rolling update to v2
	newJob := &types.Job{
		Name:         "api",
		Command:      "./api-v2",
		Count:        1,
		UpdatePolicy: types.UpdateRolling,
	}
	if err := l.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// After rolling update: exactly 1 task should remain (new v2)
	// This was previously a bug when delete-by-name was used instead of stop-by-task-ID
	if agent.TaskCount() != 1 {
		t.Errorf("Expected exactly 1 task after rolling update, got %d (possible delete-by-name bug)", agent.TaskCount())
	}
	if placed := totalPlacedForJob(l, "api"); placed != 1 {
		t.Errorf("Expected placed=1, got %d", placed)
	}
}

// TestDispatch_DuringSettlePeriod verifies that jobs dispatched during
// the settle period are stored but not sent to agents until settle completes.
func TestDispatch_DuringSettlePeriod(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	l := New("local", store, nil)
	l.SetSettleDelay(200 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	// Not yet settled
	if l.IsSettled() {
		t.Fatal("Leader should not be settled yet")
	}

	// Dispatch during settle → stored but not sent
	job := &types.Job{Name: "api", Command: "./api", Count: 3}
	err := l.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob should succeed (stores job even during settle): %v", err)
	}

	// Job should be in store
	if store.GetJob("api") == nil {
		t.Error("Job should be stored even during settle")
	}

	// But no tasks dispatched yet
	if agent.TaskCount() != 0 {
		t.Errorf("No tasks should be dispatched during settle, got %d", agent.TaskCount())
	}

	// Wait for settle to complete
	time.Sleep(300 * time.Millisecond)

	if !l.IsSettled() {
		t.Fatal("Leader should be settled now")
	}

	// After settle: reconcileJobs dispatches the stored job
	time.Sleep(100 * time.Millisecond)
	if agent.TaskCount() != 3 {
		t.Errorf("Expected 3 tasks after settle + reconcile, got %d", agent.TaskCount())
	}
}

// TestConcurrentUpdates_SameJob verifies that two concurrent updates
// to the same job are serialized: one succeeds, the other gets an error.
func TestConcurrentUpdates_SameJob(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	l, store, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent)
	time.Sleep(10 * time.Millisecond)

	// Deploy v1
	job := &types.Job{Name: "api", Command: "./api-v1", Count: 2}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Concurrent updates to v2 and v3
	done := make(chan error, 2)
	go func() {
		done <- l.UpdateJob(&types.Job{
			Name:         "api",
			Command:      "./api-v2",
			Count:        2,
			UpdatePolicy: types.UpdateRolling,
		})
	}()
	go func() {
		done <- l.UpdateJob(&types.Job{
			Name:         "api",
			Command:      "./api-v3",
			Count:        2,
			UpdatePolicy: types.UpdateRolling,
		})
	}()

	// Wait for both — one should succeed, one should fail
	var errors []error
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			errors = append(errors, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Update timed out")
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Exactly one should have failed with "already being updated"
	succeeded := 0
	for _, err := range errors {
		if err == nil {
			succeeded++
		} else {
			t.Logf("Rejected concurrent update: %v", err)
		}
	}
	if succeeded != 1 {
		t.Errorf("Expected exactly 1 successful update, got %d", succeeded)
	}

	// Job should be at one of the updated versions
	storedJob := store.GetJob("api")
	if storedJob == nil {
		t.Fatal("Job should exist in store")
	}

	t.Logf("Final state: command=%s, tasks=%d, placed=%d",
		storedJob.Command, agent.TaskCount(), totalPlacedForJob(l, "api"))
}

// TestUpdateRolling_MultiAgent_FailedTaskOnOneAgent verifies rolling
// update when one agent has a failed task and others have running tasks.
func TestUpdateRolling_MultiAgent_FailedTaskOnOneAgent(t *testing.T) {
	agent1 := newMockAgent()
	agent2 := newMockAgent()
	defer agent1.Close()
	defer agent2.Close()

	l, _, cancel := setupLeader(t)
	defer cancel()

	registerAgent(l, "a1", agent1)
	registerAgent(l, "a2", agent2)
	time.Sleep(10 * time.Millisecond)

	// Deploy count=4 → 2 per agent
	job := &types.Job{Name: "api", Command: "./api-v1", Count: 4}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	a1Tasks := agent1.TaskCount()
	a2Tasks := agent2.TaskCount()
	t.Logf("Initial: a1=%d, a2=%d", a1Tasks, a2Tasks)

	if a1Tasks+a2Tasks != 4 {
		t.Fatalf("Expected 4 total, got %d", a1Tasks+a2Tasks)
	}

	// Mark ALL tasks on agent1 as failed
	agent1.MarkJobTasksState("api", types.TaskFailed)
	if agent1.RunningTaskCount() != 0 {
		t.Fatalf("Expected 0 running on a1 after marking failed")
	}

	// Rolling update to v2
	newJob := &types.Job{
		Name:         "api",
		Command:      "./api-v2",
		Count:        4,
		UpdatePolicy: types.UpdateRolling,
	}
	if err := l.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	running := totalRunning(agent1, agent2)
	placed := totalPlacedForJob(l, "api")

	t.Logf("After update: a1_total=%d a1_running=%d, a2_total=%d a2_running=%d, placed=%d",
		agent1.TaskCount(), agent1.RunningTaskCount(),
		agent2.TaskCount(), agent2.RunningTaskCount(), placed)

	// All 4 tasks should have been replaced: 4 new running
	if running != 4 {
		t.Errorf("Expected 4 running after rolling update, got %d", running)
	}
	if placed != 4 {
		t.Errorf("Expected placed=4, got %d", placed)
	}
}

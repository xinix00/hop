package leader

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
)

// ============== MOCK AGENT FOR INTEGRATION TESTS ==============

// mockAgent simulates an hop agent for testing
type mockAgent struct {
	server         *httptest.Server
	mu             sync.Mutex
	jobs           map[string]*types.Job // jobID -> job (for backwards compat)
	tasks          []*types.Task         // all running tasks
	runCalls       int
	taskSeq        int
	failRuns       bool          // if true, all /run requests will fail with 503
	rejectAffinity bool          // if true, all /run requests will fail with 406
	failStops      bool          // if true, all /stop requests will fail with 500
	runDelay       time.Duration // delay before processing /run requests (simulates slow dispatch)
	maxCapacity    int           // if > 0, reject with 503 when len(tasks) >= maxCapacity
}

func newMockAgent() *mockAgent {
	ma := &mockAgent{
		jobs: make(map[string]*types.Job),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/run", ma.handleRun)
	mux.HandleFunc("/tasks", ma.handleTasks)
	mux.HandleFunc("/delete/", ma.handleDelete)
	mux.HandleFunc("/stop-task/", ma.handleStopTask)
	mux.HandleFunc("/stop/", ma.handleStop)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	ma.server = httptest.NewServer(mux)
	return ma
}

func (ma *mockAgent) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ma.mu.Lock()
	if ma.rejectAffinity {
		ma.mu.Unlock()
		http.Error(w, "affinity mismatch", http.StatusNotAcceptable)
		return
	}
	if ma.failRuns {
		ma.mu.Unlock()
		http.Error(w, "simulated failure", http.StatusServiceUnavailable)
		return
	}
	delay := ma.runDelay
	ma.mu.Unlock()

	var job types.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	replace := r.URL.Query().Get("replace") == "1"

	ma.mu.Lock()
	// Replace-semantiek zoals de echte agent: de eigen voorgangers tellen niet
	// mee voor de toelating en verdwijnen pas als die slaagt — een weigering
	// laat ze staan.
	occupied := len(ma.tasks)
	if replace {
		for _, t := range ma.tasks {
			if t.JobName == job.Name {
				occupied--
			}
		}
	}
	if ma.maxCapacity > 0 && occupied >= ma.maxCapacity {
		ma.mu.Unlock()
		http.Error(w, "at capacity", http.StatusServiceUnavailable)
		return
	}
	if replace {
		kept := ma.tasks[:0]
		for _, t := range ma.tasks {
			if t.JobName != job.Name {
				kept = append(kept, t)
			}
		}
		ma.tasks = kept
	}
	ma.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	ma.mu.Lock()
	ma.jobs[job.Name] = &job
	ma.runCalls++
	ma.taskSeq++
	task := &types.Task{
		ID:      fmt.Sprintf("task-%s-%d", job.Name, ma.taskSeq),
		JobName: job.Name,
		State:   types.TaskRunning,
	}
	ma.tasks = append(ma.tasks, task)
	ma.mu.Unlock()

	_ = json.NewEncoder(w).Encode(task)
}

// SetFailRuns makes all /run requests fail with 503 when set to true
func (ma *mockAgent) SetFailRuns(fail bool) {
	ma.mu.Lock()
	ma.failRuns = fail
	ma.mu.Unlock()
}

// SetRejectAffinity makes all /run requests fail with 406 when set to true
func (ma *mockAgent) SetRejectAffinity(reject bool) {
	ma.mu.Lock()
	ma.rejectAffinity = reject
	ma.mu.Unlock()
}

// SetFailStops makes all /stop requests fail with 500 when set to true
func (ma *mockAgent) SetFailStops(fail bool) {
	ma.mu.Lock()
	ma.failStops = fail
	ma.mu.Unlock()
}

// SetMaxCapacity limits how many tasks the agent will accept (0 = unlimited)
func (ma *mockAgent) SetMaxCapacity(n int) {
	ma.mu.Lock()
	ma.maxCapacity = n
	ma.mu.Unlock()
}

// SetRunDelay adds a delay before processing /run requests (simulates slow dispatch)
func (ma *mockAgent) SetRunDelay(d time.Duration) {
	ma.mu.Lock()
	ma.runDelay = d
	ma.mu.Unlock()
}

func (ma *mockAgent) URL() string {
	return ma.server.URL
}

func (ma *mockAgent) Close() {
	ma.server.Close()
}

func (ma *mockAgent) GetJobs() []*types.Job {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	jobs := make([]*types.Job, 0, len(ma.jobs))
	for _, j := range ma.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (ma *mockAgent) RunCallCount() int {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	return ma.runCalls
}

func (ma *mockAgent) ResetRunCount() {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	ma.runCalls = 0
}

func (ma *mockAgent) TaskCount() int {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	return len(ma.tasks)
}

// TasksForJob returns the number of running tasks for a specific job name.
func (ma *mockAgent) TasksForJob(jobName string) int {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	count := 0
	for _, t := range ma.tasks {
		if t.JobName == jobName {
			count++
		}
	}
	return count
}

// ClearTasks simulates agent restart (all tasks lost)
func (ma *mockAgent) ClearTasks() {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	ma.tasks = nil
	ma.jobs = make(map[string]*types.Job)
}

func (ma *mockAgent) handleTasks(w http.ResponseWriter, r *http.Request) {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	_ = json.NewEncoder(w).Encode(ma.tasks)
}

func (ma *mockAgent) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobName := r.URL.Path[len("/delete/"):]

	ma.mu.Lock()
	deleted := 0
	filtered := make([]*types.Task, 0, len(ma.tasks))
	for _, task := range ma.tasks {
		if task.JobName == jobName {
			delete(ma.jobs, task.JobName)
			deleted++
		} else {
			filtered = append(filtered, task)
		}
	}
	ma.tasks = filtered
	ma.mu.Unlock()

	_ = json.NewEncoder(w).Encode(map[string]int{"deleted": deleted})
}

func (ma *mockAgent) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ma.mu.Lock()
	if ma.failStops {
		ma.mu.Unlock()
		http.Error(w, "simulated stop failure", http.StatusInternalServerError)
		return
	}
	ma.mu.Unlock()

	jobName := r.URL.Path[len("/stop/"):]

	ma.mu.Lock()
	stopped := 0
	filtered := make([]*types.Task, 0, len(ma.tasks))
	for _, task := range ma.tasks {
		if task.JobName == jobName {
			stopped++ // remove task but keep ma.jobs entry (job definition preserved)
		} else {
			filtered = append(filtered, task)
		}
	}
	ma.tasks = filtered
	ma.mu.Unlock()

	_ = json.NewEncoder(w).Encode(map[string]int{"stopped": stopped})
}

// handleStopTask stops a single task by its ID (used by rolling/blue-green updates)
func (ma *mockAgent) handleStopTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	taskID := r.URL.Path[len("/stop-task/"):]

	ma.mu.Lock()
	stopped := 0
	filtered := make([]*types.Task, 0, len(ma.tasks))
	for _, task := range ma.tasks {
		if task.ID == taskID {
			stopped++
		} else {
			filtered = append(filtered, task)
		}
	}
	ma.tasks = filtered
	ma.mu.Unlock()

	_ = json.NewEncoder(w).Encode(map[string]int{"stopped": stopped})
}

// MarkTaskState changes the state of a specific task (for simulating failures)
func (ma *mockAgent) MarkTaskState(taskID string, state types.TaskState) {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	for _, task := range ma.tasks {
		if task.ID == taskID {
			task.State = state
			return
		}
	}
}

// MarkJobTasksState changes the state of all tasks for a job
func (ma *mockAgent) MarkJobTasksState(jobName string, state types.TaskState) {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	for _, task := range ma.tasks {
		if task.JobName == jobName {
			task.State = state
		}
	}
}

// TaskIDs returns all task IDs for a given job name
func (ma *mockAgent) TaskIDs(jobName string) []string {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	var ids []string
	for _, task := range ma.tasks {
		if task.JobName == jobName {
			ids = append(ids, task.ID)
		}
	}
	return ids
}

// RunningTaskCount returns the number of tasks in "running" state
func (ma *mockAgent) RunningTaskCount() int {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	count := 0
	for _, task := range ma.tasks {
		if task.State == types.TaskRunning {
			count++
		}
	}
	return count
}

// AddTasks pre-populates the mock agent with tasks (simulates pre-existing tasks from old leader)
func (ma *mockAgent) AddTasks(jobName string, count int) {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	for i := 0; i < count; i++ {
		ma.taskSeq++
		ma.tasks = append(ma.tasks, &types.Task{
			ID:      fmt.Sprintf("task-%s-%d", jobName, ma.taskSeq),
			JobName: jobName,
			State:   types.TaskRunning,
		})
	}
}

// PlacedCounts returns a map of jobName -> count for registration
func (ma *mockAgent) PlacedCounts() map[string]int {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	counts := make(map[string]int)
	for _, t := range ma.tasks {
		counts[t.JobName]++
	}
	return counts
}

// taskCounts creates a map of jobName -> 1 for each job (1 task per job)
func taskCounts(jobs []*types.Job) map[string]int {
	counts := make(map[string]int)
	for _, j := range jobs {
		counts[j.Name] = 1
	}
	return counts
}

// ============== LEADER FAILOVER TESTS ==============
// These tests verify that state is correctly transferred when a new leader takes over

func TestFailoverCountMinusOneDispatchesToNewAgents(t *testing.T) {
	// Scenario: count=-1 job exists on agent-1, new agent-2 joins, should receive the job
	// This tests the FULL flow with actual HTTP calls

	// Create mock agents
	agent1 := newMockAgent()
	defer agent1.Close()
	agent2 := newMockAgent()
	defer agent2.Close()

	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// De count=-1 daemon-job staat in de gecommitte staat (de nieuwe leader
	// laadt die bij failover uit S3 — niet meer uit een agent-heartbeat).
	daemonJob := &types.Job{
		Name:    "daemon",
		Command: "./daemon",
		Count:   -1,
	}
	newLeaderStore.StoreJob(daemonJob)

	// Pre-populate agent1 with the daemon task (it's already running from old leader)
	agent1.mu.Lock()
	agent1.jobs[daemonJob.Name] = daemonJob
	agent1.tasks = append(agent1.tasks, &types.Task{
		ID:      "daemon-task",
		JobName: daemonJob.Name,
		State:   types.TaskRunning,
	})
	agent1.mu.Unlock()

	newLeader.RegisterAgent("agent-1", agent1.URL(), "", map[string]int{"daemon": 1})
	newLeader.Heartbeat("agent-1", "", 0)
	time.Sleep(50 * time.Millisecond)

	// Agent 1 should NOT receive /run (already has it running)
	if agent1.RunCallCount() != 0 {
		t.Errorf("Agent 1 should NOT receive /run (already has daemon), got %d", agent1.RunCallCount())
	}

	// Now agent 2 joins via RegisterAgent - should receive the count=-1 job via dispatch
	newLeader.RegisterAgent("agent-2", agent2.URL(), "", nil)
	time.Sleep(50 * time.Millisecond)

	// Verify agent 2 received the job via /run
	if agent2.RunCallCount() != 1 {
		t.Errorf("Agent 2 should have received 1 /run call, got %d", agent2.RunCallCount())
	}

	agent2Jobs := agent2.GetJobs()
	if len(agent2Jobs) != 1 || agent2Jobs[0].Name != "daemon" {
		t.Errorf("Agent 2 should have daemon job, got %v", agent2Jobs)
	}

	// Verify placement now includes both agents
	placed := newLeader.GetPlaced("daemon")
	if len(placed) != 2 {
		t.Errorf("daemon-job should be on 2 agents, got %d: %v", len(placed), placed)
	}
}

func TestFailoverMultipleAgentsWithDaemonJob(t *testing.T) {
	// Multiple agents heartbeat, all already have the daemon job running.
	// reconcileJobs sees tasks via GetClusterStatus, so no duplicate /run calls.

	// Create 3 mock agents
	agents := make([]*mockAgent, 3)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	daemonJob := &types.Job{
		Name:    "monitoring",
		Command: "./monitor",
		Count:   -1,
	}

	// Pre-populate all agents with the daemon task (they already have it running)
	for _, agent := range agents {
		agent.mu.Lock()
		agent.jobs[daemonJob.Name] = daemonJob
		agent.tasks = append(agent.tasks, &types.Task{
			ID:      "monitoring-task",
			JobName: daemonJob.Name,
			State:   types.TaskRunning,
		})
		agent.mu.Unlock()
	}

	// All agents register with placed counts and heartbeat saying they ALREADY have the daemon job
	for i, agent := range agents {
		agentID := string(rune('a' + i))
		newLeader.RegisterAgent("agent-"+agentID, agent.URL(), "", map[string]int{"monitoring": 1})
		newLeader.Heartbeat("agent-"+agentID, "", 0)
	}
	time.Sleep(50 * time.Millisecond)

	// Verify all 3 agents are in placement
	placed := newLeader.GetPlaced("monitoring")
	if len(placed) != 3 {
		t.Errorf("monitoring should be on 3 agents, got %d: %v", len(placed), placed)
	}

	// All agents already have the task running (via GetClusterStatus), so NONE should receive /run calls
	for i, agent := range agents {
		if agent.RunCallCount() != 0 {
			t.Errorf("Agent %d should have 0 /run calls (already has job running), got %d", i, agent.RunCallCount())
		}
	}
}

func TestFailoverNewAgentGetsAllDaemonJobs(t *testing.T) {
	// New agent joins cluster with multiple count=-1 jobs - should get all of them

	existingAgent := newMockAgent()
	defer existingAgent.Close()
	newAgent := newMockAgent()
	defer newAgent.Close()

	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// De leader kent 3 daemon-jobs (nu uit zijn eigen store — de gecommitte
	// staat is de bron, niet meer een agent-heartbeat).
	for _, j := range []*types.Job{
		{Name: "hopdns", Command: "./hopdns", Count: -1},
		{Name: "hoplb", Command: "./hoplb", Count: -1},
		{Name: "monitoring", Command: "./monitor", Count: -1},
	} {
		newLeaderStore.StoreJob(j)
	}
	newLeader.RegisterAgent("existing", existingAgent.URL(), "", map[string]int{"hopdns": 1, "hoplb": 1, "monitoring": 1})
	newLeader.Heartbeat("existing", "", 0)
	time.Sleep(20 * time.Millisecond)

	// New agent joins with no jobs via RegisterAgent
	newLeader.RegisterAgent("new-agent", newAgent.URL(), "", nil)
	time.Sleep(100 * time.Millisecond)

	// New agent should have received all 3 daemon jobs
	if newAgent.RunCallCount() != 3 {
		t.Errorf("New agent should have received 3 /run calls, got %d", newAgent.RunCallCount())
	}

	newAgentJobs := newAgent.GetJobs()
	if len(newAgentJobs) != 3 {
		t.Errorf("New agent should have 3 jobs, got %d", len(newAgentJobs))
	}
}

func TestFailoverPreservesJobMetadata(t *testing.T) {
	// Verify that job metadata (tags, ports, resources) survives failover

	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// De job zit in de gecommitte staat (bron van waarheid na failover); we
	// halen 'm door een JSON round-trip zoals de S3-snapshot dat doet, en
	// laden 'm in de store van de nieuwe leader.
	originalJob := &types.Job{
		Name:        "complex",
		Command:     "./complex",
		Count:       3,
		CPUShares:   100,
		MemoryLimit: 512 * 1024 * 1024,
		Ports:       map[string]int{"http": 8080, "grpc": 9090},
		Env:         map[string]string{"ENV": "prod", "DEBUG": "false"},
		Tags:        map[string]string{"urlprefix": "api.example.com", "lb": "main"},
	}
	snap, _ := json.Marshal(persistedState{Jobs: []*types.Job{originalJob}})
	var restored persistedState
	if err := json.Unmarshal(snap, &restored); err != nil {
		t.Fatalf("snapshot round-trip: %v", err)
	}
	for _, j := range restored.Jobs {
		newLeaderStore.StoreJob(j)
	}

	newLeader.RegisterAgent("agent-1", "http://10.0.0.1:8080", "", nil)
	newLeader.Heartbeat("agent-1", "", 0)
	time.Sleep(20 * time.Millisecond)

	// Retrieve and verify
	recoveredJob := newLeaderStore.GetJob("complex")
	if recoveredJob == nil {
		t.Fatal("Job not recovered")
	}

	if recoveredJob.CPUShares != 100 {
		t.Errorf("CPUShares = %d, want 100", recoveredJob.CPUShares)
	}
	if recoveredJob.MemoryLimit != 512*1024*1024 {
		t.Errorf("MemoryLimit = %d, want %d", recoveredJob.MemoryLimit, 512*1024*1024)
	}
	if recoveredJob.Ports["http"] != 8080 || recoveredJob.Ports["grpc"] != 9090 {
		t.Errorf("Ports not preserved: %v", recoveredJob.Ports)
	}
	if recoveredJob.Env["ENV"] != "prod" {
		t.Errorf("Env not preserved: %v", recoveredJob.Env)
	}
	if recoveredJob.Tags["urlprefix"] != "api.example.com" {
		t.Errorf("Tags not preserved: %v", recoveredJob.Tags)
	}
}

func TestFailoverAgentWithEmptyJobsStillRegisters(t *testing.T) {
	// Agent with no jobs should still register (might be new/fresh agent)

	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// Agent heartbeats with no jobs
	newLeader.RegisterAgent("fresh-agent", "http://10.0.0.99:8080", "", nil)
	newLeader.Heartbeat("fresh-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	agents := newLeader.GetAgents()
	if len(agents) != 1 {
		t.Errorf("Expected 1 agent registered, got %d", len(agents))
	}
	if agents[0].ID != "fresh-agent" {
		t.Errorf("Agent ID = %q, want %q", agents[0].ID, "fresh-agent")
	}
}

func TestFailoverDispatchFailureDoesNotBreakHeartbeat(t *testing.T) {
	// If dispatch to agent fails, heartbeat should still work and agent should be registered

	// Agent that rejects /run requests
	rejectingAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/run" {
			http.Error(w, "no capacity", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer rejectingAgent.Close()

	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// First agent has daemon job (leader kent 'm uit zijn store).
	goodAgent := newMockAgent()
	defer goodAgent.Close()
	newLeaderStore.StoreJob(&types.Job{Name: "daemon", Command: "./d", Count: -1})
	newLeader.RegisterAgent("good-agent", goodAgent.URL(), "", map[string]int{"daemon": 1})
	newLeader.Heartbeat("good-agent", "", 0)
	time.Sleep(20 * time.Millisecond)

	// Rejecting agent joins via RegisterAgent - dispatch will fail but registration should succeed
	newLeader.RegisterAgent("rejecting-agent", rejectingAgent.URL, "", nil)
	time.Sleep(50 * time.Millisecond)

	// Agent should still be registered
	agents := newLeader.GetAgents()
	if len(agents) != 2 {
		t.Errorf("Expected 2 agents registered, got %d", len(agents))
	}

	// Placement should only have good-agent (rejecting agent failed to receive job)
	placed := newLeader.GetPlaced("daemon")
	if len(placed) != 1 {
		t.Errorf("daemon should only be on 1 agent (good-agent), got %d: %v", len(placed), placed)
	}
}

func TestFailoverNewLeaderDoesNotDuplicateOwnTasks(t *testing.T) {
	// BUG REPRODUCTION: When a node becomes leader, it should NOT redispatch
	// jobs that it already has running locally.
	//
	// Scenario:
	// 1. Node A (old leader) and Node B both run "my-api" (count=2, one each)
	// 2. Node A fails
	// 3. Node B becomes new leader WITH PERSISTED STATE (knows about the job)
	// 4. Node B's task from before should NOT be duplicated
	//
	// The bug: New leader doesn't learn placement from its OWN local agent
	// because of the `id != l.localAgentID` check in Heartbeat().
	// Combined with tryRescheduleUnderscheduled running BEFORE placement is learned,
	// this causes duplicates.

	// Mock agent representing the NEW LEADER's local agent
	localAgent := newMockAgent()
	defer localAgent.Close()

	// The job that was already running BEFORE this node became leader
	existingJob := &types.Job{
		Name:    "my-api",
		Command: "./api",
		Count:   2, // Desired: 2 instances total
	}

	// New leader has PERSISTED STATE from before the failover
	// (it was synced by the old leader, or loaded from disk)
	newLeaderStore := NewMockJobStore()
	newLeaderStore.StoreJob(existingJob)                        // Job already in store!
	newLeaderStore.stateTime = time.Now().Add(-1 * time.Minute) // State from before

	// The new leader's ID matches the local agent
	newLeader := New("local-agent-id", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// The local agent already has 1 running task for this job (from before failover)
	localAgent.mu.Lock()
	localAgent.jobs[existingJob.Name] = existingJob
	localAgent.tasks = append(localAgent.tasks, &types.Task{
		ID:      "existing-task-1",
		JobName: "my-api",
		State:   types.TaskRunning,
	})
	localAgent.mu.Unlock()

	// LOCAL agent registers with placed counts and heartbeats to the new leader (itself)
	// Note: stateTime is SAME as leader's (not newer), so sync path is NOT triggered
	newLeader.RegisterAgent("local-agent-id", localAgent.URL(), "", map[string]int{"my-api": 1})
	newLeader.Heartbeat("local-agent-id", "", 0)
	time.Sleep(50 * time.Millisecond)

	// BUG: The new leader dispatches instances to itself because:
	// 1. Local agent is "new" (not in agents map)
	// 2. tryRescheduleUnderscheduled runs → finds my-api in store with count=2
	// 3. Placement is EMPTY → job appears under-scheduled (0/2) → dispatch 2!
	// 4. The leader SKIPS learning placement from local agent (id != l.localAgentID)

	// EXPECTED: 0 new /run calls (1 instance already running locally, wait for other agents)
	// ACTUAL (BUG): Should dispatch, but not to itself for instances it already has!
	runCalls := localAgent.RunCallCount()
	if runCalls > 1 {
		// At most 1 call could be justified (the "missing" second instance)
		// But 2 calls means it dispatched both instances ignoring the existing one
		t.Errorf("BUG: New leader dispatched %d tasks, ignoring its own existing task", runCalls)
	}

	// Verify placement includes the local agent (critical for correct scheduling)
	placed := newLeader.GetPlaced("my-api")
	hasLocalAgent := false
	for p := range placed {
		if p == "local-agent-id" {
			hasLocalAgent = true
			break
		}
	}
	if !hasLocalAgent {
		t.Errorf("BUG: Placement should include local-agent-id, got: %v", placed)
	}
}

func TestFailoverNewLeaderLearnsFromLocalAgent(t *testing.T) {
	// KISS refactor: reconcileJobs uses GetClusterStatus to see what's running.
	// Local agent already has tasks running → no duplicate /run calls.
	// Placement is only tracked for daemon jobs (count=-1), not regular jobs.

	localAgent := newMockAgent()
	defer localAgent.Close()

	// Local agent reports jobs it's running
	localJobs := []*types.Job{
		{Name: "job-a", Command: "./a", Count: 1},
		{Name: "job-b", Command: "./b", Count: 1},
	}

	// New leader has persisted state (jobs already known)
	newLeaderStore := NewMockJobStore()
	for _, job := range localJobs {
		newLeaderStore.StoreJob(job)
	}
	newLeaderStore.stateTime = time.Now().Add(-1 * time.Minute)

	newLeader := New("local-agent-id", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// Simulate local agent already having these tasks running
	localAgent.mu.Lock()
	for _, job := range localJobs {
		localAgent.jobs[job.Name] = job
		localAgent.tasks = append(localAgent.tasks, &types.Task{
			ID:      "task-" + job.Name,
			JobName: job.Name,
			State:   types.TaskRunning,
		})
	}
	localAgent.mu.Unlock()

	// Local agent registers with placed counts and heartbeats (same stateTime, not newer)
	newLeader.RegisterAgent("local-agent-id", localAgent.URL(), "", taskCounts(localJobs))
	newLeader.Heartbeat("local-agent-id", "", 0)
	time.Sleep(50 * time.Millisecond)

	// Jobs should still be in store
	jobs := newLeaderStore.GetJobs()
	if len(jobs) != 2 {
		t.Errorf("Expected 2 jobs in store, got %d", len(jobs))
	}

	// KISS: No new dispatches because reconcileJobs sees tasks running via GetClusterStatus
	if localAgent.RunCallCount() != 0 {
		t.Errorf("Local agent should not receive new /run calls, got %d", localAgent.RunCallCount())
	}

	// Note: Placement is NOT tracked for regular jobs in KISS refactor.
	// Only daemon jobs (count=-1) track placement per-agent.
}

func TestFailoverNewLeaderReschedulesOrphanedJobs(t *testing.T) {
	// New agent triggers reconciliation automatically.
	// daemon (count=-1) + regular jobs all get dispatched on first heartbeat.

	localAgent := newMockAgent()
	defer localAgent.Close()

	// Jobs that were running on the OLD leader (now persisted in state)
	daemonJob := &types.Job{Name: "daemon", Command: "./daemon", Count: -1}
	regularJobs := []*types.Job{
		{Name: "job-1", Command: "./job1", Count: 1},
		{Name: "job-2", Command: "./job2", Count: 1},
		{Name: "job-3", Command: "./job3", Count: 1},
		{Name: "job-4", Command: "./job4", Count: 1},
		{Name: "job-5", Command: "./job5", Count: 1},
		{Name: "job-6", Command: "./job6", Count: 1},
	}

	// New leader loads jobs from persisted state (old leader's state)
	newLeaderStore := NewMockJobStore()
	newLeaderStore.StoreJob(daemonJob)
	for _, job := range regularJobs {
		newLeaderStore.StoreJob(job)
	}
	newLeaderStore.stateTime = time.Now().Add(-1 * time.Minute)

	newLeader := New("new-leader-id", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// New leader's local agent registers with NO jobs running
	// This triggers reconcileJobs → dispatches all 7 jobs
	newLeader.RegisterAgent("new-leader-id", localAgent.URL(), "", nil)
	time.Sleep(100 * time.Millisecond)

	// All 7 jobs should be dispatched: 1 daemon + 6 regular
	runCalls := localAgent.RunCallCount()
	if runCalls != 7 {
		t.Errorf("Expected 7 /run calls (daemon + 6 regular), got %d", runCalls)
	}

	// Daemon should be in placement
	daemonPlaced := newLeader.GetPlaced(daemonJob.Name)
	if len(daemonPlaced) != 1 {
		t.Errorf("Daemon should be in placement, got %d entries", len(daemonPlaced))
	}
}

func TestFailoverNewLeaderWithExistingDaemon(t *testing.T) {
	// When agent already has daemon running, reconcileJobs sees it via GetClusterStatus
	// and only dispatches missing regular jobs.

	localAgent := newMockAgent()
	defer localAgent.Close()

	daemon := &types.Job{Name: "daemon", Command: "./daemon", Count: -1}
	regularJobs := []*types.Job{
		{Name: "job-1", Command: "./job1", Count: 1},
		{Name: "job-2", Command: "./job2", Count: 1},
		{Name: "job-3", Command: "./job3", Count: 1},
		{Name: "job-4", Command: "./job4", Count: 1},
		{Name: "job-5", Command: "./job5", Count: 1},
		{Name: "job-6", Command: "./job6", Count: 1},
	}

	// New leader loads ALL jobs from persisted state
	newLeaderStore := NewMockJobStore()
	newLeaderStore.StoreJob(daemon)
	for _, job := range regularJobs {
		newLeaderStore.StoreJob(job)
	}
	newLeaderStore.stateTime = time.Now().Add(-1 * time.Minute)

	newLeader := New("new-leader-id", newLeaderStore, nil)
	newLeader.settleDelay = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// Simulate: local agent already has the daemon running (from before failover)
	localAgent.mu.Lock()
	localAgent.jobs[daemon.Name] = daemon
	localAgent.tasks = append(localAgent.tasks, &types.Task{
		ID:      "daemon-task",
		JobName: daemon.Name,
		State:   types.TaskRunning,
	})
	localAgent.mu.Unlock()

	// During settle: register with placed counts + heartbeat for keepalive (no dispatch yet)
	newLeader.RegisterAgent("new-leader-id", localAgent.URL(), "", map[string]int{"daemon": 1})
	newLeader.Heartbeat("new-leader-id", "", 0)

	// Wait for settle timer → reconcile sees daemon placed → dispatches only regular jobs
	time.Sleep(400 * time.Millisecond)

	// 6 regular jobs dispatched (daemon already running, placed from RegisterAgent)
	runCalls := localAgent.RunCallCount()
	if runCalls != 6 {
		t.Errorf("Expected 6 /run calls (regular jobs only, daemon already running), got %d", runCalls)
	}

	// Daemon placement should be learned from heartbeat
	daemonPlaced := newLeader.GetPlaced(daemon.Name)
	if len(daemonPlaced) != 1 {
		t.Errorf("Daemon should have 1 placement, got %d", len(daemonPlaced))
	}
}

func TestFailoverJobsWithAffinity(t *testing.T) {
	// Jobs with affinity constraints are dispatched to all agents.
	// Affinity is checked agent-side (agent rejects with 406 if no match).
	// The leader doesn't know about attributes — it just dispatches everywhere.

	localAgent := newMockAgent()
	defer localAgent.Close()

	// Jobs with affinity (agent-side check, mock agent accepts all)
	affinityJobs := []*types.Job{
		{Name: "job-1", Command: "./job1", Count: 1, Affinity: map[string]string{"node.id": "old-leader-id"}},
		{Name: "job-2", Command: "./job2", Count: 1, Affinity: map[string]string{"node.id": "old-leader-id"}},
		{Name: "job-3", Command: "./job3", Count: 1, Affinity: map[string]string{"node.id": "old-leader-id"}},
	}
	// Jobs without affinity
	normalJobs := []*types.Job{
		{Name: "job-4", Command: "./job4", Count: 1},
		{Name: "job-5", Command: "./job5", Count: 1},
		{Name: "job-6", Command: "./job6", Count: 1},
	}
	daemon := &types.Job{Name: "daemon", Command: "./daemon", Count: -1}

	newLeaderStore := NewMockJobStore()
	newLeaderStore.StoreJob(daemon)
	for _, job := range affinityJobs {
		newLeaderStore.StoreJob(job)
	}
	for _, job := range normalJobs {
		newLeaderStore.StoreJob(job)
	}

	newLeader := New("new-leader-id", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// New leader's local agent registers → reconcileJobs dispatches
	newLeader.RegisterAgent("new-leader-id", localAgent.URL(), "", nil)
	time.Sleep(100 * time.Millisecond)

	// Leader dispatches all 7 jobs (affinity checked agent-side, mock accepts all)
	runCalls := localAgent.RunCallCount()
	if runCalls != 7 {
		t.Errorf("Expected 7 /run calls (leader dispatches all, affinity checked agent-side), got %d", runCalls)
	}
}

func TestFailoverSecondHeartbeatDoesNotReschedule(t *testing.T) {
	// Jobs only get rescheduled on FIRST heartbeat (isNew=true).
	// This documents current behavior - not necessarily a bug.

	localAgent := newMockAgent()
	defer localAgent.Close()

	jobs := []*types.Job{
		{Name: "daemon", Command: "./daemon", Count: -1},
		{Name: "job-1", Command: "./job1", Count: 1},
	}

	newLeaderStore := NewMockJobStore()
	for _, job := range jobs {
		newLeaderStore.StoreJob(job)
	}

	newLeader := New("new-leader-id", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// First heartbeat - agent rejects all /run calls
	localAgent.SetFailRuns(true)
	newLeader.RegisterAgent("new-leader-id", localAgent.URL(), "", nil)
	newLeader.Heartbeat("new-leader-id", "", 0)
	time.Sleep(50 * time.Millisecond)

	// Now agent is healthy
	localAgent.SetFailRuns(false)

	// Second heartbeat - agent is no longer "new", no rescheduling
	newLeader.Heartbeat("new-leader-id", "", 0)
	time.Sleep(50 * time.Millisecond)

	// Current behavior: no rescheduling on second heartbeat
	t.Logf("Run calls after second heartbeat: %d", localAgent.RunCallCount())
}

func TestFailoverAgentDiesTasksRescheduled(t *testing.T) {
	// KISS refactor: This test verifies reconciliation via GetClusterStatus.
	// We need to actually dispatch jobs (not just set placement) for GetClusterStatus
	// to return accurate task counts.

	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentB.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agents first
	leader.RegisterAgent("agent-a", agentA.URL(), "", nil)
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	// Actually dispatch the job (this creates real tasks in mock agents)
	job := &types.Job{
		Name:    "ticker",
		Command: "sh -c 'while :; do echo tick; sleep 1; done'",
		Count:   20,
	}
	err := leader.DispatchJob(job)
	if err != nil {
		t.Fatalf("Failed to dispatch: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Verify initial dispatch (round-robin across both agents)
	aRuns := agentA.TaskCount()
	bRuns := agentB.TaskCount()
	if aRuns+bRuns != 20 {
		t.Fatalf("Should have 20 tasks total, got %d", aRuns+bRuns)
	}

	// Agent A dies
	agentA.Close()
	time.Sleep(300 * time.Millisecond)

	// Keep B alive
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	// Trigger dead agent check - reconcileJobs will see B's tasks and dispatch missing
	leader.checkDeadAgents()
	time.Sleep(100 * time.Millisecond)

	// Agent B should now have all 20 tasks
	finalBTasks := agentB.TaskCount()
	if finalBTasks != 20 {
		t.Errorf("Agent B should have 20 tasks after reconciliation, got %d", finalBTasks)
	}
}

func TestFailoverHeartbeatLearnsWrongPlacementCount(t *testing.T) {
	// OBSOLETE: This test was for the old heartbeat-based placement learning.
	// KISS REFACTOR: Placement is NO LONGER learned from heartbeats.
	// Instead, reconcileJobs() queries GetClusterStatus() for actual running tasks.
	//
	// This test now verifies that heartbeats still register agents and sync jobs,
	// but does NOT verify placement (which is only tracked on dispatch).

	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	job := &types.Job{
		Name:    "ticker",
		Command: "sh -c 'while :; do echo tick; sleep 1; done'",
		Count:   20,
	}
	store.StoreJob(job)

	// Agents heartbeat - they are registered but placement is NOT learned
	leader.RegisterAgent("agent-a", agentA.URL(), "", nil)
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	// Verify agents are registered
	agents := leader.GetAgents()
	if len(agents) != 2 {
		t.Errorf("Should have 2 agents, got %d", len(agents))
	}

	// KISS: Placement is NOT tracked from heartbeats.
	// GetClusterStatus() is used for reconciliation instead.
	t.Log("KISS refactor: placement is tracked on dispatch, not heartbeat")
}

// TestRedispatchDoesNotCorruptJobCount verifies that when an agent dies and its
// tasks are rescheduled, the original job's Count is NOT corrupted.
//
// BUG: redispatchJobsFrom() creates a job copy with Count=instancesOnFailedAgent,
// then DispatchJob() calls StoreJob() which OVERWRITES the original job definition.
//
// Example:
// - ticker job has Count=20, with 2 instances on dead agent
// - After redispatch, job.Count becomes 2 instead of 20!
// - caddy job has Count=-1 (run on all agents), with 1 instance on dead agent
// - After redispatch, job.Count becomes 1 instead of -1!
func TestRedispatchDoesNotCorruptJobCount(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Job with count=20
	tickerJob := &types.Job{
		Name:    "ticker",
		Command: "sh -c 'while :; do echo tick; sleep 1; done'",
		Count:   20,
	}
	store.StoreJob(tickerJob)

	// Job with count=-1 (run on all agents)
	caddyJob := &types.Job{
		Name:    "caddy",
		Command: "./caddy",
		Count:   -1,
	}
	store.StoreJob(caddyJob)

	// Register agents and set up placement
	// Agent A has 2 ticker tasks and 1 caddy task
	// Agent B has 18 ticker tasks and 1 caddy task
	leader.RegisterAgent("agent-a", agentA.URL(), "", nil)
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	// Verify initial state
	if job := store.GetJob("ticker"); job.Count != 20 {
		t.Fatalf("Initial ticker count should be 20, got %d", job.Count)
	}
	if job := store.GetJob("caddy"); job.Count != -1 {
		t.Fatalf("Initial caddy count should be -1, got %d", job.Count)
	}

	// Unregister agent A (simulating crash, but agent B stays alive to receive redispatched tasks)
	leader.UnregisterAgent("agent-a")
	time.Sleep(100 * time.Millisecond)

	// BUG CHECK: Job counts should NOT be corrupted!
	tickerAfter := store.GetJob("ticker")
	if tickerAfter == nil {
		t.Fatal("ticker job should still exist")
	}
	if tickerAfter.Count != 20 {
		t.Errorf("BUG: ticker count corrupted! Expected 20, got %d (was set to instances on dead agent)", tickerAfter.Count)
	}

	caddyAfter := store.GetJob("caddy")
	if caddyAfter == nil {
		t.Fatal("caddy job should still exist")
	}
	if caddyAfter.Count != -1 {
		t.Errorf("BUG: caddy count corrupted! Expected -1, got %d (was set to instances on dead agent)", caddyAfter.Count)
	}
}

// TestCountMinusOneNotRedispatchedOnAgentDeath verifies that count=-1 jobs
// are NOT redispatched when an agent dies.
//
// count=-1 means "run exactly once per agent". If an agent dies, its instance
// should NOT be moved to another agent - other agents already have their own instance.
//
// BUG: Current code redispatches count=-1 jobs like regular jobs, causing
// duplicate instances on surviving agents.
func TestCountMinusOneNotRedispatchedOnAgentDeath(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// count=-1 job (run on all agents, exactly once per agent)
	daemonJob := &types.Job{
		Name:    "daemon",
		Command: "./daemon",
		Count:   -1,
	}
	store.StoreJob(daemonJob)

	// Both agents have the daemon running (1 instance each)
	leader.RegisterAgent("agent-a", agentA.URL(), "", map[string]int{"daemon": 1})
	leader.RegisterAgent("agent-b", agentB.URL(), "", map[string]int{"daemon": 1})
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	// Reset run counters
	agentA.ResetRunCount()
	agentB.ResetRunCount()

	// Agent A dies
	leader.UnregisterAgent("agent-a")
	time.Sleep(100 * time.Millisecond)

	// BUG CHECK: Agent B should NOT receive any /run calls for the daemon!
	// count=-1 jobs should NOT be redispatched - agent B already has its own instance
	if agentB.RunCallCount() != 0 {
		t.Errorf("BUG: count=-1 job was redispatched! Agent B received %d /run calls, expected 0", agentB.RunCallCount())
	}

	// Verify placement only has agent-b now (not duplicated)
	placed := leader.GetPlaced(daemonJob.Name)
	if len(placed) != 1 {
		t.Errorf("BUG: Placement should have 1 entry (agent-b only), got %d: %v", len(placed), placed)
	}
}

// TestRedispatchCorrectInstanceCount verifies that reconciliation dispatches
// the correct number of missing instances based on GetClusterStatus().
//
// KISS refactor: reconcileJobs() queries actual running tasks, not placement.
func TestRedispatchCorrectInstanceCount(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agents first
	leader.RegisterAgent("agent-a", agentA.URL(), "", nil)
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	// Job with count=20 - dispatch it so placement is tracked
	job := &types.Job{
		Name:    "my-job",
		Command: "./job",
		Count:   20,
	}
	err := leader.DispatchJob(job)
	if err != nil {
		t.Fatalf("Failed to dispatch: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Verify both agents received tasks (round-robin)
	aRuns := agentA.RunCallCount()
	bRuns := agentB.RunCallCount()
	if aRuns+bRuns != 20 {
		t.Fatalf("Initial dispatch should be 20, got %d", aRuns+bRuns)
	}

	// Reset run counters
	agentA.ResetRunCount()
	agentB.ResetRunCount()

	// Agent A dies - reconcileJobs() will query GetClusterStatus() and see
	// only agent B's tasks, then dispatch the missing instances
	leader.UnregisterAgent("agent-a")
	time.Sleep(100 * time.Millisecond)

	// Agent B should receive enough /run calls to reach count=20
	// (reconciliation sees B's tasks via GetClusterStatus and dispatches missing)
	totalAfter := bRuns + agentB.RunCallCount() // B had bRuns, now gets more
	if totalAfter != 20 {
		t.Errorf("After reconciliation, agent B should have 20 total tasks, got %d", totalAfter)
	}
}

// TestRedispatchWithStalePlacement documents how the KISS refactor fixes stale data issues.
//
// OLD BEHAVIOR (pre-KISS):
// - Leader tracked placement from heartbeats
// - If heartbeat had wrong counts, redispatch would be wrong
//
// NEW BEHAVIOR (KISS refactor):
// - reconcileJobs() queries GetClusterStatus() for ACTUAL running tasks
// - Doesn't matter what heartbeat reported - we see reality
// - Result: correct reconciliation even with stale/incorrect heartbeat data
func TestRedispatchWithStalePlacement(t *testing.T) {
	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agents first
	leader.RegisterAgent("agent-a", agentA.URL(), "", nil)
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	// Job with count=20 - actually dispatch it
	job := &types.Job{
		Name:    "ticker",
		Command: "./ticker",
		Count:   20,
	}
	err := leader.DispatchJob(job)
	if err != nil {
		t.Fatalf("Failed to dispatch: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Verify initial dispatch
	aRuns := agentA.TaskCount()
	bRuns := agentB.TaskCount()
	t.Logf("Initial tasks: agent-a=%d, agent-b=%d (total %d)", aRuns, bRuns, aRuns+bRuns)
	if aRuns+bRuns != 20 {
		t.Fatalf("Should have 20 tasks total, got %d", aRuns+bRuns)
	}

	// Agent A dies - reconcileJobs() will query GetClusterStatus() and see
	// only agent B's ACTUAL tasks, then dispatch missing instances
	agentA.Close() // Close so GetClusterStatus can't reach it
	leader.UnregisterAgent("agent-a")
	time.Sleep(100 * time.Millisecond)

	// KISS FIX: reconcileJobs sees bRuns tasks on B via GetClusterStatus
	// It dispatches (20 - bRuns) = aRuns missing instances to B
	finalBTasks := agentB.TaskCount()
	t.Logf("After reconciliation: agent-b has %d tasks", finalBTasks)

	// Should reach count=20 (all on B now)
	if finalBTasks != 20 {
		t.Errorf("KISS FIX: Should have 20 tasks on agent-b, got %d", finalBTasks)
	}

	t.Log("KISS refactor: reconciliation uses actual cluster state, not stale placement")
}

func TestFailoverAgentDiesRealisticHeartbeat(t *testing.T) {
	// Tests the KISS refactor: when an agent dies, reconcileJobs() queries
	// GetClusterStatus() to find actual running tasks and dispatches missing instances.
	//
	// This is more robust than the old heartbeat-based placement tracking:
	// - Doesn't rely on heartbeat data accuracy
	// - Queries actual cluster state (via /tasks endpoint)
	// - Correctly handles any number of lost instances

	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentB.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agents FIRST (before job exists)
	leader.RegisterAgent("agent-a", agentA.URL(), "", nil)
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)
	leader.Heartbeat("agent-a", "", 0)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	// Create and dispatch job with count=20
	job := &types.Job{
		Name:    "ticker",
		Command: "sh -c 'while :; do echo tick; sleep 1; done'",
		Count:   20,
	}
	store.StoreJob(job)

	// Dispatch job - should create 20 tasks round-robin across 2 agents
	err := leader.DispatchJob(job)
	if err != nil {
		t.Fatalf("Failed to dispatch: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Check initial dispatch
	aRuns := agentA.RunCallCount()
	bRuns := agentB.RunCallCount()
	t.Logf("Initial dispatch: agent-a got %d, agent-b got %d (total %d)", aRuns, bRuns, aRuns+bRuns)

	if aRuns+bRuns != 20 {
		t.Fatalf("Should have dispatched 20 tasks, got %d", aRuns+bRuns)
	}

	// Track agent B's initial task count
	bInitialTasks := agentB.TaskCount()

	// CHAOS: Agent A crashes
	agentA.Close()
	time.Sleep(300 * time.Millisecond)

	// Keep agent B alive
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(20 * time.Millisecond)

	// Trigger dead agent check - this calls reconcileJobs()
	leader.checkDeadAgents()
	time.Sleep(100 * time.Millisecond)

	// KISS verification: agent B should now have all 20 tasks
	// (its original tasks + rescheduled from agent A)
	bFinalTasks := agentB.TaskCount()
	t.Logf("After agent-a death: agent-b has %d tasks (was %d)", bFinalTasks, bInitialTasks)

	// reconcileJobs saw bInitialTasks on B, needed 20, dispatched (20-bInitialTasks)
	if bFinalTasks != 20 {
		t.Errorf("Agent B should have 20 tasks total, got %d", bFinalTasks)
	}

	// Note: GetPlaced() may have stale entries from dead agent.
	// KISS refactor doesn't rely on placement for reconciliation - it queries GetClusterStatus().
}

// TestFailoverFailedTasksNotRedispatched verifies that failed tasks (which exhausted
// their restart counter) are NOT re-dispatched during leader failover.
//
// Failed tasks count as "placed" because the leader dispatched them and the agent's
// monitor already tried restarting them. Re-dispatching would just fail again.
//
// Scenario:
// 1. Job "my-api" has count=3
// 2. Agent A has 2 running + 1 failed task (restart counter exhausted)
// 3. Agent A registers with new leader: placed = {my-api: 3} (includes failed)
// 4. Leader sees: 3 placed, 3 desired → no re-dispatch
// 5. CORRECT: failed task stays failed, no unnecessary dispatch
func TestFailoverFailedTasksNotRedispatched(t *testing.T) {
	agentA := newMockAgent()
	defer agentA.Close()
	agentB := newMockAgent()
	defer agentB.Close()

	store := NewMockJobStore()
	job := &types.Job{
		Name:    "my-api",
		Command: "./api",
		Count:   3,
	}
	store.StoreJob(job)

	leader := New("leader", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	leader.SetSettleDelay(100 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Agent A has 2 running + 1 failed task (restart counter exhausted)
	agentA.mu.Lock()
	agentA.tasks = []*types.Task{
		{ID: "task-1", JobName: "my-api", State: types.TaskRunning},
		{ID: "task-2", JobName: "my-api", State: types.TaskRunning},
		{ID: "task-3", JobName: "my-api", State: types.TaskFailed},
	}
	agentA.mu.Unlock()

	// GetPlacedTaskCounts includes failed tasks (3 total, not 2)
	leader.RegisterAgent("agent-a", agentA.URL(), "", map[string]int{"my-api": 3})
	leader.RegisterAgent("agent-b", agentB.URL(), "", nil)

	// Wait for settle + reconciliation
	time.Sleep(300 * time.Millisecond)

	// Leader sees 3 placed (including failed), 3 desired → no action
	// Failed task already exhausted restarts, re-dispatching would just fail again
	totalRuns := agentA.RunCallCount() + agentB.RunCallCount()
	if totalRuns != 0 {
		t.Errorf("Leader should NOT re-dispatch for failed tasks (restart counter exhausted), but dispatched %d", totalRuns)
	}
}

// TestFailoverPreemptionStopFailureNoGhosts verifies that when a stop fails during
// preemption, the placed data is NOT deleted. This prevents ghost tasks (tasks running
// on agents but invisible to the leader's placed map).
func TestFailoverPreemptionStopFailureNoGhosts(t *testing.T) {
	// 2 agents, capacity 3 each = 6 slots
	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()
	agentA.SetMaxCapacity(3)
	agentB.SetMaxCapacity(3)

	// Pre-populate: each agent has 3 "low" tasks (from old leader)
	agentA.AddTasks("low", 3)
	agentB.AddTasks("low", 3)

	store := NewMockJobStore()
	l := New("leader", store, &httputil.Client{Timeout: 200 * time.Millisecond})
	l.SetSettleDelay(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)

	// Store both jobs: high prio should preempt low
	store.StoreJob(&types.Job{Name: "high", Command: "./high", Count: 6, Priority: prio(0)})
	store.StoreJob(&types.Job{Name: "low", Command: "./low", Count: 6, Priority: prio(1)})

	// Agent A's stops will fail (simulates network issue during failover)
	agentA.SetFailStops(true)

	// Register agents with their actual placed counts
	l.RegisterAgent("agent-a", agentA.URL(), "", agentA.PlacedCounts())
	l.RegisterAgent("agent-b", agentB.URL(), "", agentB.PlacedCounts())

	// Wait for settle + reconciliation
	time.Sleep(400 * time.Millisecond)

	// Agent A: stop failed → low tasks should still be tracked in placed
	// Agent B: stop succeeded → low tasks replaced by high tasks
	placed := l.GetPlacedCounts()

	// Key assertion: placed counts must match actual running tasks (no ghost tasks)
	actualA := agentA.TaskCount()
	actualB := agentB.TaskCount()
	totalPlaced := 0
	for _, c := range placed {
		totalPlaced += c
	}

	t.Logf("Agent A: %d tasks, Agent B: %d tasks, placed: %v", actualA, actualB, placed)

	if totalPlaced != actualA+actualB {
		t.Errorf("Ghost tasks detected: placed=%d but actual=%d (agent-a=%d, agent-b=%d)",
			totalPlaced, actualA+actualB, actualA, actualB)
	}

	// Low should still be in placed (agent A's stop failed, tasks still running)
	if placed["low"] == 0 {
		t.Errorf("Low job should still be tracked in placed (stop failed on agent-a), but placed[low]=%d", placed["low"])
	}

	// Agent A should still have 3 low tasks (stop failed, nothing changed)
	if agentA.TasksForJob("low") != 3 {
		t.Errorf("Agent A should still have 3 low tasks (stop failed), got %d", agentA.TasksForJob("low"))
	}
}

// TestFailoverPreemptionStopSuccessReplacesGhosts verifies the happy path:
// when stops succeed during preemption, low-priority tasks are correctly replaced.
func TestFailoverPreemptionStopSuccessReplacesGhosts(t *testing.T) {
	// 2 agents, capacity 3 each = 6 slots
	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()
	agentA.SetMaxCapacity(3)
	agentB.SetMaxCapacity(3)

	// Pre-populate: each agent has 3 "low" tasks (from old leader)
	agentA.AddTasks("low", 3)
	agentB.AddTasks("low", 3)

	store := NewMockJobStore()
	l := New("leader", store, &httputil.Client{Timeout: 200 * time.Millisecond})
	l.SetSettleDelay(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)

	// Store both jobs: high prio should preempt low
	store.StoreJob(&types.Job{Name: "high", Command: "./high", Count: 6, Priority: prio(0)})
	store.StoreJob(&types.Job{Name: "low", Command: "./low", Count: 6, Priority: prio(1)})

	// Register agents with actual placed counts (all stops will succeed)
	l.RegisterAgent("agent-a", agentA.URL(), "", agentA.PlacedCounts())
	l.RegisterAgent("agent-b", agentB.URL(), "", agentB.PlacedCounts())

	// Wait for settle + reconciliation
	time.Sleep(400 * time.Millisecond)

	placed := l.GetPlacedCounts()
	t.Logf("Agent A: %d tasks (high=%d, low=%d), Agent B: %d tasks (high=%d, low=%d), placed: %v",
		agentA.TaskCount(), agentA.TasksForJob("high"), agentA.TasksForJob("low"),
		agentB.TaskCount(), agentB.TasksForJob("high"), agentB.TasksForJob("low"), placed)

	// All 6 slots should be high-priority (low fully preempted)
	if placed["high"] != 6 {
		t.Errorf("Expected 6 high-priority tasks placed, got %d", placed["high"])
	}

	// No low tasks should remain (all preempted)
	totalLow := agentA.TasksForJob("low") + agentB.TasksForJob("low")
	if totalLow > 0 {
		t.Errorf("Expected 0 low tasks after preemption, got %d", totalLow)
	}

	// Total placed = total actual
	actualTotal := agentA.TaskCount() + agentB.TaskCount()
	totalPlaced := 0
	for _, c := range placed {
		totalPlaced += c
	}
	if totalPlaced != actualTotal {
		t.Errorf("placed=%d != actual=%d", totalPlaced, actualTotal)
	}
}

// TestRegisterAgentDuplicateIDRejected verifies that registering an agent
// with the same ID but different endpoint is rejected (409 Conflict).
func TestRegisterAgentDuplicateIDRejected(t *testing.T) {
	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// First registration succeeds
	ok := l.RegisterAgent("node-1", "http://10.0.0.1:8080", "v1", nil)
	if !ok {
		t.Fatal("first registration should succeed")
	}

	// Same ID, same endpoint = re-registration (restart), should succeed
	ok = l.RegisterAgent("node-1", "http://10.0.0.1:8080", "v2", nil)
	if !ok {
		t.Fatal("re-registration with same endpoint should succeed")
	}

	// Same ID, different endpoint = duplicate, should be rejected
	ok = l.RegisterAgent("node-1", "http://10.0.0.2:8080", "v1", nil)
	if ok {
		t.Error("duplicate agent ID with different endpoint should be rejected")
	}

	// Original agent should still be registered with its endpoint
	agents := l.GetAgents()
	if len(agents) != 1 {
		t.Fatalf("expected 1 agent, got %d", len(agents))
	}
	if agents[0].Endpoint != "http://10.0.0.1:8080" {
		t.Errorf("agent endpoint = %q, want original", agents[0].Endpoint)
	}
}

// TestRegisterAgentDuplicateIDAllowedAfterTimeout verifies that a duplicate
// agent ID is accepted when the original agent has timed out (dead).
func TestRegisterAgentDuplicateIDAllowedAfterTimeout(t *testing.T) {
	store := NewMockJobStore()
	l := New("leader", store, nil)
	l.agentTimeout = 50 * time.Millisecond // short timeout for test

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// Register original
	l.RegisterAgent("node-1", "http://10.0.0.1:8080", "v1", nil)

	// Wait for timeout
	time.Sleep(100 * time.Millisecond)

	// Same ID, different endpoint — original timed out, should succeed
	ok := l.RegisterAgent("node-1", "http://10.0.0.2:8080", "v1", nil)
	if !ok {
		t.Error("should allow re-registration after original agent timed out")
	}

	agents := l.GetAgents()
	if len(agents) != 1 {
		t.Fatalf("expected 1 agent, got %d", len(agents))
	}
	if agents[0].Endpoint != "http://10.0.0.2:8080" {
		t.Errorf("agent endpoint = %q, want new endpoint", agents[0].Endpoint)
	}
}

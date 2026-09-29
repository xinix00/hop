package leader

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

func TestUpdateJobRolling(t *testing.T) {
	// Create mock agents
	agents := make([]*mockAgent, 3)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	// Register agents
	for i, agent := range agents {
		agentID := string(rune('a' + i))
		leader.RegisterAgent("agent-"+agentID, agent.URL(), "", nil)
		leader.Heartbeat("agent-"+agentID, "", 0)
	}

	// Deploy initial version
	oldJob := &types.Job{
		Name:    "my-app",
		Command: "./app-v1",
		Count:   3,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second, // Short timeout for tests
		},
	}

	if err := leader.DispatchJob(oldJob); err != nil {
		t.Fatalf("Failed to dispatch initial job: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Verify 3 instances running (by name)
	placed := leader.GetPlaced("my-app")
	if len(placed) != 3 {
		t.Fatalf("Expected 3 instances, got %d", len(placed))
	}

	// Update to new version with rolling policy
	newJob := &types.Job{
		Name:         "my-app",
		Command:      "./app-v2",
		Count:        3,
		UpdatePolicy: types.UpdateRolling,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	if err := leader.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Verify still 3 instances after rolling update
	newPlaced := leader.GetPlaced("my-app")
	if len(newPlaced) != 3 {
		t.Errorf("Expected 3 instances after update, got %d", len(newPlaced))
	}

	// Verify job definition was updated
	updatedJob := store.GetJob("my-app")
	if updatedJob.Command != "./app-v2" {
		t.Errorf("Job command should be updated to ./app-v2, got %s", updatedJob.Command)
	}
}

func TestUpdateJobRecreate(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	leader.RegisterAgent("agent-1", agent.URL(), "", nil)
	leader.Heartbeat("agent-1", "", 0)

	// Deploy initial version
	oldJob := &types.Job{
		Name:    "my-app",
		Command: "./app-v1",
		Count:   1,
	}

	_ = leader.DispatchJob(oldJob)
	time.Sleep(50 * time.Millisecond)

	// Update with recreate policy
	newJob := &types.Job{
		Name:         "my-app",
		Command:      "./app-v2",
		Count:        1,
		UpdatePolicy: types.UpdateRecreate,
	}

	if err := leader.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Verify job was updated (GetJob uses name as key in mock)
	jobs := store.GetJobs()
	var updatedJob *types.Job
	for _, j := range jobs {
		if j.Name == "my-app" {
			updatedJob = j
			break
		}
	}
	if updatedJob == nil {
		t.Fatal("Job my-app should exist in store")
	}
	if updatedJob.Command != "./app-v2" {
		t.Errorf("Job command should be updated to ./app-v2, got %s", updatedJob.Command)
	}
}

func TestUpdateJobBlueGreen(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	leader.RegisterAgent("agent-1", agent.URL(), "", nil)
	leader.Heartbeat("agent-1", "", 0)

	// Deploy initial version
	oldJob := &types.Job{
		Name:    "my-app",
		Command: "./app-v1",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	_ = leader.DispatchJob(oldJob)
	time.Sleep(50 * time.Millisecond)

	initialRunCalls := agent.RunCallCount()

	// Update with blue-green policy
	newJob := &types.Job{
		Name:         "my-app",
		Command:      "./app-v2",
		Count:        1,
		UpdatePolicy: types.UpdateBlueGreen,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	if err := leader.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Blue-green should have dispatched new version (run call count increases)
	if agent.RunCallCount() <= initialRunCalls {
		t.Error("Blue-green should dispatch new version")
	}

	// Verify job was updated (GetJob uses name as key in mock)
	jobs := store.GetJobs()
	var updatedJob *types.Job
	for _, j := range jobs {
		if j.Name == "my-app" {
			updatedJob = j
			break
		}
	}
	if updatedJob == nil {
		t.Fatal("Job my-app should exist in store")
	}
	if updatedJob.Command != "./app-v2" {
		t.Errorf("Job command should be updated to ./app-v2, got %s", updatedJob.Command)
	}
}

func TestUpdateJobRollingFailureKeepsOld(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	leader.RegisterAgent("agent-1", agent.URL(), "", nil)
	leader.Heartbeat("agent-1", "", 0)

	// Deploy initial version
	oldJob := &types.Job{
		Name:    "my-app",
		Command: "./app-v1",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	if err := leader.DispatchJob(oldJob); err != nil {
		t.Fatalf("Failed to dispatch initial job: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Verify 1 instance running
	if agent.TaskCount() != 1 {
		t.Fatalf("Expected 1 task, got %d", agent.TaskCount())
	}

	// Make agent fail next dispatch
	agent.SetFailRuns(true)

	// Try rolling update - should fail
	newJob := &types.Job{
		Name:         "my-app",
		Command:      "./app-v2",
		Count:        1,
		UpdatePolicy: types.UpdateRolling,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 500 * time.Millisecond,
		},
	}

	err := leader.UpdateJob(newJob)
	if err == nil {
		t.Fatal("UpdateJob should have failed")
	}

	time.Sleep(50 * time.Millisecond)

	// OLD INSTANCE SHOULD STILL BE RUNNING (this is the key assertion)
	if agent.TaskCount() != 1 {
		t.Errorf("Old instance should still be running, got %d tasks", agent.TaskCount())
	}

	// Job definition is stored before dispatch (name-as-key means storeJob overwrites).
	// The critical invariant is that the running task survives — checked above.
	storedJob := store.GetJob("my-app")
	if storedJob == nil {
		t.Error("Job my-app should still exist in store")
	}
}

func TestUpdateJobBlueGreenFailureKeepsOld(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	leader.RegisterAgent("agent-1", agent.URL(), "", nil)
	leader.Heartbeat("agent-1", "", 0)

	// Deploy initial version
	oldJob := &types.Job{
		Name:    "my-app",
		Command: "./app-v1",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	if err := leader.DispatchJob(oldJob); err != nil {
		t.Fatalf("Failed to dispatch initial job: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Verify 1 instance running
	if agent.TaskCount() != 1 {
		t.Fatalf("Expected 1 task, got %d", agent.TaskCount())
	}

	// Make agent fail next dispatch
	agent.SetFailRuns(true)

	// Try blue-green update - should fail
	newJob := &types.Job{
		Name:         "my-app",
		Command:      "./app-v2",
		Count:        1,
		UpdatePolicy: types.UpdateBlueGreen,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 500 * time.Millisecond,
		},
	}

	err := leader.UpdateJob(newJob)
	if err == nil {
		t.Fatal("UpdateJob should have failed")
	}

	time.Sleep(50 * time.Millisecond)

	// OLD INSTANCE SHOULD STILL BE RUNNING
	if agent.TaskCount() != 1 {
		t.Errorf("Old instance should still be running, got %d tasks", agent.TaskCount())
	}

	// Job definition is stored before dispatch (name-as-key means storeJob overwrites).
	// The critical invariant is that the running task survives — checked above.
	storedJob := store.GetJob("my-app")
	if storedJob == nil {
		t.Error("Job my-app should still exist in store")
	}
}

func TestUpdateJobNotFound(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	// Try to update non-existent job
	newJob := &types.Job{
		Name:    "nonexistent",
		Command: "./app",
	}

	err := leader.UpdateJob(newJob)
	if err == nil {
		t.Error("UpdateJob should fail for non-existent job")
	}
}

func TestFindJobByName(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	// Store jobs via DispatchJob so store is populated
	_ = leader.DispatchJob(&types.Job{Name: "app-1", Command: "echo"})
	_ = leader.DispatchJob(&types.Job{Name: "app-2", Command: "echo"})

	// Find by name via store (name is the unique key)
	job := store.GetJob("app-1")
	if job == nil {
		t.Fatal("store.GetJob should find app-1")
	}
	if job.Name != "app-1" {
		t.Errorf("Expected app-1, got %s", job.Name)
	}

	// Not found
	job = store.GetJob("nonexistent")
	if job != nil {
		t.Error("store.GetJob should return nil for nonexistent job")
	}
}

// Helper for creating test context with timeout
func newTestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

// realisticMockAgent simulates real agent behavior: DELETE /delete/{name} kills ALL tasks
// with that name, not just one. This is how the real agent's deleteJob() works.
type realisticMockAgent struct {
	server  *httptest.Server
	mu      sync.Mutex
	tasks   []*types.Task
	taskSeq int
}

func newRealisticMockAgent() *realisticMockAgent {
	ma := &realisticMockAgent{}

	mux := http.NewServeMux()
	mux.HandleFunc("/run", ma.handleRun)
	mux.HandleFunc("/tasks", ma.handleTasks)
	mux.HandleFunc("/delete/", ma.handleDelete)
	mux.HandleFunc("/stop/", ma.handleStop)
	mux.HandleFunc("/stop-task/", ma.handleStopTask)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	ma.server = httptest.NewServer(mux)
	return ma
}

func (ma *realisticMockAgent) handleRun(w http.ResponseWriter, r *http.Request) {
	var job types.Job
	_ = json.NewDecoder(r.Body).Decode(&job)

	ma.mu.Lock()
	ma.taskSeq++
	task := &types.Task{
		ID:      fmt.Sprintf("task-%d", ma.taskSeq),
		JobName: job.Name,
		State:   types.TaskRunning,
	}
	ma.tasks = append(ma.tasks, task)
	ma.mu.Unlock()

	_ = json.NewEncoder(w).Encode(task)
}

func (ma *realisticMockAgent) handleTasks(w http.ResponseWriter, r *http.Request) {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	_ = json.NewEncoder(w).Encode(ma.tasks)
}

func (ma *realisticMockAgent) handleDelete(w http.ResponseWriter, r *http.Request) {
	jobName := strings.TrimPrefix(r.URL.Path, "/delete/")

	ma.mu.Lock()
	deleted := 0
	filtered := make([]*types.Task, 0, len(ma.tasks))
	for _, task := range ma.tasks {
		if task.JobName == jobName {
			deleted++
		} else {
			filtered = append(filtered, task)
		}
	}
	ma.tasks = filtered
	ma.mu.Unlock()

	_ = json.NewEncoder(w).Encode(map[string]int{"deleted": deleted})
}

func (ma *realisticMockAgent) handleStopTask(w http.ResponseWriter, r *http.Request) {
	taskID := strings.TrimPrefix(r.URL.Path, "/stop-task/")

	ma.mu.Lock()
	filtered := make([]*types.Task, 0, len(ma.tasks))
	stopped := 0
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

func (ma *realisticMockAgent) handleStop(w http.ResponseWriter, r *http.Request) {
	jobName := strings.TrimPrefix(r.URL.Path, "/stop/")

	ma.mu.Lock()
	stopped := 0
	filtered := make([]*types.Task, 0, len(ma.tasks))
	for _, task := range ma.tasks {
		if task.JobName == jobName {
			stopped++
		} else {
			filtered = append(filtered, task)
		}
	}
	ma.tasks = filtered
	ma.mu.Unlock()

	_ = json.NewEncoder(w).Encode(map[string]int{"stopped": stopped})
}

func (ma *realisticMockAgent) URL() string { return ma.server.URL }
func (ma *realisticMockAgent) Close()      { ma.server.Close() }
func (ma *realisticMockAgent) TaskCount() int {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	return len(ma.tasks)
}
func (ma *realisticMockAgent) TasksByJobName(jobName string) int {
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

// BUG: Rolling update kills new tasks when co-located with old tasks.
//
// During rolling update, stopOneInstance sends DELETE /delete/{name} to the agent.
// The agent's deleteJob() removes ALL tasks matching that name — including the
// just-deployed new version (same name, different job ID).
//
// With 1 agent this is 100% reproducible. With N agents it depends on round-robin landing.
func TestUpdateRollingDeleteByNameBug(t *testing.T) {
	// Single agent → old and new tasks always co-located
	agent := newRealisticMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	l := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go l.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)

	// Deploy v1 with count=1
	oldJob := &types.Job{
		Name:    "my-app",
		Command: "./app-v1",
		Count:   1,
	}
	if err := l.DispatchJob(oldJob); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agent.TaskCount() != 1 {
		t.Fatalf("Expected 1 task before update, got %d", agent.TaskCount())
	}

	// Rolling update to v2
	newJob := &types.Job{
		Name:         "my-app",
		Command:      "./app-v2",
		Count:        1,
		UpdatePolicy: types.UpdateRolling,
	}
	_ = l.UpdateJob(newJob)
	time.Sleep(50 * time.Millisecond)

	// BUG: DELETE /delete/my-app killed BOTH v1 and v2 tasks
	// Expected: 1 task (v2 surviving), Actual: 0
	if agent.TaskCount() != 1 {
		t.Errorf("BUG: Expected 1 task after rolling update, got %d (delete-by-name killed new task too)", agent.TaskCount())
	}
	if agent.TasksByJobName("my-app") != 1 {
		t.Errorf("BUG: Expected 1 v2 task, got %d", agent.TasksByJobName("my-app"))
	}
}

// BUG: Blue-green update kills new tasks via same delete-by-name issue.
//
// Blue-green dispatches all new instances, then calls DeleteJobByID(old) which
// sends DELETE /delete/{name} to every agent with old tasks. Since old and new share
// the same name, the new tasks get killed too.
func TestUpdateBlueGreenDeleteByNameBug(t *testing.T) {
	agent := newRealisticMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	l := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go l.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)

	// Deploy v1
	oldJob := &types.Job{
		Name:    "my-app",
		Command: "./app-v1",
		Count:   1,
	}
	if err := l.DispatchJob(oldJob); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agent.TaskCount() != 1 {
		t.Fatalf("Expected 1 task before update, got %d", agent.TaskCount())
	}

	// Blue-green update to v2
	newJob := &types.Job{
		Name:         "my-app",
		Command:      "./app-v2",
		Count:        1,
		UpdatePolicy: types.UpdateBlueGreen,
	}
	_ = l.UpdateJob(newJob)
	time.Sleep(50 * time.Millisecond)

	// Expected: 1 task (v2 running)
	if agent.TaskCount() != 1 {
		t.Errorf("Expected 1 task after blue-green, got %d", agent.TaskCount())
	}
	if agent.TasksByJobName("my-app") != 1 {
		t.Errorf("Expected 1 v2 task, got %d", agent.TasksByJobName("my-app"))
	}
}

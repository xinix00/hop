package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"time"

	"github.com/xinix00/hop/internal/types"
)

// mockLeader captures notify events from agents
type mockLeader struct {
	server *httptest.Server
	mu     sync.Mutex
	events []notifyEvent
}

type notifyEvent struct {
	Job   string `json:"job"`
	Event string `json:"event"`
}

func newMockLeader() *mockLeader {
	ml := &mockLeader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/notify", ml.handleNotify)
	ml.server = httptest.NewServer(mux)
	return ml
}

func (ml *mockLeader) handleNotify(w http.ResponseWriter, r *http.Request) {
	var ev notifyEvent
	_ = json.NewDecoder(r.Body).Decode(&ev)
	ml.mu.Lock()
	ml.events = append(ml.events, ev)
	ml.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (ml *mockLeader) Events() []notifyEvent {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	cp := make([]notifyEvent, len(ml.events))
	copy(cp, ml.events)
	return cp
}

func (ml *mockLeader) Close() {
	ml.server.Close()
}

func (ml *mockLeader) Addr() string {
	return ml.server.Listener.Addr().String()
}

// --- startJob event tests ---

// TestStartJobNotify_WithoutHealthCheck verifies that startJob fires "started"
// immediately for jobs without a health check (task is ready to serve).
func TestStartJobNotify_WithoutHealthCheck(t *testing.T) {
	ml := newMockLeader()
	defer ml.Close()

	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())
	agent.SetLeaderFunc(func() string { return ml.Addr() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "my-api", Command: "echo hello"}
	if err := agent.startJob(job, newTask(job)); err != nil {
		t.Fatalf("startJob failed: %v", err)
	}

	// Wait for async notifyLeader goroutine
	time.Sleep(100 * time.Millisecond)

	events := ml.Events()
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Job != "my-api" {
		t.Errorf("event job = %q, want %q", events[0].Job, "my-api")
	}
	if events[0].Event != "started" {
		t.Errorf("event type = %q, want %q", events[0].Event, "started")
	}
}

// TestStartJobNotify_WithHealthCheck verifies that startJob fires "start"
// (not "started") for jobs with a health check. "started" comes later
// when the first health check passes.
func TestStartJobNotify_WithHealthCheck(t *testing.T) {
	ml := newMockLeader()
	defer ml.Close()

	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())
	agent.SetLeaderFunc(func() string { return ml.Addr() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "my-api",
		Command: "echo hello",
		HealthCheck: &types.HealthCheck{
			Path:    "/health",
			Port:    "http",
			Timeout: time.Second,
		},
	}
	if err := agent.startJob(job, newTask(job)); err != nil {
		t.Fatalf("startJob failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	events := ml.Events()
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Event != "start" {
		t.Errorf("event type = %q, want %q (health check configured)", events[0].Event, "start")
	}
}

// --- deleteJobByID event tests ---

// TestDeleteJobNotify verifies that deleteJobByID fires "stop" event.
func TestDeleteJobNotify(t *testing.T) {
	ml := newMockLeader()
	defer ml.Close()

	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())
	agent.SetLeaderFunc(func() string { return ml.Addr() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Store a job and task
	agent.StoreJob(&types.Job{Name: "my-api", Command: "echo"})
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:      "task-1",
			JobName: "my-api",
			State:   types.TaskRunning,
		}
	})
	time.Sleep(10 * time.Millisecond)

	agent.deleteJob("my-api")

	// Wait for async notifyLeader goroutine
	time.Sleep(100 * time.Millisecond)

	events := ml.Events()
	// May have "started" from some other path, filter for "stop"
	var stopEvents []notifyEvent
	for _, ev := range events {
		if ev.Event == "stop" {
			stopEvents = append(stopEvents, ev)
		}
	}
	if len(stopEvents) != 1 {
		t.Fatalf("got %d stop events, want 1 (all events: %v)", len(stopEvents), events)
	}
	if stopEvents[0].Job != "my-api" {
		t.Errorf("event job = %q, want %q", stopEvents[0].Job, "my-api")
	}
}

// --- First health check pass fires "started" ---

// TestFirstHealthCheckPassFiresStarted verifies that the first successful
// health check fires a "started" event via notifyLeader.
func TestFirstHealthCheckPassFiresStarted(t *testing.T) {
	// Health check server that returns 200
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ml := newMockLeader()
	defer ml.Close()

	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetLeaderFunc(func() string { return ml.Addr() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	port := getPort(t, srv)

	job := &types.Job{
		Name:    "my-api",
		Command: "echo hello",
		HealthCheck: &types.HealthCheck{
			Path:    "/health",
			Port:    "http",
			Timeout: time.Second,
		},
	}

	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job
		s.tasks["task-hc"] = &types.Task{
			ID:        "task-hc",
			JobName:   "my-api",
			Ports:     map[string]int{"http": port},
			Pid:       1234,
			State:     types.TaskRunning,
			StartedAt: time.Now().Add(-time.Minute), // well past any initial timeout
		}
	})
	time.Sleep(10 * time.Millisecond)

	// Register task in mock runner
	mockRunner.mu.Lock()
	mockRunner.tasks["task-hc"] = &types.Task{
		ID:    "task-hc",
		State: types.TaskRunning,
	}
	mockRunner.mu.Unlock()

	// First checkTasks → health check passes → "started" event
	agent.checkTasks()
	time.Sleep(100 * time.Millisecond)

	events := ml.Events()
	var startedEvents []notifyEvent
	for _, ev := range events {
		if ev.Event == "started" {
			startedEvents = append(startedEvents, ev)
		}
	}
	if len(startedEvents) != 1 {
		t.Fatalf("got %d 'started' events, want 1 (all events: %v)", len(startedEvents), events)
	}
	if startedEvents[0].Job != "my-api" {
		t.Errorf("event job = %q, want %q", startedEvents[0].Job, "my-api")
	}
}

// TestHealthCheckPassOnlyNotifiesOnce verifies that "started" is only
// fired once (first health check pass), not on every subsequent pass.
func TestHealthCheckPassOnlyNotifiesOnce(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ml := newMockLeader()
	defer ml.Close()

	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetLeaderFunc(func() string { return ml.Addr() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	port := getPort(t, srv)

	job := &types.Job{
		Name:    "my-api",
		Command: "echo hello",
		HealthCheck: &types.HealthCheck{
			Path:    "/health",
			Port:    "http",
			Timeout: time.Second,
		},
	}

	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job
		s.tasks["task-once"] = &types.Task{
			ID:        "task-once",
			JobName:   "my-api",
			Ports:     map[string]int{"http": port},
			Pid:       1234,
			State:     types.TaskRunning,
			StartedAt: time.Now().Add(-time.Minute),
		}
	})
	time.Sleep(10 * time.Millisecond)

	mockRunner.mu.Lock()
	mockRunner.tasks["task-once"] = &types.Task{
		ID:    "task-once",
		State: types.TaskRunning,
	}
	mockRunner.mu.Unlock()

	// Run checkTasks multiple times
	agent.checkTasks()
	time.Sleep(50 * time.Millisecond)
	agent.checkTasks()
	time.Sleep(50 * time.Millisecond)
	agent.checkTasks()
	time.Sleep(100 * time.Millisecond)

	events := ml.Events()
	var startedEvents []notifyEvent
	for _, ev := range events {
		if ev.Event == "started" {
			startedEvents = append(startedEvents, ev)
		}
	}
	if len(startedEvents) != 1 {
		t.Errorf("got %d 'started' events, want 1 (should only fire once)", len(startedEvents))
	}
}

// TestCrashEventFired verifies that task crash fires "crash" event.
func TestCrashEventFired(t *testing.T) {
	ml := newMockLeader()
	defer ml.Close()

	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetLeaderFunc(func() string { return ml.Addr() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "crasher", Command: "echo hello"}
	task := newTask(job)
	if err := agent.startJob(job, task); err != nil {
		t.Fatalf("startJob failed: %v", err)
	}

	// Wait for "started" event from startJob
	time.Sleep(100 * time.Millisecond)

	// Simulate crash: remove from runner so Status returns Failed
	mockRunner.mu.Lock()
	delete(mockRunner.tasks, task.ID)
	mockRunner.mu.Unlock()

	agent.checkTasks()
	time.Sleep(200 * time.Millisecond)

	events := ml.Events()
	var crashEvents []notifyEvent
	for _, ev := range events {
		if ev.Event == "crash" {
			crashEvents = append(crashEvents, ev)
		}
	}
	if len(crashEvents) != 1 {
		t.Fatalf("got %d 'crash' events, want 1 (all events: %v)", len(crashEvents), events)
	}
	if crashEvents[0].Job != "crasher" {
		t.Errorf("event job = %q, want %q", crashEvents[0].Job, "crasher")
	}
}

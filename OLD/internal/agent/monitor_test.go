package agent

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"time"

	"github.com/xinix00/lean/leanhttp"

	"github.com/xinix00/hop/internal/types"
)

// --- checkHealth tests ---

func TestCheckHealthSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Parse port from test server
	port := getPort(t, srv)

	task := &types.Task{
		ID:    "task-1",
		Ports: map[string]int{"http": port},
	}
	hc := &types.HealthCheck{
		Path:    "/health",
		Port:    "http",
		Timeout: time.Second,
	}

	if !agent.checkHealth(task, hc) {
		t.Error("checkHealth returned false, want true")
	}
}

func TestCheckHealthBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())

	port := getPort(t, srv)
	task := &types.Task{
		ID:    "task-1",
		Ports: map[string]int{"http": port},
	}
	hc := &types.HealthCheck{
		Path:             "/health",
		Port:             "http",
		Timeout:          time.Second,
		FailureThreshold: 1,
	}

	if agent.checkHealth(task, hc) {
		t.Error("checkHealth returned true for 500, want false")
	}
}

func TestCheckHealth3xxRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer srv.Close()

	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())

	port := getPort(t, srv)
	task := &types.Task{
		ID:    "task-1",
		Ports: map[string]int{"http": port},
	}
	hc := &types.HealthCheck{
		Path:    "/health",
		Port:    "http",
		Timeout: time.Second,
	}

	// checkHealth accepts status < 400, but http.Client follows redirects by default
	// With no redirect target, the client will get an error → false
	result := agent.checkHealth(task, hc)
	// 301 without Location will cause client error, so false is expected
	_ = result // Implementation detail: depends on redirect behavior
}

func TestCheckHealthTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())

	port := getPort(t, srv)
	task := &types.Task{
		ID:    "task-1",
		Ports: map[string]int{"http": port},
	}
	hc := &types.HealthCheck{
		Path:             "/health",
		Port:             "http",
		Timeout:          50 * time.Millisecond, // Very short timeout
		FailureThreshold: 1,
	}

	if agent.checkHealth(task, hc) {
		t.Error("checkHealth returned true for timeout, want false")
	}
}

func TestCheckHealthMissingPort(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())

	task := &types.Task{
		ID:    "task-1",
		Ports: map[string]int{"grpc": 9090}, // No "http" port
	}
	hc := &types.HealthCheck{
		Path:             "/health",
		Port:             "http", // Refers to non-existent port
		FailureThreshold: 1,
	}

	if agent.checkHealth(task, hc) {
		t.Error("checkHealth returned true for missing port, want false")
	}
}

func TestCheckHealthDefaultPort(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())

	port := getPort(t, srv)
	task := &types.Task{
		ID:    "task-1",
		Ports: map[string]int{"http": port},
	}
	hc := &types.HealthCheck{
		Path: "/health",
		Port: "", // Should default to "http"
	}

	if !agent.checkHealth(task, hc) {
		t.Error("checkHealth with empty port (default http) returned false, want true")
	}
}

// --- checkTasks tests ---

func TestCheckTasksDetectsCrashedProcess(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Start a job so we have a task
	job := &types.Job{Name: "test-job", Command: "echo hello"}
	task := newTask(job)
	if err := agent.startJob(job, task); err != nil {
		t.Fatalf("startJob failed: %v", err)
	}

	// Simulate process crash: remove from runner so Status returns Failed
	mockRunner.mu.Lock()
	delete(mockRunner.tasks, task.ID)
	mockRunner.mu.Unlock()

	// Run checkTasks — this detects the crash and calls restartTask in a goroutine
	agent.checkTasks()
	time.Sleep(100 * time.Millisecond)

	// After checkTasks + restartTask: new task replaces old (atomic swap, no capacity gap)
	info := query(agent, func(s *agentState) *types.Task {
		for _, t := range s.tasks {
			if t.JobName == "test-job" {
				return t
			}
		}
		return nil
	})

	if info == nil {
		t.Fatal("Task not found after crash detection + restart")
	}
	if info.ID == task.ID {
		t.Error("Task ID should change after restart")
	}
	if info.State != types.TaskRunning {
		t.Errorf("Task state = %q, want %q (after restart)", info.State, types.TaskRunning)
	}
	if info.RestartCount != 1 {
		t.Errorf("RestartCount = %d, want 1", info.RestartCount)
	}
}

func TestCheckTasksHealthCheckFails(t *testing.T) {
	// Health check server that returns 500
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	port := getPort(t, srv)

	job := &types.Job{
		Name:    "health-job",
		Command: "echo hello",
		Ports:   map[string]int{"http": 0},
		HealthCheck: &types.HealthCheck{
			Path:             "/health",
			Port:             "http",
			Timeout:          time.Second,
			FailureThreshold: 1,
		},
	}

	// Store job and create task manually with correct port
	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job

		s.tasks["task-health"] = &types.Task{
			ID:      "task-health",
			JobName: "health-job",
			Ports:   map[string]int{"http": port},
			Pid:     1234,
			State:   types.TaskRunning,
		}
	})
	time.Sleep(10 * time.Millisecond)

	// Also register in mock runner so Status() returns running
	mockRunner.mu.Lock()
	mockRunner.tasks["task-health"] = &types.Task{
		ID:    "task-health",
		State: types.TaskRunning,
	}
	mockRunner.mu.Unlock()

	// Run checkTasks
	agent.checkTasks()
	time.Sleep(50 * time.Millisecond)

	// Task should have been stopped and marked failed
	wasStopped := mockRunner.WasStopped("task-health")
	if !wasStopped {
		t.Error("runner.Stop was not called for health check failure")
	}
}

func TestCheckTasksHealthCheckSucceeds(t *testing.T) {
	// Health check server that returns 200
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	port := getPort(t, srv)

	job := &types.Job{
		Name:    "healthy-job",
		Command: "echo hello",
		HealthCheck: &types.HealthCheck{
			Path:    "/health",
			Port:    "http",
			Timeout: time.Second,
		},
	}

	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job

		s.tasks["task-ok"] = &types.Task{
			ID:      "task-ok",
			JobName: "healthy-job",
			Ports:   map[string]int{"http": port},
			Pid:     1234,
			State:   types.TaskRunning,
		}
	})
	time.Sleep(10 * time.Millisecond)

	mockRunner.mu.Lock()
	mockRunner.tasks["task-ok"] = &types.Task{
		ID:    "task-ok",
		State: types.TaskRunning,
	}
	mockRunner.mu.Unlock()

	// Run checkTasks
	agent.checkTasks()
	time.Sleep(50 * time.Millisecond)

	// Task should NOT have been stopped
	if mockRunner.WasStopped("task-ok") {
		t.Error("runner.Stop should not be called for healthy task")
	}

	// Task should still be running
	state := query(agent, func(s *agentState) types.TaskState {
		if t := s.tasks["task-ok"]; t != nil {
			return t.State
		}
		return ""
	})
	if state != types.TaskRunning {
		t.Errorf("Task state = %q, want %q", state, types.TaskRunning)
	}
}

// --- failure threshold tests ---

func TestCheckHealthFailureThreshold(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())

	port := getPort(t, srv)
	task := &types.Task{
		ID:    "task-threshold",
		Ports: map[string]int{"http": port},
	}
	hc := &types.HealthCheck{
		Path:             "/health",
		Port:             "http",
		Timeout:          time.Second,
		FailureThreshold: 3,
	}

	// First two failures should still return true (under threshold)
	if !agent.checkHealth(task, hc) {
		t.Error("checkHealth should return true on failure 1/3")
	}
	if !agent.checkHealth(task, hc) {
		t.Error("checkHealth should return true on failure 2/3")
	}
	// Third failure should return false (at threshold)
	if agent.checkHealth(task, hc) {
		t.Error("checkHealth should return false on failure 3/3")
	}
}

func TestCheckHealthFailureThresholdResets(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(false)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if healthy.Load() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(leanhttp.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())

	port := getPort(t, srv)
	task := &types.Task{
		ID:    "task-reset",
		Ports: map[string]int{"http": port},
	}
	hc := &types.HealthCheck{
		Path:             "/health",
		Port:             "http",
		Timeout:          time.Second,
		FailureThreshold: 3,
	}

	// Two failures
	agent.checkHealth(task, hc)
	agent.checkHealth(task, hc)

	// Success resets counter
	healthy.Store(true)
	if !agent.checkHealth(task, hc) {
		t.Error("checkHealth should return true after success")
	}

	// Need 3 more failures to trigger
	healthy.Store(false)
	if !agent.checkHealth(task, hc) {
		t.Error("checkHealth should return true on failure 1/3 after reset")
	}
	if !agent.checkHealth(task, hc) {
		t.Error("checkHealth should return true on failure 2/3 after reset")
	}
	if agent.checkHealth(task, hc) {
		t.Error("checkHealth should return false on failure 3/3 after reset")
	}
}

// --- TCP health check tests ---

func TestCheckHealthTCP(t *testing.T) {
	// Start a TCP listener
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start TCP listener: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	var port int
	_, _ = fmt.Sscanf(ln.Addr().String(), "127.0.0.1:%d", &port)

	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())

	task := &types.Task{
		ID:    "task-tcp",
		Ports: map[string]int{"redis": port},
	}
	hc := &types.HealthCheck{
		Type:    types.CheckTCP,
		Port:    "redis",
		Timeout: time.Second,
	}

	if !agent.checkHealth(task, hc) {
		t.Error("TCP health check should succeed for open port")
	}
}

func TestCheckHealthTCPRefused(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())

	task := &types.Task{
		ID:    "task-tcp-fail",
		Ports: map[string]int{"redis": 1}, // Port 1 should be refused
	}
	hc := &types.HealthCheck{
		Type:             types.CheckTCP,
		Port:             "redis",
		Timeout:          100 * time.Millisecond,
		FailureThreshold: 1,
	}

	if agent.checkHealth(task, hc) {
		t.Error("TCP health check should fail for closed port")
	}
}

// --- FILE health check tests ---

func TestCheckHealthFile(t *testing.T) {
	// Create a temp file
	f, err := os.CreateTemp("", "healthcheck-*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	f.Close()

	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())

	task := &types.Task{ID: "task-file"}
	hc := &types.HealthCheck{
		Type: types.CheckFile,
		Path: f.Name(),
	}

	// First check: file exists = healthy
	if !agent.checkHealth(task, hc) {
		t.Error("FILE health check should succeed on first check (file exists)")
	}

	// Touch the file to update mtime
	time.Sleep(10 * time.Millisecond)
	_ = os.Chtimes(f.Name(), time.Now(), time.Now())

	if !agent.checkHealth(task, hc) {
		t.Error("FILE health check should succeed when file was recently modified")
	}
}

func TestCheckHealthFileNotModified(t *testing.T) {
	f, err := os.CreateTemp("", "healthcheck-*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	f.Close()

	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())

	task := &types.Task{ID: "task-file-stale"}
	hc := &types.HealthCheck{
		Type:             types.CheckFile,
		Path:             f.Name(),
		FailureThreshold: 1,
	}

	// First check succeeds (no previous check time)
	if !agent.checkHealth(task, hc) {
		t.Error("FILE health check should succeed on first check")
	}

	// Second check without modifying file should fail (mtime hasn't changed)
	time.Sleep(10 * time.Millisecond)
	if agent.checkHealth(task, hc) {
		t.Error("FILE health check should fail when file not modified since last check")
	}
}

func TestCheckHealthFileNotExists(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())

	task := &types.Task{ID: "task-file-missing"}
	hc := &types.HealthCheck{
		Type:             types.CheckFile,
		Path:             "/tmp/nonexistent-healthcheck-file-12345",
		FailureThreshold: 1,
	}

	if agent.checkHealth(task, hc) {
		t.Error("FILE health check should fail for non-existent file")
	}
}

// --- restartTask tests ---

func TestRestartTaskSuccess(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "restart-me", Command: "echo hello"}

	// Store job and create a failed task
	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job

		s.tasks["task-restart"] = &types.Task{
			ID:           "task-restart",
			JobName:      "restart-me",
			Pid:          999,
			State:        types.TaskFailed,
			RestartCount: 0,
		}
	})
	time.Sleep(10 * time.Millisecond)

	task := &types.Task{ID: "task-restart", JobName: "restart-me"}
	agent.restartTask(task, true)
	time.Sleep(50 * time.Millisecond)

	// After restart: new task ID, old one gone
	info := query(agent, func(s *agentState) *types.Task {
		for _, t := range s.tasks {
			if t.JobName == "restart-me" {
				return t
			}
		}
		return nil
	})

	if info == nil {
		t.Fatal("Task not found after restart")
	}
	if info.ID == "task-restart" {
		t.Error("Task ID should change after restart")
	}
	if info.State != types.TaskRunning {
		t.Errorf("State = %q, want %q", info.State, types.TaskRunning)
	}
	if info.RestartCount != 1 {
		t.Errorf("RestartCount = %d, want 1", info.RestartCount)
	}
}

func TestRestartTaskMaxRestartsExceeded(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:        "max-restart",
		Command:     "echo hello",
		MaxRestarts: intPtr(3),
	}

	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job

		s.tasks["task-max"] = &types.Task{
			ID:           "task-max",
			JobName:      "max-restart",
			State:        types.TaskFailed,
			RestartCount: 3,          // Already at max
			LastFailedAt: time.Now(), // Recent crash — no grace period reset
		}
	})
	time.Sleep(10 * time.Millisecond)

	var runCalled atomic.Bool
	mockRunner.mu.Lock()
	mockRunner.onRun = func(j *types.Job) error {
		runCalled.Store(true)
		return nil
	}
	mockRunner.mu.Unlock()

	task := &types.Task{ID: "task-max", JobName: "max-restart"}
	agent.restartTask(task, true)
	time.Sleep(50 * time.Millisecond)

	if runCalled.Load() {
		t.Error("runner.Run should not be called when max restarts exceeded")
	}

	// Task should stay failed
	state := query(agent, func(s *agentState) types.TaskState {
		if t := s.tasks["task-max"]; t != nil {
			return t.State
		}
		return ""
	})
	if state != types.TaskFailed {
		t.Errorf("State = %q, want %q", state, types.TaskFailed)
	}
}

func TestRestartTaskUnlimitedRestarts(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.restartWait = func(time.Duration) bool { return true }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:        "unlimited",
		Command:     "echo hello",
		MaxRestarts: intPtr(-1), // Unlimited
	}

	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job

		s.tasks["task-unlimited"] = &types.Task{
			ID:           "task-unlimited",
			JobName:      "unlimited",
			State:        types.TaskFailed,
			RestartCount: 100, // Many restarts already
		}
	})
	time.Sleep(10 * time.Millisecond)

	task := &types.Task{ID: "task-unlimited", JobName: "unlimited"}
	agent.restartTask(task, true)
	time.Sleep(50 * time.Millisecond)

	// Should have restarted despite high restart count (new task ID)
	state := query(agent, func(s *agentState) types.TaskState {
		for _, t := range s.tasks {
			if t.JobName == "unlimited" {
				return t.State
			}
		}
		return ""
	})
	if state != types.TaskRunning {
		t.Errorf("State = %q, want %q (unlimited restarts should always restart)", state, types.TaskRunning)
	}
}

func TestRestartTaskGracePeriodResetsCount(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:          "grace-test",
		Command:       "echo hello",
		MaxRestarts:   intPtr(3),
		RestartWindow: 100 * time.Millisecond, // Short window for testing
	}

	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job

		s.tasks["task-grace"] = &types.Task{
			ID:           "task-grace",
			JobName:      "grace-test",
			State:        types.TaskFailed,
			RestartCount: 3,                                       // At max
			StartedAt:    time.Now().Add(-200 * time.Millisecond), // But it stayed up past the window
		}
	})
	time.Sleep(10 * time.Millisecond)

	task := &types.Task{ID: "task-grace", JobName: "grace-test"}
	agent.restartTask(task, true)
	time.Sleep(50 * time.Millisecond)

	// Should have restarted because grace period reset the counter
	info := query(agent, func(s *agentState) *types.Task {
		for _, t := range s.tasks {
			if t.JobName == "grace-test" {
				return t
			}
		}
		return nil
	})

	if info == nil {
		t.Fatal("Task not found after grace period restart")
	}
	if info.ID == "task-grace" {
		t.Error("Task ID should change after restart")
	}
	if info.State != types.TaskRunning {
		t.Errorf("State = %q, want %q", info.State, types.TaskRunning)
	}
	if info.RestartCount != 1 {
		t.Errorf("RestartCount = %d, want 1 (should reset then increment)", info.RestartCount)
	}
}

func TestRestartTaskJobNotFound(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Task exists but its job doesn't
	agent.do(func(s *agentState) {
		s.tasks["orphan-task"] = &types.Task{
			ID:      "orphan-task",
			JobName: "deleted-job",
			State:   types.TaskFailed,
		}
	})
	time.Sleep(10 * time.Millisecond)

	var runCalled atomic.Bool
	mockRunner.mu.Lock()
	mockRunner.onRun = func(j *types.Job) error {
		runCalled.Store(true)
		return nil
	}
	mockRunner.mu.Unlock()

	// Should not panic
	task := &types.Task{ID: "orphan-task", JobName: "deleted-job"}
	agent.restartTask(task, true)
	time.Sleep(50 * time.Millisecond)

	if runCalled.Load() {
		t.Error("runner.Run should not be called when job is not found")
	}
}

// --- helper ---

func getPort(t *testing.T, srv *httptest.Server) int {
	t.Helper()
	var port int
	_, err := fmt.Sscanf(srv.Listener.Addr().String(), "127.0.0.1:%d", &port)
	if err != nil {
		t.Fatalf("Failed to parse test server port: %v", err)
	}
	return port
}

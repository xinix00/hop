package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"time"

	"github.com/xinix00/lean/leanhttp"

	"github.com/xinix00/hop/internal/runner"
	"github.com/xinix00/hop/internal/types"
)

// ============== HTTP HANDLER TESTS ==============

func TestHandleHealth(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := leanhttp.NewRequest(leanhttp.MethodGet, "/health", nil)
	w := leanhttp.NewRecorder()

	agent.handleHealth(w, req)

	if w.Code != leanhttp.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusOK)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp["status"] != "ok" {
		t.Errorf("status = %q, want %q", resp["status"], "ok")
	}
}

func TestHandleTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Add some tasks
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:      "task-1",
			JobName: "job-1",
			State:   types.TaskRunning,
			Pid:     1234,
		}
		s.tasks["task-2"] = &types.Task{
			ID:      "task-2",
			JobName: "job-2",
			State:   types.TaskFailed,
			Pid:     5678,
		}
	})

	time.Sleep(10 * time.Millisecond)

	req := leanhttp.NewRequest(leanhttp.MethodGet, "/tasks", nil)
	w := leanhttp.NewRecorder()

	agent.handleTasks(w, req)

	if w.Code != leanhttp.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusOK)
	}

	var tasks []*types.Task
	if err := json.NewDecoder(w.Body).Decode(&tasks); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(tasks) != 2 {
		t.Errorf("Got %d tasks, want 2", len(tasks))
	}
}

func TestHandleRunSuccess(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:    "test",
		Command: "echo hello",
	}

	body, _ := json.Marshal(job)
	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	// Job is accepted asynchronously
	if w.Code != leanhttp.StatusAccepted {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusAccepted)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp["status"] != "accepted" {
		t.Errorf("status = %q, want %q", resp["status"], "accepted")
	}
	if resp["job"] != "test" {
		t.Errorf("job = %q, want %q", resp["job"], "test")
	}

	// Wait for async job start
	time.Sleep(50 * time.Millisecond)

	// Verify task was created
	tasks := query(agent, func(s *agentState) []*types.Task {
		var result []*types.Task
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})

	if len(tasks) != 1 {
		t.Errorf("Got %d tasks, want 1", len(tasks))
	}
	if len(tasks) > 0 && tasks[0].JobName != "test" {
		t.Errorf("Task JobName = %q, want %q", tasks[0].JobName, "test")
	}
}

func TestHandleRunMethodNotAllowed(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := leanhttp.NewRequest(leanhttp.MethodGet, "/run", nil)
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != leanhttp.StatusMethodNotAllowed {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusMethodNotAllowed)
	}
}

func TestHandleRunInvalidJSON(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader([]byte("not json")))
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != leanhttp.StatusBadRequest {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusBadRequest)
	}
}

func TestHandleRunInsufficientCapacity(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024}) // 1024 CPU shares

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:      "test",
		Command:   "echo",
		CPUShares: 2000, // Exceeds 1024 shares
	}

	body, _ := json.Marshal(job)
	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != leanhttp.StatusServiceUnavailable {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusServiceUnavailable)
	}
}

func TestHandleRunRunnerError(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	mockRunner.SetRunError(ErrSimulated)
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:    "test",
		Command: "echo",
	}

	body, _ := json.Marshal(job)
	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	// Job is accepted (fire-and-forget), error happens async
	if w.Code != leanhttp.StatusAccepted {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusAccepted)
	}

	// Wait for async job attempt
	time.Sleep(50 * time.Millisecond)

	// A failed start now lingers in state (marked failed, then retried with
	// backoff — see restartTask), so we no longer expect zero tasks. What must
	// hold is that a failed runner never produces a RUNNING task.
	running := query(agent, func(s *agentState) int {
		n := 0
		for _, t := range s.tasks {
			if t.State == types.TaskRunning {
				n++
			}
		}
		return n
	})

	if running != 0 {
		t.Errorf("Got %d running tasks, want 0 (runner should have failed)", running)
	}
}

func TestHandleDeleteSuccess(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Add a job and running task
	agent.StoreJob(&types.Job{Name: "test", Command: "echo"})
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:      "task-1",
			JobName: "test",
			State:   types.TaskRunning,
		}
	})

	time.Sleep(10 * time.Millisecond)

	req := leanhttp.NewRequest(leanhttp.MethodDelete, "/delete/test", nil)
	w := leanhttp.NewRecorder()

	agent.handleDelete(w, req)

	if w.Code != leanhttp.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusOK)
	}

	var resp map[string]int
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp["deleted"] != 1 {
		t.Errorf("deleted = %d, want 1", resp["deleted"])
	}
}

func TestHandleDeleteMethodNotAllowed(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := leanhttp.NewRequest(leanhttp.MethodPost, "/delete/job-1", nil)
	w := leanhttp.NewRecorder()

	agent.handleDelete(w, req)

	if w.Code != leanhttp.StatusMethodNotAllowed {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusMethodNotAllowed)
	}
}

func TestHandleDeleteMissingJobID(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := leanhttp.NewRequest(leanhttp.MethodDelete, "/delete/", nil)
	w := leanhttp.NewRecorder()

	agent.handleDelete(w, req)

	if w.Code != leanhttp.StatusBadRequest {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusBadRequest)
	}
}

func TestHandleDeleteNonExistentJob(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	req := leanhttp.NewRequest(leanhttp.MethodDelete, "/delete/nonexistent", nil)
	w := leanhttp.NewRecorder()

	agent.handleDelete(w, req)

	// Should succeed with 0 deleted
	if w.Code != leanhttp.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusOK)
	}

	var resp map[string]int
	_ = json.NewDecoder(w.Body).Decode(&resp)

	if resp["deleted"] != 0 {
		t.Errorf("deleted = %d, want 0", resp["deleted"])
	}
}

func TestHandleLogsInvalidPath(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := leanhttp.NewRequest(leanhttp.MethodGet, "/logs/invalid", nil)
	w := leanhttp.NewRecorder()

	agent.handleLogs(w, req)

	if w.Code != leanhttp.StatusBadRequest {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusBadRequest)
	}
}

func TestHandleLogsInvalidStream(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := leanhttp.NewRequest(leanhttp.MethodGet, "/logs/task-1/invalid", nil)
	w := leanhttp.NewRecorder()

	agent.handleLogs(w, req)

	if w.Code != leanhttp.StatusBadRequest {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusBadRequest)
	}
}

func TestHandleLogsTaskNotFound(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := leanhttp.NewRequest(leanhttp.MethodGet, "/logs/nonexistent/stdout", nil)
	w := leanhttp.NewRecorder()

	agent.handleLogs(w, req)

	if w.Code != leanhttp.StatusNotFound {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusNotFound)
	}
}

func TestHandleRunEmptyJSON(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()

	// Configure mock to fail on empty command (simulating real runner validation)
	mockRunner.onRun = func(job *types.Job) error {
		if job.Command == "" {
			return errors.New("command is required")
		}
		return nil
	}

	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Empty JSON object - job accepted, but will fail async
	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader([]byte("{}")))
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	// Job is accepted (fire-and-forget), validation happens async
	if w.Code != leanhttp.StatusAccepted {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusAccepted)
	}

	// Wait for async job attempt
	time.Sleep(50 * time.Millisecond)

	// A failed start now lingers in state (marked failed, then retried), so we
	// no longer expect zero tasks — only that no task ends up RUNNING.
	running := query(agent, func(s *agentState) int {
		n := 0
		for _, t := range s.tasks {
			if t.State == types.TaskRunning {
				n++
			}
		}
		return n
	})

	if running != 0 {
		t.Errorf("Got %d running tasks, want 0 (empty command should fail)", running)
	}
}

func TestHandleRunWithPorts(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:    "test",
		Command: "echo",
		Ports:   map[string]int{"http": 0, "grpc": 0},
	}

	body, _ := json.Marshal(job)
	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != leanhttp.StatusAccepted {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusAccepted)
	}

	// Wait for async job start
	time.Sleep(50 * time.Millisecond)

	// Check that task has allocated ports
	tasks := query(agent, func(s *agentState) []*types.Task {
		var result []*types.Task
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})

	if len(tasks) != 1 {
		t.Fatalf("Got %d tasks, want 1", len(tasks))
	}

	task := tasks[0]
	if len(task.Ports) != 2 {
		t.Errorf("Ports = %d, want 2", len(task.Ports))
	}

	if _, ok := task.Ports["http"]; !ok {
		t.Error("Should have http port")
	}
	if _, ok := task.Ports["grpc"]; !ok {
		t.Error("Should have grpc port")
	}
}

func TestHandleRunWithFixedPorts(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:    "test",
		Command: "echo",
		Ports:   map[string]int{"http": 18083, "grpc": 0}, // http fixed (unique high port), grpc dynamic
	}

	body, _ := json.Marshal(job)
	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != leanhttp.StatusAccepted {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusAccepted)
	}

	// Wait for async job start
	time.Sleep(50 * time.Millisecond)

	// Check that task has correct ports
	tasks := query(agent, func(s *agentState) []*types.Task {
		var result []*types.Task
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})

	if len(tasks) != 1 {
		t.Fatalf("Got %d tasks, want 1", len(tasks))
	}

	task := tasks[0]
	// http should be fixed at the requested port
	if task.Ports["http"] != 18083 {
		t.Errorf("http port = %d, want 18083", task.Ports["http"])
	}

	// grpc should be dynamic (non-zero)
	if task.Ports["grpc"] == 0 {
		t.Error("grpc port should be dynamically allocated")
	}
}

func TestHandleRunPortInUse(t *testing.T) {
	// Occupy a port
	listener, err := net.Listen("tcp", ":19876")
	if err != nil {
		t.Fatalf("Failed to occupy port: %v", err)
	}
	defer listener.Close()

	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:    "test",
		Command: "echo",
		Ports:   map[string]int{"http": 19876}, // Port is in use
	}

	body, _ := json.Marshal(job)
	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	// Job is accepted (fire-and-forget), port check happens async
	if w.Code != leanhttp.StatusAccepted {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusAccepted)
	}

	// Wait for async job attempt
	time.Sleep(50 * time.Millisecond)

	// Port stays occupied, so the start keeps failing — the task lingers (failed
	// + retried) rather than being removed. The invariant is that it never runs.
	running := query(agent, func(s *agentState) int {
		n := 0
		for _, t := range s.tasks {
			if t.State == types.TaskRunning {
				n++
			}
		}
		return n
	})

	if running != 0 {
		t.Errorf("Got %d running tasks, want 0 (port in use should fail)", running)
	}
}

// ============== CAPACITY HANDLER TESTS ==============

func TestHandleCapacity(t *testing.T) {
	cfg := testConfig()
	// testConfig sets Capacity = {CPUShares: 1000, Memory: 1GiB}. With the
	// cap-aware capacity wiring those values are now reported back.
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 8, MemoryBytes: 16 * 1024 * 1024 * 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	req := leanhttp.NewRequest(leanhttp.MethodGet, "/capacity", nil)
	w := leanhttp.NewRecorder()

	agent.handleCapacity(w, req)

	if w.Code != leanhttp.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusOK)
	}

	var resp CapacityResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	// Config cap CPUShares=1000 < detected (8*1024) → CPUCores reports 1000/1024 = 0.
	// Since we floor-divide and the cap isn't a multiple of 1024, the helper
	// falls back to sysInfo.CPUCores rather than reporting 0.
	if resp.CPUCores != 8 {
		t.Errorf("CPUCores = %d, want 8", resp.CPUCores)
	}
	if resp.MemoryBytes != 1*1024*1024*1024 {
		t.Errorf("MemoryBytes = %d, want %d (config cap)", resp.MemoryBytes, 1*1024*1024*1024)
	}
	if resp.TasksRunning != 0 {
		t.Errorf("TasksRunning = %d, want 0", resp.TasksRunning)
	}
	if resp.CPUUsedShares != 0 {
		t.Errorf("CPUUsedShares = %d, want 0", resp.CPUUsedShares)
	}
	if resp.MemoryUsedBytes != 0 {
		t.Errorf("MemoryUsedBytes = %d, want 0", resp.MemoryUsedBytes)
	}
}

// ============== LEADER HANDLER TESTS ==============

func TestHandleLeader(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	// No leader published yet: the answer comes from the state loop.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	req := leanhttp.NewRequest(leanhttp.MethodGet, "/leader", nil)
	w := leanhttp.NewRecorder()

	agent.handleLeader(w, req)

	if w.Code != leanhttp.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusOK)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp["leader"] != "" {
		t.Errorf("leader = %q, want empty string", resp["leader"])
	}
}

// The tick loop publishes the leader into the agent state; /leader reports it.
func TestHandleLeaderFromState(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	agent.SetLeaderAddr("10.0.0.7:9080")

	w := leanhttp.NewRecorder()
	agent.handleLeader(w, leanhttp.NewRequest(leanhttp.MethodGet, "/leader", nil))
	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["leader"] != "10.0.0.7:9080" {
		t.Errorf("leader = %q, want 10.0.0.7:9080", resp["leader"])
	}

	agent.SetLeaderAddr("")
	if got := agent.LeaderAddr(); got != "" {
		t.Errorf("after clearing LeaderAddr() = %q, want empty", got)
	}

	// Leading: the lease expiry rides along; not leading: it is absent.
	exp := time.Now().Add(90 * time.Second).Truncate(time.Second)
	agent.SetLeaseExpiresAt(exp)
	w = leanhttp.NewRecorder()
	agent.handleLeader(w, leanhttp.NewRequest(leanhttp.MethodGet, "/leader", nil))
	var withLease map[string]any
	if err := json.NewDecoder(w.Body).Decode(&withLease); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got, _ := withLease["lease_expires_at"].(string); got == "" {
		t.Fatalf("lease_expires_at missing while leading: %v", withLease)
	}
	agent.SetLeaseExpiresAt(time.Time{})
	w = leanhttp.NewRecorder()
	agent.handleLeader(w, leanhttp.NewRequest(leanhttp.MethodGet, "/leader", nil))
	var without map[string]any
	_ = json.NewDecoder(w.Body).Decode(&without)
	if _, ok := without["lease_expires_at"]; ok {
		t.Fatalf("lease_expires_at present while not leading: %v", without)
	}
}

func TestHandleLeaderWithFunc(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx) // /leader also reads the lease expiry from the state
	agent.SetLeaderFunc(func() string {
		return "10.0.0.1:9080"
	})

	req := leanhttp.NewRequest(leanhttp.MethodGet, "/leader", nil)
	w := leanhttp.NewRecorder()

	agent.handleLeader(w, req)

	if w.Code != leanhttp.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusOK)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp["leader"] != "10.0.0.1:9080" {
		t.Errorf("leader = %q, want %q", resp["leader"], "10.0.0.1:9080")
	}
}

// ============== PROXY TO LEADER TESTS ==============

func TestProxyToLeaderNoFunc(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	// No leader published: the state loop answers "" and the proxy says 503.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	req := leanhttp.NewRequest(leanhttp.MethodGet, "/v1/agents", nil)
	w := leanhttp.NewRecorder()

	agent.proxyToLeader(w, req)

	if w.Code != leanhttp.StatusServiceUnavailable {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusServiceUnavailable)
	}
}

func TestProxyToLeaderNoLeader(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetLeaderFunc(func() string {
		return "" // No leader available
	})

	req := leanhttp.NewRequest(leanhttp.MethodGet, "/v1/agents", nil)
	w := leanhttp.NewRecorder()

	agent.proxyToLeader(w, req)

	if w.Code != leanhttp.StatusServiceUnavailable {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusServiceUnavailable)
	}
}

func TestProxyToLeaderSuccess(t *testing.T) {
	// Mock leader server
	leaderServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"id":"agent-1"}]`))
	}))
	defer leaderServer.Close()

	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetLeaderFunc(func() string {
		return leaderServer.Listener.Addr().String()
	})

	req := leanhttp.NewRequest(leanhttp.MethodGet, "/v1/agents", nil)
	w := leanhttp.NewRecorder()

	agent.proxyToLeader(w, req)

	if w.Code != leanhttp.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusOK)
	}

	// Verify response was proxied
	body := w.Body.String()
	if body != `[{"id":"agent-1"}]` {
		t.Errorf("Body = %q, want agent list", body)
	}
}

func TestProxyToLeaderPostForward(t *testing.T) {
	var receivedBody string
	var receivedMethod string

	leaderServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		data, _ := io.ReadAll(r.Body)
		receivedBody = string(data)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer leaderServer.Close()

	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetLeaderFunc(func() string {
		return leaderServer.Listener.Addr().String()
	})

	reqBody := `{"name":"test","command":"echo"}`
	req := leanhttp.NewRequest(leanhttp.MethodPost, "/v1/jobs", bytes.NewReader([]byte(reqBody)))
	req.Header.Set("Content-Type", "application/json")
	w := leanhttp.NewRecorder()

	agent.proxyToLeader(w, req)

	if w.Code != leanhttp.StatusCreated {
		t.Errorf("Status code = %d, want %d", w.Code, leanhttp.StatusCreated)
	}
	if receivedMethod != leanhttp.MethodPost {
		t.Errorf("Proxied method = %q, want %q", receivedMethod, leanhttp.MethodPost)
	}
	if receivedBody != reqBody {
		t.Errorf("Proxied body = %q, want %q", receivedBody, reqBody)
	}
}

// ============== LOG STREAMING TESTS ==============

func TestHandleLogsStdoutStream(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Start a job to create a task with log broadcasters
	job := &types.Job{Name: "log-test", Command: "echo hello"}
	task := newTask(job)
	if err := agent.startJob(job, task); err != nil {
		t.Fatalf("startJob failed: %v", err)
	}

	// Write to the stdout broadcaster
	broadcaster := mockRunner.GetStdout(task.ID)
	if broadcaster == nil {
		t.Fatal("No stdout broadcaster for task")
	}

	// Create a request with a context we can cancel
	reqCtx, reqCancel := context.WithCancel(context.Background())
	req := leanhttp.NewRequest(leanhttp.MethodGet, "/logs/"+task.ID+"/stdout", nil)
	req = req.WithContext(reqCtx)
	w := leanhttp.NewRecorder()

	// Run handleLogs in a goroutine (it blocks on the SSE stream)
	done := make(chan struct{})
	go func() {
		agent.handleLogs(w, req)
		close(done)
	}()

	// Write a log line
	time.Sleep(20 * time.Millisecond)
	_, _ = broadcaster.Write([]byte("test log line"))

	// Give time for the message to be processed
	time.Sleep(50 * time.Millisecond)

	// Cancel the request to stop the stream
	reqCancel()
	<-done

	body := w.Body.String()
	if !bytes.Contains([]byte(body), []byte("data: test log line")) {
		t.Errorf("Body = %q, want SSE format with 'data: test log line'", body)
	}
}

// TestStopDuringStartDoesNotResurrectTask verifies that when /stop is called
// while startJob is still running (goroutine), the task does not come back
// as a zombie in "stopping" state.
func TestStopDuringStartDoesNotResurrectTask(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 4096 // 4 cores

	// Mock runner with a delay on Run() to simulate slow process start
	startCh := make(chan struct{})
	mockRunner := NewMockRunner()
	mockRunner.onRun = func(job *types.Job) error {
		<-startCh // block until test releases
		return nil
	}

	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 4, MemoryBytes: 4 * 1024 * 1024 * 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Dispatch a job via handleRun — task is added to state, startJob blocks
	job := types.Job{Name: "myapp", Command: "./app", Count: 1, CPUShares: 1024}
	body, _ := json.Marshal(job)
	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()
	agent.handleRun(w, req)

	if w.Code != leanhttp.StatusAccepted {
		t.Fatalf("handleRun: status %d, want %d", w.Code, leanhttp.StatusAccepted)
	}

	// Task should be in state (capacity reserved)
	count := query(agent, func(s *agentState) int { return len(s.tasks) })
	if count != 1 {
		t.Fatalf("Expected 1 task in state after dispatch, got %d", count)
	}

	// Stop the job while startJob is still blocked
	stopReq := leanhttp.NewRequest(leanhttp.MethodPost, "/stop/myapp", nil)
	stopW := leanhttp.NewRecorder()
	agent.handleStop(stopW, stopReq)

	// Task should be removed from state
	count = query(agent, func(s *agentState) int { return len(s.tasks) })
	if count != 0 {
		t.Fatalf("Expected 0 tasks after stop, got %d", count)
	}

	// Now let startJob complete — it should NOT resurrect the task
	close(startCh)
	time.Sleep(100 * time.Millisecond) // let goroutine finish

	// Verify task did NOT come back
	count = query(agent, func(s *agentState) int { return len(s.tasks) })
	if count != 0 {
		t.Errorf("Ghost task! Expected 0 tasks after startJob completed, got %d", count)
	}
}

// ============== EARLY FAILURE TESTS ==============

// TestHandleRun_EarlyFailure_TaskStaysFailed verifies that when startJob
// fails (e.g., volume doesn't exist, runner error), the task stays in state
// with state=failed instead of being deleted.
func TestHandleRun_EarlyFailure_TaskStaysFailed(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	mockRunner.SetRunError(ErrSimulated)
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := types.Job{Name: "failing-app", Command: "echo hello"}
	body, _ := json.Marshal(job)
	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != leanhttp.StatusAccepted {
		t.Fatalf("Expected 202 Accepted, got %d", w.Code)
	}

	// Wait for background startJob to fail
	time.Sleep(50 * time.Millisecond)

	// Task should still exist with state=failed (NOT deleted)
	tasks := query(agent, func(s *agentState) []*types.Task {
		var result []*types.Task
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})

	if len(tasks) != 1 {
		t.Fatalf("Expected 1 task (failed), got %d (task was deleted instead of marked failed)", len(tasks))
	}
	if tasks[0].State != types.TaskFailed {
		t.Errorf("Task state should be 'failed', got %q", tasks[0].State)
	}
	if tasks[0].JobName != "failing-app" {
		t.Errorf("Task job name should be 'failing-app', got %q", tasks[0].JobName)
	}
}

// TestHandleRun_EarlyFailure_VisibleInTaskList verifies that a failed task
// shows up in the /tasks endpoint response.
func TestHandleRun_EarlyFailure_VisibleInTaskList(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	mockRunner.SetRunError(ErrSimulated)
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Dispatch job that will fail
	job := types.Job{Name: "broken", Command: "echo"}
	body, _ := json.Marshal(job)
	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()
	agent.handleRun(w, req)
	time.Sleep(50 * time.Millisecond)

	// GET /tasks should show the failed task
	req = leanhttp.NewRequest(leanhttp.MethodGet, "/tasks", nil)
	w = leanhttp.NewRecorder()
	agent.handleTasks(w, req)

	var tasks []*types.Task
	if err := json.NewDecoder(w.Body).Decode(&tasks); err != nil {
		t.Fatalf("Failed to decode tasks: %v", err)
	}

	if len(tasks) != 1 {
		t.Fatalf("Expected 1 task in /tasks response, got %d", len(tasks))
	}
	if tasks[0].State != types.TaskFailed {
		t.Errorf("Task in /tasks should be 'failed', got %q", tasks[0].State)
	}
}

// TestHandleRun_EarlyFailure_ThenSuccess verifies that after an early failure,
// a subsequent successful dispatch works correctly alongside the failed task.
func TestHandleRun_EarlyFailure_ThenSuccess(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// First dispatch: will fail
	mockRunner.SetRunError(ErrSimulated)
	job1 := types.Job{Name: "app", Command: "echo v1"}
	body, _ := json.Marshal(job1)
	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()
	agent.handleRun(w, req)
	time.Sleep(50 * time.Millisecond)

	// Second dispatch: will succeed
	mockRunner.SetRunError(nil)
	job2 := types.Job{Name: "app", Command: "echo v2"}
	body, _ = json.Marshal(job2)
	req = leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w = leanhttp.NewRecorder()
	agent.handleRun(w, req)
	time.Sleep(50 * time.Millisecond)

	tasks := query(agent, func(s *agentState) []*types.Task {
		var result []*types.Task
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})

	// Should have 2 tasks: 1 failed + 1 running
	failed := 0
	running := 0
	for _, task := range tasks {
		switch task.State {
		case types.TaskFailed:
			failed++
		case types.TaskRunning:
			running++
		}
	}

	if failed != 1 {
		t.Errorf("Expected 1 failed task, got %d", failed)
	}
	if running != 1 {
		t.Errorf("Expected 1 running task, got %d", running)
	}
}

// ============== MAX RESTARTS SEMANTICS ==============

// TestMaxRestartsZeroMeansNoRestarts pins the MaxRestarts=0 contract: the
// first crash is final. restartTask must give up immediately (mark the task
// failed) without ever invoking the runner again.
func TestMaxRestartsZeroMeansNoRestarts(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()

	var runCalls atomic.Int32
	mockRunner.onRun = func(job *types.Job) error {
		runCalls.Add(1)
		return ErrSimulated
	}

	agent := New(cfg, "test-agent", mockRunner)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "no-restarts", Command: "./app", MaxRestarts: intPtr(0)}
	task := newTask(job)
	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job
		s.tasks[task.ID] = task
	})

	// Simulate the crash path: monitor detects death → restartTask.
	agent.restartTask(task, true)
	time.Sleep(50 * time.Millisecond)

	if n := runCalls.Load(); n != 0 {
		t.Errorf("Runner was invoked %d time(s), want 0 (MaxRestarts=0 means no restarts)", n)
	}
	state := query(agent, func(s *agentState) types.TaskState {
		if tk := s.tasks[task.ID]; tk != nil {
			return tk.State
		}
		return ""
	})
	if state != types.TaskFailed {
		t.Errorf("Task state = %q, want %q", state, types.TaskFailed)
	}
}

// TestMaxRestartsOneAllowsExactlyOneRestart guards the boundary of the
// >= comparison: MaxRestarts=1 must allow exactly one restart attempt,
// then give up.
func TestMaxRestartsOneAllowsExactlyOneRestart(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()

	var runCalls atomic.Int32
	mockRunner.onRun = func(job *types.Job) error {
		runCalls.Add(1)
		return ErrSimulated // every attempt fails → recursion must stop at the limit
	}

	agent := New(cfg, "test-agent", mockRunner)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "one-restart", Command: "./app", MaxRestarts: intPtr(1)}
	task := newTask(job)
	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job
		s.tasks[task.ID] = task
	})

	agent.restartTask(task, true)
	time.Sleep(100 * time.Millisecond)

	if n := runCalls.Load(); n != 1 {
		t.Errorf("Runner was invoked %d time(s), want exactly 1 (MaxRestarts=1)", n)
	}
	failed := query(agent, func(s *agentState) int {
		n := 0
		for _, tk := range s.tasks {
			if tk.State == types.TaskFailed {
				n++
			}
		}
		return n
	})
	if failed != 1 {
		t.Errorf("Expected 1 failed task in state, got %d", failed)
	}
}

// TestSlowFailingStartDoesNotResetRestartCount pint het verschil tussen "de app
// heeft gedraaid" en "de startpoging duurde lang". Alleen het eerste verdient
// een schone lei.
//
// Gemeten LicheeRV 07-08: de apploader valt na vijf minuten op zijn
// HTTP-timeout — precies defaultRestartWindow. Zolang de genadeperiode naar de
// vorige FOUT keek in plaats van naar uptime, was elke mislukte start uit
// zichzelf al "lang genoeg geleden": teller terug naar nul, maxRestarts nooit
// bereikt, en de node bleef eeuwig herstarten met "restart #1" op het scherm.
func TestSlowFailingStartDoesNotResetRestartCount(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()

	const window = 40 * time.Millisecond
	var runCalls atomic.Int32
	mockRunner.onRun = func(job *types.Job) error {
		n := runCalls.Add(1)
		time.Sleep(2 * window) // de poging duurt zélf langer dan het venster
		if n > 20 {
			return nil // noodrem: zonder dit spint een kapotte teller dit proces vol
		}
		return ErrSimulated
	}

	agent := New(cfg, "test-agent", mockRunner)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "slow-fail", Command: "./app", MaxRestarts: intPtr(2), RestartWindow: window}
	task := newTask(job)
	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job
		s.tasks[task.ID] = task
	})

	done := make(chan struct{})
	go func() { agent.restartTask(task, true); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("restartTask gaf nooit op: %d runner-aanroepen en doorgaand", runCalls.Load())
	}

	if n := runCalls.Load(); n != 2 {
		t.Errorf("Runner was invoked %d time(s), want exactly 2 (MaxRestarts=2)", n)
	}
}

// TestHealthyUptimeStillResetsRestartCount is de tegenhanger: een app die het
// venster lang overeind stond, houdt zijn schone lei. Zonder reset zou deze
// task (RestartCount == MaxRestarts) meteen opgegeven worden.
func TestHealthyUptimeStillResetsRestartCount(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()

	var runCalls atomic.Int32
	mockRunner.onRun = func(job *types.Job) error {
		runCalls.Add(1)
		return nil // deze start slaagt
	}

	agent := New(cfg, "test-agent", mockRunner)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "long-lived", Command: "./app", MaxRestarts: intPtr(2), RestartWindow: 20 * time.Millisecond}
	task := newTask(job)
	task.RestartCount = 2 // zonder reset is dit "opgebruikt"
	task.StartedAt = time.Now().Add(-time.Second)
	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job
		s.tasks[task.ID] = task
	})

	agent.restartTask(task, true)

	if n := runCalls.Load(); n != 1 {
		t.Fatalf("Runner was invoked %d time(s), want 1 (uptime > restart window = clean slate)", n)
	}
	count := query(agent, func(s *agentState) int {
		for _, tk := range s.tasks {
			if tk.JobName == job.Name {
				return tk.RestartCount
			}
		}
		return -1
	})
	if count != 1 {
		t.Errorf("RestartCount = %d, want 1 (teller was gereset en telt vanaf nul)", count)
	}
}

// TestUnplaceableJobIsNotRestarted pins the anti-storm rule: a job the node
// cannot PLACE (runner.ErrNoCapacity — no free core, or the sharegroup pool does
// not fit) must be handed back to the leader, not restarted. Before the fix the
// agent treated it as a crash and retried with backoff (1s, 2s, 4s, 8s…) while
// the leader kept re-dispatching, so one unplaceable app pegged a whole node
// and starved the tasks that did run (measured 26-07 on a 3-core QEMU node).
func TestUnplaceableJobIsNotRestarted(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()

	var mu sync.Mutex
	runs := 0
	mockRunner.onRun = func(*types.Job) error {
		mu.Lock()
		runs++
		mu.Unlock()
		return fmt.Errorf("hop driver: start apploader slot 5: %w", runner.ErrNoCapacity)
	}

	agent := New(cfg, "test-agent", mockRunner)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "browser", Command: "./browser"}
	task := newTask(job)
	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job
		s.tasks[task.ID] = task
	})

	agent.restartTask(task, true)

	// Exactly one attempt: no backoff ladder, no recursion.
	mu.Lock()
	got := runs
	mu.Unlock()
	if got != 1 {
		t.Fatalf("runner.Run called %d times, want 1 (an unplaceable job must not be retried)", got)
	}

	// And the task is gone, so the capacity accounting is restored and the next
	// dispatch is refused up front by admission instead of failing in the runner.
	left := query(agent, func(s *agentState) int { return len(s.tasks) })
	if left != 0 {
		t.Fatalf("%d task(s) left in state, want 0 (task must be handed back to the leader)", left)
	}
}

func TestHandleFlip(t *testing.T) {
	sha := "e7a4da1f76c3e5d11a346362f58757fcee0f5720888a5bf52935daae1f52e4f7"
	body := func(url, sha string) *bytes.Reader {
		b, _ := json.Marshal(map[string]string{"url": url, "sha256": sha})
		return bytes.NewReader(b)
	}

	// Zonder haak: eerlijk 501, en er wordt niets uitgevoerd.
	a := New(testConfig(), "test-agent", NewMockRunner())
	w := leanhttp.NewRecorder()
	a.handleFlip(w, leanhttp.NewRequest(leanhttp.MethodPost, "/flip", body("http://h/k.flip", sha)))
	if w.Code != leanhttp.StatusNotImplemented {
		t.Errorf("zonder haak: status = %d, wil %d", w.Code, leanhttp.StatusNotImplemented)
	}

	// Met haak: 202, en de haak krijgt exact url+sha.
	got := make(chan [2]string, 1)
	a.SetFlipFunc(func(url, s string) error { got <- [2]string{url, s}; return nil })
	w = leanhttp.NewRecorder()
	a.handleFlip(w, leanhttp.NewRequest(leanhttp.MethodPost, "/flip", body("http://h/k.flip", sha)))
	if w.Code != leanhttp.StatusAccepted {
		t.Errorf("status = %d, wil %d", w.Code, leanhttp.StatusAccepted)
	}
	select {
	case g := <-got:
		if g[0] != "http://h/k.flip" || g[1] != sha {
			t.Errorf("haak kreeg %v", g)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("haak nooit aangeroepen")
	}

	// Misvormd: geen actie, wel een duidelijke fout.
	for name, r := range map[string]*bytes.Reader{
		"kale hostnaam": body("h/k.flip", sha),
		"korte som":     body("http://h/k.flip", "deadbeef"),
		"geen hex":      body("http://h/k.flip", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"),
		"kapotte json":  bytes.NewReader([]byte("{")),
	} {
		w = leanhttp.NewRecorder()
		a.handleFlip(w, leanhttp.NewRequest(leanhttp.MethodPost, "/flip", r))
		if w.Code != leanhttp.StatusBadRequest {
			t.Errorf("%s: status = %d, wil %d", name, w.Code, leanhttp.StatusBadRequest)
		}
	}
	if w = leanhttp.NewRecorder(); true {
		a.handleFlip(w, leanhttp.NewRequest(leanhttp.MethodGet, "/flip", nil))
		if w.Code != leanhttp.StatusMethodNotAllowed {
			t.Errorf("GET: status = %d, wil %d", w.Code, leanhttp.StatusMethodNotAllowed)
		}
	}
}

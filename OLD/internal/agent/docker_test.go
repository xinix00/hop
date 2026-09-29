package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"time"

	"github.com/xinix00/lean/leanhttp"

	"github.com/xinix00/hop/internal/types"
)

// ============== RUNNER SELECTION TESTS ==============

func TestRunnerForExec(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	if agent.runnerFor(types.DriverExec) != agent.execRunner {
		t.Error("runnerFor(DriverExec) should return execRunner")
	}
}

func TestRunnerForDocker(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	if agent.runnerFor(types.DriverDocker) != agent.dockerRunner {
		t.Error("runnerFor(DriverDocker) should return dockerRunner")
	}
}

// ============== PORT ALLOCATION TESTS ==============

func TestAllocatePortsForProcessJob(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	// Process job: fixed port should be preserved. Uses a unique high port
	// (not the shared 8080) so a stray port bind from another test can't race
	// this availability check.
	job := &types.Job{
		Name:    "process-job",
		Command: "echo",
		Ports:   map[string]int{"http": 18082, "grpc": 0},
	}

	ports, err := agent.allocatePortsForJob(job)
	if err != nil {
		t.Fatalf("allocatePortsForJob failed: %v", err)
	}

	if ports["http"] != 18082 {
		t.Errorf("http port = %d, want 18082 (fixed)", ports["http"])
	}
	if ports["grpc"] == 0 {
		t.Error("grpc port should be dynamically allocated (non-zero)")
	}
}

func TestAllocatePortsForDockerJob(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	// Docker ports follow the same rule as process jobs (commit e6bf2fd:
	// "container port = host port"): a fixed value (>0) is used as-is, 0 is
	// dynamically allocated. Unprivileged ports so the check passes without root.
	job := &types.Job{
		Name:   "docker-job",
		Driver: types.DriverDocker,
		Image:  "nginx:latest",
		Ports:  map[string]int{"http": 18080, "grpc": 0},
	}

	ports, err := agent.allocatePortsForJob(job)
	if err != nil {
		t.Fatalf("allocatePortsForJob failed: %v", err)
	}

	// Fixed port is preserved (host == container).
	if ports["http"] != 18080 {
		t.Errorf("Docker http host port = %d, want 18080 (fixed)", ports["http"])
	}
	// Zero port is dynamically allocated.
	if ports["grpc"] == 0 {
		t.Error("Docker grpc host port should be dynamically allocated (non-zero)")
	}
}

func TestAllocatePortsForDockerJobNoPorts(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	// Docker job without ports
	job := &types.Job{
		Name:   "docker-job",
		Driver: types.DriverDocker,
		Image:  "redis:7",
	}

	ports, err := agent.allocatePortsForJob(job)
	if err != nil {
		t.Fatalf("allocatePortsForJob failed: %v", err)
	}

	if len(ports) != 0 {
		t.Errorf("Expected empty ports for job without ports, got %v", ports)
	}
}

// ============== DOCKER JOB DISPATCH TESTS ==============

func TestStartDockerJob(t *testing.T) {
	cfg := testConfig()
	mockProcess := NewMockRunner()
	mockDocker := NewMockRunner()

	agent := New(cfg, "test-agent", mockProcess)
	agent.dockerRunner = mockDocker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:  "docker-test",
		Image: "nginx:latest",
		Ports: map[string]int{"http": 18081}, // unprivileged: bindable without root
	}

	task := newTask(job)
	if err := agent.startJob(job, task); err != nil {
		t.Fatalf("startJob failed: %v", err)
	}

	if task.Image != "nginx:latest" {
		t.Errorf("task.Image = %q, want %q", task.Image, "nginx:latest")
	}
	if task.JobName != "docker-test" {
		t.Errorf("task.JobName = %q, want %q", task.JobName, "docker-test")
	}

	// Process runner should NOT have been called
	if mockProcess.GetTask(task.ID) != nil {
		t.Error("ExecRunner should not have been used for Docker job")
	}

	// Docker runner should have the task
	if mockDocker.GetTask(task.ID) == nil {
		t.Error("DockerRunner should have the task")
	}
}

func TestStartProcessJob(t *testing.T) {
	cfg := testConfig()
	mockProcess := NewMockRunner()
	mockDocker := NewMockRunner()

	agent := New(cfg, "test-agent", mockProcess)
	agent.dockerRunner = mockDocker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "process-test",
		Command: "echo hello",
	}

	task := newTask(job)
	if err := agent.startJob(job, task); err != nil {
		t.Fatalf("startJob failed: %v", err)
	}

	if task.Image != "" {
		t.Errorf("task.Image = %q, want empty for process job", task.Image)
	}

	// Process runner should have the task
	if mockProcess.GetTask(task.ID) == nil {
		t.Error("ExecRunner should have the task")
	}

	// Docker runner should NOT have been called
	if mockDocker.GetTask(task.ID) != nil {
		t.Error("DockerRunner should not have been used for process job")
	}
}

// ============== DOCKER JOB HTTP HANDLER TESTS ==============

func TestHandleRunDockerJob(t *testing.T) {
	cfg := testConfig()
	mockProcess := NewMockRunner()
	mockDocker := NewMockRunner()

	agent := New(cfg, "test-agent", mockProcess)
	agent.dockerRunner = mockDocker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:  "docker-http-test",
		Image: "redis:7",
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

	// Verify task was created via docker runner
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
	if tasks[0].Image != "redis:7" {
		t.Errorf("Task image = %q, want %q", tasks[0].Image, "redis:7")
	}
}

// ============== DOCKER DELETE TESTS ==============

func TestDeleteDockerJob(t *testing.T) {
	cfg := testConfig()
	mockProcess := NewMockRunner()
	mockDocker := NewMockRunner()

	agent := New(cfg, "test-agent", mockProcess)
	agent.dockerRunner = mockDocker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Add a docker job and task
	agent.StoreJob(&types.Job{Name: "docker-del", Driver: types.DriverDocker, Image: "nginx:latest"})
	agent.do(func(s *agentState) {
		s.tasks["docker-task-1"] = &types.Task{
			ID:      "docker-task-1",
			JobName: "docker-del",
			Driver:  types.DriverDocker,
			Image:   "nginx:latest",
			State:   types.TaskRunning,
		}
	})
	// Register task in docker runner so Stop finds it
	mockDocker.mu.Lock()
	mockDocker.tasks["docker-task-1"] = &types.Task{ID: "docker-task-1", State: types.TaskRunning}
	mockDocker.mu.Unlock()

	time.Sleep(10 * time.Millisecond)

	deleted := agent.deleteJob("docker-del")
	if deleted != 1 {
		t.Errorf("deleteJob returned %d, want 1", deleted)
	}
	time.Sleep(50 * time.Millisecond) // wait for async stop goroutine

	// Docker runner should have been called for stop
	if !mockDocker.WasStopped("docker-task-1") {
		t.Error("DockerRunner.Stop should have been called for docker task")
	}

	// Process runner should NOT have been called
	if mockProcess.WasStopped("docker-task-1") {
		t.Error("ExecRunner.Stop should NOT have been called for docker task")
	}
}

// ============== DOCKER STOP ALL TESTS ==============

func TestStopAllMixedTasks(t *testing.T) {
	cfg := testConfig()
	mockProcess := NewMockRunner()
	mockDocker := NewMockRunner()

	agent := New(cfg, "test-agent", mockProcess)
	agent.dockerRunner = mockDocker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Add a process task and a docker task
	agent.do(func(s *agentState) {
		s.tasks["process-task"] = &types.Task{
			ID:      "process-task",
			JobName: "process-job",
			Driver:  types.DriverExec,
			State:   types.TaskRunning,
		}
		s.tasks["docker-task"] = &types.Task{
			ID:      "docker-task",
			JobName: "docker-job",
			Driver:  types.DriverDocker,
			Image:   "nginx:latest",
			State:   types.TaskRunning,
		}
	})

	time.Sleep(10 * time.Millisecond)

	agent.StopAllTasks()
	time.Sleep(10 * time.Millisecond)

	// Process runner should stop process task
	if !mockProcess.WasStopped("process-task") {
		t.Error("ExecRunner should stop process task")
	}
	// Process runner should NOT stop docker task
	if mockProcess.WasStopped("docker-task") {
		t.Error("ExecRunner should NOT stop docker task")
	}

	// Docker runner should stop docker task
	if !mockDocker.WasStopped("docker-task") {
		t.Error("DockerRunner should stop docker task")
	}
	// Docker runner should NOT stop process task
	if mockDocker.WasStopped("process-task") {
		t.Error("DockerRunner should NOT stop process task")
	}
}

// ============== DOCKER LOG ROUTING TESTS ==============

func TestHandleLogsDockerTask(t *testing.T) {
	cfg := testConfig()
	mockProcess := NewMockRunner()
	mockDocker := NewMockRunner()

	agent := New(cfg, "test-agent", mockProcess)
	agent.dockerRunner = mockDocker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Start a docker job
	job := &types.Job{Name: "log-docker", Image: "nginx:latest"}
	task := newTask(job)
	if err := agent.startJob(job, task); err != nil {
		t.Fatalf("startJob failed: %v", err)
	}

	// Write to docker runner's stdout broadcaster
	broadcaster := mockDocker.GetStdout(task.ID)
	if broadcaster == nil {
		t.Fatal("No stdout broadcaster for docker task")
	}

	// Create request for log streaming
	reqCtx, reqCancel := context.WithCancel(context.Background())
	req := leanhttp.NewRequest(leanhttp.MethodGet, "/logs/"+task.ID+"/stdout", nil)
	req = req.WithContext(reqCtx)
	w := leanhttp.NewRecorder()

	done := make(chan struct{})
	go func() {
		agent.handleLogs(w, req)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	_, _ = broadcaster.Write([]byte("docker log line"))
	time.Sleep(50 * time.Millisecond)

	reqCancel()
	<-done

	body := w.Body.String()
	if !bytes.Contains([]byte(body), []byte("data: docker log line")) {
		t.Errorf("Body = %q, want SSE with 'data: docker log line'", body)
	}
}

// ============== DOCKER MONITOR TESTS ==============

func TestCheckTasksDockerCrash(t *testing.T) {
	cfg := testConfig()
	mockProcess := NewMockRunner()
	mockDocker := NewMockRunner()

	agent := New(cfg, "test-agent", mockProcess)
	agent.dockerRunner = mockDocker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Start a docker job
	job := &types.Job{Name: "crash-docker", Image: "myapp:v1"}
	task := newTask(job)
	if err := agent.startJob(job, task); err != nil {
		t.Fatalf("startJob failed: %v", err)
	}

	// Simulate container crash: remove from docker runner so Status returns Failed
	mockDocker.mu.Lock()
	delete(mockDocker.tasks, task.ID)
	mockDocker.mu.Unlock()

	// Run checkTasks
	agent.checkTasks()
	time.Sleep(100 * time.Millisecond)

	// Should have restarted via docker runner (new task, old ID gone)
	info := query(agent, func(s *agentState) *types.Task {
		for _, t := range s.tasks {
			if t.JobName == "crash-docker" {
				return t
			}
		}
		return nil
	})

	if info == nil {
		t.Fatal("Task not found after crash detection + restart")
	}
	if info.State != types.TaskRunning {
		t.Errorf("Task state = %q, want %q (after restart)", info.State, types.TaskRunning)
	}
}

func TestCheckTasksDockerHealthCheckFails(t *testing.T) {
	// Health check server that returns 500
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := testConfig()
	mockProcess := NewMockRunner()
	mockDocker := NewMockRunner()

	agent := New(cfg, "test-agent", mockProcess)
	agent.dockerRunner = mockDocker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	port := getPort(t, srv)

	job := &types.Job{
		Name:  "docker-health",
		Image: "myapp:v1",
		Ports: map[string]int{"http": 80},
		HealthCheck: &types.HealthCheck{
			Path:             "/health",
			Port:             "http",
			Timeout:          time.Second,
			FailureThreshold: 1,
		},
	}

	// Store job and create task manually
	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job
		s.tasks["docker-health-task"] = &types.Task{
			ID:      "docker-health-task",
			JobName: "docker-health",
			Driver:  types.DriverDocker,
			Image:   "myapp:v1",
			Ports:   map[string]int{"http": port},
			State:   types.TaskRunning,
		}
	})
	time.Sleep(10 * time.Millisecond)

	// Register in docker runner so Status() returns running
	mockDocker.mu.Lock()
	mockDocker.tasks["docker-health-task"] = &types.Task{
		ID:    "docker-health-task",
		State: types.TaskRunning,
	}
	mockDocker.mu.Unlock()

	// Run checkTasks
	agent.checkTasks()
	time.Sleep(50 * time.Millisecond)

	// Docker runner should have stopped the task
	if !mockDocker.WasStopped("docker-health-task") {
		t.Error("DockerRunner.Stop should be called for health check failure")
	}

	// Process runner should NOT have been involved
	if mockProcess.WasStopped("docker-health-task") {
		t.Error("ExecRunner.Stop should NOT be called for docker task health failure")
	}
}

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"time"

	"github.com/xinix00/lean/leanhttp"

	"github.com/xinix00/hop/internal/types"
)

// ============== CAPACITY CHECK TESTS ==============

// checkCapacity replicates the capacity check from handleRun for use in tests.
// Returns true if the agent has enough resources to run job.
func checkCapacity(a *Agent, job *types.Job) bool {
	return query(a, func(s *agentState) bool {
		usedCPU, usedMem := s.resourceUsage()
		if job.CPUShares > 0 && usedCPU+job.CPUShares > a.sysInfo.CPUCores*1024 {
			return false
		}
		if job.MemoryLimit > 0 && usedMem+job.MemoryLimit > a.sysInfo.MemoryBytes {
			return false
		}
		return true
	})
}

func TestAgentHasCapacity(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	// Set fake system info for predictable testing
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024}) // 1 core = 1024 shares

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Small job should fit
	smallJob := &types.Job{
		Name:        "small",
		CPUShares:   500,
		MemoryLimit: 512,
	}
	if !checkCapacity(agent, smallJob) {
		t.Error("hasCapacity should return true for small job")
	}

	// Large job should not fit (exceeds 1024 CPU shares)
	largeJob := &types.Job{
		Name:        "large",
		CPUShares:   2000,
		MemoryLimit: 2048,
	}
	if checkCapacity(agent, largeJob) {
		t.Error("hasCapacity should return false for job exceeding capacity")
	}
}

func TestAgentCapacityWithRunningTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024}) // 1024 CPU shares, 1024 bytes

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Store a job and create a running task
	job := &types.Job{
		Name:        "existing",
		Command:     "echo",
		CPUShares:   600,
		MemoryLimit: 600,
	}
	agent.StoreJob(job)

	// Add a running task that uses capacity
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:          "task-1",
			JobName:     "existing",
			State:       types.TaskRunning,
			CPUShares:   600,
			MemoryLimit: 600,
		}
	})

	time.Sleep(10 * time.Millisecond)

	// New job that would fit if alone, but not with existing task
	newJob := &types.Job{
		Name:        "new-job",
		CPUShares:   500,
		MemoryLimit: 500,
	}

	// Should not have capacity (600 + 500 > 1024)
	if checkCapacity(agent, newJob) {
		t.Error("hasCapacity should return false when existing tasks consume capacity")
	}
}

func TestAgentCapacityCountsFailedTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// A failed task is still a placed task until explicitly removed.
	job := &types.Job{
		Name:      "failed-job",
		CPUShares: 800,
	}
	agent.StoreJob(job)

	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:        "task-1",
			JobName:   "failed-job",
			State:     types.TaskFailed,
			CPUShares: 800,
		}
	})

	time.Sleep(10 * time.Millisecond)

	newJob := &types.Job{
		Name:      "new-job",
		CPUShares: 500,
	}

	if checkCapacity(agent, newJob) {
		t.Error("hasCapacity should count a failed task that is still present")
	}
}

func TestAgentCapacityFreedWhenTaskIsGone(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	job := &types.Job{
		Name:      "old-job",
		CPUShares: 800,
	}
	agent.StoreJob(job)

	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:        "task-1",
			JobName:   "old-job",
			State:     types.TaskRunning,
			CPUShares: 800,
		}
	})

	time.Sleep(10 * time.Millisecond)

	newJob := &types.Job{
		Name:      "new-job",
		CPUShares: 500,
	}

	if checkCapacity(agent, newJob) {
		t.Fatal("hasCapacity ignored a task that was still present")
	}
	agent.do(func(s *agentState) { delete(s.tasks, "task-1") })
	if !checkCapacity(agent, newJob) {
		t.Error("hasCapacity was not restored after the stopped task was removed")
	}
}

func TestAgentCapacityUsesSystemDefaults(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 0 // Not configured - uses system default
	cfg.Capacity.Memory = 0    // Not configured - uses system default

	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// When config=0, system values are used as limits
	// A job within system capacity should fit
	smallJob := &types.Job{
		Name:        "small-job",
		CPUShares:   100,         // Should fit on any system
		MemoryLimit: 1024 * 1024, // 1MB - should fit on any system
	}

	if !checkCapacity(agent, smallJob) {
		t.Error("hasCapacity should return true for small job within system capacity")
	}

	// A job exceeding system capacity should NOT fit
	hugeJob := &types.Job{
		Name:        "huge-job",
		CPUShares:   1000000,          // More than any system has
		MemoryLimit: 1000000000000000, // 1PB - more than any system
	}

	if checkCapacity(agent, hugeJob) {
		t.Error("hasCapacity should return false for job exceeding system capacity")
	}
}

func TestAgentCapacityJobWithNoLimits(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Job with no resource limits (0 means "don't check")
	noLimitJob := &types.Job{
		Name:        "no-limit-job",
		CPUShares:   0,
		MemoryLimit: 0,
	}

	// Should always fit (no limits to check)
	if !checkCapacity(agent, noLimitJob) {
		t.Error("hasCapacity should return true for job with no limits")
	}
}

func TestAgentCapacityExceedsMemory(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 100, MemoryBytes: 1024}) // lots of CPU, little memory

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Job fits in CPU but exceeds memory
	bigMemJob := &types.Job{
		Name:        "big-mem",
		CPUShares:   100,  // Fits (100 cores * 1024 = 102400 shares)
		MemoryLimit: 2048, // Exceeds 1024 bytes
	}

	if checkCapacity(agent, bigMemJob) {
		t.Error("hasCapacity should fail when memory exceeds system limit")
	}
}

func TestAgentCapacityExceedsCPU(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024 * 1024 * 1024}) // little CPU, lots of memory

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Job fits in memory but exceeds CPU
	bigCPUJob := &types.Job{
		Name:        "big-cpu",
		CPUShares:   2000,        // Exceeds 1024 shares (1 core)
		MemoryLimit: 1024 * 1024, // Fits in 1GB
	}

	if checkCapacity(agent, bigCPUJob) {
		t.Error("hasCapacity should fail when CPU exceeds system limit")
	}
}

func TestAgentConcurrentCapacityCheck(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 10, MemoryBytes: 1024 * 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	var wg sync.WaitGroup
	results := make(chan bool, 100)

	// Many concurrent capacity checks
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			job := &types.Job{
				Name:      "job-" + string(rune('0'+n%10)),
				CPUShares: 100, // Fits in 10 cores * 1024 = 10240 shares
			}
			results <- checkCapacity(agent, job)
		}(i)
	}

	wg.Wait()
	close(results)

	// All should succeed (no running tasks)
	for r := range results {
		if !r {
			t.Error("hasCapacity should return true for all concurrent checks when no tasks running")
		}
	}
}

func TestAgentCapacityExactLimit(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024}) // 1024 CPU shares, 1024 bytes

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Job exactly at limit should fit
	exactJob := &types.Job{
		Name:        "exact-job",
		CPUShares:   1024,
		MemoryLimit: 1024,
	}

	if !checkCapacity(agent, exactJob) {
		t.Error("hasCapacity should return true for job at exact limit")
	}
}

// TestAgentCapacityWithoutJobDefinition verifies that running tasks track their own
// resource usage (CPUShares/MemoryLimit on Task) instead of looking up the Job definition.
// After leader recovery, job definitions might not be in the store yet, but the agent
// must still correctly report capacity based on what's actually running.
func TestAgentCapacityWithoutJobDefinition(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024}) // 1024 CPU shares, 1024 bytes

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Running task WITHOUT job definition in store (simulates post-recovery state)
	// The task uses 600 CPU shares and 600 bytes memory
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:          "task-1",
			JobName:     "orphaned-job",
			State:       types.TaskRunning,
			CPUShares:   600,
			MemoryLimit: 600,
		}
	})

	time.Sleep(10 * time.Millisecond)

	// New job that would fit if alone, but not with existing task
	newJob := &types.Job{
		Name:        "new-job",
		CPUShares:   500,
		MemoryLimit: 500,
	}

	// BUG: without fix, hasCapacity looks up Job by name, finds nil (no job in store),
	// and counts the running task's resource usage as 0. This allows over-provisioning.
	if checkCapacity(agent, newJob) {
		t.Error("hasCapacity should return false: running task uses 600 CPU + 600 mem, " +
			"new job needs 500 CPU + 500 mem, but total capacity is only 1024 each. " +
			"Task resource usage must be tracked on the Task itself, not looked up from Job definition")
	}
}

// TestConcurrentDispatchRespectsCapacity verifies that concurrent /run requests
// cannot over-provision an agent. With 2 CPU cores (2048 shares) and 3 concurrent
// requests each needing 1024 shares, exactly 2 should be accepted and 1 rejected.
//
// BUG: handleRun checks hasCapacity THEN starts the job in a goroutine.
// Between the check and the task appearing in state, other requests can pass
// the same check (TOCTOU race). This test fails until the race is fixed.
func TestConcurrentDispatchRespectsCapacity(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()

	// Slow runner: widens the race window so all 3 requests check capacity
	// before any task is added to state
	mockRunner.onRun = func(job *types.Job) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 2, MemoryBytes: 1024 * 1024 * 1024}) // 2 cores = 2048 shares

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Send 3 concurrent requests, each needing 1024 shares (1 core)
	var wg sync.WaitGroup
	codes := make([]int, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			job := types.Job{
				Name:      fmt.Sprintf("concurrent-%d", n),
				Command:   "echo hello",
				CPUShares: 1024,
			}
			body, _ := json.Marshal(job)
			req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
			w := leanhttp.NewRecorder()
			agent.handleRun(w, req)
			codes[n] = w.Code
		}(i)
	}

	wg.Wait()

	// Wait for goroutines to finish starting tasks
	time.Sleep(200 * time.Millisecond)

	accepted := 0
	rejected := 0
	for _, code := range codes {
		switch code {
		case 202:
			accepted++
		case 503:
			rejected++
		}
	}

	// Agent has 2048 shares, each job needs 1024 → max 2 jobs
	if accepted > 2 {
		t.Errorf("accepted %d jobs but agent only has capacity for 2 (2048 shares, 1024 each)", accepted)
	}
	if rejected < 1 {
		t.Errorf("expected at least 1 rejection but got accepted=%d rejected=%d", accepted, rejected)
	}
}

// TestTaskInStateImmediatelyAfterAccept verifies that after handleRun returns
// 202 Accepted, the task is immediately visible in state (resourceUsage counts it).
//
// BUG: Currently, handleRun uses a reservation system (reservedCPU/reservedMem)
// as a placeholder until the goroutine finishes runner.Run() and adds the task
// to state. But restartTask doesn't use reservations, and the reservation system
// adds complexity. The real fix: add the task to state BEFORE running the process.
func TestTaskInStateImmediatelyAfterAccept(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()

	// Slow runner: simulates process startup taking time
	started := make(chan struct{})
	mockRunner.onRun = func(job *types.Job) error {
		<-started // block until test releases
		return nil
	}

	// testConfig caps Capacity.CPUShares at 1000; clear it so SetSysInfo drives
	// the effective capacity (effectiveCPUShares takes the min of cap and detected).
	cfg.Capacity.CPUShares = 0
	cfg.Capacity.Memory = 0

	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 2, MemoryBytes: 1024 * 1024 * 1024}) // 2 cores = 2048 shares

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Send /run request (1024 CPU shares = 1 core)
	job := types.Job{
		Name:      "test-job",
		Command:   "echo hello",
		CPUShares: 1024,
	}
	body, _ := json.Marshal(job)
	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()
	agent.handleRun(w, req)

	if w.Code != 202 {
		t.Fatalf("expected 202, got %d", w.Code)
	}

	// Runner is still blocked — task should already be in state
	tasks := query(agent, func(s *agentState) []*types.Task {
		result := make([]*types.Task, 0)
		for _, task := range s.tasks {
			result = append(result, task)
		}
		return result
	})

	if len(tasks) != 1 {
		t.Errorf("expected 1 task in state immediately after accept, got %d "+
			"(task only appears after runner.Run completes — overbooking window!)", len(tasks))
	}

	// Resource usage should reflect the accepted task
	cpu := query(agent, func(s *agentState) int {
		c, _ := s.resourceUsage()
		return c
	})

	if cpu < 1024 {
		t.Errorf("expected resourceUsage CPU >= 1024 after accept, got %d "+
			"(only reservedCPU counts it, not a real task)", cpu)
	}

	// Release runner so goroutine can finish
	close(started)
	time.Sleep(50 * time.Millisecond)
}

func TestAgentCapacityMultipleRunningTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024 * 1024}) // 1024 CPU shares

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Add 3 jobs using 300 CPU each (total 900)
	for i := 0; i < 3; i++ {
		jobName := "job-" + string(rune('a'+i))
		agent.StoreJob(&types.Job{
			Name:      jobName,
			CPUShares: 300,
		})
		agent.do(func(s *agentState) {
			s.tasks["task-"+string(rune('a'+i))] = &types.Task{
				ID:        "task-" + string(rune('a'+i)),
				JobName:   jobName,
				State:     types.TaskRunning,
				CPUShares: 300,
			}
		})
	}

	time.Sleep(10 * time.Millisecond)

	// Used: 900, remaining: 124
	smallJob := &types.Job{Name: "small", CPUShares: 100}
	if !checkCapacity(agent, smallJob) {
		t.Error("hasCapacity should fit 100 CPU (900 used, 124 remaining)")
	}

	// This should not fit
	mediumJob := &types.Job{Name: "medium", CPUShares: 150}
	if checkCapacity(agent, mediumJob) {
		t.Error("hasCapacity should not fit 150 CPU (900 used, 124 remaining)")
	}
}

// TestCrashedTaskStillReservesCapacity verifies that when a task crashes and the
// monitor detects it, capacity is NOT freed before restartTask runs.
//
// BUG: monitor sets state to TaskFailed → resourceUsage() no longer counts it →
// handleRun sees free capacity → accepts new dispatch → restartTask also creates
// replacement → over-provisioning (more tasks than CPU cores).
//
// The fix: monitor sets state to TaskStopping (still counts for capacity) instead
// of TaskFailed. restartTask handles the final state transition.
func TestCrashedTaskStillReservesCapacity(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024 * 1024}) // 1024 CPU shares

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Fill capacity: 1 task using all 1024 CPU shares
	job := &types.Job{
		Name:      "my-job",
		CPUShares: 1024,
	}
	agent.StoreJob(job)
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:        "task-1",
			JobName:   "my-job",
			State:     types.TaskRunning,
			CPUShares: 1024,
		}
	})
	time.Sleep(10 * time.Millisecond)

	// Simulate process crash: remove from runner so Status() returns TaskFailed
	mockRunner.mu.Lock()
	delete(mockRunner.tasks, "task-1")
	mockRunner.mu.Unlock()

	// Make runner.Stop() slow to simulate Docker cleanup (restartTask calls Stop before swap)
	stopStarted := make(chan struct{})
	stopDone := make(chan struct{})
	mockRunner.onStop = func(task *types.Task) error {
		close(stopStarted)
		<-stopDone // block until test signals
		return nil
	}

	// Run one monitor cycle (this detects the crash and starts restartTask in background)
	agent.checkTasks()

	// Wait for restartTask to begin (it's inside the slow Stop)
	<-stopStarted

	// BUG: During restart (Stop still running), capacity should still be reserved.
	// A new task with 1024 CPU should NOT fit.
	newJob := &types.Job{Name: "new-job", CPUShares: 1024}
	if checkCapacity(agent, newJob) {
		t.Error("BUG: capacity freed after crash before restart completed — " +
			"crashed task should still reserve capacity until restart decision is made")
	}

	// Unblock restartTask so test cleanup works
	close(stopDone)
	time.Sleep(20 * time.Millisecond)
}

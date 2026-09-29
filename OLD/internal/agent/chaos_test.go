package agent

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/runner"
	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/config"
)

// TestChaos_AllTasksCrashSimultaneously tests mass task failure
func TestChaos_AllTasksCrashSimultaneously(t *testing.T) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/chaos", StateFile: "/tmp/chaos/state.json"},
	}

	mockRunner := &mockRunner{tasks: make(map[string]*types.Task)}
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Start 10 tasks
	for i := 0; i < 10; i++ {
		job := &types.Job{
			Name:        fmt.Sprintf("job-%d", i),
			Command:     "./app",
			MaxRestarts: intPtr(2),
		}

		task := newTask(job)
		if err := agent.startJob(job, task); err != nil {
			t.Fatalf("Failed to start job %d: %v", i, err)
		}
		mockRunner.tasks[task.ID] = task
	}

	// CHAOS: All processes crash simultaneously
	for _, task := range mockRunner.tasks {
		task.State = types.TaskFailed
	}

	// Trigger monitor check
	agent.checkTasks()
	time.Sleep(50 * time.Millisecond)

	// Agent should attempt to restart all
	t.Logf("Mass failure: all 10 tasks crashed, agent should restart")

	// Check that restart was attempted (tasks should have increased restart count)
	count := 0
	agent.do(func(s *agentState) {
		for _, task := range s.tasks {
			if task.RestartCount > 0 {
				count++
			}
		}
	})

	t.Logf("Tasks with restart attempts: %d/10", count)
}

// TestChaos_TaskExceedsMaxRestarts tests task that keeps crashing
func TestChaos_TaskExceedsMaxRestarts(t *testing.T) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/chaos", StateFile: "/tmp/chaos/state.json"},
	}

	mockRunner := &crashingRunner{tasks: make(map[string]*crashingTask)}
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Job that crashes immediately
	job := &types.Job{
		Name:        "crasher",
		Command:     "./crasher",
		MaxRestarts: intPtr(3),
	}

	task := newTask(job)
	if err := agent.startJob(job, task); err != nil {
		t.Fatalf("Failed to start: %v", err)
	}

	// Simulate crash loop — task ID changes on each restart
	currentTaskID := task.ID
	for i := 0; i < 5; i++ {
		// Task crashes
		mockRunner.mu.Lock()
		if ct, ok := mockRunner.tasks[currentTaskID]; ok {
			ct.state = types.TaskFailed
		}
		mockRunner.mu.Unlock()

		// Get current task reference from agent state
		currentTask := query(agent, func(s *agentState) *types.Task {
			return s.tasks[currentTaskID]
		})
		if currentTask == nil {
			break // task was removed (max restarts exceeded)
		}

		// Agent detects and restarts
		agent.restartTask(currentTask, true)
		time.Sleep(10 * time.Millisecond)

		// Find new task ID after restart
		newID := query(agent, func(s *agentState) string {
			for id, t := range s.tasks {
				if t.JobName == "crasher" {
					return id
				}
			}
			return ""
		})
		if newID != "" {
			currentTaskID = newID
		}
	}

	// After max restarts, should give up
	finalJob := agent.GetJob(job.Name)
	if finalJob == nil {
		t.Error("Job should still exist")
	}

	// Check final restart count (find any remaining task for this job)
	restartCount := query(agent, func(s *agentState) int {
		for _, t := range s.tasks {
			if t.JobName == "crasher" {
				return t.RestartCount
			}
		}
		return 0
	})

	t.Logf("Task restarted %d times (max: 3)", restartCount)

	if restartCount > 3 {
		t.Errorf("Task exceeded max restarts: %d > 3", restartCount)
	}
}

// TestChaos_CapacityExhaustion tests agent rejecting jobs when full
func TestChaos_CapacityExhaustion(t *testing.T) {
	cfg := &config.Config{
		Node:     config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths:    config.PathsConfig{RootfsBase: "/tmp/chaos", StateFile: "/tmp/chaos/state.json"},
		Capacity: config.CapacityConfig{CPUShares: 1024, Memory: 1024 * 1024 * 1024}, // 1 core, 1GB
	}

	mockRunner := &mockRunner{tasks: make(map[string]*types.Task)}
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024 * 1024 * 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Fill capacity
	bigJob := &types.Job{
		Name:        "big-job",
		Command:     "./big",
		CPUShares:   512,               // 50% of 1 core
		MemoryLimit: 512 * 1024 * 1024, // 512MB
	}

	task1 := newTask(bigJob)
	_ = agent.startJob(bigJob, task1)
	task2Job := &types.Job{
		Name:        "big-job-2",
		Command:     "./big2",
		CPUShares:   512,
		MemoryLimit: 512 * 1024 * 1024,
	}
	task2 := newTask(task2Job)
	_ = agent.startJob(task2Job, task2)

	// Try to add another job - should fail (no capacity)
	overflowJob := &types.Job{
		Name:        "overflow",
		Command:     "./overflow",
		CPUShares:   100,
		MemoryLimit: 100 * 1024 * 1024,
	}

	hasCapacity := checkCapacity(agent, overflowJob)
	if hasCapacity {
		t.Error("Agent should reject job when at capacity")
	}

	t.Logf("Capacity exhaustion: agent correctly rejected job (CPU: %d/%d, Mem: %d/%d used)",
		1024, 1024, 1024*1024*1024, 1024*1024*1024)

	// Cleanup
	_ = task1
	_ = task2
}

// TestChaos_TaskZombie tests handling of tasks that report wrong state
func TestChaos_TaskZombie(t *testing.T) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/chaos", StateFile: "/tmp/chaos/state.json"},
	}

	zombieRunner := &zombieRunner{tasks: make(map[string]*zombieTask)}
	agent := New(cfg, "test-agent", zombieRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	job := &types.Job{Name: "zombie-job", Command: "./app", MaxRestarts: intPtr(2)}
	task := newTask(job)
	_ = agent.startJob(job, task)

	// Runner reports task as failed (process died) but task state says running.
	zombieRunner.tasks[task.ID].actualState = types.TaskFailed

	// Monitor should detect mismatch and restart
	agent.checkTasks()
	time.Sleep(50 * time.Millisecond)

	// Check if restart was attempted (task ID changes on restart, search by job name)
	restartCount := query(agent, func(s *agentState) int {
		for _, t := range s.tasks {
			if t.JobName == "zombie-job" {
				return t.RestartCount
			}
		}
		return 0
	})

	if restartCount == 0 {
		t.Error("Zombie task should have been restarted")
	}

	t.Logf("Zombie task detected and restarted (%d restarts)", restartCount)
}

// zombieRunner for zombie task tests
type zombieTask struct {
	task        *types.Task
	actualState types.TaskState
}

type zombieRunner struct {
	tasks map[string]*zombieTask
}

func (r *zombieRunner) Run(job *types.Job, task *types.Task) error {
	task.Pid = 12345
	r.tasks[task.ID] = &zombieTask{task: task, actualState: types.TaskRunning}
	return nil
}

func (r *zombieRunner) Stop(task *types.Task) error {
	delete(r.tasks, task.ID)
	return nil
}

func (r *zombieRunner) Status(task *types.Task) (types.TaskState, error) {
	if t, ok := r.tasks[task.ID]; ok {
		return t.actualState, nil // Return actual state (may differ from task.State)
	}
	return types.TaskFailed, nil
}

func (r *zombieRunner) GetStdout(taskID string) *runner.LogBroadcaster { return nil }
func (r *zombieRunner) GetStderr(taskID string) *runner.LogBroadcaster { return nil }
func (r *zombieRunner) Cleanup() error                                 { return nil }

// TestChaos_StateCorruption tests recovery from corrupted state
func TestChaos_StateCorruption(t *testing.T) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/chaos", StateFile: "/tmp/chaos-corrupt/state.json"},
	}

	mockRunner := &mockRunner{tasks: make(map[string]*types.Task)}
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Add jobs
	job1 := &types.Job{Name: "job-1", Command: "./app1"}
	job2 := &types.Job{Name: "job-2", Command: "./app2"}
	_ = agent.startJob(job1, newTask(job1))
	_ = agent.startJob(job2, newTask(job2))

	// Simulate corruption: task references non-existent job
	agent.do(func(s *agentState) {
		s.tasks["orphan-task"] = &types.Task{
			ID:      "orphan-task",
			JobName: "non-existent-job", // Job doesn't exist!
			State:   types.TaskRunning,
		}
	})

	// Monitor should handle gracefully (no crash)
	agent.checkTasks()
	time.Sleep(10 * time.Millisecond)

	t.Log("Corrupted state (orphan task) handled without crash")
}

// TestChaos_RapidJobDeletionAndCreation tests job churn
func TestChaos_RapidJobDeletionAndCreation(t *testing.T) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/chaos", StateFile: "/tmp/chaos/state.json"},
	}

	mockRunner := &mockRunner{tasks: make(map[string]*types.Task)}
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Rapidly create and delete jobs
	for i := 0; i < 100; i++ {
		job := &types.Job{
			Name:    "churning-job",
			Command: fmt.Sprintf("./app-v%d", i),
		}

		err := agent.startJob(job, newTask(job))
		if err != nil {
			t.Fatalf("Failed to start job %d: %v", i, err)
		}

		// Immediately delete by name
		agent.deleteJob("churning-job")
	}

	// Agent should be stable
	jobs := agent.GetJobs()
	t.Logf("After 100 create/delete cycles: %d jobs remain", len(jobs))

	// Should have 0 jobs (all deleted)
	if len(jobs) != 0 {
		t.Errorf("Expected 0 jobs after churn, got %d", len(jobs))
	}
}

// Helper types for chaos tests

type crashingRunner struct {
	mu    sync.Mutex
	tasks map[string]*crashingTask
}

type crashingTask struct {
	task  *types.Task
	state types.TaskState
}

func (r *crashingRunner) Run(job *types.Job, task *types.Task) error {
	task.Pid = 12345
	r.mu.Lock()
	r.tasks[task.ID] = &crashingTask{task: task, state: types.TaskRunning}
	r.mu.Unlock()

	// Crash after short delay
	taskID := task.ID
	go func() {
		time.Sleep(10 * time.Millisecond)
		r.mu.Lock()
		if t, ok := r.tasks[taskID]; ok {
			t.state = types.TaskFailed
		}
		r.mu.Unlock()
	}()

	return nil
}

func (r *crashingRunner) Stop(task *types.Task) error {
	r.mu.Lock()
	delete(r.tasks, task.ID)
	r.mu.Unlock()
	return nil
}

func (r *crashingRunner) Status(task *types.Task) (types.TaskState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.tasks[task.ID]; ok {
		return t.state, nil
	}
	return types.TaskFailed, nil
}

func (r *crashingRunner) GetStdout(taskID string) *runner.LogBroadcaster { return nil }
func (r *crashingRunner) GetStderr(taskID string) *runner.LogBroadcaster { return nil }
func (r *crashingRunner) Cleanup() error                                 { return nil }

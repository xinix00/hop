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

// BenchmarkTaskCreation measures task creation overhead
func BenchmarkTaskCreation(b *testing.B) {
	cfg := &config.Config{
		Node: config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{
			RootfsBase: "/tmp/hop-bench",
			StateFile:  "/tmp/hop-bench/state.json",
		},
	}

	mockRunner := &mockRunner{tasks: make(map[string]*types.Task)}
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		job := &types.Job{
			Name:    fmt.Sprintf("bench-job-%d", i),
			Command: "echo test",
			Count:   1,
		}

		ports := map[string]int{"http": 8080 + i}
		task := &types.Task{
			ID:      fmt.Sprintf("task-%d", i),
			JobName: job.Name,
			Ports:   ports,
			State:   types.TaskRunning,
		}
		mockRunner.tasks[task.ID] = task

		agent.do(func(s *agentState) {
			s.jobs[job.Name] = job

			s.tasks[task.ID] = task
		})
	}
}

// BenchmarkGetJobs measures job list retrieval performance
func BenchmarkGetJobs(b *testing.B) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/bench", StateFile: "/tmp/bench/state.json"},
	}

	agent := New(cfg, "test-agent", &mockRunner{tasks: make(map[string]*types.Task)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Pre-populate with 1000 jobs
	for i := 0; i < 1000; i++ {
		job := &types.Job{
			Name:    fmt.Sprintf("job-%d", i),
			Command: "echo test",
		}
		agent.do(func(s *agentState) {
			s.jobs[job.Name] = job

		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		jobs := agent.GetJobs()
		if len(jobs) != 1000 {
			b.Fatalf("Expected 1000 jobs, got %d", len(jobs))
		}
	}
}

// BenchmarkSyncJobs measures job synchronization overhead
func BenchmarkSyncJobs(b *testing.B) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/bench", StateFile: "/tmp/bench/state.json"},
	}

	agent := New(cfg, "test-agent", &mockRunner{tasks: make(map[string]*types.Task)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Create jobs for sync
	jobs := make([]*types.Job, 100)
	for i := 0; i < 100; i++ {
		jobs[i] = &types.Job{
			Name:    fmt.Sprintf("job-%d", i),
			Command: "echo test",
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		agent.SyncJobs(jobs, time.Now())
	}
}

// BenchmarkCapacityCheck measures capacity checking performance
func BenchmarkCapacityCheck(b *testing.B) {
	cfg := &config.Config{
		Node:     config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths:    config.PathsConfig{RootfsBase: "/tmp/bench", StateFile: "/tmp/bench/state.json"},
		Capacity: config.CapacityConfig{CPUShares: 8192, Memory: 16 * 1024 * 1024 * 1024},
	}

	agent := New(cfg, "test-agent", &mockRunner{tasks: make(map[string]*types.Task)})
	agent.SetSysInfo(SystemInfo{CPUCores: 8, MemoryBytes: 16 * 1024 * 1024 * 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Add some running tasks
	for i := 0; i < 50; i++ {
		job := &types.Job{
			Name:        fmt.Sprintf("job-%d", i),
			Command:     "echo test",
			CPUShares:   100,
			MemoryLimit: 100 * 1024 * 1024,
		}
		task := &types.Task{
			ID:          fmt.Sprintf("task-%d", i),
			JobName:     job.Name,
			State:       types.TaskRunning,
			CPUShares:   100,
			MemoryLimit: 100 * 1024 * 1024,
		}
		agent.do(func(s *agentState) {
			s.jobs[job.Name] = job
			s.tasks[task.ID] = task
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Benchmark the resourceUsage query (replaces hasCapacity)
		_ = query(agent, func(s *agentState) int {
			cpu, _ := s.resourceUsage()
			return cpu
		})
	}
}

// BenchmarkStateQuery measures state query performance
func BenchmarkStateQuery(b *testing.B) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/bench", StateFile: "/tmp/bench/state.json"},
	}

	agent := New(cfg, "test-agent", &mockRunner{tasks: make(map[string]*types.Task)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Populate state
	for i := 0; i < 100; i++ {
		job := &types.Job{Name: fmt.Sprintf("job-%d", i), Command: "echo test"}
		task := &types.Task{ID: fmt.Sprintf("task-%d", i), JobName: job.Name, State: types.TaskRunning}
		agent.do(func(s *agentState) {
			s.jobs[job.Name] = job

			s.tasks[task.ID] = task
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = query(agent, func(s *agentState) int {
			return len(s.tasks)
		})
	}
}

// BenchmarkConcurrentStateAccess measures concurrent state access
func BenchmarkConcurrentStateAccess(b *testing.B) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/bench", StateFile: "/tmp/bench/state.json"},
	}

	agent := New(cfg, "test-agent", &mockRunner{tasks: make(map[string]*types.Task)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			jobName := fmt.Sprintf("job-%d", i%100)
			_ = agent.GetJob(jobName)
			i++
		}
	})
}

// Mock runner for benchmarks
type mockRunner struct {
	mu    sync.Mutex
	tasks map[string]*types.Task
}

func (m *mockRunner) Run(job *types.Job, task *types.Task) error {
	task.Pid = 12345
	m.mu.Lock()
	m.tasks[task.ID] = task
	m.mu.Unlock()
	return nil
}

func (m *mockRunner) Stop(task *types.Task) error {
	m.mu.Lock()
	delete(m.tasks, task.ID)
	m.mu.Unlock()
	return nil
}

func (m *mockRunner) Status(task *types.Task) (types.TaskState, error) {
	m.mu.Lock()
	t, ok := m.tasks[task.ID]
	m.mu.Unlock()
	if ok {
		return t.State, nil
	}
	return types.TaskFailed, nil
}

func (m *mockRunner) GetStdout(taskID string) *runner.LogBroadcaster { return nil }
func (m *mockRunner) GetStderr(taskID string) *runner.LogBroadcaster { return nil }
func (m *mockRunner) Cleanup() error                                 { return nil }

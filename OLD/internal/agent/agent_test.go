package agent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// TestResourceUsageCollapsesSharegroup bewijst dat de capaciteit sharegroup-
// leden tot hun pool collapst: twee apps in dezelfde sharegroup (pool 2) tellen
// als 2 cores samen, niet 2×2. Zonder collapse zou een 3-core node onterecht
// "vol" melden bij het tweede lid (de dubbeltelling-bug).
func TestResourceUsageCollapsesSharegroup(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	agent.do(func(s *agentState) {
		// Sharegroup "web", pool = 2 cores (CPUShares 2048), twee leden.
		s.jobs["web"] = &types.Job{Name: "web", CPUShares: 2048, Tags: map[string]string{"sharegroup": "web"}}
		s.tasks["w1"] = &types.Task{ID: "w1", JobName: "web", CPUShares: 2048, MemoryLimit: 64 << 20, State: types.TaskRunning}
		s.tasks["w2"] = &types.Task{ID: "w2", JobName: "web", CPUShares: 2048, MemoryLimit: 64 << 20, State: types.TaskRunning}
		// Eén dedicated app van 1 core.
		s.jobs["solo"] = &types.Job{Name: "solo", CPUShares: 1024}
		s.tasks["s1"] = &types.Task{ID: "s1", JobName: "solo", CPUShares: 1024, MemoryLimit: 64 << 20, State: types.TaskRunning}
	})

	cpu := query(agent, func(s *agentState) int { c, _ := s.resourceUsage(); return c })
	// web-pool één keer (2048) + solo (1024) = 3072 — NIET 2048+2048+1024=5120.
	if cpu != 3072 {
		t.Fatalf("resourceUsage CPU = %d, want 3072 (pool één keer geteld)", cpu)
	}
	// Geheugen telt WÉL per lid (eigen partitie): 3 × 64MB.
	mem := query(agent, func(s *agentState) uint64 { _, m := s.resourceUsage(); return m })
	if mem != 3*(64<<20) {
		t.Fatalf("resourceUsage mem = %d, want 3×64MB (per lid)", mem)
	}
}

// TestEveryTaskInStateHoldsItsReservation pins the accounting rule: capacity is
// decided by PRESENCE in the task map, not by task state. A task is inserted
// when it is admitted (the record IS the reservation) and deleted the moment it
// really lets go — stop, delete, preemption, shutdown, the restart swap, an
// unplaceable hand-back. So a crashed or crashing task still holds its core:
// it is about to be restarted right here, or waiting for an operator.
//
// Filtering on state is how this drifted before: Failed counted as free, so a
// new job was admitted into a core that the pending restart was about to
// reclaim, and the two fought over it (26-07: an unplaceable app slipped in
// during a restart flap and stormed a 3-core node).
func TestEveryTaskInStateHoldsItsReservation(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "test-agent", NewMockRunner())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	states := []types.TaskState{
		types.TaskRunning,
		types.TaskStopping, // crashed, restart in flight
		types.TaskFailed,   // out of restarts, waiting for an operator
	}
	agent.do(func(s *agentState) {
		for i, st := range states {
			name := fmt.Sprintf("job-%d", i)
			s.jobs[name] = &types.Job{Name: name, CPUShares: 1024}
			s.tasks[name] = &types.Task{ID: name, JobName: name, CPUShares: 1024, MemoryLimit: 64 << 20, State: st}
		}
	})

	want := len(states) * 1024
	cpu := query(agent, func(s *agentState) int { c, _ := s.resourceUsage(); return c })
	if cpu != want {
		t.Fatalf("resourceUsage CPU = %d, want %d — every task in state holds its core, whatever its state", cpu, want)
	}
	mem := query(agent, func(s *agentState) uint64 { _, m := s.resourceUsage(); return m })
	if mem != uint64(len(states))*(64<<20) {
		t.Fatalf("resourceUsage mem = %d, want %d", mem, uint64(len(states))*(64<<20))
	}

	// Same rule for a sharegroup: a failed member keeps the pool reserved, so a
	// joining member is still free instead of paying for the pool twice.
	agent.do(func(s *agentState) {
		s.jobs["grp"] = &types.Job{Name: "grp", CPUShares: 2048, Tags: map[string]string{"sharegroup": "pool"}}
		s.tasks["g1"] = &types.Task{ID: "g1", JobName: "grp", CPUShares: 2048, MemoryLimit: 64 << 20, State: types.TaskFailed}
	})
	if !query(agent, func(s *agentState) bool { return s.sharegroupRunning("pool") }) {
		t.Fatal("sharegroupRunning = false with a failed member, want true (the pool stays reserved)")
	}

	// And releasing is deletion, not a state change: drop the record and the
	// core comes back.
	agent.do(func(s *agentState) { delete(s.tasks, "job-0") })
	if cpu := query(agent, func(s *agentState) int { c, _ := s.resourceUsage(); return c }); cpu != want-1024+2048 {
		t.Fatalf("after deleting one task CPU = %d, want %d (deletion is what frees a core)", cpu, want-1024+2048)
	}
}

// ============== BASIC AGENT TESTS ==============

func TestAgentNew(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()

	agent := New(cfg, "test-agent", mockRunner)

	if agent.ID() != "test-agent" {
		t.Errorf("ID() = %q, want %q", agent.ID(), "test-agent")
	}
	if agent.Endpoint() != "http://127.0.0.1:8080" {
		t.Errorf("Endpoint() = %q, want %q", agent.Endpoint(), "http://127.0.0.1:8080")
	}
}

func TestAgentNewWithNilRunner(t *testing.T) {
	cfg := testConfig()

	// New with nil runner should create default ExecRunner
	agent := New(cfg, "test-agent", nil)

	if agent.execRunner == nil {
		t.Error("Agent execRunner should not be nil when created with nil runner")
	}
}

func TestAgentInit(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	// Keep this unit test independent of whether the host running it happens to
	// have a Docker CLI/daemon; DockerRunner cleanup has its own focused tests.
	agent.dockerRunner = NewMockRunner()

	// Init should call Cleanup on runner
	err := agent.Init()
	if err != nil {
		t.Errorf("Init() returned error: %v", err)
	}
}

// ============== TASK MANAGEMENT TESTS ==============

func TestAgentStartJob(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "test-job",
		Command: "echo hello",
	}

	task := newTask(job)
	err := agent.startJob(job, task)
	if err != nil {
		t.Fatalf("startJob failed: %v", err)
	}

	if task.JobName != "test-job" {
		t.Errorf("task.JobName = %q, want %q", task.JobName, "test-job")
	}
	if task.State != types.TaskRunning {
		t.Errorf("task.State = %q, want %q", task.State, types.TaskRunning)
	}
}

func TestAgentStartJobRunnerError(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	mockRunner.SetRunError(ErrSimulated)

	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "failing-job",
		Command: "echo",
	}

	task := newTask(job)
	err := agent.startJob(job, task)
	if err == nil {
		t.Error("startJob should fail when runner returns error")
	}
}

func TestAgentDeleteJob(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Store job and task
	agent.StoreJob(&types.Job{Name: "test-job", Command: "echo"})
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:      "task-1",
			JobName: "test-job",
			State:   types.TaskRunning,
		}
	})

	time.Sleep(10 * time.Millisecond)

	deleted := agent.deleteJob("test-job")

	if deleted != 1 {
		t.Errorf("deleteJob returned %d, want 1", deleted)
	}

	// Job should be removed
	if agent.GetJob("test-job") != nil {
		t.Error("Job should be removed after delete")
	}
}

func TestAgentStopAllTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Create some running tasks
	for i := 0; i < 3; i++ {
		jobName := "job-" + string(rune('a'+i))
		job := &types.Job{
			Name:    jobName,
			Command: "echo",
		}
		agent.StoreJob(job)
		agent.do(func(s *agentState) {
			s.tasks["task-"+string(rune('a'+i))] = &types.Task{
				ID:      "task-" + string(rune('a'+i)),
				JobName: jobName,
				State:   types.TaskRunning,
			}
		})
	}

	time.Sleep(10 * time.Millisecond)

	// Stop all tasks
	agent.StopAllTasks()
	time.Sleep(10 * time.Millisecond)

	// All tasks should be stopped via runner
	for i := 0; i < 3; i++ {
		taskID := "task-" + string(rune('a'+i))
		if !mockRunner.WasStopped(taskID) {
			t.Errorf("Task %s should be stopped", taskID)
		}
	}

	// Tasks should be removed from state
	taskCount := query(agent, func(s *agentState) int { return len(s.tasks) })
	if taskCount != 0 {
		t.Errorf("Expected 0 tasks in state after StopAllTasks, got %d", taskCount)
	}

	// Jobs should still be in state (definitions survive isolation)
	jobCount := query(agent, func(s *agentState) int { return len(s.jobs) })
	if jobCount != 3 {
		t.Errorf("Expected 3 jobs in state after StopAllTasks, got %d", jobCount)
	}

	// Placed task counts should be 0 (no tasks = nothing placed)
	placed := agent.GetPlacedTaskCounts()
	if len(placed) != 0 {
		t.Errorf("Expected empty placed counts after StopAllTasks, got %v", placed)
	}
}

func TestAgentDeleteJobWithMultipleTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// One job, multiple tasks
	agent.StoreJob(&types.Job{Name: "test-job", Command: "echo"})
	agent.do(func(s *agentState) {
		for i := 0; i < 3; i++ {
			s.tasks["task-"+string(rune('a'+i))] = &types.Task{
				ID:      "task-" + string(rune('a'+i)),
				JobName: "test-job",
				State:   types.TaskRunning,
			}
		}
	})

	time.Sleep(10 * time.Millisecond)

	deleted := agent.deleteJob("test-job")

	if deleted != 3 {
		t.Errorf("deleteJob returned %d, want 3", deleted)
	}
}

func TestAgentDeleteJobNonExistent(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	deleted := agent.deleteJob("nonexistent")

	if deleted != 0 {
		t.Errorf("deleteJob returned %d, want 0", deleted)
	}
}

func TestDeleteJobRemovesTasksBeforeStop(t *testing.T) {
	// Verify that deleteJob removes tasks from state immediately,
	// so checkTasks() won't see them and try to restart during shutdown.
	// This prevents the race: monitor detects "crashed" tasks while
	// docker stop is still running on other tasks.
	for _, driver := range []string{types.DriverExec, types.DriverDocker} {
		t.Run(driver, func(t *testing.T) {
			cfg := testConfig()
			mockExec := NewMockRunner()
			mockDocker := NewMockRunner()

			agent := New(cfg, "test-agent", mockExec)
			agent.dockerRunner = mockDocker

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go agent.stateLoop(ctx)

			agent.StoreJob(&types.Job{Name: "del-test", Driver: driver, Command: "echo"})
			agent.do(func(s *agentState) {
				for i := 0; i < 3; i++ {
					tid := fmt.Sprintf("task-%d", i)
					s.tasks[tid] = &types.Task{
						ID:      tid,
						JobName: "del-test",
						Driver:  driver,
						State:   types.TaskRunning,
					}
				}
			})
			time.Sleep(10 * time.Millisecond)

			// Delete the job
			deleted := agent.deleteJob("del-test")
			if deleted != 3 {
				t.Fatalf("deleteJob returned %d, want 3", deleted)
			}

			// deleteJob blocks until all stops complete — tasks should be gone
			remainingTasks := query(agent, func(s *agentState) int {
				return len(s.tasks)
			})
			if remainingTasks != 0 {
				t.Errorf("Expected 0 tasks after delete, got %d", remainingTasks)
			}

			// Job should be gone
			if agent.GetJob("del-test") != nil {
				t.Error("Job should be removed from state after delete")
			}

			// checkTasks should be a no-op (nothing left)
			agent.checkTasks()

			// The correct runner should have been called to stop, not the other
			runner := mockExec
			other := mockDocker
			if driver == types.DriverDocker {
				runner = mockDocker
				other = mockExec
			}

			for i := 0; i < 3; i++ {
				tid := fmt.Sprintf("task-%d", i)
				if !runner.WasStopped(tid) {
					t.Errorf("Runner should have stopped %s", tid)
				}
				if other.WasStopped(tid) {
					t.Errorf("Other runner should NOT have stopped %s", tid)
				}
			}
		})
	}
}

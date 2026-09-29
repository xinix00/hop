package agent

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

func startLifecycleState(t *testing.T, a *Agent) {
	t.Helper()
	go a.stateLoop(context.Background())
	// A round-trip is a deterministic readiness barrier for the state owner.
	query(a, func(*agentState) struct{} { return struct{}{} })
}

func TestRestartTerminalPathCleansRunnerBeforeFailed(t *testing.T) {
	r := NewMockRunner()
	var stops atomic.Int32
	r.onStop = func(*types.Task) error {
		stops.Add(1)
		return nil
	}
	a := New(testConfig(), "terminal-cleanup", r)
	startLifecycleState(t, a)

	job := &types.Job{Name: "terminal", MaxRestarts: intPtr(0)}
	task := &types.Task{ID: "terminal-task", JobName: job.Name, State: types.TaskStopping}
	query(a, func(s *agentState) struct{} {
		s.jobs[job.Name] = job
		s.tasks[task.ID] = task
		return struct{}{}
	})

	a.restartTask(task, true)

	if got := stops.Load(); got != 1 {
		t.Fatalf("Runner.Stop calls = %d, want 1 before terminal failure", got)
	}
	state := query(a, func(s *agentState) types.TaskState { return s.tasks[task.ID].State })
	if state != types.TaskFailed {
		t.Fatalf("terminal task state = %q, want %q", state, types.TaskFailed)
	}
}

func TestRestartCleansGhostWhenStopWinsDuringRunnerStart(t *testing.T) {
	r := NewMockRunner()
	runEntered := make(chan struct{})
	releaseRun := make(chan struct{})
	r.onRun = func(*types.Job) error {
		close(runEntered)
		<-releaseRun
		return nil
	}
	a := New(testConfig(), "restart-ghost", r)
	startLifecycleState(t, a)

	job := &types.Job{Name: "restart-ghost", Driver: types.DriverExec, Command: "app", MaxRestarts: intPtr(1)}
	old := &types.Task{ID: "old-attempt", JobName: job.Name, Driver: job.Driver, State: types.TaskStopping}
	query(a, func(s *agentState) struct{} {
		s.jobs[job.Name] = job
		s.tasks[old.ID] = old
		return struct{}{}
	})

	restarted := make(chan struct{})
	go func() {
		a.restartTask(old, true)
		close(restarted)
	}()
	select {
	case <-runEntered:
	case <-time.After(time.Second):
		t.Fatal("replacement Runner.Run did not start")
	}

	replacement := query(a, func(s *agentState) *types.Task {
		for _, task := range s.tasks {
			if task.ID != old.ID {
				task.State = types.TaskStopping
				return task
			}
		}
		return nil
	})
	if replacement == nil {
		t.Fatal("replacement was not published before Runner.Run")
	}
	// This Stop sees no mock-runner ownership yet and removes the reservation,
	// exactly like shutdown's first snapshot can do while a real Run is blocked.
	if err := a.stopAndRemove(replacement); err != nil {
		t.Fatalf("racing Stop: %v", err)
	}
	close(releaseRun)
	select {
	case <-restarted:
	case <-time.After(time.Second):
		t.Fatal("restart worker did not finish")
	}

	r.mu.Lock()
	_, leaked := r.tasks[replacement.ID]
	stopped := r.stopped[replacement.ID]
	r.mu.Unlock()
	if leaked || !stopped {
		t.Fatalf("ghost replacement cleanup: leaked=%v stopped=%v", leaked, stopped)
	}
}

func TestStopFailureStillRemovesLogicalTask(t *testing.T) {
	r := NewMockRunner()
	a := New(testConfig(), "stop-once", r)
	startLifecycleState(t, a)
	task := &types.Task{ID: "stop-task", JobName: "job", State: types.TaskStopping}
	query(a, func(s *agentState) struct{} {
		s.tasks[task.ID] = task
		return struct{}{}
	})

	r.SetStopError(ErrSimulated)
	if err := a.stopAndRemove(task); !errors.Is(err, ErrSimulated) {
		t.Fatalf("stop error = %v, want %v", err, ErrSimulated)
	}
	if kept := query(a, func(s *agentState) bool { return s.tasks[task.ID] != nil }); kept {
		t.Fatal("task remained after its single stop attempt")
	}
}

func TestShutdownStopsOnceAndRemovesLogicalTask(t *testing.T) {
	r := NewMockRunner()
	var stops atomic.Int32
	r.onStop = func(*types.Task) error {
		stops.Add(1)
		return ErrSimulated
	}
	a := New(testConfig(), "shutdown-once", r)
	startLifecycleState(t, a)
	task := &types.Task{ID: "shutdown-task", State: types.TaskStopping}
	query(a, func(s *agentState) struct{} {
		s.tasks[task.ID] = task
		return struct{}{}
	})

	a.stopTasks([]*types.Task{task})

	if got := stops.Load(); got != 1 {
		t.Fatalf("shutdown cleanup calls = %d, want 1", got)
	}
	if kept := query(a, func(s *agentState) bool { return s.tasks[task.ID] != nil }); kept {
		t.Fatal("task remained after shutdown's stop attempt")
	}
}

func TestRestartDelaySaturatesWithoutOverflow(t *testing.T) {
	for _, count := range []int{6, 63, 64, 1000, math.MaxInt} {
		delay := restartDelay(count)
		if delay <= 0 || delay > maxRestartDelay {
			t.Fatalf("restartDelay(%d) = %s, want (0, %s]", count, delay, maxRestartDelay)
		}
	}
}

func TestCheckStatesPrunedByMonitorOwner(t *testing.T) {
	a := New(testConfig(), "check-prune", NewMockRunner())
	startLifecycleState(t, a)
	a.checkStates["removed-task"] = &checkState{failCount: 2}

	a.checkTasks()

	if _, ok := a.checkStates["removed-task"]; ok {
		t.Fatal("monitor retained check state for a task absent from agent state")
	}
}

func TestFailedTasksIncludedInShutdownCleanup(t *testing.T) {
	failed := &types.Task{ID: "failed", State: types.TaskFailed}
	state := &agentState{tasks: map[string]*types.Task{failed.ID: failed}}

	tasks := markAllStopping(state)
	if len(tasks) != 1 || tasks[0] != failed {
		t.Fatalf("shutdown cleanup selected %#v, want failed task", tasks)
	}
	if failed.State != types.TaskStopping {
		t.Fatalf("failed task state = %q, want %q", failed.State, types.TaskStopping)
	}
}

package agent

import (
	"errors"
	"sync"

	"github.com/xinix00/hop/internal/runner"
	"github.com/xinix00/hop/internal/types"
)

// ErrSimulated is returned by mock runner when configured to fail
var ErrSimulated = errors.New("simulated runner error")

// MockRunner implements runner.Runner for testing
type MockRunner struct {
	mu      sync.Mutex
	tasks   map[string]*types.Task
	stopped map[string]bool
	runErr  error
	stopErr error
	nextPid int
	stdout  map[string]*runner.LogBroadcaster
	stderr  map[string]*runner.LogBroadcaster

	// Hooks for testing
	onRun  func(job *types.Job) error
	onStop func(task *types.Task) error
}

// NewMockRunner creates a new mock runner
func NewMockRunner() *MockRunner {
	return &MockRunner{
		tasks:   make(map[string]*types.Task),
		stopped: make(map[string]bool),
		nextPid: 1000,
		stdout:  make(map[string]*runner.LogBroadcaster),
		stderr:  make(map[string]*runner.LogBroadcaster),
	}
}

// Run implements runner.Runner
func (m *MockRunner) Run(job *types.Job, task *types.Task) error {
	// Snapshot the hook under the lock, then invoke it WITHOUT holding the lock.
	// Tests use onRun to block a start mid-flight (e.g. on a channel); holding
	// m.mu across that block would deadlock a concurrent Stop() waiting on m.mu.
	m.mu.Lock()
	onRun := m.onRun
	m.mu.Unlock()

	if onRun != nil {
		if err := onRun(job); err != nil {
			return err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.runErr != nil {
		return m.runErr
	}

	m.nextPid++
	task.Pid = m.nextPid

	m.tasks[task.ID] = task
	m.stdout[task.ID] = runner.NewLogBroadcaster()
	m.stderr[task.ID] = runner.NewLogBroadcaster()

	return nil
}

// Stop implements runner.Runner
func (m *MockRunner) Stop(task *types.Task) error {
	// Same as Run: invoke the hook without holding m.mu so a Stop concurrent
	// with a blocked Run doesn't deadlock on the mutex.
	m.mu.Lock()
	onStop := m.onStop
	m.mu.Unlock()

	if onStop != nil {
		if err := onStop(task); err != nil {
			return err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopErr != nil {
		return m.stopErr
	}

	m.stopped[task.ID] = true
	delete(m.tasks, task.ID)
	return nil
}

// Status implements runner.Runner
func (m *MockRunner) Status(task *types.Task) (types.TaskState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t, ok := m.tasks[task.ID]; ok {
		return t.State, nil
	}
	return types.TaskFailed, nil
}

// GetStdout implements runner.Runner
func (m *MockRunner) GetStdout(taskID string) *runner.LogBroadcaster {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stdout[taskID]
}

// GetStderr implements runner.Runner
func (m *MockRunner) GetStderr(taskID string) *runner.LogBroadcaster {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stderr[taskID]
}

// Cleanup implements runner.Runner
func (m *MockRunner) Cleanup() error {
	return nil
}

// Test helpers

// WasStopped returns true if the task was stopped
func (m *MockRunner) WasStopped(taskID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopped[taskID]
}

// SetRunError configures the runner to return an error on Run
func (m *MockRunner) SetRunError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runErr = err
}

// SetStopError configures the runner to return an error on Stop
func (m *MockRunner) SetStopError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopErr = err
}

// SetTaskState sets the state of a task (for testing status checks)
func (m *MockRunner) SetTaskState(taskID string, state types.TaskState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task, ok := m.tasks[taskID]; ok {
		task.State = state
	}
}

// GetTask returns a task by ID
func (m *MockRunner) GetTask(taskID string) *types.Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tasks[taskID]
}

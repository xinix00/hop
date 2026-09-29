//go:build !darwin && !linux

package runner

import (
	"errors"

	"github.com/xinix00/hop/internal/types"
)

// ExecRunner stub for targets without a POSIX process model (HopOS/tamago).
// Construction succeeds so agent wiring stays identical; running exec tasks
// fails with a clear error instead of an obscure syscall failure.
type ExecRunner struct{}

var errNoExec = errors.New("exec driver is not supported on this platform (use driver \"hop\")")

// NewExecRunner matches the POSIX constructor; config is ignored.
func NewExecRunner(config *Config) *ExecRunner { return &ExecRunner{} }

func (r *ExecRunner) Run(job *types.Job, task *types.Task) error { return errNoExec }
func (r *ExecRunner) Stop(task *types.Task) error                { return nil }
func (r *ExecRunner) Status(task *types.Task) (types.TaskState, error) {
	return types.TaskFailed, errNoExec
}
func (r *ExecRunner) GetStdout(taskID string) *LogBroadcaster { return nil }
func (r *ExecRunner) GetStderr(taskID string) *LogBroadcaster { return nil }
func (r *ExecRunner) Cleanup() error                          { return nil }

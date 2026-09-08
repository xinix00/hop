package runner

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xinix00/hop/internal/types"
)

// ErrNoCapacity is what a runner reports when it cannot PLACE a task for lack of
// room — on HopOS: no free app core, or the sharegroup pool doesn't fit. It is
// not a crash: the task is fine and belongs on another node, or here once
// something frees up, so the agent hands it back to the leader instead of
// restarting it (restarting an unplaceable task is a storm — every retry
// re-downloads the image and fails again).
//
// Driver-agnostic on purpose: the agent talks to runners, not to HopOS. The hop
// driver is the only place that knows hopos.ErrNoCapacity and translates it
// onto this one.
var ErrNoCapacity = errors.New("runner: no capacity to place the task")

// envKey converts a name to an uppercase environment variable key
// (e.g. "node.os" → "NODE_OS", "http-port" → "HTTP_PORT")
func envKey(name string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' {
			return r - 32
		}
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, name)
}

// PortEnvVars builds ER_PORT_<NAME>=<port> environment variables for all ports
func PortEnvVars(ports map[string]int) []string {
	vars := make([]string, 0, len(ports))
	for name, port := range ports {
		vars = append(vars, fmt.Sprintf("ER_PORT_%s=%d", envKey(name), port))
	}
	return vars
}

// AttrEnvVars builds ER_ATTR_<KEY>=<value> environment variables for node attributes
// Dots and dashes in keys are replaced with underscores (e.g. node.os → ER_ATTR_NODE_OS)
func AttrEnvVars(attrs map[string]string) []string {
	vars := make([]string, 0, len(attrs))
	for name, val := range attrs {
		vars = append(vars, fmt.Sprintf("ER_ATTR_%s=%s", envKey(name), val))
	}
	return vars
}

// Runner interface for executing jobs
type Runner interface {
	// Run starts a job process. The task is pre-created by the caller;
	// the runner fills in process-specific fields (Pid) and registers
	// internal state (process handle, log broadcasters).
	Run(job *types.Job, task *types.Task) error

	// Stop stops a running task
	Stop(task *types.Task) error

	// Status returns the current state of a task
	Status(task *types.Task) (types.TaskState, error)

	// GetStdout returns the stdout log broadcaster for a task (nil if not available)
	GetStdout(taskID string) *LogBroadcaster

	// GetStderr returns the stderr log broadcaster for a task (nil if not available)
	GetStderr(taskID string) *LogBroadcaster

	// Cleanup removes all task directories (called at startup)
	Cleanup() error
}

// Config holds configuration for the runner
type Config struct {
	// RootfsBase is the base path for task directories
	RootfsBase string

	// MaxCPUShares is the total CPU shares for nice calculation (0 = auto-detect from CPU cores)
	MaxCPUShares int

	// Isolate enables process isolation (chroot on Linux, sandbox on macOS)
	// Default: true for security
	Isolate bool

	// NodeAttrs are injected as ER_ATTR_* env vars in every process (node-specific, not persisted)
	NodeAttrs map[string]string

	// Logs is how much task output is kept (tail lines, and for how long
	// after the task stopped). Zero fields mean DefaultLogPolicy.
	Logs LogPolicy
}

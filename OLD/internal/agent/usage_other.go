//go:build !darwin && !linux

package agent

import "errors"

// getProcessUsage is POSIX-only: off Linux/macOS there is no process model
// to measure (on HopOS, task health comes from slot heartbeats instead).
// The monitor treats the error as "no usage data" and moves on.
func getProcessUsage(pid int) (cpuSeconds float64, memBytes uint64, err error) {
	return 0, 0, errors.New("process usage not supported on this platform")
}

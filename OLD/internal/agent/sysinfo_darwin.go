//go:build darwin

package agent

import (
	"os/exec"
	"strconv"
	"strings"
)

func getSystemMemory() uint64 {
	// Use sysctl to get physical memory
	out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
	if err != nil {
		return 0
	}
	mem, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0
	}
	return mem
}

package agent

import (
	"runtime"
)

// SystemInfo contains detected system resources
type SystemInfo struct {
	CPUCores    int    // number of CPU cores
	MemoryBytes uint64 // total system memory in bytes
}

// GetSystemInfo detects system CPU and memory
func GetSystemInfo() SystemInfo {
	return SystemInfo{
		CPUCores:    runtime.NumCPU(),
		MemoryBytes: getSystemMemory(),
	}
}

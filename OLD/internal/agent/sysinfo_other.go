//go:build !darwin && !linux

package agent

// getSystemMemory has no auto-detection off Linux/macOS. On HopOS the real
// value is injected via SetSysInfo (pkg/agentboot); 0 means "unknown".
func getSystemMemory() uint64 { return 0 }

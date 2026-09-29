//go:build linux

package agent

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// userHZ is the kernel's clock-tick rate (sysconf(_SC_CLK_TCK)). Hardcoded
// to 100 — the value on every mainstream Linux build. Reading it via cgo
// would drag CGO into a pure-Go binary just to confirm a constant nobody
// changes.
const userHZ = 100

// getProcessUsage returns CPU+RSS for the task's whole process tree.
// task.Pid is the /bin/sh wrapper; the actual workload (RavenDB, …) lives
// in its descendants. Walks /proc/<pid>/task/<pid>/children recursively
// instead of scanning the whole /proc — bounded by the task's own tree.
func getProcessUsage(pid int) (cpuSeconds float64, memBytes uint64, err error) {
	var ticks, rssPages uint64
	walk(pid, &ticks, &rssPages)
	return float64(ticks) / float64(userHZ), rssPages * uint64(os.Getpagesize()), nil
}

// walk reads pid's stat and recurses into every direct child. Errors are
// ignored: processes routinely vanish between read and recurse, and missing
// one accounting tick is better than failing the whole measurement.
func walk(pid int, ticks, rssPages *uint64) {
	if u, r, err := readPidStat(pid); err == nil {
		*ticks += u
		*rssPages += r
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", pid, pid))
	if err != nil {
		return
	}
	for _, f := range strings.Fields(string(data)) {
		if c, err := strconv.Atoi(f); err == nil {
			walk(c, ticks, rssPages)
		}
	}
}

// readPidStat parses utime+stime (ticks) and rss (pages) from /proc/<pid>/stat.
func readPidStat(pid int) (cpuTicks uint64, rssPages uint64, err error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, err
	}
	// `comm` is wrapped in parens and may itself contain spaces or ')'. The
	// safe split is on the LAST ')' — fields after it are whitespace-separated.
	s := string(data)
	end := strings.LastIndex(s, ")")
	if end < 0 || end+1 >= len(s) {
		return 0, 0, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	fields := strings.Fields(s[end+1:])
	// proc(5): 14=utime, 15=stime, 24=rss. After dropping pid+comm+state
	// the offsets here become 11/12/21.
	if len(fields) < 22 {
		return 0, 0, fmt.Errorf("/proc/%d/stat: only %d fields after comm", pid, len(fields))
	}
	utime, _ := strconv.ParseUint(fields[11], 10, 64)
	stime, _ := strconv.ParseUint(fields[12], 10, 64)
	rss, _ := strconv.ParseUint(fields[21], 10, 64)
	return utime + stime, rss, nil
}

package agent

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/xinix00/hop/internal/types"
)

// refreshMeminfo rewrites the per-task synthetic /proc/meminfo with the
// freshly measured RSS so readers inside the chroot see live MemFree
// instead of the static snapshot from start. Safe on any platform/config:
// when fakeMeminfo didn't write the file (macOS, Isolate=false, no
// MemoryLimit), os.Stat fails and we return without touching anything.
func (a *Agent) refreshMeminfo(task *types.Task, used uint64) {
	if task.MemoryLimit == 0 {
		return
	}
	base := a.config.Paths.RootfsBase
	if base == "" {
		base = "/tmp/hop"
	}
	src := filepath.Join(base, task.ID, ".hop-meminfo")
	if _, err := os.Stat(src); err != nil {
		return
	}
	if used > task.MemoryLimit {
		used = task.MemoryLimit
	}
	free := task.MemoryLimit - used
	content := fmt.Sprintf(
		"MemTotal:       %d kB\n"+
			"MemFree:        %d kB\n"+
			"MemAvailable:   %d kB\n"+
			"Buffers:               0 kB\n"+
			"Cached:                0 kB\n"+
			"SwapTotal:             0 kB\n"+
			"SwapFree:              0 kB\n",
		task.MemoryLimit/1024, free/1024, free/1024,
	)
	// Write through the source path — the bind in the chroot points at this
	// same inode, so the next read on /proc/meminfo inside the chroot sees
	// the new content. Atomic-rename would break the bind (new inode).
	_ = os.WriteFile(src, []byte(content), 0644)
}

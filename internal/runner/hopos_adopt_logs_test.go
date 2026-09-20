package runner

import (
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/hopos"
)

// Logs must be reachable after a flip without restarting the surviving app.
func TestAdoptRunningRestoresLogs(t *testing.T) {
	sm := newFakeSlotManager(4)
	lines := make(chan string, 4)
	sm.slots[1] = &fakeSlot{coreOn: true, app: hopos.SlotReady, logs: lines}
	r := NewHopRunner(sm, nil)
	slots := map[string]int{"survivor": 1}
	r.AdoptRunning(slots)
	stdout, stderr := r.GetStdout("survivor"), r.GetStderr("survivor")
	if stdout == nil || stderr == nil {
		t.Fatal("adopted task has no log endpoint")
	}
	ch := stdout.Subscribe()
	defer stdout.Unsubscribe(ch)
	r.AdoptRunning(slots)
	if r.GetStdout("survivor") != stdout || r.GetStderr("survivor") != stderr {
		t.Fatal("repeated adoption replaced a live log stream")
	}
	for _, line := range []string{"still running", "after repeated adoption"} {
		lines <- line
		select {
		case got := <-ch:
			if got != line {
				t.Fatalf("log = %q, want %q", got, line)
			}
		case <-time.After(time.Second):
			t.Fatal("adopted app logs no longer forwarded")
		}
	}
	if err := r.Stop(&types.Task{ID: "survivor", Pid: 1}); err != nil {
		t.Fatal(err)
	}
	if sm.Status(1).CoreOn {
		t.Fatal("adopted task did not stop")
	}
	if r.GetStdout("survivor") != stdout {
		t.Fatal("stopped task lost its retained logs")
	}
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("stopped log stream remains open")
		}
	case <-time.After(time.Second):
		t.Fatal("stopped log stream did not close")
	}
	if got := stdout.Tail(); len(got) != 2 {
		t.Fatalf("retained %d lines, want 2", len(got))
	}
}

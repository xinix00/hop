package leader

import (
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// TestTrimReturningAgentSurplus: the desired count is the authority. A
// returning agent's tasks are stopped when the job is already satisfied
// elsewhere (kill the just-rejoined surplus — at worst a stale version), kept
// when they fill a real capacity gap, and never touched for daemons.
func TestTrimReturningAgentSurplus(t *testing.T) {
	returning := newMockAgent()
	defer returning.Close()

	setup := func(t *testing.T, jobCount int) (*Leader, func()) {
		t.Helper()
		store := NewMockJobStore()
		l := New("local", store, nil)
		l.SetSettleDelay(time.Hour) // never settle → RegisterAgent won't auto-trim/reconcile
		ctx, cancel := newTestContext()
		go l.Run(ctx)
		time.Sleep(10 * time.Millisecond)

		store.StoreJob(&types.Job{Name: "app", Count: jobCount})
		// Two other agents already run one instance each; the returning agent
		// re-registers claiming a third.
		l.RegisterAgent("b", "http://10.0.0.2:8080", "", map[string]int{"app": 1})
		l.RegisterAgent("c", "http://10.0.0.3:8080", "", map[string]int{"app": 1})
		l.RegisterAgent("returning", returning.URL(), "", map[string]int{"app": 1})
		return l, cancel
	}

	t.Run("surplus is trimmed", func(t *testing.T) {
		l, cancel := setup(t, 2) // desired 2, three placed → returning is surplus
		defer cancel()

		l.trimReturningAgentSurplus("returning")

		if got := l.GetPlacedCounts()["app"]; got != 2 {
			t.Fatalf("expected surplus trimmed to 2, got %d", got)
		}
		if l.GetPlaced("app")["returning"] != 0 {
			t.Fatal("returning agent's surplus placement should be removed from the books")
		}
	})

	t.Run("gap is kept", func(t *testing.T) {
		l, cancel := setup(t, 3) // desired 3, three placed → returning fills a real gap
		defer cancel()

		l.trimReturningAgentSurplus("returning")

		if got := l.GetPlacedCounts()["app"]; got != 3 {
			t.Fatalf("returning agent fills a gap; expected 3 kept, got %d", got)
		}
	})

	t.Run("daemon is never trimmed", func(t *testing.T) {
		l, cancel := setup(t, -1) // count -1 = run everywhere, never surplus
		defer cancel()

		l.trimReturningAgentSurplus("returning")

		if got := l.GetPlacedCounts()["app"]; got != 3 {
			t.Fatalf("daemon placements must not be trimmed, got %d", got)
		}
	})
}

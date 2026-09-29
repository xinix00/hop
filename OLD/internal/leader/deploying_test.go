package leader

import (
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// TestDeployingReflectsRolloutTruth: the flag tells the truth about a rollout.
// A plain dispatch is not a rollout; a completed update clears it; a broken
// update leaves it set ("we broke down half way" is a state), so status never
// reports a false "healthy".
func TestDeployingReflectsRolloutTruth(t *testing.T) {
	old := RollingUpdateDelay
	RollingUpdateDelay = 0
	defer func() { RollingUpdateDelay = old }()

	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	l := New("local", store, nil)
	ctx, cancel := newTestContext()
	defer cancel()
	go l.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	l.RegisterAgent("a", agent.URL(), "", nil)
	l.Heartbeat("a", "", 0)

	if err := l.DispatchJob(&types.Job{Name: "app", Command: "./v1", Count: 1}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if store.GetJob("app").Deploying {
		t.Fatal("a plain dispatch is not a rollout")
	}

	if err := l.UpdateJob(&types.Job{Name: "app", Command: "./v2", Count: 1}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if store.GetJob("app").Deploying {
		t.Fatal("a completed rollout must not stay marked deploying")
	}

	agent.SetFailRuns(true)
	if err := l.UpdateJob(&types.Job{Name: "app", Command: "./v3", Count: 1}); err == nil {
		t.Fatal("expected the rollout to fail")
	}
	if !store.GetJob("app").Deploying {
		t.Fatal("a broken rollout must stay marked deploying (the honest state)")
	}
}

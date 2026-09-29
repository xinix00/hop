package leader

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
)

func TestUpdateJobUnknownPolicyDoesNotStrandDispatchLock(t *testing.T) {
	store := NewMockJobStore()
	store.StoreJob(&types.Job{Name: "app", Command: "old"})
	l := New("leader", store, &httputil.Client{Timeout: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)
	_ = l.GetAgents() // state-loop readiness barrier

	err := l.UpdateJob(&types.Job{
		Name:         "app",
		Command:      "new",
		UpdatePolicy: types.UpdatePolicy("not-a-policy"),
	})
	if err == nil {
		t.Fatal("unknown policy unexpectedly succeeded")
	}
	if locked := query(l, func(s *leaderState) bool { return s.dispatching["app"] }); locked {
		t.Fatal("unknown policy left dispatching lock behind")
	}

	if err := l.UpdateJob(&types.Job{
		Name:         "app",
		Command:      "new",
		UpdatePolicy: types.UpdateRolling,
	}); err != nil {
		t.Fatalf("valid update after rejected policy: %v", err)
	}
	if locked := query(l, func(s *leaderState) bool { return s.dispatching["app"] }); locked {
		t.Fatal("valid update did not release dispatching lock")
	}
}

func TestStateOperationsReturnAfterStateLoopStops(t *testing.T) {
	l := New("leader", NewMockJobStore(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	go l.stateLoop(ctx)
	_ = l.GetAgents() // readiness barrier
	cancel()

	select {
	case <-l.stateDone:
	case <-time.After(time.Second):
		t.Fatal("state loop did not stop")
	}

	done := make(chan struct{})
	go func() {
		l.MarkUnplaced("gone", "job")
		_ = l.GetAgents()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("do/query remained blocked after state-loop cancellation")
	}
}

func TestConcurrentDaemonReconcileClaimsJobOnce(t *testing.T) {
	agent := newMockAgent()
	agent.SetRunDelay(100 * time.Millisecond)
	defer agent.Close()

	store := NewMockJobStore()
	l := New("leader", store, &httputil.Client{Timeout: time.Second})
	l.SetSettleDelay(time.Hour) // registration must not start its own reconcile
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)
	_ = l.GetAgents()

	if !l.RegisterAgent("node-1", agent.URL(), "", nil) {
		t.Fatal("register agent")
	}
	job := &types.Job{Name: "daemon", Command: "./daemon", Count: -1}
	store.StoreJob(job)
	agents := l.GetAgents()

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = l.reconcileJob(job, agents)
		}()
	}
	close(start)
	wg.Wait()

	if got := agent.RunCallCount(); got != 1 {
		t.Fatalf("concurrent daemon run calls = %d, want 1", got)
	}
	if got := l.GetPlaced("daemon")["node-1"]; got != 1 {
		t.Fatalf("daemon placement count = %d, want 1", got)
	}
}

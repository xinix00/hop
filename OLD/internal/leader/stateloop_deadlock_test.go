package leader

import (
	"context"
	"testing"
	"time"
)

// TestRegisterAgentDeadlocksWithoutStateLoop verifies that RegisterAgent
// deadlocks when stateLoop is not running. This was the root cause of
// all agents becoming leader: becomeLeader() called RegisterAgent() before
// starting stateLoop, so the leader API port never opened, other agents
// saw "connection refused" and all claimed leadership.
func TestRegisterAgentDeadlocksWithoutStateLoop(t *testing.T) {
	store := NewMockJobStore()
	l := New("agent-1", store, nil)
	l.EnableSettle()

	// DO NOT start stateLoop — reproduces the original bug

	done := make(chan struct{})
	go func() {
		l.RegisterAgent("agent-1", "http://10.0.0.1:8080", "v0.5.3", nil)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("RegisterAgent should have deadlocked without stateLoop running")
	case <-time.After(500 * time.Millisecond):
		// Expected: deadlock because stateLoop isn't running
	}
}

// TestRegisterAgentWorksWithStateLoop verifies the fix: when stateLoop
// is started BEFORE RegisterAgent (as becomeLeader now does), no deadlock.
func TestRegisterAgentWorksWithStateLoop(t *testing.T) {
	store := NewMockJobStore()
	l := New("agent-1", store, nil)
	l.EnableSettle()

	// Start stateLoop first — this is the fix
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)

	done := make(chan struct{})
	go func() {
		l.RegisterAgent("agent-1", "http://10.0.0.1:8080", "v0.5.3", nil)
		close(done)
	}()

	select {
	case <-done:
		// Good: RegisterAgent completed
	case <-time.After(2 * time.Second):
		t.Fatal("RegisterAgent deadlocked even with stateLoop running!")
	}
}

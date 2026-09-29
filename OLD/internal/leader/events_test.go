package leader

import (
	"sync"
	"testing"
	"time"
)

func TestEventBusLatestWins(t *testing.T) {
	bus := NewEventBus()
	ch := bus.Subscribe()
	defer bus.Unsubscribe(ch)

	// Fill the buffer with a stale event
	bus.Notify("job:my-api:started")

	// Send a newer event — should replace the stale one
	bus.Notify("job:my-api")

	select {
	case msg := <-ch:
		if msg != "job:my-api" {
			t.Errorf("expected latest event 'job:my-api', got %q", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("no event received")
	}

	// Channel should be empty now
	select {
	case msg := <-ch:
		t.Errorf("unexpected extra event: %q", msg)
	default:
	}
}

func TestEventBusBurstCoalescing(t *testing.T) {
	bus := NewEventBus()
	ch := bus.Subscribe()
	defer bus.Unsubscribe(ch)

	// Simulate 14 identical "started" events in a burst
	for i := 0; i < 14; i++ {
		bus.Notify("job:my-api:started")
	}

	// Should get exactly 1 event (coalesced)
	select {
	case msg := <-ch:
		if msg != "job:my-api:started" {
			t.Errorf("expected 'job:my-api:started', got %q", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("no event received")
	}

	select {
	case msg := <-ch:
		t.Errorf("unexpected extra event: %q", msg)
	default:
	}
}

func TestEventBusJobEventAfterStartedBurst(t *testing.T) {
	// Simulates the real scenario: 14 "started" events followed by a "job" event.
	// The job event (dispatch complete) must not be lost.
	bus := NewEventBus()
	ch := bus.Subscribe()
	defer bus.Unsubscribe(ch)

	// Burst of started events
	for i := 0; i < 14; i++ {
		bus.Notify("job:my-api:started")
	}

	// Then the dispatch-complete event
	bus.Notify("job:my-api")

	// The latest event should be the job event
	msg := <-ch
	if msg != "job:my-api" {
		t.Errorf("expected dispatch event 'job:my-api', got %q", msg)
	}
}

func TestEventBusConcurrentNotify(t *testing.T) {
	// Simulates the real race: 14 agents call /v1/notify concurrently,
	// then 1 dispatch-complete event fires. Nothing must be permanently lost.
	bus := NewEventBus()
	ch := bus.Subscribe()
	defer bus.Unsubscribe(ch)

	// 14 concurrent "started" notifications (like agents calling /v1/notify)
	var wg sync.WaitGroup
	for i := 0; i < 14; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bus.Notify("job:my-api:started")
		}()
	}
	wg.Wait()

	// Drain whatever coalesced event is in the channel
	select {
	case <-ch:
	default:
		t.Fatal("no event after concurrent burst")
	}

	// Now the important one: dispatch-complete must get through
	bus.Notify("job:my-api")

	select {
	case msg := <-ch:
		if msg != "job:my-api" {
			t.Errorf("expected 'job:my-api', got %q", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatch event lost after concurrent burst")
	}
}

func TestEventBusConcurrentBurstThenEvent(t *testing.T) {
	// Stress test: run 100 times to catch races
	for round := 0; round < 100; round++ {
		bus := NewEventBus()
		ch := bus.Subscribe()

		// Concurrent burst
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				bus.Notify("job:my-api:started")
			}()
		}
		wg.Wait()

		// Drain
		select {
		case <-ch:
		default:
		}

		// Final event must arrive
		bus.Notify("job:my-api")

		select {
		case msg := <-ch:
			if msg != "job:my-api" {
				t.Errorf("round %d: expected 'job:my-api', got %q", round, msg)
			}
		case <-time.After(time.Second):
			t.Fatalf("round %d: final event lost", round)
		}

		bus.Unsubscribe(ch)
	}
}

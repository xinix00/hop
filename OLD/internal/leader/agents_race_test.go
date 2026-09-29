package leader

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// TestGetAgentsNoRaceWithHeartbeat guards the copy in GetAgents: heartbeats
// mutate an agent's LastSeen/Version in the state loop while GET /v1/agents
// marshals the list on an HTTP goroutine. Run with -race; if GetAgents ever
// hands back live map pointers again, this flags it.
func TestGetAgentsNoRaceWithHeartbeat(t *testing.T) {
	store := NewMockJobStore()
	l := New("local", store, nil)
	ctx, cancel := newTestContext()
	defer cancel()
	go l.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	l.RegisterAgent("a", "http://10.0.0.1:8080", "v1", nil)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: heartbeats mutate LastSeen/Version inside the state loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				l.Heartbeat("a", "v2", 0)
			}
		}
	}()

	// Reader: list + marshal, exactly like the /v1/agents handler.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := json.Marshal(l.GetAgents()); err != nil {
					t.Errorf("marshal agents: %v", err)
					return
				}
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

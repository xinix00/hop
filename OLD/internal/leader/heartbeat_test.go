package leader

import (
	"context"
	"sync"
	"testing"
	"time"
)

// ============== HEARTBEAT TESTS ==============

func TestLeaderHeartbeatRegistersAgent(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Send heartbeat from remote agent
	leader.RegisterAgent("remote-agent", "http://192.168.1.10:8080", "", nil)
	leader.Heartbeat("remote-agent", "", 0)

	time.Sleep(10 * time.Millisecond)

	agents := leader.GetAgents()
	if len(agents) != 1 {
		t.Errorf("GetAgents() returned %d agents, want 1", len(agents))
	}

	if agents[0].ID != "remote-agent" {
		t.Errorf("Agent ID = %q, want %q", agents[0].ID, "remote-agent")
	}
	if agents[0].Endpoint != "http://192.168.1.10:8080" {
		t.Errorf("Agent Endpoint = %q, want %q", agents[0].Endpoint, "http://192.168.1.10:8080")
	}
}

func TestLeaderHeartbeatUpdatesLastSeen(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// First heartbeat
	leader.RegisterAgent("remote-agent", "http://192.168.1.10:8080", "", nil)
	leader.Heartbeat("remote-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	agents := leader.GetAgents()
	firstSeen := agents[0].LastSeen

	// Wait and send another heartbeat
	time.Sleep(50 * time.Millisecond)
	leader.Heartbeat("remote-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	agents = leader.GetAgents()
	if !agents[0].LastSeen.After(firstSeen) {
		t.Error("LastSeen should be updated after second heartbeat")
	}
}

func TestLeaderGetAgents(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register multiple agents
	for i := 0; i < 5; i++ {
		leader.RegisterAgent("agent-"+string(rune('a'+i)), "http://host:8080", "", nil)
		leader.Heartbeat("agent-"+string(rune('a'+i)), "", 0)
	}

	time.Sleep(10 * time.Millisecond)

	agents := leader.GetAgents()
	if len(agents) != 5 {
		t.Errorf("GetAgents() returned %d agents, want 5", len(agents))
	}
}

func TestLeaderUnregisterAgent(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agent
	leader.RegisterAgent("remote-agent", "http://192.168.1.10:8080", "", nil)
	leader.Heartbeat("remote-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	if len(leader.GetAgents()) != 1 {
		t.Fatal("Agent not registered")
	}

	// Unregister
	leader.UnregisterAgent("remote-agent")
	time.Sleep(10 * time.Millisecond)

	if len(leader.GetAgents()) != 0 {
		t.Error("Agent should be unregistered")
	}
}

func TestLeaderConcurrentHeartbeats(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Pre-register all agents
	for i := 0; i < 10; i++ {
		agentID := "agent-" + string(rune('a'+i))
		leader.RegisterAgent(agentID, "http://host:8080", "", nil)
	}

	var wg sync.WaitGroup

	// Concurrent heartbeats from multiple agents
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			agentID := "agent-" + string(rune('a'+n%10))
			for j := 0; j < 10; j++ {
				leader.Heartbeat(agentID, "", 0)
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(10 * time.Millisecond)

	// Should have 10 unique agents (a-j)
	agents := leader.GetAgents()
	if len(agents) != 10 {
		t.Errorf("GetAgents() returned %d agents, want 10", len(agents))
	}
}

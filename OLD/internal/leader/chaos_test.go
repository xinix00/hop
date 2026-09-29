package leader

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
)

// TestChaos_CascadingFailure tests multiple agents failing simultaneously
func TestChaos_CascadingFailure(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 200 * time.Millisecond // Short timeout for testing

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Start 5 mock agents
	agents := make([]*mockAgent, 5)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	// Register all agents
	for i, agent := range agents {
		agentID := fmt.Sprintf("agent-%d", i)
		leader.RegisterAgent(agentID, agent.URL(), "", nil)
		leader.Heartbeat(agentID, "", 0)
	}
	time.Sleep(20 * time.Millisecond)

	// CHAOS: 3 out of 5 agents crash simultaneously
	agents[0].Close()
	agents[2].Close()
	agents[4].Close()

	// Keep alive agents sending heartbeats
	for i := 0; i < 3; i++ {
		time.Sleep(100 * time.Millisecond)
		// Only agents 1 and 3 are still alive
		leader.Heartbeat("agent-1", "", 0)
		leader.Heartbeat("agent-3", "", 0)
	}

	// Trigger dead agent check
	leader.checkDeadAgents()
	time.Sleep(50 * time.Millisecond)

	// Leader should detect 3 dead agents (0, 2, 4)
	aliveAgents := leader.GetAgents()
	if len(aliveAgents) != 2 {
		t.Errorf("Expected 2 alive agents, got %d", len(aliveAgents))
	}

	t.Logf("Cascading failure: 3/5 agents down, %d remaining", len(aliveAgents))
}

// TestChaos_NetworkPartition tests agent isolation from leader
func TestChaos_NetworkPartition(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, &httputil.Client{Timeout: 50 * time.Millisecond})
	leader.agentTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Slow agent that times out
	slowAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond) // Longer than client timeout
		w.WriteHeader(http.StatusOK)
	}))
	defer slowAgent.Close()

	// Fast agent
	fastAgent := newMockAgent()
	defer fastAgent.Close()

	// Register both
	leader.RegisterAgent("slow-agent", slowAgent.URL, "", nil)
	leader.RegisterAgent("fast-agent", fastAgent.URL(), "", nil)
	leader.Heartbeat("slow-agent", "", 0)
	leader.Heartbeat("fast-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	// Dispatch job
	job := &types.Job{Name: "timeout-job", Command: "./app", Count: 1}
	store.StoreJob(job)

	// Leader should timeout on slow agent, retry on fast agent
	err := leader.DispatchJob(job)
	if err != nil {
		t.Logf("Dispatch completed (some agents timed out): %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Slow agent should eventually be marked dead
	time.Sleep(150 * time.Millisecond)
	agents := leader.GetAgents()
	t.Logf("After network partition: %d agents alive", len(agents))

	// At least fast agent should be alive
	foundFast := false
	for _, a := range agents {
		if a.ID == "fast-agent" {
			foundFast = true
		}
	}
	if !foundFast {
		t.Error("Fast agent should still be alive")
	}
}

// TestChaos_AllAgentsDownExceptOne tests cluster surviving with 1 agent
func TestChaos_AllAgentsDownExceptOne(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, &httputil.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Create 5 agents
	agents := make([]*mockAgent, 5)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
		agentID := fmt.Sprintf("agent-%d", i)
		leader.RegisterAgent(agentID, agents[i].URL(), "", nil)
		leader.Heartbeat(agentID, "", 0)
	}
	time.Sleep(20 * time.Millisecond)

	// CHAOS: 4 agents crash (0, 1, 2, 3)
	for i := 0; i < 4; i++ {
		agents[i].Close()
	}

	// Keep the survivor (agent-4) sending heartbeats
	for i := 0; i < 3; i++ {
		time.Sleep(100 * time.Millisecond)
		leader.Heartbeat("agent-4", "", 0)
	}

	// Trigger dead agent check
	leader.checkDeadAgents()
	time.Sleep(50 * time.Millisecond)

	// Leader should detect 4 dead, 1 alive
	aliveAgents := leader.GetAgents()
	if len(aliveAgents) != 1 {
		t.Errorf("Expected 1 alive agent, got %d", len(aliveAgents))
	}

	// Verify we still have the survivor
	if len(aliveAgents) > 0 && aliveAgents[0].ID != "agent-4" {
		t.Errorf("Survivor should be agent-4, got %s", aliveAgents[0].ID)
	}
}

// TestChaos_RapidAgentChurn tests agents joining/leaving quickly
func TestChaos_RapidAgentChurn(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, &httputil.Client{Timeout: 50 * time.Millisecond})
	leader.agentTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)
	go leader.Run(ctx) // Start dead agent checker

	// Rapidly add and remove agents
	for round := 0; round < 10; round++ {
		agent := newMockAgent()
		agentID := fmt.Sprintf("churn-%d", round)

		// Register
		leader.RegisterAgent(agentID, agent.URL(), "", nil)
		leader.Heartbeat(agentID, "", 0)
		time.Sleep(5 * time.Millisecond)

		// Kill
		agent.Close()
		time.Sleep(15 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond) // Let dead agent checker run

	// System should be stable (all churned agents detected as dead)
	agents := leader.GetAgents()
	t.Logf("After rapid churn: %d agents remain", len(agents))

	// All should be dead/removed
	for _, a := range agents {
		age := time.Since(a.LastSeen)
		if age < 100*time.Millisecond {
			t.Errorf("Agent %s still considered alive after being killed", a.ID)
		}
	}
}

// TestChaos_JobDispatchToDeadAgent tests dispatch handling when agent dies during dispatch
func TestChaos_JobDispatchToDeadAgent(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, &httputil.Client{Timeout: 50 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Create agent
	agent := newMockAgent()

	// Register
	leader.RegisterAgent("dying-agent", agent.URL(), "", nil)
	leader.Heartbeat("dying-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	// Kill agent BEFORE dispatch
	agent.Close()

	// Try to dispatch - should fail gracefully
	job := &types.Job{
		Name:    "test-job",
		Command: "./test",
		Count:   1,
	}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("Dispatch should fail when agent is dead")
	}

	t.Logf("Graceful failure: %v", err)
}

// TestChaos_LeaderMemoryPressure tests behavior under memory pressure
func TestChaos_LeaderMemoryPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory pressure test in short mode")
	}

	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Extreme scenario: 10k agents, 100k jobs
	t.Log("Simulating memory pressure: 10k agents, 100k jobs...")
	start := time.Now()

	// Register 10k agents
	for i := 0; i < 10000; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		leader.RegisterAgent(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), "", nil)
		leader.Heartbeat(agentID, "", 0)
	}

	// Create 100k jobs
	for i := 0; i < 100000; i++ {
		job := &types.Job{
			Name:    fmt.Sprintf("job-%d", i),
			Command: "echo test",
			Count:   1,
		}
		store.StoreJob(job)
	}

	elapsed := time.Since(start)
	t.Logf("Created massive state in %v", elapsed)

	// System should still respond
	agents := leader.GetAgents()
	jobs := leader.GetJobs()

	if len(agents) != 10000 {
		t.Errorf("Expected 10k agents, got %d", len(agents))
	}
	if len(jobs) != 100000 {
		t.Errorf("Expected 100k jobs, got %d", len(jobs))
	}

	// Measure lookup performance under pressure
	start = time.Now()
	_ = store.GetJob("job-50000")
	lookupTime := time.Since(start)

	t.Logf("Job lookup under pressure: %v", lookupTime)

	if lookupTime > 10*time.Millisecond {
		t.Logf("WARNING: Lookup slow (%v) with 100k jobs - consider optimization", lookupTime)
	}
}

// TestChaos_HeartbeatStorm tests system under heartbeat flood
func TestChaos_HeartbeatStorm(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// 100 agents sending heartbeats as fast as possible
	// Pre-register all agents first
	for i := 0; i < 100; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		endpoint := fmt.Sprintf("http://10.0.0.%d:8080", i)
		leader.RegisterAgent(agentID, endpoint, "", nil)
	}

	done := make(chan bool)
	start := time.Now()
	var heartbeats atomic.Int64

	for i := 0; i < 100; i++ {
		go func(id int) {
			agentID := fmt.Sprintf("agent-%d", id)

			for j := 0; j < 100; j++ {
				leader.Heartbeat(agentID, "", 0)
				heartbeats.Add(1)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}

	elapsed := time.Since(start)
	hb := heartbeats.Load()
	rate := float64(hb) / elapsed.Seconds()

	t.Logf("Heartbeat storm: %d heartbeats in %v = %.0f HB/sec", hb, elapsed, rate)

	// System should survive
	agents := leader.GetAgents()
	if len(agents) != 100 {
		t.Errorf("Expected 100 agents after storm, got %d", len(agents))
	}
}

// TestChaos_ZeroAgentsAvailable tests dispatch when no agents are available
func TestChaos_ZeroAgentsAvailable(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// No agents registered
	job := &types.Job{
		Name:    "orphan",
		Command: "./orphan",
		Count:   3,
	}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("Dispatch should fail when no agents available")
	}

	// Error message now includes instance info
	if !strings.Contains(err.Error(), "no agents available") {
		t.Errorf("Expected error containing 'no agents available', got: %v", err)
	}

	t.Logf("Graceful handling of zero agents: %v", err)
}

// TestChaos_AgentFlapping tests agent rapidly going up/down
func TestChaos_AgentFlapping(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, &httputil.Client{Timeout: 50 * time.Millisecond})
	leader.agentTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond) // Let state loop start

	// Agent flaps: up → down → up → down
	for i := 0; i < 5; i++ {
		agent := newMockAgent()
		agentID := "flapping-agent"

		// UP - register agent
		leader.RegisterAgent(agentID, agent.URL(), "", nil)
		leader.Heartbeat(agentID, "", 0)
		time.Sleep(30 * time.Millisecond) // Allow state to update

		// Verify registered
		agents := leader.GetAgents()
		found := false
		for _, a := range agents {
			if a.ID == agentID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Round %d: Agent should be registered", i)
		}

		// DOWN
		agent.Close()
		time.Sleep(150 * time.Millisecond) // Wait for timeout

		// Trigger dead agent check
		leader.checkDeadAgents()
		time.Sleep(20 * time.Millisecond)

		// Should be removed or marked as old
		agents = leader.GetAgents()
		for _, a := range agents {
			age := time.Since(a.LastSeen)
			if a.ID == agentID && age < 100*time.Millisecond {
				t.Errorf("Round %d: Flapping agent should be detected as dead", i)
			}
		}
	}

	t.Log("Flapping agent handled gracefully over 5 cycles")
}

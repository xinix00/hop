package leader

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
)

// ============== EDGE CASE TESTS ==============

func TestLeaderCheckDeadAgents(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)
	leader.agentTimeout = 50 * time.Millisecond // Short timeout for testing

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agent
	leader.RegisterAgent("dying-agent", "http://192.168.1.10:8080", "", nil)
	leader.Heartbeat("dying-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	if len(leader.GetAgents()) != 1 {
		t.Fatal("Agent not registered")
	}

	// Wait for timeout
	time.Sleep(100 * time.Millisecond)

	// Manually trigger dead agent check
	leader.checkDeadAgents()
	time.Sleep(10 * time.Millisecond)

	// Agent should be removed
	if len(leader.GetAgents()) != 0 {
		t.Error("Dead agent should be removed")
	}
}

func TestLeaderRedispatchJobsFromDeadAgent(t *testing.T) {
	store := NewMockJobStore()
	store.StoreJob(&types.Job{
		Name:    "test-job",
		Command: "echo",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	})

	// Create mock agent that accepts jobs and returns tasks
	acceptCount := 0
	var tasks []*types.Task
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/run":
			mu.Lock()
			acceptCount++
			tasks = append(tasks, &types.Task{
				ID:      fmt.Sprintf("task-%d", acceptCount),
				JobName: "test-job",
				State:   types.TaskRunning,
			})
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
		case "/tasks":
			mu.Lock()
			_ = json.NewEncoder(w).Encode(tasks)
			mu.Unlock()
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)
	leader.agentTimeout = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register two agents: one will "die", one will accept redispatched jobs
	leader.RegisterAgent("dying-agent", "http://dead.host:8080", "", nil)
	leader.RegisterAgent("healthy-agent", server.URL, "", nil)
	leader.Heartbeat("dying-agent", "", 0)
	leader.Heartbeat("healthy-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	// Manually add placement for job on dying agent (by job ID)
	leader.do(func(s *leaderState) {
		if s.placed["dying-agent"] == nil {
			s.placed["dying-agent"] = make(map[string]int)
		}
		s.placed["dying-agent"]["test-job"] = 1
	})
	time.Sleep(10 * time.Millisecond)

	// Wait for dying-agent to timeout
	time.Sleep(100 * time.Millisecond)

	// Keep healthy-agent alive
	leader.Heartbeat("healthy-agent", "", 0)

	// Trigger dead agent check
	leader.checkDeadAgents()
	time.Sleep(50 * time.Millisecond)

	// Job should have been redispatched to healthy-agent
	if acceptCount == 0 {
		t.Error("Job should have been redispatched to healthy agent")
	}
}

func TestLeaderAgentTimeoutConfigurable(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	// Default timeout should be 30 seconds
	if leader.agentTimeout != 30*time.Second {
		t.Errorf("Default agentTimeout = %v, want 30s", leader.agentTimeout)
	}

	// Can be modified
	leader.agentTimeout = 5 * time.Second
	if leader.agentTimeout != 5*time.Second {
		t.Error("agentTimeout should be modifiable")
	}
}

func TestLeaderGetClusterStatusWithFailingAgent(t *testing.T) {
	store := NewMockJobStore()

	// One working agent, one failing
	workingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tasks" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"task-1","state":"running"}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer workingServer.Close()

	failingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failingServer.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("working-agent", workingServer.URL, "", nil)
	leader.RegisterAgent("failing-agent", failingServer.URL, "", nil)
	leader.Heartbeat("working-agent", "", 0)
	leader.Heartbeat("failing-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	status := leader.GetClusterStatus()

	// Should have tasks from working agent, not from failing
	if _, ok := status["working-agent"]; !ok {
		t.Error("Should have status from working agent")
	}

	// Failing agent might not be in result (error case)
	if tasks, ok := status["failing-agent"]; ok && len(tasks) > 0 {
		t.Error("Should not have tasks from failing agent")
	}
}

func TestLeaderDispatchWithHTTPTimeout(t *testing.T) {
	store := NewMockJobStore()

	// Create a server that times out
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond) // Simulate slow response
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Use a short timeout client
	shortTimeoutClient := &httputil.Client{Timeout: 50 * time.Millisecond}
	leader := New("local-agent", store, shortTimeoutClient)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("slow-agent", server.URL, "", nil)
	leader.Heartbeat("slow-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "timeout-job",
		Command: "echo",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("DispatchJob should fail when agent times out")
	}
}

func TestLeaderMultipleAgentsPartialFailure(t *testing.T) {
	store := NewMockJobStore()

	successCount := 0
	var tasks []*types.Task

	// First agent succeeds and returns tasks
	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/run":
			successCount++
			tasks = append(tasks, &types.Task{ID: "task-1", JobName: "test", State: types.TaskRunning})
			w.WriteHeader(http.StatusCreated)
		case "/tasks":
			_ = json.NewEncoder(w).Encode(tasks)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer successServer.Close()

	// Second agent fails
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer failServer.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("success-agent", successServer.URL, "", nil)
	leader.RegisterAgent("fail-agent", failServer.URL, "", nil)
	leader.Heartbeat("success-agent", "", 0)
	leader.Heartbeat("fail-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	// Dispatch job with 1 instance - should succeed on first agent
	job := &types.Job{
		Name:    "test",
		Command: "echo",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob should succeed with at least one working agent: %v", err)
	}

	if successCount == 0 {
		t.Error("Job should have been dispatched to successful agent")
	}
}

func TestLeaderDeleteJobNotFound(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Deleting non-existent job should not panic
	leader.DeleteJobByName("nonexistent-job")
	// If we get here without panic, test passes
}

func TestLeaderEmptyClusterStatus(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	status := leader.GetClusterStatus()

	// Should return empty map, not nil
	if status == nil {
		t.Error("GetClusterStatus should return empty map, not nil")
	}
	if len(status) != 0 {
		t.Errorf("GetClusterStatus should be empty, got %d entries", len(status))
	}
}

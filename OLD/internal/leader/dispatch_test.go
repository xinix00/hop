package leader

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// ============== DISPATCH TESTS ==============
// Uses mockAgent from failover_test.go

func TestLeaderDispatchJobToAgent(t *testing.T) {
	store := NewMockJobStore()
	agent := newMockAgent()
	defer agent.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("mock-agent", agent.URL(), "", nil)
	leader.Heartbeat("mock-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "test-job",
		Command: "echo hello",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	if store.GetJob("test-job") == nil {
		t.Error("Job should be stored after dispatch")
	}
}

func TestLeaderDispatchJobNoAgents(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "test-job", Command: "echo", HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("DispatchJob should fail when no agents available")
	}
}

func TestLeaderDispatchAllAgentsReject(t *testing.T) {
	store := NewMockJobStore()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("rejecting-agent", server.URL, "", nil)
	leader.Heartbeat("rejecting-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "test-job", Command: "echo", Count: 1, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("DispatchJob should fail when all agents reject")
	}
}

func TestLeaderDispatchMultipleInstances(t *testing.T) {
	store := NewMockJobStore()

	agents := make([]*mockAgent, 3)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	for i, a := range agents {
		leader.RegisterAgent("agent-"+string(rune('a'+i)), a.URL(), "", nil)
		leader.Heartbeat("agent-"+string(rune('a'+i)), "", 0)
	}
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "multi-job", Command: "echo", Count: 6, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	total := 0
	for _, a := range agents {
		total += a.TaskCount()
	}
	if total != 6 {
		t.Errorf("Total dispatched = %d, want 6", total)
	}
}

func TestLeaderDeleteJobOnMultipleAgents(t *testing.T) {
	store := NewMockJobStore()
	// Store job AFTER agents are registered to avoid tryRescheduleUnderscheduled
	job := &types.Job{Name: "test-job", Command: "echo"}

	deleteCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/delete/test-job" {
			mu.Lock()
			deleteCount++
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agents first (no jobs in store yet, so no rescheduling triggered)
	leader.RegisterAgent("agent-a", server.URL, "", nil)
	leader.Heartbeat("agent-a", "", 0)
	leader.RegisterAgent("agent-b", server.URL, "", nil)
	leader.Heartbeat("agent-b", "", 0)
	time.Sleep(10 * time.Millisecond)

	// Now store the job and set up placement manually
	store.StoreJob(job)
	leader.do(func(s *leaderState) {
		for _, agentID := range []string{"agent-a", "agent-b"} {
			if s.placed[agentID] == nil {
				s.placed[agentID] = make(map[string]int)
			}
			s.placed[agentID]["test-job"]++
		}
	})
	time.Sleep(10 * time.Millisecond)

	leader.DeleteJobByName("test-job")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if deleteCount != 2 {
		t.Errorf("Delete requests = %d, want 2", deleteCount)
	}
	mu.Unlock()
}

func TestLeaderDispatchJobWithZeroCount(t *testing.T) {
	store := NewMockJobStore()
	agent := newMockAgent()
	defer agent.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("agent-1", agent.URL(), "", nil)
	leader.Heartbeat("agent-1", "", 0)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "zero-count-job", Command: "echo", Count: 0, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	if agent.TaskCount() != 1 {
		t.Errorf("JobCount = %d, want 1 (default)", agent.TaskCount())
	}
}

func TestLeaderDispatchCountMinusOne(t *testing.T) {
	store := NewMockJobStore()

	agents := make([]*mockAgent, 3)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	for i, a := range agents {
		leader.RegisterAgent("agent-"+string(rune('a'+i)), a.URL(), "", nil)
		leader.Heartbeat("agent-"+string(rune('a'+i)), "", 0)
	}
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "hopdns", Command: "/usr/bin/hopdns", Count: -1, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	total := 0
	for _, a := range agents {
		total += a.TaskCount()
	}
	if total != 3 {
		t.Errorf("Total dispatched = %d, want 3 (all agents)", total)
	}
}

func TestLeaderCountMinusOneNewAgent(t *testing.T) {
	store := NewMockJobStore()
	agent1 := newMockAgent()
	defer agent1.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("agent-a", agent1.URL(), "", nil)
	leader.Heartbeat("agent-a", "", 0)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "hopdns", Command: "/usr/bin/hopdns", Count: -1, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}
	_ = leader.DispatchJob(job)
	time.Sleep(10 * time.Millisecond)

	if agent1.TaskCount() != 1 {
		t.Errorf("Expected 1 dispatch, got %d", agent1.TaskCount())
	}

	// Add new agent via RegisterAgent - should automatically get the job
	agent2 := newMockAgent()
	defer agent2.Close()
	leader.RegisterAgent("agent-b", agent2.URL(), "", nil)
	time.Sleep(100 * time.Millisecond) // Allow time for reconciliation

	if agent2.TaskCount() != 1 {
		t.Errorf("New agent should get job, got %d", agent2.TaskCount())
	}
}

func TestLeaderConcurrentDispatchAndDelete(t *testing.T) {
	store := NewMockJobStore()
	agent := newMockAgent()
	defer agent.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("agent-1", agent.URL(), "", nil)
	leader.Heartbeat("agent-1", "", 0)
	time.Sleep(10 * time.Millisecond)

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			job := &types.Job{Name: "job-" + string(rune('0'+n)), Command: "echo", HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}
			_ = leader.DispatchJob(job)
		}(i)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			leader.DeleteJobByName("job-" + string(rune('0'+n)))
		}(i)
	}

	wg.Wait()
}

// ============== HTTP STATUS CODE TESTS ==============

// TestLeaderDispatchAccepts202 tests that 202 Accepted is treated as success
func TestLeaderDispatchAccepts202(t *testing.T) {
	store := NewMockJobStore()

	// Create agent that returns 202 Accepted (async job handling)
	taskReturned := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/run":
			w.WriteHeader(http.StatusAccepted) // 202
		case "/tasks":
			// Return running task after first /run call
			if taskReturned {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`[{"id":"task-1","job_name":"async-job","state":"running"}]`))
			} else {
				taskReturned = true
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`[{"id":"task-1","job_name":"async-job","state":"running"}]`))
			}
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("async-agent", server.URL, "", nil)
	leader.Heartbeat("async-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "async-job",
		Command: "echo async",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob should accept 202 Accepted, got error: %v", err)
	}

	if store.GetJob("async-job") == nil {
		t.Error("Job should be stored after 202 response")
	}
}

// TestLeaderDispatchAccepts201 tests that 201 Created is treated as success
func TestLeaderDispatchAccepts201(t *testing.T) {
	store := NewMockJobStore()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/run":
			w.WriteHeader(http.StatusCreated) // 201
		case "/tasks":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"task-1","job_name":"created-job","state":"running"}]`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("create-agent", server.URL, "", nil)
	leader.Heartbeat("create-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "created-job",
		Command: "echo created",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob should accept 201 Created, got error: %v", err)
	}
}

// TestLeaderDispatchRejects500 tests that 500 errors are rejected
func TestLeaderDispatchRejects500(t *testing.T) {
	store := NewMockJobStore()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError) // 500
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("error-agent", server.URL, "", nil)
	leader.Heartbeat("error-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "error-job",
		Command: "echo error",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("DispatchJob should reject 500 Internal Server Error")
	}
}

// TestLeaderDispatchRejects400 tests that 400 errors are rejected
func TestLeaderDispatchRejects400(t *testing.T) {
	store := NewMockJobStore()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest) // 400
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.RegisterAgent("bad-agent", server.URL, "", nil)
	leader.Heartbeat("bad-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "bad-job",
		Command: "echo bad",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("DispatchJob should reject 400 Bad Request")
	}
}

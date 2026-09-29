package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/leader"
	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
	"github.com/xinix00/lean/leanhttp"
)

// mockAgentServer returns a test server that responds instantly to agent API calls.
// This prevents benchmarks from hanging on HTTP timeouts to fake agent endpoints.
func mockAgentServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/tasks":
			_ = json.NewEncoder(w).Encode([]interface{}{})
		case r.URL.Path == "/run":
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "task_id": "bench-task"})
		default:
			w.WriteHeader(200)
		}
	}))
}

// agentEndpoint returns the mock server URL for use as agent endpoint.
func agentEndpoint(ts *httptest.Server) string {
	return ts.URL
}

// BenchmarkGetAgentsEndpoint measures /v1/agents endpoint throughput
func BenchmarkGetAgentsEndpoint(b *testing.B) {
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	l.SetSettleDelay(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	server := NewServer(l, ":9080", "", "test")

	// Register 100 agents
	for i := 0; i < 100; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		endpoint := fmt.Sprintf("http://10.0.0.%d:8080", i)
		l.RegisterAgent(agentID, endpoint, "", nil)
		l.Heartbeat(agentID, "", 0)
	}

	req := leanhttp.NewRequest("GET", "/v1/agents", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := leanhttp.NewRecorder()
		server.mux.ServeHTTP(w, req)
		if w.Code != 200 {
			b.Fatalf("Expected 200, got %d", w.Code)
		}
	}
}

// BenchmarkGetJobsEndpoint measures /v1/jobs endpoint throughput
func BenchmarkGetJobsEndpoint(b *testing.B) {
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	l.SetSettleDelay(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	server := NewServer(l, ":9080", "", "test")

	// Create 1000 jobs
	for i := 0; i < 1000; i++ {
		job := &types.Job{
			Name:    fmt.Sprintf("job-%d", i),
			Command: "echo test",
		}
		store.jobs[job.Name] = job
	}

	req := leanhttp.NewRequest("GET", "/v1/jobs", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := leanhttp.NewRecorder()
		server.mux.ServeHTTP(w, req)
		if w.Code != 200 {
			b.Fatalf("Expected 200, got %d", w.Code)
		}
	}
}

// BenchmarkPostJobEndpoint measures POST /v1/jobs throughput
func BenchmarkPostJobEndpoint(b *testing.B) {
	ts := mockAgentServer()
	defer ts.Close()

	store := newMockJobStore()
	l := leader.New("local-agent", store, &httputil.Client{})
	l.SetSettleDelay(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	server := NewServer(l, ":9080", "", "test")

	// Register agents pointing to mock server
	for i := 0; i < 10; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		l.RegisterAgent(agentID, agentEndpoint(ts), "", nil)
		l.Heartbeat(agentID, "", 0)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		job := types.Job{
			Name:    fmt.Sprintf("bench-job-%d", i),
			Command: "echo test",
			Count:   1,
		}
		body, _ := json.Marshal(job)
		req := leanhttp.NewRequest("POST", "/v1/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		w := leanhttp.NewRecorder()
		server.mux.ServeHTTP(w, req)
		if w.Code != 201 && w.Code != 200 {
			b.Fatalf("Expected 201 or 200, got %d", w.Code)
		}
	}
}

// BenchmarkHeartbeatEndpoint measures POST /v1/heartbeat throughput
func BenchmarkHeartbeatEndpoint(b *testing.B) {
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	l.SetSettleDelay(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	server := NewServer(l, ":9080", "", "test")

	// Pre-register 100 agents so heartbeats succeed
	for i := 0; i < 100; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		endpoint := fmt.Sprintf("http://10.0.0.%d:8080", i)
		l.RegisterAgent(agentID, endpoint, "", nil)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		hb := map[string]string{
			"id":       fmt.Sprintf("agent-%d", i%100),
			"endpoint": fmt.Sprintf("http://10.0.0.%d:8080", i%100),
		}
		body, _ := json.Marshal(hb)
		req := leanhttp.NewRequest("POST", "/v1/heartbeat", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		w := leanhttp.NewRecorder()
		server.mux.ServeHTTP(w, req)
		if w.Code != 200 {
			b.Fatalf("Expected 200, got %d", w.Code)
		}
	}
}

// BenchmarkStatusEndpoint measures /v1/status endpoint throughput
func BenchmarkStatusEndpoint(b *testing.B) {
	ts := mockAgentServer()
	defer ts.Close()

	store := newMockJobStore()
	l := leader.New("local-agent", store, &httputil.Client{})
	l.SetSettleDelay(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	server := NewServer(l, ":9080", "", "test")

	// Register agents pointing to mock server
	for i := 0; i < 10; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		l.RegisterAgent(agentID, agentEndpoint(ts), "", nil)
		l.Heartbeat(agentID, "", 0)
	}

	req := leanhttp.NewRequest("GET", "/v1/status", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := leanhttp.NewRecorder()
		server.mux.ServeHTTP(w, req)
		if w.Code != 200 {
			b.Fatalf("Expected 200, got %d", w.Code)
		}
	}
}

// BenchmarkConcurrentRequests measures concurrent API request handling
func BenchmarkConcurrentRequests(b *testing.B) {
	ts := mockAgentServer()
	defer ts.Close()

	store := newMockJobStore()
	l := leader.New("local-agent", store, &httputil.Client{})
	l.SetSettleDelay(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	server := NewServer(l, ":9080", "", "test")

	// Register agents pointing to mock server
	for i := 0; i < 10; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		l.RegisterAgent(agentID, agentEndpoint(ts), "", nil)
		l.Heartbeat(agentID, "", 0)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			var req *leanhttp.Request
			switch i % 3 {
			case 0:
				req = leanhttp.NewRequest("GET", "/v1/agents", nil)
			case 1:
				req = leanhttp.NewRequest("GET", "/v1/jobs", nil)
			case 2:
				req = leanhttp.NewRequest("GET", "/v1/status", nil)
			}

			w := leanhttp.NewRecorder()
			server.mux.ServeHTTP(w, req)
			i++
		}
	})
}

// BenchmarkJSONEncoding measures JSON encoding overhead
func BenchmarkJSONEncoding(b *testing.B) {
	agents := make([]*types.Agent, 100)
	for i := 0; i < 100; i++ {
		agents[i] = &types.Agent{
			ID:       fmt.Sprintf("agent-%d", i),
			Endpoint: fmt.Sprintf("http://10.0.0.%d:8080", i),
			LastSeen: time.Now(),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(agents)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkJSONDecoding measures JSON decoding overhead
func BenchmarkJSONDecoding(b *testing.B) {
	job := types.Job{
		Name:        "test-job",
		Command:     "echo test",
		Count:       3,
		CPUShares:   2048,
		MemoryLimit: 1024 * 1024 * 1024,
		Env:         map[string]string{"KEY": "value"},
		Tags:        map[string]string{"service": "api"},
	}
	data, _ := json.Marshal(job)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var decoded types.Job
		err := json.Unmarshal(data, &decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// mockJobStore for API benchmarks
type mockJobStore struct {
	mu   sync.Mutex
	jobs map[string]*types.Job
}

func newMockJobStore() *mockJobStore {
	return &mockJobStore{
		jobs: make(map[string]*types.Job),
	}
}

func (m *mockJobStore) GetJobs() []*types.Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	jobs := make([]*types.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (m *mockJobStore) GetJob(name string) *types.Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobs[name]
}

func (m *mockJobStore) StoreJob(job *types.Job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.Name] = job
}

func (m *mockJobStore) UpdateJob(job *types.Job) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[job.Name]; !ok {
		return false
	}
	m.jobs[job.Name] = job
	return true
}

func (m *mockJobStore) SetJobPriority(name string, priority int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.jobs[name]
	if !ok {
		return false
	}
	cp := *cur
	p := priority
	cp.Priority = &p
	m.jobs[name] = &cp
	return true
}

func (m *mockJobStore) SetJobDeploying(name string, deploying bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.jobs[name]
	if !ok {
		return false
	}
	cp := *cur
	cp.Deploying = deploying
	m.jobs[name] = &cp
	return true
}

func (m *mockJobStore) DeleteJob(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.jobs, name)
}

func (m *mockJobStore) GetStateTime() time.Time {
	return time.Now()
}

func (m *mockJobStore) SyncJobs(jobs []*types.Job, updated time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, job := range jobs {
		m.jobs[job.Name] = job
	}
}

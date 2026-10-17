package dns

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hoplib"
)

func TestWatcherRefresh(t *testing.T) {
	mux := http.NewServeMux()
	var serverURL string

	mux.HandleFunc("/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]hoplib.Job{
			{Name: "myapp"},
			{Name: "other"},
		})
	})

	mux.HandleFunc("/v1/jobs/myapp/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"agents": []hoplib.Agent{{ID: "agent1", Endpoint: serverURL}},
			"tasks_by_agent": map[string][]hoplib.Task{
				"agent1": {
					{ID: "task1", JobName: "myapp", State: "running"},
					{ID: "task2", JobName: "myapp", State: "running"},
				},
			},
		})
	})

	mux.HandleFunc("/v1/jobs/other/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"agents": []hoplib.Agent{{ID: "agent1", Endpoint: serverURL}},
			"tasks_by_agent": map[string][]hoplib.Task{
				"agent1": {
					{ID: "task3", JobName: "other", State: "stopped"},
				},
			},
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()
	serverURL = server.URL

	cache := NewCache()
	watcher := NewWatcher(server.URL, cache, "")
	watcher.cluster = "test-cluster"

	watcher.refresh()

	ips := cache.GetCluster("test-cluster", "myapp")
	if len(ips) != 1 {
		t.Errorf("Expected 1 IP for myapp, got %d", len(ips))
	}

	otherIPs := cache.GetCluster("test-cluster", "other")
	if len(otherIPs) != 0 {
		t.Errorf("Expected no IPs for stopped job, got %d", len(otherIPs))
	}
}

func TestExtractIP(t *testing.T) {
	tests := []struct {
		endpoint string
		want     string
	}{
		{"http://192.168.1.10:8080", "192.168.1.10"},
		{"https://10.0.0.1:443", "10.0.0.1"},
		{"http://127.0.0.1:8080", "127.0.0.1"},
	}

	for _, tt := range tests {
		got := extractIP(tt.endpoint)
		if got == nil {
			t.Errorf("extractIP(%q) = nil, want %s", tt.endpoint, tt.want)
			continue
		}
		if got.String() != tt.want {
			t.Errorf("extractIP(%q) = %s, want %s", tt.endpoint, got.String(), tt.want)
		}
	}
}

func TestExtractIPInvalid(t *testing.T) {
	got := extractIP("not-a-url")
	if got != nil {
		t.Errorf("extractIP(invalid) = %v, want nil", got)
	}
}

func TestWatcherNoAgents(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]hoplib.Job{})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	cache := NewCache()
	cache.Set("test-cluster", "oldapp", []net.IP{net.ParseIP("10.0.0.1")})

	watcher := NewWatcher(server.URL, cache, "")
	watcher.cluster = "test-cluster"
	watcher.refresh()

	// Cache should be cleared since no jobs
	if ips := cache.GetCluster("test-cluster", "oldapp"); len(ips) != 0 {
		t.Error("Cache should be empty after refresh with no jobs")
	}
}

func TestWatcherDiscoverCluster(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cluster_name": "prod-eu",
			"agents":       1,
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	cache := NewCache()
	watcher := NewWatcher(server.URL, cache, "")

	name, err := watcher.discoverCluster()
	if err != nil {
		t.Fatalf("discoverCluster() error: %v", err)
	}
	if name != "prod-eu" {
		t.Errorf("discoverCluster() = %q, want %q", name, "prod-eu")
	}
}

func TestWatcherDiscoverClusterMissing(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"agents": 1,
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	cache := NewCache()
	watcher := NewWatcher(server.URL, cache, "")

	_, err := watcher.discoverCluster()
	if err == nil {
		t.Fatal("discoverCluster() should fail when cluster_name is missing")
	}
}

// ============== SSE END-TO-END TESTS ==============

// mockHop simulates an hop agent with SSE events, jobs, and task status.
type mockHop struct {
	mu         sync.Mutex
	serverURL  string
	jobs       map[string][]hoplib.Task // jobName → tasks on "agent1"
	sseClients []http.ResponseWriter
	sseFlushed []http.Flusher
	sseDone    chan struct{} // close to disconnect all SSE clients
}

func newMockHop() *mockHop {
	return &mockHop{
		jobs:    make(map[string][]hoplib.Task),
		sseDone: make(chan struct{}),
	}
}

func (m *mockHop) setJob(name string, tasks []hoplib.Task) {
	m.mu.Lock()
	m.jobs[name] = tasks
	m.mu.Unlock()
}

func (m *mockHop) sendSSE(event, data string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, w := range m.sseClients {
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
		m.sseFlushed[i].Flush()
	}
}

func (m *mockHop) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cluster_name": "test-cluster",
		})
	})

	mux.HandleFunc("/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		var jobs []hoplib.Job
		for name := range m.jobs {
			jobs = append(jobs, hoplib.Job{Name: name})
		}
		m.mu.Unlock()
		_ = json.NewEncoder(w).Encode(jobs)
	})

	mux.HandleFunc("/v1/jobs/", func(w http.ResponseWriter, r *http.Request) {
		// Extract job name from /v1/jobs/{name}/status
		path := r.URL.Path
		// trim prefix and suffix
		name := path[len("/v1/jobs/"):]
		if idx := len(name) - len("/status"); idx > 0 && name[idx:] == "/status" {
			name = name[:idx]
		}

		m.mu.Lock()
		tasks := m.jobs[name]
		m.mu.Unlock()

		_ = json.NewEncoder(w).Encode(map[string]any{
			"agents": []hoplib.Agent{{ID: "agent1", Endpoint: m.serverURL}},
			"tasks_by_agent": map[string][]hoplib.Task{
				"agent1": tasks,
			},
		})
	})

	mux.HandleFunc("/v1/events", func(w http.ResponseWriter, r *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flusher", 500)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)

		// Send ping
		fmt.Fprintf(w, "event: ping\ndata: {}\n\n")
		f.Flush()

		m.mu.Lock()
		m.sseClients = append(m.sseClients, w)
		m.sseFlushed = append(m.sseFlushed, f)
		m.mu.Unlock()

		// Block until client disconnects or test closes SSE
		select {
		case <-r.Context().Done():
		case <-m.sseDone:
		}
	})

	return mux
}

// TestSSE_JobEvent verifies the full pipeline:
// SSE job event → debounce → refreshJob → cache updated.
func TestSSE_JobEvent(t *testing.T) {
	mock := newMockHop()
	mock.setJob("api", []hoplib.Task{
		{ID: "t1", JobName: "api", State: "running"},
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()
	mock.serverURL = server.URL

	cache := NewCache()
	watcher := NewWatcher(server.URL, cache, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watcher.Run(ctx)

	// Wait for SSE connect + initial refresh
	time.Sleep(300 * time.Millisecond)

	// Initial refresh should have populated cache
	if ips := cache.GetCluster("test-cluster", "api"); len(ips) != 1 {
		t.Fatalf("Expected 1 IP after initial refresh, got %d", len(ips))
	}

	// Add a new task to the job (simulating scale-up)
	mock.setJob("api", []hoplib.Task{
		{ID: "t1", JobName: "api", State: "running"},
		{ID: "t2", JobName: "api", State: "running"},
	})

	// Send SSE job event
	mock.sendSSE("job", `{"name":"api"}`)

	// Wait for debounce (500ms) + refresh
	time.Sleep(800 * time.Millisecond)

	// Cache should still show 1 IP (same agent, deduped)
	ips := cache.GetCluster("test-cluster", "api")
	if len(ips) != 1 {
		t.Errorf("Expected 1 IP (same agent), got %d", len(ips))
	}
}

// TestSSE_TaskEvent verifies task lifecycle events update the cache.
func TestSSE_TaskEvent(t *testing.T) {
	mock := newMockHop()
	mock.setJob("worker", []hoplib.Task{
		{ID: "t1", JobName: "worker", State: "running"},
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()
	mock.serverURL = server.URL

	cache := NewCache()
	watcher := NewWatcher(server.URL, cache, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watcher.Run(ctx)

	time.Sleep(300 * time.Millisecond)

	if ips := cache.GetCluster("test-cluster", "worker"); len(ips) != 1 {
		t.Fatalf("Expected 1 IP after initial refresh, got %d", len(ips))
	}

	// Task crashes → no running tasks
	mock.setJob("worker", []hoplib.Task{
		{ID: "t1", JobName: "worker", State: "failed"},
	})

	// Send task event (uses "job" field)
	mock.sendSSE("task", `{"job":"worker","event":"crash"}`)
	time.Sleep(800 * time.Millisecond)

	ips := cache.GetCluster("test-cluster", "worker")
	if len(ips) != 0 {
		t.Errorf("Expected 0 IPs after task crash, got %d", len(ips))
	}
}

// TestSSE_NewJobAppears verifies that a brand new job appearing via SSE
// triggers a refresh and populates the cache.
func TestSSE_NewJobAppears(t *testing.T) {
	mock := newMockHop()
	// Start with no jobs
	server := httptest.NewServer(mock.handler())
	defer server.Close()
	mock.serverURL = server.URL

	cache := NewCache()
	watcher := NewWatcher(server.URL, cache, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watcher.Run(ctx)

	time.Sleep(300 * time.Millisecond)

	// No jobs initially
	if ips := cache.GetCluster("test-cluster", "newapp"); len(ips) != 0 {
		t.Fatalf("Expected 0 IPs initially, got %d", len(ips))
	}

	// New job appears
	mock.setJob("newapp", []hoplib.Task{
		{ID: "t1", JobName: "newapp", State: "running"},
	})

	mock.sendSSE("job", `{"name":"newapp"}`)
	time.Sleep(800 * time.Millisecond)

	ips := cache.GetCluster("test-cluster", "newapp")
	if len(ips) != 1 {
		t.Errorf("Expected 1 IP after new job event, got %d", len(ips))
	}
}

// TestSSE_Debounce verifies that rapid events are coalesced into one refresh.
func TestSSE_Debounce(t *testing.T) {
	var refreshCount int
	var mu sync.Mutex

	mock := newMockHop()
	mock.setJob("api", []hoplib.Task{
		{ID: "t1", JobName: "api", State: "running"},
	})

	mux := http.NewServeMux()
	innerHandler := mock.handler()

	// Wrap to count refreshes
	mux.HandleFunc("/v1/jobs/api/status", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		refreshCount++
		mu.Unlock()
		innerHandler.ServeHTTP(w, r)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		innerHandler.ServeHTTP(w, r)
	})

	server := httptest.NewServer(mux)
	defer server.Close()
	mock.serverURL = server.URL

	cache := NewCache()
	watcher := NewWatcher(server.URL, cache, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watcher.Run(ctx)

	time.Sleep(300 * time.Millisecond)

	// Reset counter after initial refresh
	mu.Lock()
	refreshCount = 0
	mu.Unlock()

	// Send 5 rapid events for the same job
	for i := 0; i < 5; i++ {
		mock.sendSSE("task", `{"job":"api","event":"start"}`)
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for debounce
	time.Sleep(800 * time.Millisecond)

	mu.Lock()
	count := refreshCount
	mu.Unlock()

	// Should have been debounced to 1 refresh (not 5)
	if count != 1 {
		t.Errorf("Expected 1 debounced refresh, got %d", count)
	}
}

// TestSSE_Disconnect verifies that cache is cleared when SSE disconnects.
func TestSSE_Disconnect(t *testing.T) {
	mock := newMockHop()
	mock.setJob("api", []hoplib.Task{
		{ID: "t1", JobName: "api", State: "running"},
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()
	mock.serverURL = server.URL

	cache := NewCache()
	watcher := NewWatcher(server.URL, cache, "")
	watcher.interval = 5 * time.Second // slow reconnect: observe the post-disconnect window

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watcher.Run(ctx)

	time.Sleep(300 * time.Millisecond)

	// Cache should be populated
	if ips := cache.GetCluster("test-cluster", "api"); len(ips) != 1 {
		t.Fatalf("Expected 1 IP before disconnect, got %d", len(ips))
	}

	// Disconnect the SSE stream. The watcher must KEEP the last-known-good
	// cache (stale) — a failover must not black out DNS while tasks run. The
	// slow interval means no reconnect happens in the window below, so a
	// non-empty result proves the cache was kept, not re-seeded.
	close(mock.sseDone)
	time.Sleep(200 * time.Millisecond)

	if ips := cache.GetCluster("test-cluster", "api"); len(ips) != 1 {
		t.Errorf("Expected stale cache kept (1 IP) after disconnect, got %d", len(ips))
	}
}

// TestSSE_MultipleJobEvents verifies that events for different jobs each
// trigger their own refresh.
func TestSSE_MultipleJobEvents(t *testing.T) {
	mock := newMockHop()
	mock.setJob("api", []hoplib.Task{
		{ID: "t1", JobName: "api", State: "running"},
	})
	mock.setJob("worker", []hoplib.Task{
		{ID: "t2", JobName: "worker", State: "running"},
	})

	server := httptest.NewServer(mock.handler())
	defer server.Close()
	mock.serverURL = server.URL

	cache := NewCache()
	watcher := NewWatcher(server.URL, cache, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watcher.Run(ctx)

	time.Sleep(300 * time.Millisecond)

	// Both jobs should be in cache after initial refresh
	if ips := cache.GetCluster("test-cluster", "api"); len(ips) != 1 {
		t.Errorf("Expected 1 IP for api, got %d", len(ips))
	}
	if ips := cache.GetCluster("test-cluster", "worker"); len(ips) != 1 {
		t.Errorf("Expected 1 IP for worker, got %d", len(ips))
	}

	// Stop worker tasks
	mock.setJob("worker", []hoplib.Task{
		{ID: "t2", JobName: "worker", State: "stopped"},
	})

	// Send event only for worker
	mock.sendSSE("job", `{"name":"worker"}`)
	time.Sleep(800 * time.Millisecond)

	// api should be unchanged
	if ips := cache.GetCluster("test-cluster", "api"); len(ips) != 1 {
		t.Errorf("api should still have 1 IP, got %d", len(ips))
	}
	// worker should be cleared
	if ips := cache.GetCluster("test-cluster", "worker"); len(ips) != 0 {
		t.Errorf("worker should have 0 IPs after stop, got %d", len(ips))
	}
}

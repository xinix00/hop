package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/leader"
	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/lean/leanhttp"
)

func init() {
	// Fast timeouts for tests
	leader.HTTPClientTimeout = 10 * time.Millisecond
}

func setupTestServer(t *testing.T) (*Server, *leader.Leader, context.CancelFunc) {
	t.Helper()
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go l.Run(ctx)
	time.Sleep(10 * time.Millisecond) // allow stateLoop to start
	server := NewServer(l, ":9080", "", "test")
	return server, l, cancel
}

// doRequest routes through the server's own mux, so the wildcards in patterns
// like /v1/jobs/{name}/status are filled exactly as they are in production.
func doRequest(server *Server, method, path string, body any) *leanhttp.Recorder {
	payload := ""
	if body != nil {
		data, _ := json.Marshal(body)
		payload = string(data)
	}
	req := leanhttp.NewRequest(method, path, strings.NewReader(payload))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	w := leanhttp.NewRecorder()
	server.mux.ServeHTTP(w, req)
	return w
}

func decodeJSON(t *testing.T, w *leanhttp.Recorder, v any) {
	t.Helper()
	if err := json.NewDecoder(w.Body).Decode(v); err != nil {
		t.Fatalf("Failed to decode response: %v (body: %s)", err, w.Body.String())
	}
}

// --- Health ---

func TestHealthEndpoint(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "GET", "/health", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var resp map[string]string
	decodeJSON(t, w, &resp)
	if resp["status"] != "ok" {
		t.Errorf("status = %q, want %q", resp["status"], "ok")
	}
}

// --- Agents ---

func TestGetAgentsEmpty(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "GET", "/v1/agents", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var agents []any
	decodeJSON(t, w, &agents)
	if len(agents) != 0 {
		t.Errorf("Got %d agents, want 0", len(agents))
	}
}

func TestGetAgentsWithRegistered(t *testing.T) {
	server, l, cancel := setupTestServer(t)
	defer cancel()

	for i := 0; i < 3; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		endpoint := fmt.Sprintf("http://10.0.0.%d:8080", i)
		l.RegisterAgent(agentID, endpoint, "", nil)
		l.Heartbeat(agentID, "", 0)
	}
	time.Sleep(10 * time.Millisecond)

	w := doRequest(server, "GET", "/v1/agents", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var agents []*types.Agent
	decodeJSON(t, w, &agents)
	if len(agents) != 3 {
		t.Errorf("Got %d agents, want 3", len(agents))
	}
}

// --- Heartbeat ---

func TestHeartbeatSuccess(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	body := map[string]string{
		"id":       "agent-1",
		"endpoint": "http://10.0.0.1:8080",
	}

	// Register agent first
	doRequest(server, "POST", "/v1/agents", body)

	w := doRequest(server, "POST", "/v1/heartbeat", body)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var resp map[string]any
	decodeJSON(t, w, &resp)
	if resp["status"] != "ok" {
		t.Errorf("status = %v, want %q", resp["status"], "ok")
	}
	// Heartbeat is puur liveness (16-07): geen state_time/jobs meer in de
	// response — gewenste staat heeft één auteur (leader → S3).
}

func TestHeartbeatMissingFields(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	tests := []struct {
		name string
		body map[string]string
	}{
		{"missing id", map[string]string{"endpoint": "http://10.0.0.1:8080"}},
		{"missing endpoint", map[string]string{"id": "agent-1"}},
		{"both empty", map[string]string{"id": "", "endpoint": ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := doRequest(server, "POST", "/v1/heartbeat", tt.body)
			if w.Code != 400 {
				t.Errorf("Status = %d, want 400", w.Code)
			}
		})
	}
}

func TestHeartbeatInvalidJSON(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	req := leanhttp.NewRequest("POST", "/v1/heartbeat", strings.NewReader("not json"))
	w := leanhttp.NewRecorder()
	server.mux.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

func TestHeartbeatRegistersNewAgent(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	body := map[string]string{
		"id":       "new-agent",
		"endpoint": "http://10.0.0.1:8080",
	}

	// Register agent first
	doRequest(server, "POST", "/v1/agents", body)

	w := doRequest(server, "POST", "/v1/heartbeat", body)
	if w.Code != 200 {
		t.Fatalf("Heartbeat status = %d, want 200", w.Code)
	}

	// Verify agent is now visible
	time.Sleep(10 * time.Millisecond)
	w = doRequest(server, "GET", "/v1/agents", nil)

	var agents []*types.Agent
	decodeJSON(t, w, &agents)
	if len(agents) != 1 {
		t.Errorf("Got %d agents, want 1", len(agents))
	}
	if len(agents) > 0 && agents[0].ID != "new-agent" {
		t.Errorf("Agent ID = %q, want %q", agents[0].ID, "new-agent")
	}
}

// --- Jobs ---

func TestGetJobsEmpty(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "GET", "/v1/jobs", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var jobs []any
	decodeJSON(t, w, &jobs)
	if len(jobs) != 0 {
		t.Errorf("Got %d jobs, want 0", len(jobs))
	}
}

func TestGetJobsWithStored(t *testing.T) {
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 5; i++ {
		store.StoreJob(&types.Job{
			Name: fmt.Sprintf("job-%d", i),
		})
	}

	server := NewServer(l, ":9080", "", "test")
	w := doRequest(server, "GET", "/v1/jobs", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var jobs []any
	decodeJSON(t, w, &jobs)
	if len(jobs) != 5 {
		t.Errorf("Got %d jobs, want 5", len(jobs))
	}
}

func TestRunJobMissingName(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "POST", "/v1/jobs", types.Job{Command: "echo test"})

	if w.Code != 400 {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

func TestRunJobMissingCommand(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "POST", "/v1/jobs", types.Job{Name: "test"})

	if w.Code != 400 {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

func TestRunJobInvalidJSON(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	req := leanhttp.NewRequest("POST", "/v1/jobs", strings.NewReader("not json"))
	w := leanhttp.NewRecorder()
	server.mux.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

func TestRunJobCreatesNew(t *testing.T) {
	// Need a mock agent to accept the dispatch
	mockAgent := newTestMockAgent()
	defer mockAgent.Close()

	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	l.RegisterAgent("mock-agent", mockAgent.URL(), "", nil)
	l.Heartbeat("mock-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	server := NewServer(l, ":9080", "", "test")

	w := doRequest(server, "POST", "/v1/jobs", types.Job{
		Name:    "test-job",
		Command: "echo hello",
		Count:   1,
	})

	if w.Code != 201 {
		t.Errorf("Status = %d, want 201 (body: %s)", w.Code, w.Body.String())
	}

	var resp map[string]string
	decodeJSON(t, w, &resp)
	if resp["status"] != "dispatched" {
		t.Errorf("status = %q, want %q", resp["status"], "dispatched")
	}
	if resp["name"] != "test-job" {
		t.Errorf("name = %q, want %q", resp["name"], "test-job")
	}
}

func TestRunJobNoAgentsAvailable(t *testing.T) {
	server, store, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "POST", "/v1/jobs", types.Job{
		Name:    "test-job",
		Command: "echo hello",
	})

	// Job should be stored (201) even when no agents are available
	if w.Code != 201 {
		t.Errorf("Status = %d, want 201", w.Code)
	}

	var resp map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "pending" {
		t.Errorf("status = %q, want %q", resp["status"], "pending")
	}

	// Job should exist in store for later reconciliation
	jobs := store.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job in store, got %d", len(jobs))
	}
	if jobs[0].Name != "test-job" {
		t.Errorf("stored job name = %q, want %q", jobs[0].Name, "test-job")
	}
}

func TestRunJobUpdateExisting(t *testing.T) {
	mockAgent := newTestMockAgent()
	defer mockAgent.Close()

	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	l.RegisterAgent("mock-agent", mockAgent.URL(), "", nil)
	l.Heartbeat("mock-agent", "", 0)
	time.Sleep(10 * time.Millisecond)

	server := NewServer(l, ":9080", "", "test")

	// First: create job
	w := doRequest(server, "POST", "/v1/jobs", types.Job{
		Name:    "update-test",
		Command: "echo v1",
		Count:   1,
	})
	if w.Code != 201 {
		t.Fatalf("Create status = %d, want 201 (body: %s)", w.Code, w.Body.String())
	}

	// Second: apply same name → triggers update (upsert)
	w = doRequest(server, "POST", "/v1/jobs", types.Job{
		Name:    "update-test",
		Command: "echo v2",
		Count:   1,
	})

	if w.Code != 200 {
		t.Errorf("Update status = %d, want 200 (body: %s)", w.Code, w.Body.String())
	}

	var resp map[string]string
	decodeJSON(t, w, &resp)
	if resp["status"] != "updated" {
		t.Errorf("status = %q, want %q", resp["status"], "updated")
	}
}

// --- Delete ---

func TestDeleteJobByName(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "DELETE", "/v1/jobs/myapp", nil)

	if w.Code != 204 {
		t.Errorf("Status = %d, want 204", w.Code)
	}
}

func TestDeleteJobEmptyName(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "DELETE", "/v1/jobs/", nil)

	if w.Code != 400 {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

// --- Unregister Agent ---

func TestUnregisterAgent(t *testing.T) {
	server, l, cancel := setupTestServer(t)
	defer cancel()

	l.RegisterAgent("agent-1", "http://10.0.0.1:8080", "", nil)
	l.Heartbeat("agent-1", "", 0)
	time.Sleep(10 * time.Millisecond)

	w := doRequest(server, "DELETE", "/v1/agents/agent-1", nil)

	if w.Code != 204 {
		t.Errorf("Status = %d, want 204", w.Code)
	}

	// Verify agent is removed
	time.Sleep(10 * time.Millisecond)
	w = doRequest(server, "GET", "/v1/agents", nil)
	var agents []any
	decodeJSON(t, w, &agents)
	if len(agents) != 0 {
		t.Errorf("Got %d agents after unregister, want 0", len(agents))
	}
}

func TestUnregisterAgentEmptyID(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "DELETE", "/v1/agents/", nil)

	if w.Code != 400 {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

// --- Status ---

func TestStatusEndpointEmpty(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "GET", "/v1/status", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var resp map[string]any
	decodeJSON(t, w, &resp)
	if resp["agents"].(float64) != 0 {
		t.Errorf("agents = %v, want 0", resp["agents"])
	}
	if resp["jobs"].(float64) != 0 {
		t.Errorf("jobs = %v, want 0", resp["jobs"])
	}
}

func TestStatusEndpointWithAgents(t *testing.T) {
	server, l, cancel := setupTestServer(t)
	defer cancel()

	for i := 0; i < 3; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		endpoint := fmt.Sprintf("http://10.0.0.%d:8080", i)
		l.RegisterAgent(agentID, endpoint, "", nil)
		l.Heartbeat(agentID, "", 0)
	}
	time.Sleep(10 * time.Millisecond)

	w := doRequest(server, "GET", "/v1/status", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var resp map[string]any
	decodeJSON(t, w, &resp)
	if resp["agents"].(float64) != 3 {
		t.Errorf("agents = %v, want 3", resp["agents"])
	}
}

// --- Notify / SSE Event Tests ---

func TestNotifyWithEventType(t *testing.T) {
	_, l, cancel := setupTestServer(t)
	defer cancel()

	// Subscribe to EventBus before sending notify
	ch := l.EventBus().Subscribe()
	defer l.EventBus().Unsubscribe(ch)

	// Drain the initial event (SSE sends one on subscribe via handleEvents)
	// EventBus.Notify is called directly, so no initial event here.

	// Simulate agent sending notify with event type
	l.EventBus().Notify("job:my-api:started")

	select {
	case msg := <-ch:
		if msg != "job:my-api:started" {
			t.Errorf("EventBus received %q, want %q", msg, "job:my-api:started")
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for EventBus notification")
	}
}

func TestNotifyEndpointWithEvent(t *testing.T) {
	server, l, cancel := setupTestServer(t)
	defer cancel()

	ch := l.EventBus().Subscribe()
	defer l.EventBus().Unsubscribe(ch)

	// POST to /v1/notify with event field
	w := doRequest(server, "POST", "/v1/notify", map[string]string{
		"job":   "my-api",
		"event": "started",
	})

	if w.Code != 204 {
		t.Errorf("Status = %d, want 204", w.Code)
	}

	select {
	case msg := <-ch:
		if msg != "job:my-api:started" {
			t.Errorf("EventBus received %q, want %q", msg, "job:my-api:started")
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for EventBus notification")
	}
}

func TestNotifyEndpointWithoutEvent(t *testing.T) {
	server, l, cancel := setupTestServer(t)
	defer cancel()

	ch := l.EventBus().Subscribe()
	defer l.EventBus().Unsubscribe(ch)

	// POST without event field (backwards compatible)
	w := doRequest(server, "POST", "/v1/notify", map[string]string{
		"job": "my-api",
	})

	if w.Code != 204 {
		t.Errorf("Status = %d, want 204", w.Code)
	}

	select {
	case msg := <-ch:
		if msg != "job:my-api" {
			t.Errorf("EventBus received %q, want %q", msg, "job:my-api")
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for EventBus notification")
	}
}

func TestNotifyEventTypes(t *testing.T) {
	server, l, cancel := setupTestServer(t)
	defer cancel()

	events := []string{"start", "started", "crash", "stop"}
	for _, event := range events {
		t.Run(event, func(t *testing.T) {
			ch := l.EventBus().Subscribe()
			defer l.EventBus().Unsubscribe(ch)

			w := doRequest(server, "POST", "/v1/notify", map[string]string{
				"job":   "test-job",
				"event": event,
			})

			if w.Code != 204 {
				t.Errorf("Status = %d, want 204", w.Code)
			}

			select {
			case msg := <-ch:
				want := "job:test-job:" + event
				if msg != want {
					t.Errorf("EventBus received %q, want %q", msg, want)
				}
			case <-time.After(time.Second):
				t.Fatal("Timeout waiting for EventBus notification")
			}
		})
	}
}

// TestSSEEventFormat verifies the SSE output includes event type.
// Uses a real HTTP server because the handler only returns when its client goes
// away: the events have to be read off a live connection while it still runs.
func TestSSEEventFormat(t *testing.T) {
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	server := NewServer(l, addr, "", "test")
	go func() { _ = server.Run(ctx) }()
	defer server.Stop()
	waitForPort(t, addr)

	// Connect to SSE endpoint
	resp, err := http.Get("http://" + addr + "/v1/events")
	if err != nil {
		t.Fatalf("Failed to connect to SSE: %v", err)
	}
	defer resp.Body.Close()

	// Read initial "ping" event (sent on connect)
	buf := make([]byte, 4096)
	n, err := resp.Body.Read(buf)
	if err != nil {
		t.Fatalf("Failed to read initial SSE event: %v", err)
	}
	initial := string(buf[:n])
	if !containsSSEData(initial, "{}") {
		t.Errorf("Initial event should be {}, got: %s", initial)
	}

	// Fire a notify with event type
	l.EventBus().Notify("job:my-api:started")

	n, err = resp.Body.Read(buf)
	if err != nil {
		t.Fatalf("Failed to read SSE event: %v", err)
	}
	event := string(buf[:n])

	// Should be a "task" event with job name and event type
	if !containsSSEData(event, "event: task") {
		t.Errorf("SSE event type should be 'task', got: %s", event)
	}
	if !containsSSEData(event, `"event":"started"`) {
		t.Errorf("SSE event should contain event type, got: %s", event)
	}
	if !containsSSEData(event, `"job":"my-api"`) {
		t.Errorf("SSE event should contain job name, got: %s", event)
	}
}

func containsSSEData(sse, substr string) bool {
	return bytes.Contains([]byte(sse), []byte(substr))
}

// --- Mock Agent for dispatch tests ---

type testMockAgent struct {
	server *httptest.Server
	mu     sync.Mutex
	tasks  []*types.Task
	seq    int
}

func newTestMockAgent() *testMockAgent {
	ma := &testMockAgent{}

	mux := http.NewServeMux()
	mux.HandleFunc("/run", ma.handleRun)
	mux.HandleFunc("/tasks", ma.handleTasks)
	mux.HandleFunc("/delete/", ma.handleDelete)

	ma.server = httptest.NewServer(mux)
	return ma
}

func (ma *testMockAgent) handleRun(w http.ResponseWriter, r *http.Request) {
	var job types.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	ma.mu.Lock()
	ma.seq++
	task := &types.Task{
		ID:      fmt.Sprintf("task-%s-%d", job.Name, ma.seq),
		JobName: job.Name,
		State:   types.TaskRunning,
	}
	ma.tasks = append(ma.tasks, task)
	ma.mu.Unlock()

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(task)
}

func (ma *testMockAgent) handleTasks(w http.ResponseWriter, r *http.Request) {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	_ = json.NewEncoder(w).Encode(ma.tasks)
}

func (ma *testMockAgent) handleDelete(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]int{"deleted": 1})
}

func (ma *testMockAgent) URL() string {
	return ma.server.URL
}

func (ma *testMockAgent) Close() {
	ma.server.Close()
}

// Een hop-job mag meerdere artifacts hebben: zo dekt één job meerdere
// architecturen. Elk artifact draagt een `match` op node-attributen en de agent
// kiest per node (resolveJobForRun), dus de runner ziet er alsnog één. De
// validatie eiste eerst exact één en gaf 400 — dat brak de multi-arch
// init-regel van HopOS' boot-config.
func TestRunJobHopDriverMultipleArtifacts(t *testing.T) {
	server, store, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "POST", "/v1/jobs", types.Job{
		Name:   "welcome",
		Driver: types.DriverHop,
		Artifacts: []types.Artifact{
			{URL: "https://example.com/welcome-arm64.elf", Match: map[string]string{"node.arch": "arm64"}},
			{URL: "https://example.com/welcome-riscv64.elf", Match: map[string]string{"node.arch": "riscv64"}},
		},
		MemoryLimit: 67108864,
		Ports:       map[string]int{"http": 80},
	})

	if w.Code != 201 {
		t.Fatalf("Status = %d, want 201 — body %s", w.Code, w.Body.String())
	}
	jobs := store.GetJobs()
	if len(jobs) != 1 || len(jobs[0].Artifacts) != 2 {
		t.Fatalf("expected 1 stored job with 2 artifacts, got %+v", jobs)
	}
}

// Zonder command, image én artifacts blijft het een fout: er is niets om te
// draaien. Dat is de andere kant van dezelfde regel.
func TestRunJobHopDriverWithoutArtifacts(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "POST", "/v1/jobs", types.Job{Name: "leeg", Driver: types.DriverHop})

	if w.Code != 400 {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

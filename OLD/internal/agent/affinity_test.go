package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"runtime"
	"testing"

	"time"

	"github.com/xinix00/lean/leanhttp"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/config"
)

func TestMatchesAffinity(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "node-1", NewMockRunner())

	// Agent has auto-detected: node.id=node-1, node.arch=<runtime>, node.os=<runtime>
	tests := []struct {
		name     string
		affinity map[string]string
		want     bool
	}{
		{"nil affinity", nil, true},
		{"empty affinity", map[string]string{}, true},
		{"match node.id", map[string]string{"node.id": "node-1"}, true},
		{"mismatch node.id", map[string]string{"node.id": "node-2"}, false},
		{"missing key", map[string]string{"gpu": "true"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := agent.matchesAffinity(tt.affinity); got != tt.want {
				t.Errorf("matchesAffinity(%v) = %v, want %v (attrs=%v)", tt.affinity, got, tt.want, agent.attributes)
			}
		})
	}
}

func TestMatchesAffinityWithConfigAttributes(t *testing.T) {
	cfg := testConfig()
	cfg.Node.Attributes = map[string]string{
		"region": "eu-west-1",
		"gpu":    "true",
	}
	agent := New(cfg, "node-1", NewMockRunner())

	tests := []struct {
		name     string
		affinity map[string]string
		want     bool
	}{
		{"match custom attr", map[string]string{"region": "eu-west-1"}, true},
		{"match multiple", map[string]string{"region": "eu-west-1", "gpu": "true"}, true},
		{"mismatch custom", map[string]string{"region": "us-east-1"}, false},
		{"mix auto+custom", map[string]string{"node.id": "node-1", "gpu": "true"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := agent.matchesAffinity(tt.affinity); got != tt.want {
				t.Errorf("matchesAffinity(%v) = %v, want %v", tt.affinity, got, tt.want)
			}
		})
	}
}

func TestConfigOverridesAutoDetected(t *testing.T) {
	cfg := testConfig()
	cfg.Node.Attributes = map[string]string{
		"node.id": "custom-id", // override auto-detected
	}
	agent := New(cfg, "auto-id", NewMockRunner())

	if agent.attributes["node.id"] != "custom-id" {
		t.Errorf("node.id = %q, want %q (config should override auto-detected)", agent.attributes["node.id"], "custom-id")
	}
}

func TestAutoDetectedAttributes(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "test-node", NewMockRunner())

	// node.id comes from the id parameter
	if agent.attributes["node.id"] != "test-node" {
		t.Errorf("node.id = %q, want %q", agent.attributes["node.id"], "test-node")
	}
	// node.arch and node.os come from runtime
	if agent.attributes["node.arch"] != runtime.GOARCH {
		t.Errorf("node.arch = %q, want %q", agent.attributes["node.arch"], runtime.GOARCH)
	}
	if agent.attributes["node.os"] != runtime.GOOS {
		t.Errorf("node.os = %q, want %q", agent.attributes["node.os"], runtime.GOOS)
	}
}

func TestMatchesAffinityMultipleConstraintsAND(t *testing.T) {
	// All constraints must match (AND logic)
	cfg := testConfig()
	cfg.Node.Attributes = map[string]string{
		"region": "eu-west-1",
		"gpu":    "true",
	}
	agent := New(cfg, "node-1", NewMockRunner())

	// Both match → true
	if !agent.matchesAffinity(map[string]string{"node.id": "node-1", "region": "eu-west-1"}) {
		t.Error("Expected match when both constraints match")
	}

	// First matches, second doesn't → false (AND)
	if agent.matchesAffinity(map[string]string{"node.id": "node-1", "region": "us-east-1"}) {
		t.Error("Expected no match when second constraint fails (AND logic)")
	}

	// Three constraints, one fails → false
	if agent.matchesAffinity(map[string]string{"node.id": "node-1", "region": "eu-west-1", "gpu": "false"}) {
		t.Error("Expected no match when third constraint fails")
	}

	// Three constraints, all match → true
	if !agent.matchesAffinity(map[string]string{"node.id": "node-1", "region": "eu-west-1", "gpu": "true"}) {
		t.Error("Expected match when all three constraints match")
	}
}

func TestMatchesAffinityOSFiltering(t *testing.T) {
	// Simulates the user's scenario: daemon with node.os=darwin on a linux node
	cfg := testConfig()
	// Override node.os to simulate a linux agent
	cfg.Node.Attributes = map[string]string{"node.os": "linux"}
	linuxAgent := New(cfg, "linux-node", NewMockRunner())

	// Job wants only darwin nodes
	darwinAffinity := map[string]string{"node.os": "darwin"}
	if linuxAgent.matchesAffinity(darwinAffinity) {
		t.Error("Linux agent should NOT match darwin affinity")
	}

	// Job wants only linux nodes
	linuxAffinity := map[string]string{"node.os": "linux"}
	if !linuxAgent.matchesAffinity(linuxAffinity) {
		t.Error("Linux agent should match linux affinity")
	}
}

func TestHandleRunAffinityMismatch(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "node-1", NewMockRunner())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Job requires node-2, but this agent is node-1
	job := types.Job{
		Name:     "test-job",
		Command:  "echo hello",
		Affinity: map[string]string{"node.id": "node-2"},
	}
	body, _ := json.Marshal(job)

	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != leanhttp.StatusNotAcceptable {
		t.Errorf("Status = %d, want %d (406 Not Acceptable)", w.Code, leanhttp.StatusNotAcceptable)
	}

	var resp map[string]string
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp["error"] != "affinity mismatch" {
		t.Errorf("error = %q, want %q", resp["error"], "affinity mismatch")
	}
}

func TestHandleRunAffinityMatch(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "node-1", NewMockRunner())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Job requires node-1, this agent IS node-1
	job := types.Job{
		Name:     "test-job",
		Command:  "echo hello",
		Affinity: map[string]string{"node.id": "node-1"},
	}
	body, _ := json.Marshal(job)

	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != leanhttp.StatusAccepted {
		t.Errorf("Status = %d, want %d (202 Accepted)", w.Code, leanhttp.StatusAccepted)
	}
}

func TestHandleRunNoAffinity(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "node-1", NewMockRunner())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Job without affinity should be accepted
	job := types.Job{
		Name:    "test-job",
		Command: "echo hello",
	}
	body, _ := json.Marshal(job)

	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != leanhttp.StatusAccepted {
		t.Errorf("Status = %d, want %d (202 Accepted)", w.Code, leanhttp.StatusAccepted)
	}
}

func TestHandleRunAffinityBeforeCapacity(t *testing.T) {
	// Affinity check should happen BEFORE capacity check.
	// Even if we have zero capacity, affinity mismatch should return 406 not 503.
	cfg := testConfig()
	cfg.Capacity.CPUShares = 0 // minimal capacity
	cfg.Capacity.Memory = 0
	agent := New(cfg, "node-1", NewMockRunner())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:        "test-job",
		Command:     "echo hello",
		CPUShares:   99999, // exceeds any capacity
		MemoryLimit: 999999999999,
		Affinity:    map[string]string{"node.id": "wrong-node"},
	}
	body, _ := json.Marshal(job)

	req := leanhttp.NewRequest(leanhttp.MethodPost, "/run", bytes.NewReader(body))
	w := leanhttp.NewRecorder()

	agent.handleRun(w, req)

	// Should be 406 (affinity), not 503 (capacity)
	if w.Code != leanhttp.StatusNotAcceptable {
		t.Errorf("Status = %d, want %d (affinity should be checked before capacity)", w.Code, leanhttp.StatusNotAcceptable)
	}
}

func TestResolveArtifactFirstMatch(t *testing.T) {
	cfg := testConfig()
	cfg.Node.Attributes = map[string]string{"node.os": "darwin"}
	agent := New(cfg, "mac-node", NewMockRunner())

	artifacts := []types.Artifact{
		{URL: "https://example.com/app-linux.tar.gz", Match: map[string]string{"node.os": "linux"}},
		{URL: "https://example.com/app-darwin.tar.gz", Match: map[string]string{"node.os": "darwin"}},
		{URL: "https://example.com/app-windows.tar.gz", Match: map[string]string{"node.os": "windows"}},
	}

	resolved := agent.resolveArtifact(artifacts)
	if resolved == nil {
		t.Fatal("Expected a matching artifact")
	}
	if resolved.URL != "https://example.com/app-darwin.tar.gz" {
		t.Errorf("URL = %q, want darwin artifact", resolved.URL)
	}
}

func TestResolveArtifactCatchAll(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "node-1", NewMockRunner())

	artifacts := []types.Artifact{
		{URL: "https://example.com/app-linux.tar.gz", Match: map[string]string{"node.os": "linux"}},
		{URL: "https://example.com/app-fallback.tar.gz"}, // no Match = catch-all
	}

	// node.os won't be "linux" (it's the test node's auto-detected OS)
	// but the catch-all should always match
	resolved := agent.resolveArtifact(artifacts)
	if resolved == nil {
		t.Fatal("Expected catch-all artifact to match")
	}
	// Either linux matched (if running on linux) or fallback matched
	if resolved.URL != "https://example.com/app-linux.tar.gz" && resolved.URL != "https://example.com/app-fallback.tar.gz" {
		t.Errorf("Unexpected URL: %q", resolved.URL)
	}
}

func TestResolveArtifactNoMatch(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "node-1", NewMockRunner())

	artifacts := []types.Artifact{
		{URL: "https://example.com/app-windows.tar.gz", Match: map[string]string{"node.os": "windows"}},
	}

	resolved := agent.resolveArtifact(artifacts)
	if resolved != nil {
		t.Errorf("Expected no match, got %q", resolved.URL)
	}
}

func TestResolveArtifactMultipleConstraints(t *testing.T) {
	cfg := testConfig()
	cfg.Node.Attributes = map[string]string{"node.os": "linux"}
	agent := New(cfg, "linux-arm", NewMockRunner())
	// agent has: node.id=linux-arm, node.os=linux, node.arch=<runtime>

	artifacts := []types.Artifact{
		{URL: "https://example.com/app-linux-amd64.tar.gz", Match: map[string]string{"node.os": "linux", "node.arch": "amd64"}},
		{URL: "https://example.com/app-linux-arm64.tar.gz", Match: map[string]string{"node.os": "linux", "node.arch": "arm64"}},
		{URL: "https://example.com/app-darwin-arm64.tar.gz", Match: map[string]string{"node.os": "darwin", "node.arch": "arm64"}},
	}

	resolved := agent.resolveArtifact(artifacts)
	if resolved == nil {
		t.Fatal("Expected a match (linux + current arch)")
	}
	// Should match the linux entry for the current runtime arch
	if resolved.Match["node.os"] != "linux" {
		t.Errorf("Expected linux match, got %v", resolved.Match)
	}
}

func TestResolveArtifactEmptySlice(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "node-1", NewMockRunner())

	resolved := agent.resolveArtifact(nil)
	if resolved != nil {
		t.Error("Expected nil for empty artifacts")
	}

	resolved = agent.resolveArtifact([]types.Artifact{})
	if resolved != nil {
		t.Error("Expected nil for empty artifacts slice")
	}
}

func TestCapacityIncludesAttributes(t *testing.T) {
	cfg := &config.Config{
		Node: config.NodeConfig{
			IP:   "127.0.0.1",
			Port: 8080,
			Attributes: map[string]string{
				"region": "eu-west-1",
			},
		},
		Paths: config.PathsConfig{
			RootfsBase: "/tmp/test-hop",
			StateFile:  "/tmp/test-hop/state.json",
		},
		Capacity: config.CapacityConfig{
			CPUShares: 1000,
			Memory:    1024 * 1024 * 1024,
		},
	}
	agent := New(cfg, "node-1", NewMockRunner())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	req := leanhttp.NewRequest(leanhttp.MethodGet, "/capacity", nil)
	w := leanhttp.NewRecorder()

	agent.handleCapacity(w, req)

	var resp CapacityResponse
	_ = json.NewDecoder(w.Body).Decode(&resp)

	if resp.Attributes["node.id"] != "node-1" {
		t.Errorf("attributes[node.id] = %q, want %q", resp.Attributes["node.id"], "node-1")
	}
	if resp.Attributes["region"] != "eu-west-1" {
		t.Errorf("attributes[region] = %q, want %q", resp.Attributes["region"], "eu-west-1")
	}
}

// ============== ARTIFACT-FILTERING INVARIANT (REGRESSION) ==============
//
// The runner expects job.Artifacts to contain at most one entry (the matched
// one). Every code path that calls runner.Run must first funnel through
// resolveJobForRun. Previously, restartTask bypassed this and called Run
// with the full unfiltered list, so process.go's Artifacts[0] picked the
// wrong entry on every restart (e.g. darwin-arm64 on a linux node).
//
// These tests pin both ends of that contract:
//   - resolveJobForRun returns a filtered copy without touching the original
//   - every runner.Run call (initial + restart) sees exactly one matched entry

func TestResolveJobForRunFiltersToSingleMatch(t *testing.T) {
	cfg := testConfig()
	cfg.Node.Attributes = map[string]string{"node.os": "linux", "node.arch": "amd64"}
	agent := New(cfg, "linux-node", NewMockRunner())

	job := &types.Job{
		Name: "test",
		Artifacts: []types.Artifact{
			{URL: "darwin-arm64", Match: map[string]string{"node.os": "darwin", "node.arch": "arm64"}},
			{URL: "linux-amd64", Match: map[string]string{"node.os": "linux", "node.arch": "amd64"}},
		},
	}

	runJob, err := agent.resolveJobForRun(job)
	if err != nil {
		t.Fatalf("resolveJobForRun: %v", err)
	}
	if len(runJob.Artifacts) != 1 {
		t.Fatalf("Got %d artifacts, want 1", len(runJob.Artifacts))
	}
	if runJob.Artifacts[0].URL != "linux-amd64" {
		t.Errorf("URL = %q, want linux-amd64", runJob.Artifacts[0].URL)
	}
	// Stored job must not be mutated — the leader pushes the full list and
	// other agents (with different OS/arch) may resolve to a different entry.
	if len(job.Artifacts) != 2 {
		t.Errorf("Original job was mutated: got %d artifacts, want 2", len(job.Artifacts))
	}
}

func TestResolveJobForRunNoMatch(t *testing.T) {
	cfg := testConfig()
	cfg.Node.Attributes = map[string]string{"node.os": "linux", "node.arch": "amd64"}
	agent := New(cfg, "linux-node", NewMockRunner())

	job := &types.Job{
		Artifacts: []types.Artifact{
			{URL: "darwin-arm64", Match: map[string]string{"node.os": "darwin"}},
			{URL: "windows-amd64", Match: map[string]string{"node.os": "windows"}},
		},
	}

	if _, err := agent.resolveJobForRun(job); err == nil {
		t.Error("Expected error when no artifact matches, got nil")
	}
}

func TestResolveJobForRunPassthroughWithoutArtifacts(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "node-1", NewMockRunner())

	job := &types.Job{Name: "no-artifacts", Command: "./app"}
	runJob, err := agent.resolveJobForRun(job)
	if err != nil {
		t.Fatalf("resolveJobForRun: %v", err)
	}
	if runJob != job {
		t.Error("Expected same pointer when job has no artifacts (no copy needed)")
	}
}

// TestRunnerNeverSeesMismatchedArtifactOnRestart is the regression test for
// the bug fixed in v0.19.4: restartTask bypassed resolveArtifact, so a Linux
// node would fetch the darwin-arm64 artifact on restart because process.go
// blindly grabbed Artifacts[0].
//
// The contract is platform-agnostic: whichever artifact the agent picks on
// the first start, every restart must pick the same one — and the runner
// must never see more than that one entry.
func TestRunnerNeverSeesMismatchedArtifactOnRestart(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()

	// Capture what every Run() call sees. The lock here is the MockRunner's
	// internal mutex (taken by Run before invoking onRun), so direct reads
	// are safe.
	var seen []types.Artifact
	mockRunner.onRun = func(job *types.Job) error {
		// Defensive copy so later mutations to job can't poison the record.
		artifacts := append([]types.Artifact(nil), job.Artifacts...)
		seen = append(seen, artifacts...)
		return ErrSimulated // force restart on every attempt
	}

	agent := New(cfg, "test-node", mockRunner)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Build a job with two artifacts. The match for the current host is the
	// "wanted" one — whichever it is, restart must keep picking it.
	wantedURL := "current-" + runtime.GOOS + "-" + runtime.GOARCH
	otherURL := "other-platform"
	job := &types.Job{
		Name:    "regression",
		Command: "./app",
		Artifacts: []types.Artifact{
			{URL: otherURL, Match: map[string]string{"node.os": "no-such-os"}},
			{URL: wantedURL, Match: map[string]string{"node.os": runtime.GOOS, "node.arch": runtime.GOARCH}},
		},
		MaxRestarts: intPtr(1),
	}

	task := newTask(job)
	// Mimic handleRun's capacity reservation: job + task are in state BEFORE
	// startJob runs. Without this, startJob's Run failure leaves no task for
	// restartTask's atomic swap to find — it would return early and Run would
	// never be called twice, hiding the bug we're trying to catch.
	agent.do(func(s *agentState) {
		s.jobs[job.Name] = job
		s.tasks[task.ID] = task
	})

	_ = agent.startJob(job, task)

	// startJob fails → handleRun's caller (or test driver here) calls restartTask.
	// We mimic that path directly to keep the test focused on the runner-facing
	// invariant rather than the dispatch wrapper.
	agent.restartTask(task, true)

	// Backoff is 1s before the first restart (see handlers.go).
	time.Sleep(1500 * time.Millisecond)

	if len(seen) == 0 {
		t.Fatal("MockRunner.Run was never called")
	}
	for i, art := range seen {
		if art.URL != wantedURL {
			t.Errorf("Run call #%d saw artifact %q, want %q — runner must only ever see the matched entry", i, art.URL, wantedURL)
		}
	}
}

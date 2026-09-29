package types

import (
	"encoding/json"
	"testing"
	"time"
)

func intPtr(n int) *int { return &n }

func TestTaskStateConstants(t *testing.T) {
	tests := []struct {
		state TaskState
		want  string
	}{
		{TaskRunning, "running"},
		{TaskFailed, "failed"},
	}

	for _, tt := range tests {
		if string(tt.state) != tt.want {
			t.Errorf("TaskState = %q, want %q", tt.state, tt.want)
		}
	}
}

func TestJobJSONRoundtrip(t *testing.T) {
	job := Job{
		Name:        "my-app",
		Command:     "echo hello",
		Count:       3,
		Ports:       map[string]int{"http": 0, "grpc": 0},
		CPUShares:   100,
		MemoryLimit: 512 * 1024 * 1024,
		Env:         map[string]string{"FOO": "bar"},
		Tags:        map[string]string{"env": "prod"},
		Artifacts: []Artifact{
			{
				URL:     "https://example.com/app-arm64.tar.gz",
				Match:   map[string]string{"node.arch": "arm64"},
				Headers: map[string]string{"Authorization": "Bearer token"},
			},
			{
				URL:   "https://example.com/app-amd64.tar.gz",
				Match: map[string]string{"node.arch": "amd64"},
			},
		},
		HealthCheck: &HealthCheck{
			Path:     "/health",
			Port:     "http",
			Interval: 10 * time.Second,
			Timeout:  5 * time.Second,
		},
		MaxRestarts: intPtr(5),
	}

	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded Job
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify fields
	if decoded.Name != job.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, job.Name)
	}
	if decoded.Command != job.Command {
		t.Errorf("Command = %q, want %q", decoded.Command, job.Command)
	}
	if decoded.Count != job.Count {
		t.Errorf("Count = %d, want %d", decoded.Count, job.Count)
	}
	if len(decoded.Ports) != len(job.Ports) {
		t.Errorf("Ports length = %d, want %d", len(decoded.Ports), len(job.Ports))
	}
	if decoded.CPUShares != job.CPUShares {
		t.Errorf("CPUShares = %d, want %d", decoded.CPUShares, job.CPUShares)
	}
	if decoded.MemoryLimit != job.MemoryLimit {
		t.Errorf("MemoryLimit = %d, want %d", decoded.MemoryLimit, job.MemoryLimit)
	}
	if len(decoded.Artifacts) != 2 {
		t.Errorf("Artifacts length = %d, want 2", len(decoded.Artifacts))
	} else {
		if decoded.Artifacts[0].URL != job.Artifacts[0].URL {
			t.Errorf("Artifacts[0].URL = %q, want %q", decoded.Artifacts[0].URL, job.Artifacts[0].URL)
		}
		if decoded.Artifacts[0].Match["node.arch"] != "arm64" {
			t.Errorf("Artifacts[0].Match[node.arch] = %q, want %q", decoded.Artifacts[0].Match["node.arch"], "arm64")
		}
	}
	if decoded.HealthCheck == nil {
		t.Error("HealthCheck is nil")
	} else if decoded.HealthCheck.Path != job.HealthCheck.Path {
		t.Errorf("HealthCheck.Path = %q, want %q", decoded.HealthCheck.Path, job.HealthCheck.Path)
	}
}

func TestTaskJSONRoundtrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	task := Task{
		ID:           "task-456",
		JobName:      "my-app",
		Ports:        map[string]int{"http": 8080, "grpc": 9090},
		Pid:          12345,
		State:        TaskRunning,
		StartedAt:    now,
		RestartCount: 2,
	}

	data, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded Task
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.ID != task.ID {
		t.Errorf("ID = %q, want %q", decoded.ID, task.ID)
	}
	if decoded.State != task.State {
		t.Errorf("State = %q, want %q", decoded.State, task.State)
	}
	if decoded.Pid != task.Pid {
		t.Errorf("Pid = %d, want %d", decoded.Pid, task.Pid)
	}
	if len(decoded.Ports) != len(task.Ports) {
		t.Errorf("Ports length = %d, want %d", len(decoded.Ports), len(task.Ports))
	}
	if decoded.Ports["http"] != 8080 {
		t.Errorf("Ports[http] = %d, want 8080", decoded.Ports["http"])
	}
}

func TestAgentJSONRoundtrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	agent := Agent{
		ID:       "agent-789",
		Endpoint: "http://192.168.1.10:8080",
		LastSeen: now,
	}

	data, err := json.Marshal(agent)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded Agent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.ID != agent.ID {
		t.Errorf("ID = %q, want %q", decoded.ID, agent.ID)
	}
	if decoded.Endpoint != agent.Endpoint {
		t.Errorf("Endpoint = %q, want %q", decoded.Endpoint, agent.Endpoint)
	}
}

func TestArtifactAuthHelpers(t *testing.T) {
	artifact := Artifact{
		URL: "s3://bucket/key",
		Auth: map[string]string{
			"access_key": "AKIAIOSFODNN7EXAMPLE",
			"secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			"region":     "us-east-1",
		},
	}

	data, err := json.Marshal(artifact)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded Artifact
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.Auth["access_key"] != artifact.Auth["access_key"] {
		t.Errorf("Auth[access_key] = %q, want %q", decoded.Auth["access_key"], artifact.Auth["access_key"])
	}
	if decoded.Auth["region"] != artifact.Auth["region"] {
		t.Errorf("Auth[region] = %q, want %q", decoded.Auth["region"], artifact.Auth["region"])
	}
}

func TestJobDefaults(t *testing.T) {
	// Test that omitempty fields can be absent
	jsonStr := `{"name": "test", "command": "echo"}`

	var job Job
	if err := json.Unmarshal([]byte(jsonStr), &job); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if job.Count != 0 {
		t.Errorf("Count = %d, want 0 (default)", job.Count)
	}
	if len(job.Artifacts) != 0 {
		t.Error("Artifacts should be empty")
	}
	if job.HealthCheck != nil {
		t.Error("HealthCheck should be nil")
	}
	if job.Ports != nil {
		t.Error("Ports should be nil")
	}
}

package agent

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/config"
)

// intPtr returns a pointer to an int literal (helper for *int struct fields).
func intPtr(n int) *int { return &n }

// testConfig creates a test configuration
func testConfig() *config.Config {
	return &config.Config{
		Node: config.NodeConfig{
			IP:   "127.0.0.1",
			Port: 8080,
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
}

// ============== JOB STORAGE TESTS ==============

func TestAgentStoreAndGetJob(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	job := &types.Job{
		Name:    "test-job",
		Command: "echo hello",
	}

	agent.StoreJob(job)
	time.Sleep(10 * time.Millisecond)

	// GetJob looks up by name
	got := agent.GetJob("test-job")
	if got == nil {
		t.Fatal("GetJob returned nil")
	}
	if got.Name != "test-job" {
		t.Errorf("GetJob().Name = %q, want %q", got.Name, "test-job")
	}
}

func TestAgentGetJobs(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	for i := 0; i < 3; i++ {
		agent.StoreJob(&types.Job{
			Name:    "job-" + string(rune('a'+i)),
			Command: "echo",
		})
	}

	time.Sleep(10 * time.Millisecond)

	jobs := agent.GetJobs()
	if len(jobs) != 3 {
		t.Errorf("GetJobs() returned %d jobs, want 3", len(jobs))
	}
}

func TestAgentGetJobNotFound(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := agent.GetJob("nonexistent")
	if job != nil {
		t.Error("GetJob should return nil for nonexistent job")
	}
}

func TestAgentStoreJobOverwrite(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Same Name = overwrite
	agent.StoreJob(&types.Job{
		Name:    "original",
		Command: "echo original",
	})

	time.Sleep(10 * time.Millisecond)

	agent.StoreJob(&types.Job{
		Name:    "original",
		Command: "echo updated",
	})

	time.Sleep(10 * time.Millisecond)

	job := agent.GetJob("original")
	if job.Command != "echo updated" {
		t.Errorf("Job command = %q, want %q (overwrite failed)", job.Command, "echo updated")
	}

	jobs := agent.GetJobs()
	if len(jobs) != 1 {
		t.Errorf("GetJobs() returned %d jobs, want 1", len(jobs))
	}
}

// ============== STATE SYNC TESTS ==============

func TestAgentSyncJobs(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	agent.StoreJob(&types.Job{
		Name:    "old",
		Command: "old",
	})

	time.Sleep(10 * time.Millisecond)
	beforeSync := time.Now()

	newJobs := []*types.Job{
		{Name: "new1", Command: "new1"},
		{Name: "new2", Command: "new2"},
	}

	agent.SyncJobs(newJobs, beforeSync)
	time.Sleep(10 * time.Millisecond)

	jobs := agent.GetJobs()
	if len(jobs) != 3 {
		t.Errorf("GetJobs() returned %d jobs, want 3", len(jobs))
	}

	stateTime := agent.GetStateTime()
	if stateTime.IsZero() {
		t.Error("GetStateTime() should not be zero after SyncJobs")
	}
	if stateTime.Before(beforeSync) {
		t.Errorf("GetStateTime() = %v, should be after %v", stateTime, beforeSync)
	}
}

func TestAgentGetStateTime(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	stateTime := agent.GetStateTime()
	if !stateTime.IsZero() {
		t.Errorf("initial GetStateTime() = %v, want zero", stateTime)
	}
}

func TestAgentStateTimeUpdatesOnStoreJob(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Initial stateTime should be zero
	initialTime := agent.GetStateTime()
	if !initialTime.IsZero() {
		t.Errorf("initial GetStateTime() = %v, want zero", initialTime)
	}

	// StoreJob should update stateTime
	agent.StoreJob(&types.Job{Name: "test", Command: "echo"})
	time.Sleep(10 * time.Millisecond)

	afterStore := agent.GetStateTime()
	if afterStore.IsZero() {
		t.Error("GetStateTime() should not be zero after StoreJob")
	}
}

// ============== CONCURRENT ACCESS TESTS ==============

func TestAgentConcurrentStateAccess(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			agent.StoreJob(&types.Job{
				Name:    "job-" + string(rune('0'+n)),
				Command: "echo",
			})
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			agent.GetJobs()
			agent.GetStateTime()
		}()
	}

	wg.Wait()

	jobs := agent.GetJobs()
	if len(jobs) != 10 {
		t.Errorf("GetJobs() returned %d jobs, want 10", len(jobs))
	}
}

func TestAgentConcurrentJobStoreAndGet(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	var wg sync.WaitGroup

	// Store and immediately get
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			jobName := "job-" + string(rune('a'+n%26))
			agent.StoreJob(&types.Job{
				Name:    jobName,
				Command: "echo",
			})
			// Immediately try to get it by name
			agent.GetJob(jobName)
		}(i)
	}

	wg.Wait()
	// Should not panic or deadlock
}

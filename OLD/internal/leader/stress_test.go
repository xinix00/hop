package leader

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// startStressLeader creates a leader with settle delay to prevent reconciliation
// with fake agent endpoints during benchmarks.
func startStressLeader(store JobStore) (*Leader, func()) {
	l := New("local-agent", store, nil)
	l.SetSettleDelay(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	go l.stateLoop(ctx)
	return l, cancel
}

// TestMassiveScale tests system behavior with large numbers
func TestMassiveScale(t *testing.T) {
	tests := []struct {
		name       string
		agents     int
		jobsPerJob int
		totalJobs  int
	}{
		{"100 agents, 1000 jobs", 100, 1, 1000},
		{"1000 agents, 10k jobs", 1000, 1, 10000},
		{"10 agents, 100 jobs with 10 instances each", 10, 10, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewMockJobStore()
			leader, cancel := startStressLeader(store)
			defer cancel()

			// Register agents
			start := time.Now()
			for i := 0; i < tt.agents; i++ {
				agentID := fmt.Sprintf("agent-%d", i)
				leader.RegisterAgent(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), "", nil)
				leader.Heartbeat(agentID, "", 0)
			}
			registerTime := time.Since(start)

			// Create jobs
			start = time.Now()
			for i := 0; i < tt.totalJobs; i++ {
				job := &types.Job{
					Name:    fmt.Sprintf("job-%d", i),
					Command: "echo test",
					Count:   tt.jobsPerJob,
				}
				store.StoreJob(job)
			}
			createTime := time.Since(start)

			// Measure agent retrieval
			start = time.Now()
			agents := leader.GetAgents()
			getAgentsTime := time.Since(start)

			// Measure job retrieval
			start = time.Now()
			jobs := leader.GetJobs()
			getJobsTime := time.Since(start)

			t.Logf("Scale test results:")
			t.Logf("  Register %d agents: %v (%.0f agents/sec)", tt.agents, registerTime, float64(tt.agents)/registerTime.Seconds())
			t.Logf("  Create %d jobs: %v (%.0f jobs/sec)", tt.totalJobs, createTime, float64(tt.totalJobs)/createTime.Seconds())
			t.Logf("  Get %d agents: %v", len(agents), getAgentsTime)
			t.Logf("  Get %d jobs: %v", len(jobs), getJobsTime)
			t.Logf("  Total instances: %d", tt.totalJobs*tt.jobsPerJob)

			if len(agents) != tt.agents {
				t.Errorf("Expected %d agents, got %d", tt.agents, len(agents))
			}
			if len(jobs) != tt.totalJobs {
				t.Errorf("Expected %d jobs, got %d", tt.totalJobs, len(jobs))
			}
		})
	}
}

// BenchmarkMassiveAgents measures performance with many agents
func BenchmarkMassiveAgents(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d_agents", size), func(b *testing.B) {
			store := NewMockJobStore()
			leader, cancel := startStressLeader(store)
			defer cancel()

			// Register agents once
			for i := 0; i < size; i++ {
				agentID := fmt.Sprintf("agent-%d", i)
				leader.RegisterAgent(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), "", nil)
				leader.Heartbeat(agentID, "", 0)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				agents := leader.GetAgents()
				if len(agents) != size {
					b.Fatalf("Expected %d agents, got %d", size, len(agents))
				}
			}
		})
	}
}

// BenchmarkMassiveJobs measures performance with many jobs
func BenchmarkMassiveJobs(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d_jobs", size), func(b *testing.B) {
			store := NewMockJobStore()
			leader, cancel := startStressLeader(store)
			defer cancel()

			// Register agents
			for i := 0; i < 10; i++ {
				agentID := fmt.Sprintf("agent-%d", i)
				leader.RegisterAgent(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), "", nil)
				leader.Heartbeat(agentID, "", 0)
			}

			// Create jobs
			for i := 0; i < size; i++ {
				job := &types.Job{
					Name:    fmt.Sprintf("job-%d", i),
					Command: "echo test",
					Count:   1,
				}
				store.StoreJob(job)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				jobs := leader.GetJobs()
				if len(jobs) != size {
					b.Fatalf("Expected %d jobs, got %d", size, len(jobs))
				}
			}
		})
	}
}

// BenchmarkJobLookupScale measures job lookup with different scales
func BenchmarkJobLookupScale(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d_jobs", size), func(b *testing.B) {
			store := NewMockJobStore()
			_, cancel := startStressLeader(store)
			defer cancel()

			// Create jobs
			for i := 0; i < size; i++ {
				job := &types.Job{
					Name:    fmt.Sprintf("job-%d", i),
					Command: "echo test",
				}
				store.StoreJob(job)
			}
			time.Sleep(10 * time.Millisecond)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				name := fmt.Sprintf("job-%d", i%size)
				job := store.GetJob(name)
				if job == nil {
					b.Fatal("Job not found")
				}
			}

			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/lookup")
		})
	}
}

// BenchmarkHeartbeatScale measures heartbeat processing at scale
func BenchmarkHeartbeatScale(b *testing.B) {
	agentCounts := []int{10, 100, 1000}

	for _, count := range agentCounts {
		b.Run(fmt.Sprintf("%d_agents", count), func(b *testing.B) {
			store := NewMockJobStore()
			leader, cancel := startStressLeader(store)
			defer cancel()

			// Pre-register agents
			for i := 0; i < count; i++ {
				agentID := fmt.Sprintf("agent-%d", i)
				leader.RegisterAgent(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), "", nil)
				leader.Heartbeat(agentID, "", 0)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				agentID := fmt.Sprintf("agent-%d", i%count)
				leader.Heartbeat(agentID, "", 0)
			}

			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "heartbeats/sec")
		})
	}
}

// BenchmarkPlacementScale measures placement tracking at scale
func BenchmarkPlacementScale(b *testing.B) {
	scenarios := []struct {
		agents int
		jobs   int
		count  int // instances per job
	}{
		{10, 100, 3},
		{100, 1000, 5},
		{1000, 10000, 1},
	}

	for _, s := range scenarios {
		name := fmt.Sprintf("%dA_%dJ_%dI", s.agents, s.jobs, s.count)
		b.Run(name, func(b *testing.B) {
			store := NewMockJobStore()
			leader, cancel := startStressLeader(store)
			defer cancel()

			// Register agents
			for i := 0; i < s.agents; i++ {
				agentID := fmt.Sprintf("agent-%d", i)
				leader.RegisterAgent(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), "", nil)
				leader.Heartbeat(agentID, "", 0)
			}

			// Create placement for all jobs
			for i := 0; i < s.jobs; i++ {
				jobID := fmt.Sprintf("job-%d", i)
				for j := 0; j < s.count; j++ {
					agentID := fmt.Sprintf("agent-%d", (i*s.count+j)%s.agents)
					leader.do(func(state *leaderState) {
						if state.placed[agentID] == nil {
							state.placed[agentID] = make(map[string]int)
						}
						state.placed[agentID][jobID]++
					})
				}
			}

			totalInstances := s.jobs * s.count

			b.ResetTimer()
			b.ReportAllocs()

			// Measure placed lookups
			for i := 0; i < b.N; i++ {
				jobID := fmt.Sprintf("job-%d", i%s.jobs)
				placed := leader.GetPlaced(jobID)
				if len(placed) != s.count {
					b.Fatalf("Expected %d placements, got %d", s.count, len(placed))
				}
			}

			b.ReportMetric(float64(totalInstances), "total_instances")
		})
	}
}

// BenchmarkMemoryFootprint measures memory usage with large state
func BenchmarkMemoryFootprint(b *testing.B) {
	store := NewMockJobStore()
	leader, cancel := startStressLeader(store)
	defer cancel()

	// Massive scale: 1000 agents, 10000 jobs, 3 instances each = 30k placements
	for i := 0; i < 1000; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		leader.RegisterAgent(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), "", nil)
		leader.Heartbeat(agentID, "", 0)
	}

	for i := 0; i < 10000; i++ {
		jobName := fmt.Sprintf("job-%d", i)
		job := &types.Job{
			Name:        jobName,
			Command:     "echo test",
			Count:       3,
			CPUShares:   1024,
			MemoryLimit: 512 * 1024 * 1024,
			Tags:        map[string]string{"service": "api", "env": "prod"},
		}
		store.StoreJob(job)

		// Simulate placement
		for j := 0; j < 3; j++ {
			agentID := fmt.Sprintf("agent-%d", (i*3+j)%1000)
			leader.do(func(s *leaderState) {
				if s.placed[agentID] == nil {
					s.placed[agentID] = make(map[string]int)
				}
				s.placed[agentID][jobName]++
			})
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Read operations
		_ = leader.GetAgents()
		_ = leader.GetJobs()
		name := fmt.Sprintf("job-%d", i%10000)
		_ = store.GetJob(name)
	}

	b.Logf("State size: 1000 agents, 10k jobs, 30k placed")
}

# Performance Benchmarks

Comprehensive performance tests for hop's critical components.

## Running Benchmarks

```bash
# Run all benchmarks
go test -bench=. -benchmem ./internal/...

# Run specific component
go test -bench=. -benchmem ./internal/leader
go test -bench=. -benchmem ./internal/agent
go test -bench=. -benchmem ./internal/api

# Run with CPU profiling
go test -bench=BenchmarkDispatchJob -cpuprofile=cpu.prof ./internal/leader
go tool pprof cpu.prof

# Run with memory profiling
go test -bench=BenchmarkGetJobs -memprofile=mem.prof ./internal/agent
go tool pprof mem.prof

# Compare before/after (save baseline first)
go test -bench=. -benchmem ./internal/leader > old.txt
# Make changes...
go test -bench=. -benchmem ./internal/leader > new.txt
benchstat old.txt new.txt
```

## Latest Results

Measured on Apple M4 Pro (14 cores, 48GB RAM), Go 1.24.3.

### Agent Benchmarks (`internal/agent/benchmark_test.go`)

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|-----------|---------|-------|------|-----------|
| TaskCreation | 1,533,097 | 748 | 941 | 11 |
| GetJobs (1k) | 139,086 | 8,390 | 8,360 | 4 |
| SyncJobs (100) | 913,783 | 1,352 | 64 | 1 |
| CapacityCheck | 1,662,961 | 722 | 168 | 3 |
| StateQuery | 3,520,426 | 339 | 144 | 2 |
| ConcurrentStateAccess | 1,823,829 | 677 | 184 | 5 |

### Leader Benchmarks (`internal/leader/benchmark_test.go`)

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|-----------|---------|-------|------|-----------|
| DispatchJob | 3,753,243 | 375 | 366 | 5 |
| Heartbeat (100 agents) | 1,657,237 | 735 | 232 | 6 |
| GetAgents (1k) | 144,132 | 8,382 | 8,360 | 4 |
| FindJobByName (10k) | 1,885,209 | 630 | 199 | 5 |
| RoundRobinSelection (100) | 2,760,856 | 430 | 152 | 3 |
| ConcurrentHeartbeats (1k) | 1,299,387 | 916 | 243 | 6 |
| PlacedUpdate | 4,830,908 | 248 | 70 | 3 |

### Leader Stress Benchmarks (`internal/leader/stress_test.go`)

**Agent retrieval at scale:**

| Agents | ops/sec | ns/op | B/op |
|--------|---------|-------|------|
| 100 | 963,338 | 1,212 | 1,064 |
| 1,000 | 138,171 | 8,561 | 8,360 |
| 10,000 | 16,786 | 71,756 | 82,089 |

**Job retrieval at scale:**

| Jobs | ops/sec | ns/op | B/op |
|------|---------|-------|------|
| 100 | 2,164,993 | 556 | 896 |
| 1,000 | 190,893 | 6,232 | 8,192 |
| 10,000 | 18,963 | 63,683 | 81,921 |

**Job lookup by name (O(1) name→ID index):**

| Jobs | ops/sec | ns/op | B/op |
|------|---------|-------|------|
| 100 | 2,187,598 | 554 | 191 |
| 1,000 | 2,069,329 | 578 | 198 |
| 10,000 | 1,906,104 | 625 | 199 |
| 100,000 | 1,742,995 | 680 | 207 |

**Heartbeat throughput at scale:**

| Agents | ops/sec | heartbeats/sec |
|--------|---------|----------------|
| 10 | 1,795,068 | 1,500,054 |
| 100 | 1,803,712 | 1,497,100 |
| 1,000 | 1,704,639 | 1,434,369 |

**Placement lookup at scale:**

| Scale | ops/sec | ns/op |
|-------|---------|-------|
| 10A / 100J / 3I | 1,453,537 | 829 |
| 100A / 1kJ / 5I | 380,205 | 3,134 |
| 1kA / 10kJ / 1I | 72,043 | 16,449 |

**Memory footprint (1k agents, 10k jobs, 30k placed):**
- 16,854 ops/sec, 71,096 ns/op, 90,501 B/op, 11 allocs/op

### API Benchmarks (`internal/api/benchmark_test.go`)

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|-----------|---------|-------|------|-----------|
| GetAgentsEndpoint (100) | 39,637 | 29,791 | 19,211 | 114 |
| GetJobsEndpoint (1k) | 7,250 | 162,048 | 67,582 | 11 |
| PostJobEndpoint | 129,594 | 7,739 | 9,297 | 54 |
| HeartbeatEndpoint | 288,327 | 4,145 | 9,065 | 56 |
| StatusEndpoint | 427,372 | 2,807 | 2,322 | 32 |
| ConcurrentRequests | 269,851 | 4,784 | 7,405 | 31 |
| JSONEncoding (100 agents) | 50,179 | 23,669 | 17,142 | 102 |
| JSONDecoding | 988,722 | 1,177 | 1,216 | 20 |

**Note:** PostJobEndpoint defers dispatch during settle period (stores job only). StatusEndpoint uses placed data from leader state (no HTTP calls to agents).

## Benchmark Descriptions

### Agent

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkTaskCreation` | Task creation and state tracking overhead |
| `BenchmarkGetJobs` | Job list retrieval with 1000 jobs |
| `BenchmarkSyncJobs` | Job synchronization overhead (100 jobs) |
| `BenchmarkCapacityCheck` | Capacity checking with running tasks |
| `BenchmarkStateQuery` | State query via ops channel |
| `BenchmarkConcurrentStateAccess` | Concurrent state access (read-only) |

### Leader

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkDispatchJob` | Job store throughput (mock dispatch) |
| `BenchmarkHeartbeat` | Heartbeat processing with 100 agents |
| `BenchmarkGetAgents` | Agent list retrieval with 1000 agents |
| `BenchmarkFindJobByName` | Job lookup with 10k jobs (O(1) name→ID index) |
| `BenchmarkRoundRobinSelection` | Round-robin agent selection with 100 agents (cached sorted list) |
| `BenchmarkConcurrentHeartbeats` | Concurrent heartbeat handling with 1000 agents |
| `BenchmarkPlacedUpdate` | Placement tracking overhead |

### Leader Stress

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkMassiveAgents` | Agent retrieval at 100/1k/10k agents |
| `BenchmarkMassiveJobs` | Job retrieval at 100/1k/10k jobs |
| `BenchmarkJobLookupScale` | Job lookup at 100/1k/10k/100k jobs (O(1) name→ID index) |
| `BenchmarkHeartbeatScale` | Heartbeat throughput at 10/100/1k agents |
| `BenchmarkPlacementScale` | Placement lookup at different scales |
| `BenchmarkMemoryFootprint` | Read ops with 1k agents + 10k jobs + 30k placed |

### API

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkGetAgentsEndpoint` | GET /v1/agents throughput (100 agents) |
| `BenchmarkGetJobsEndpoint` | GET /v1/jobs throughput (1k jobs) |
| `BenchmarkPostJobEndpoint` | POST /v1/jobs throughput (mock dispatch) |
| `BenchmarkHeartbeatEndpoint` | POST /v1/heartbeat throughput |
| `BenchmarkStatusEndpoint` | GET /v1/status throughput (placed-based, no HTTP calls) |
| `BenchmarkConcurrentRequests` | Concurrent mixed API requests |
| `BenchmarkJSONEncoding` | JSON serialization overhead (100 agents) |
| `BenchmarkJSONDecoding` | JSON deserialization overhead |

## Performance Targets

### Latency (p99)

| Operation | Target | Acceptable |
|-----------|--------|------------|
| Job dispatch | <10ms | <50ms |
| Heartbeat | <1ms | <5ms |
| API requests | <10ms | <50ms |
| State queries | <100us | <1ms |

### Throughput

| Component | Target | Scale |
|-----------|--------|-------|
| Leader | 10k jobs/sec | Horizontal (multiple clusters) |
| Agent | 1k tasks/node | Vertical (bigger nodes) |
| API | 50k req/sec | Horizontal (load balancer) |

## Profiling

### CPU Profile

```bash
go test -bench=BenchmarkDispatchJob -cpuprofile=cpu.prof -benchtime=30s ./internal/leader
go tool pprof -http=:8080 cpu.prof
```

### Memory Profile

```bash
go test -bench=BenchmarkTaskCreation -memprofile=mem.prof ./internal/agent
go tool pprof -alloc_space mem.prof
```

### Trace

```bash
go test -bench=BenchmarkConcurrentHeartbeats -trace=trace.out ./internal/leader
go tool trace trace.out
```

## Known Bottlenecks

1. **JSON serialization**: High allocation rate (102 allocs for 100 agents)
   - Fix: Use sync.Pool for buffers
   - Impact: Moderate at >10k req/sec

# Performance Characteristics

Real-world performance measurements on Apple M4 Pro (14 cores, 48GB RAM), Go 1.24.3.

## Operation Throughput

| Operation | Throughput | Latency | Notes |
|-----------|-----------|---------|-------|
| Task creation | 1.5M/sec | 748 ns | Agent state ops channel |
| State query | 3.5M/sec | 339 ns | Channel-based, very fast |
| Placed update | 4.8M/sec | 248 ns | Fire-and-forget via ops channel |
| Job sync (100 jobs) | 914k/sec | 1.4 us | Efficient sync path |
| Heartbeat processing | 1.7M/sec | 735 ns | 100 agents |
| Concurrent heartbeats | 1.3M/sec | 916 ns | 1000 agents, parallel |
| Job list (1k jobs) | 139k/sec | 8.4 us | Copy + return |
| Capacity check | 1.7M/sec | 722 ns | With running task scan |
| Job dispatch (store) | 3.8M/sec | 375 ns | Mock dispatch |
| Round-robin selection | 2.8M/sec | 430 ns | Cached sorted agents |
| Job lookup (100 jobs) | 2.2M/sec | 554 ns | O(1) name→ID index |
| Job lookup (1k jobs) | 2.1M/sec | 578 ns | O(1) name→ID index |
| Job lookup (10k jobs) | 1.9M/sec | 625 ns | O(1) name→ID index |
| Job lookup (100k jobs) | 1.7M/sec | 680 ns | O(1) name→ID index |
| JSON decode | 989k/sec | 1.2 us | Fast |
| JSON encode (100 agents) | 50k/sec | 23.7 us | 102 allocs |
| GET /v1/agents (100) | 40k/sec | 30 us | Full HTTP roundtrip |
| GET /v1/jobs (1k) | 7.3k/sec | 162 us | Full HTTP roundtrip |
| POST /v1/jobs | 130k/sec | 7.7 us | Deferred dispatch (settle) |
| POST /v1/heartbeat | 288k/sec | 4.1 us | Full HTTP roundtrip |
| GET /v1/status | 427k/sec | 2.8 us | Placed-based (no HTTP calls) |

**Key Insights:**
- **State ops are fast** — channel-based model handles >3.5M ops/sec
- **Heartbeat scales well** — 1.3M heartbeats/sec even with 1000 agents
- **Round-robin is fast** — cached sorted agents: 2.8M selections/sec
- **Job lookup is O(1)** — name→ID index in leader: 1.7M lookups/sec even at 100k jobs
- **Status endpoint is fast** — uses placed data from leader state, no HTTP calls to agents
- **JSON encoding is allocation-heavy** — 102 allocs for 100 agents

## Heartbeat Scale

Heartbeat throughput stays remarkably consistent as agent count grows:

| Agents | Throughput | Latency |
|--------|-----------|---------|
| 10 | 1.5M/sec | 667 ns |
| 100 | 1.5M/sec | 668 ns |
| 1,000 | 1.4M/sec | 697 ns |

Only ~4% throughput drop going from 10 to 1000 agents. The channel-based state model handles concurrent heartbeats well.

## Tested Scale Limits

### Cluster Capacity

| Agents | Jobs | Tasks | Memory | Status |
|--------|------|-------|--------|--------|
| 100 | 1,000 | 3,000 | ~50 MB | Excellent |
| 1,000 | 10,000 | 30,000 | ~500 MB | Good |
| 10,000 | 100,000 | 300,000 | ~5 GB | High memory |

**Recommended limits for production:**
- **Agents:** 1,000 max per leader
- **Jobs:** 10,000 max per cluster
- **Tasks:** 50,000 max

Above these limits, use multiple clusters (shard by region/environment).

### Memory Footprint

Per-entity memory usage (approximate):

| Entity | Size | 1,000 entities | 10k entities |
|--------|------|----------------|--------------|
| Agent | ~200 bytes | 200 KB | 2 MB |
| Job | ~500 bytes | 500 KB | 5 MB |
| Task | ~300 bytes | 300 KB | 3 MB |
| Placement entry | ~50 bytes | 50 KB | 500 KB |

**Example cluster (100 agents, 1k jobs, 3k tasks):**
- State data: ~1.5 MB
- Go runtime + buffers: ~50 MB
- **Total: ~50-60 MB per leader**

## Remaining Bottlenecks

### 1. JSON Encoding Allocations

**Current:** 102 allocs for encoding 100 agents (23.7 us).

**Fix:** Use `sync.Pool` for JSON encoder buffers.

## Optimizations Applied

### Cached Sorted Agents

`nextAgent()` previously built a new slice from map + sorted it on every call (O(n log n)). Now uses a pre-sorted cache that's rebuilt only when agents change.

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Latency | 5,920 ns | 430 ns | **14x faster** |
| Allocations | 13 allocs | 3 allocs | **77% fewer** |
| Memory | 2,376 B | 152 B | **94% less** |

### State Channel Buffer

Increased from 64 to 256 entries in both leader and agent. Reduces contention under high load (>1000 ops/sec).

### Name→ID Index in Leader

`FindJobByName` uses O(1) map lookup via `leaderState.nameToID` instead of scanning all jobs.

| Jobs | Before | After | Improvement |
|------|--------|-------|-------------|
| 100 | 618 ns | 554 ns | **1.1x** |
| 1,000 | 7,073 ns | 578 ns | **12x faster** |
| 10,000 | 72,572 ns | 625 ns | **116x faster** |
| 100,000 | 581,574 ns | 680 ns | **855x faster** |

Index maintained by DispatchJob, DeleteJob, UpdateJob, and SyncJobs. Agent stores only job IDs — no name-based indexing at the agent level.

### Status Endpoint Optimization

`GET /v1/status` previously called `GetClusterStatus()` which made N HTTP requests to all agents to fetch tasks. Now uses `placed` data already tracked in leader state from heartbeats.

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Latency | 1,323,729 ns | 2,807 ns | **472x faster** |
| Throughput | 2.1k/sec | 427k/sec | **203x more** |
| Allocations | 1,197 allocs | 32 allocs | **97% fewer** |
| Memory | 166,546 B | 2,322 B | **99% less** |

Also eliminated `GetClusterStatus` from `reconcileJobs` hot path. Task details available via per-job status `GET /v1/jobs/{name}/status` which only queries agents with that job placed.

## Real-World Performance

**Small cluster (10 nodes, 50 jobs, 150 tasks):**
- Heartbeat latency: <1 ms
- Job dispatch: <10 ms
- API response: <5 ms
- Memory usage: ~50 MB
- **Status:** Excellent

**Medium cluster (100 nodes, 500 jobs, 1500 tasks):**
- Heartbeat latency: <2 ms
- Job dispatch: <50 ms
- API response: <20 ms
- Memory usage: ~100 MB
- **Status:** Good

**Large cluster (1000 nodes, 5000 jobs, 15k tasks):**
- Heartbeat latency: <10 ms
- Job dispatch: <200 ms
- API response: <100 ms
- Memory usage: ~500 MB
- **Status:** Acceptable, consider optimizations

**Very large (10k nodes, 50k jobs, 150k tasks):**
- Heartbeat latency: >100 ms (state channel saturation)
- Job dispatch: >1s
- Memory usage: ~5 GB
- **Status:** Not recommended, use multiple clusters

## Stress Testing

```bash
# Benchmark at different scales
go test -bench=BenchmarkJobLookupScale -benchmem ./internal/leader

# Full benchmark suite
go test -bench=. -benchmem ./internal/... > results.txt

# Memory footprint
go test -bench=BenchmarkMemoryFootprint -benchmem ./internal/leader
```

## Optimization Roadmap

| Symptom | Cause | Fix |
|---------|-------|-----|
| High memory (>1GB) | Too many jobs/agents | Split into multiple clusters |

## Horizontal Scaling

Instead of optimizing a single leader, **scale horizontally:**

```
Cluster 1 (region=us-east): 100 agents, 500 jobs
Cluster 2 (region=us-west): 100 agents, 500 jobs
Total capacity: 200 agents, 1000 jobs
```

**Benefits:** Simpler codebase, fault isolation, better performance per leader.
**Trade-offs:** Manual orchestration across clusters, no automatic cross-cluster job migration.

# Performance Benchmarks

Comprehensive performance tests for hopdns's critical components.

## Running Benchmarks

```bash
# Run all benchmarks
go test -bench=. -benchmem ./internal/...

# Run with CPU profiling
go test -bench=BenchmarkHandleQuery -cpuprofile=cpu.prof ./internal/dns
go tool pprof cpu.prof

# Run with memory profiling
go test -bench=BenchmarkCacheGetAll -memprofile=mem.prof ./internal/dns
go tool pprof mem.prof

# Compare before/after (save baseline first)
go test -bench=. -benchmem ./internal/dns > old.txt
# Make changes...
go test -bench=. -benchmem ./internal/dns > new.txt
benchstat old.txt new.txt
```

## Latest Results

Measured on Apple M4 Pro (14 cores, 48GB RAM), Go 1.24.3.

### Cache Benchmarks

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|-----------|---------|-------|------|-----------|
| CacheGet (50 jobs) | 109,426,808 | 10 | 0 | 0 |
| ConcurrentCacheGet | 7,667,800 | 135 | 7 | 1 |
| CacheSet | 68,070,140 | 17 | 0 | 0 |
| CacheUpdate | 199,816,362 | 6 | 0 | 0 |

**GetAll at scale (deep copy):**

| Jobs | ops/sec | ns/op | B/op | allocs/op |
|------|---------|-------|------|-----------|
| 10 | 3,382,946 | 352 | 1,144 | 5 |
| 50 | 655,605 | 1,821 | 5,304 | 9 |
| 200 | 158,324 | 7,613 | 21,624 | 13 |

### DNS Server Benchmarks

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|-----------|---------|-------|------|-----------|
| HandleQuery (3 IPs) | 8,229,896 | 144 | 480 | 9 |
| ConcurrentHandleQuery | 2,701,369 | 442 | 402 | 10 |

**HandleQuery at scale (IPs per job):**

| IPs | ops/sec | ns/op | B/op | allocs/op |
|-----|---------|-------|------|-----------|
| 1 | 14,113,302 | 85 | 256 | 5 |
| 5 | 5,983,718 | 200 | 736 | 12 |
| 10 | 3,928,122 | 307 | 1,312 | 18 |

### Watcher Benchmarks

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|-----------|---------|-------|------|-----------|
| ExtractIP | 9,357,770 | 128 | 160 | 2 |
| ParseJobFromData | 5,726,674 | 208 | 266 | 7 |

## Benchmark Descriptions

### Cache

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkCacheGet` | RLock + map lookup with 50 jobs, zero allocs |
| `BenchmarkConcurrentCacheGet` | RWMutex read contention under parallel DNS queries |
| `BenchmarkCacheSet` | Write lock + map write (single job update) |
| `BenchmarkCacheUpdate` | Atomic full map replacement (watcher refresh) |
| `BenchmarkCacheGetAll` | Deep copy at 10/50/200 jobs |

### DNS Server

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkHandleQuery` | Full query path: parse question, cache lookup, build A records |
| `BenchmarkHandleQueryScale` | Query scaling with 1/5/10 IPs per job |
| `BenchmarkConcurrentHandleQuery` | Parallel DNS queries across 20 jobs |

### Watcher

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkExtractIP` | URL parsing to extract IP from agent endpoint |
| `BenchmarkParseJobFromData` | SSE data line JSON parsing |

## Performance Targets

### Latency (p99)

| Operation | Target | Acceptable |
|-----------|--------|------------|
| Cache lookup | <50ns | <200ns |
| DNS query (1 IP) | <200ns | <1us |
| DNS query (10 IPs) | <500ns | <2us |
| ExtractIP | <200ns | <500ns |

### Throughput

| Component | Target | Scale |
|-----------|--------|-------|
| Cache reads | 10M reads/sec | Zero-alloc hot path |
| DNS queries | 5M queries/sec | Per-instance (mock writer) |
| Cache updates | 100M updates/sec | Pointer swap, zero-alloc |

## Key Insights

- **Cache Get is zero-alloc at 10ns** — RLock + map lookup, no copy needed for reads
- **Cache Update is 6ns** — just a pointer swap under write lock, zero allocs regardless of job count
- **HandleQuery scales linearly with IPs** — 85ns for 1 IP, 307ns for 10 IPs (~25ns per additional A record)
- **Concurrent queries are fast** — 442ns under parallel load (3x single-threaded), RWMutex handles read-heavy workload well
- **ParseJobFromData allocates 7 times** — JSON unmarshal overhead, but 208ns is acceptable for SSE events
- **GetAll is the most expensive operation** — deep copy scales linearly, 7.6us for 200 jobs

## Known Bottlenecks

No significant bottlenecks. All hot paths (cache read, DNS query) are sub-microsecond with minimal allocations.

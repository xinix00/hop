# Performance Benchmarks

Comprehensive performance tests for hoplb's critical components.

## Running Benchmarks

```bash
# Run all benchmarks
go test -bench=. -benchmem ./internal/...

# Run specific component
go test -bench=. -benchmem ./internal/lb
go test -bench=. -benchmem ./internal/metrics

# Run with CPU profiling
go test -bench=BenchmarkProxyHandler -cpuprofile=cpu.prof ./internal/lb
go tool pprof cpu.prof

# Run with memory profiling
go test -bench=BenchmarkExporter -memprofile=mem.prof ./internal/metrics
go tool pprof mem.prof

# Compare before/after (save baseline first)
go test -bench=. -benchmem ./internal/lb > old.txt
# Make changes...
go test -bench=. -benchmem ./internal/lb > new.txt
benchstat old.txt new.txt
```

## Latest Results

Measured on Apple M4 Pro (14 cores, 48GB RAM), Go 1.24.3.

### Load Balancer Benchmarks (`internal/lb/benchmark_test.go`)

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|-----------|---------|-------|------|-----------|
| RouteMatch (100 routes) | 61,982,324 | 20 | 0 | 0 |
| GetHealthyBackend (10) | 735,981,096 | 1.6 | 0 | 0 |
| ConcurrentRouteMatch | 8,027,778 | 159 | 24 | 1 |
| BuildRoutes (100 jobs) | 20,426 | 58,686 | 52,793 | 2,018 |
| ProxyHandler | 31,776 | 35,983 | 44,917 | 88 |
| ParseJobFromData | 3,312,985 | 359 | 312 | 7 |

**Route matching at scale (O(1) wildcard lookup):**

| Routes | ops/sec | ns/op | B/op |
|--------|---------|-------|------|
| 10 | 49,098,915 | 24 | 0 |
| 100 | 48,284,151 | 24 | 0 |
| 1,000 | 46,947,897 | 25 | 0 |

### Metrics Benchmarks (`internal/metrics/benchmark_test.go`)

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|-----------|---------|-------|------|-----------|
| RecordRequest | 22,373,196 | 54 | 32 | 0 |
| ConcurrentRecordRequest | 2,974,650 | 396 | 72 | 2 |
| Percentile (10k samples) | 49,987,329 | 23 | 8 | 1 |
| Exporter (10 domains) | 10,000 | 105,709 | 197,435 | 1,053 |

**Percentile calculation at scale (O(1) cached reads):**

| Samples | ops/sec | ns/op | B/op |
|---------|---------|-------|------|
| 100 | 51,723,765 | 23 | 8 |
| 1,000 | 52,187,622 | 23 | 8 |
| 10,000 | 52,182,704 | 23 | 8 |

**Domain listing at scale:**

| Domains | ops/sec | ns/op | B/op |
|---------|---------|-------|------|
| 10 | 8,393,953 | 158 | 160 |
| 100 | 442,442 | 2,631 | 1,792 |
| 1,000 | 22,468 | 53,574 | 16,384 |

## Benchmark Descriptions

### Load Balancer

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkRouteMatch` | Route matching with 100 routes (70 exact + 30 wildcard), zero allocs |
| `BenchmarkRouteMatchScale` | Route matching at 10/100/1000 routes (O(1) wildcard lookup) |
| `BenchmarkGetHealthyBackend` | Atomic round-robin backend selection with 10 backends |
| `BenchmarkConcurrentRouteMatch` | RWMutex read contention under parallel load |
| `BenchmarkBuildRoutes` | Route table reconstruction from watcher cache (100 jobs, 10 agents, 5 tasks/job) |
| `BenchmarkProxyHandler` | Full ServeHTTP path including reverse proxy to mock backend |
| `BenchmarkParseJobFromData` | SSE event line JSON parsing |

### Metrics

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkRecordRequest` | Single-threaded request recording (mutex + map + slice append) |
| `BenchmarkConcurrentRecordRequest` | Parallel request recording (mutex write contention) |
| `BenchmarkPercentile` | p99 calculation with 10k samples (cached sorted snapshot) |
| `BenchmarkPercentileScale` | Percentile scaling at 100/1k/10k samples (O(1) with cache) |
| `BenchmarkExporter` | Full /metrics Prometheus output (10 domains x 3 backends x 1k samples) |
| `BenchmarkAllDomains` | Domain listing at 10/100/1000 domains |

## Performance Targets

### Latency (p99)

| Operation | Target | Acceptable |
|-----------|--------|------------|
| Route match | <1us | <10us |
| Backend selection | <10ns | <100ns |
| Proxy handler | <50ms | <100ms |
| Metrics recording | <1us | <10us |
| /metrics export | <5ms | <50ms |

### Throughput

| Component | Target | Scale |
|-----------|--------|-------|
| Route matching | 1M req/sec | Zero-alloc hot path |
| Proxy handler | 30k req/sec | Per-instance |
| Metrics recording | 10M rec/sec | Lock-free reads |

## Key Insights

- **Route matching is O(1) and zero-alloc** — exact map lookup + wildcard suffix lookup, ~20 ns/op
- **Wildcard matching scales perfectly** — O(1) regardless of route count (24ns at 10, 25ns at 1000)
- **Backend selection is sub-2ns** — atomic round-robin, no allocation
- **RecordRequest is fast** — 54 ns single-threaded, 396 ns under contention (7x slower)
- **Percentile reads are O(1)** — cached sorted snapshot, 23ns regardless of sample count (was 95us for 10k)
- **Exporter is fast** — batch percentiles + sorted cache = 106us for 10 domains x 3 backends (was 1.3ms)

## Optimizations Applied

### O(1) Wildcard Route Matching

`RouteTable.Match()` previously did a linear scan of all routes for wildcard matching. Now uses a separate `wildcards` map with O(1) suffix lookup (`"*" + host[firstDot:]`).

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| 10 routes | 72 ns | 24 ns | **3x faster** |
| 100 routes | 309 ns | 24 ns | **13x faster** |
| 1,000 routes | 3,468 ns | 25 ns | **139x faster** |

### Batch Percentile Calculation + Sorted Cache

Exporter previously called `Percentile()` 5 times per domain/backend pair, each doing a full copy+sort. Two optimizations applied:

1. **Batch method** (`Percentiles()`): one copy+sort extracts all quantiles
2. **Sorted cache**: sorted snapshot is cached per domain/backend, invalidated on write. Repeated reads (Prometheus scrapes) are O(1) with zero sort overhead.

| Metric | Before | After (batch) | After (batch+cache) | Total improvement |
|--------|--------|---------------|---------------------|-------------------|
| Exporter latency | 1,273,000 ns | 356,411 ns | 105,709 ns | **12x faster** |
| Exporter memory | 1,427,149 B | 443,539 B | 197,435 B | **86% less** |
| Exporter allocs | 1,177 | 1,084 | 1,053 | **11% fewer** |
| Percentile (10k) | 96,552 ns | 96,552 ns | 23 ns | **4,200x faster** |
| Percentile memory | 81,929 B | 81,929 B | 8 B | **99.99% less** |

### BuildRoutes Allocation Reduction

Pre-sized route map and replaced `fmt.Sprintf("%s:%d", ...)` with `strconv.Itoa` + string concatenation.

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Latency | 71,275 ns | 58,686 ns | **18% faster** |
| Memory | 55,796 B | 52,793 B | **5% less** |
| Allocations | 2,511 | 2,018 | **20% fewer** |

## Known Bottlenecks

No significant bottlenecks remaining. All hot paths are O(1) with minimal allocations.

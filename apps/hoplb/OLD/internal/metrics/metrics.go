package metrics

import (
	"sort"
	"sync"
	"time"
)

// Metrics tracks HTTP request statistics for Prometheus
type Metrics struct {
	mu sync.RWMutex

	// Request counters: domain -> backend -> status code -> count
	requests map[string]map[string]map[int]int64

	// Latency samples: domain -> backend -> []duration (for percentiles)
	latencySamples map[string]map[string][]float64

	// Latency sums: domain -> backend -> total seconds
	latencySum map[string]map[string]float64

	// Cached sorted snapshots: domain -> backend -> sorted []float64
	// Invalidated on write, reused on read (O(1) percentile lookups between writes)
	sortedCache map[string]map[string][]float64

	// Max samples to keep per domain/backend (rolling window)
	maxSamples int
}

// New creates a new metrics collector
func New() *Metrics {
	return &Metrics{
		requests:       make(map[string]map[string]map[int]int64),
		latencySamples: make(map[string]map[string][]float64),
		latencySum:     make(map[string]map[string]float64),
		sortedCache:    make(map[string]map[string][]float64),
		maxSamples:     10000, // Keep last 10k samples for percentiles
	}
}

// RecordRequest records a request with its status code and duration
func (m *Metrics) RecordRequest(domain, backend string, statusCode int, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Initialize nested maps if needed
	if m.requests[domain] == nil {
		m.requests[domain] = make(map[string]map[int]int64)
	}
	if m.requests[domain][backend] == nil {
		m.requests[domain][backend] = make(map[int]int64)
	}

	// Increment counter
	m.requests[domain][backend][statusCode]++

	// Record latency
	if m.latencySamples[domain] == nil {
		m.latencySamples[domain] = make(map[string][]float64)
	}
	if m.latencySum[domain] == nil {
		m.latencySum[domain] = make(map[string]float64)
	}
	m.latencySum[domain][backend] += duration.Seconds()

	samples := m.latencySamples[domain][backend]
	samples = append(samples, duration.Seconds())

	// Keep only recent samples (rolling window)
	if len(samples) > m.maxSamples {
		samples = samples[len(samples)-m.maxSamples:]
	}

	m.latencySamples[domain][backend] = samples

	// Invalidate sorted cache for this domain/backend
	if m.sortedCache[domain] != nil {
		delete(m.sortedCache[domain], backend)
	}
}

// RequestCounts returns all request counts
// Returns: domain -> backend -> status code -> count
func (m *Metrics) RequestCounts() map[string]map[string]map[int]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Deep copy to avoid race conditions
	result := make(map[string]map[string]map[int]int64)
	for domain, backends := range m.requests {
		result[domain] = make(map[string]map[int]int64)
		for backend, codes := range backends {
			result[domain][backend] = make(map[int]int64)
			for code, count := range codes {
				result[domain][backend][code] = count
			}
		}
	}
	return result
}

// Percentile calculates the given percentile (0.0-1.0) from samples
func (m *Metrics) Percentile(domain, backend string, p float64) float64 {
	results := m.Percentiles(domain, backend, []float64{p})
	return results[0]
}

// Percentiles calculates multiple percentiles from a single copy+sort.
// Uses a sorted cache that persists between calls — repeated reads (e.g.,
// Prometheus scrapes) are O(1) with zero allocations until new samples arrive.
func (m *Metrics) Percentiles(domain, backend string, ps []float64) []float64 {
	results := make([]float64, len(ps))

	// Try read lock first for cached path
	m.mu.RLock()

	samples, ok := m.latencySamples[domain][backend]
	if !ok || len(samples) == 0 {
		m.mu.RUnlock()
		return results
	}

	// Check if we have a valid cached sort
	sorted := m.sortedCache[domain][backend]
	if sorted != nil && len(sorted) == len(samples) {
		// Cache hit — use cached sorted snapshot
		n := len(sorted)
		for i, p := range ps {
			idx := int(float64(n-1) * p)
			if idx < 0 {
				idx = 0
			}
			if idx >= n {
				idx = n - 1
			}
			results[i] = sorted[idx]
		}
		m.mu.RUnlock()
		return results
	}
	m.mu.RUnlock()

	// Cache miss — upgrade to write lock, copy+sort, cache result
	m.mu.Lock()
	defer m.mu.Unlock()

	samples = m.latencySamples[domain][backend]
	if len(samples) == 0 {
		return results
	}

	// Double-check: another goroutine may have populated the cache
	sorted = m.sortedCache[domain][backend]
	if sorted == nil || len(sorted) != len(samples) {
		sorted = make([]float64, len(samples))
		copy(sorted, samples)
		sort.Float64s(sorted)

		if m.sortedCache[domain] == nil {
			m.sortedCache[domain] = make(map[string][]float64)
		}
		m.sortedCache[domain][backend] = sorted
	}

	n := len(sorted)
	for i, p := range ps {
		idx := int(float64(n-1) * p)
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		results[i] = sorted[idx]
	}

	return results
}

// AllDomains returns all tracked domains
func (m *Metrics) AllDomains() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	domains := make([]string, 0, len(m.requests))
	for domain := range m.requests {
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	return domains
}

// AllBackends returns all backends for a domain
func (m *Metrics) AllBackends(domain string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	backends := make([]string, 0)
	if domainMap, ok := m.requests[domain]; ok {
		for backend := range domainMap {
			backends = append(backends, backend)
		}
	}
	sort.Strings(backends)
	return backends
}

// LatencySum returns the total latency sum for a domain/backend
func (m *Metrics) LatencySum(domain, backend string) float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if s, ok := m.latencySum[domain]; ok {
		return s[backend]
	}
	return 0
}

// SampleCount returns the number of latency samples for a domain/backend
func (m *Metrics) SampleCount(domain, backend string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if samples, ok := m.latencySamples[domain][backend]; ok {
		return len(samples)
	}
	return 0
}

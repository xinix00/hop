package metrics

import (
	"testing"
	"time"
)

func TestMetricsRecordRequest(t *testing.T) {
	m := New()

	// Record some requests
	m.RecordRequest("api.example.com", "10.0.1.5:8080", 200, 23*time.Millisecond)
	m.RecordRequest("api.example.com", "10.0.1.5:8080", 200, 45*time.Millisecond)
	m.RecordRequest("api.example.com", "10.0.1.5:8080", 500, 100*time.Millisecond)
	m.RecordRequest("api.example.com", "10.0.1.6:8080", 200, 30*time.Millisecond)

	// Check request counts
	counts := m.RequestCounts()
	if counts["api.example.com"]["10.0.1.5:8080"][200] != 2 {
		t.Errorf("Expected 2 requests with 200, got %d", counts["api.example.com"]["10.0.1.5:8080"][200])
	}
	if counts["api.example.com"]["10.0.1.5:8080"][500] != 1 {
		t.Errorf("Expected 1 request with 500, got %d", counts["api.example.com"]["10.0.1.5:8080"][500])
	}

	// Check sample count
	if m.SampleCount("api.example.com", "10.0.1.5:8080") != 3 {
		t.Errorf("Expected 3 samples, got %d", m.SampleCount("api.example.com", "10.0.1.5:8080"))
	}
}

func TestMetricsPercentile(t *testing.T) {
	m := New()

	// Record requests with known latencies
	for i := 1; i <= 100; i++ {
		m.RecordRequest("api.example.com", "10.0.1.5:8080", 200, time.Duration(i)*time.Millisecond)
	}

	// Test percentiles
	p50 := m.Percentile("api.example.com", "10.0.1.5:8080", 0.5)
	if p50 < 0.050 || p50 > 0.051 {
		t.Errorf("p50 should be ~0.050s, got %.6f", p50)
	}

	p99 := m.Percentile("api.example.com", "10.0.1.5:8080", 0.99)
	if p99 < 0.099 || p99 > 0.100 {
		t.Errorf("p99 should be ~0.099s, got %.6f", p99)
	}
}

func TestMetricsRollingWindow(t *testing.T) {
	m := New()
	m.maxSamples = 10 // Set low for testing

	// Record more than maxSamples
	for i := 0; i < 20; i++ {
		m.RecordRequest("api.example.com", "10.0.1.5:8080", 200, time.Duration(i)*time.Millisecond)
	}

	// Should only keep last 10
	count := m.SampleCount("api.example.com", "10.0.1.5:8080")
	if count != 10 {
		t.Errorf("Expected 10 samples (rolling window), got %d", count)
	}
}

func TestMetricsMultipleDomains(t *testing.T) {
	m := New()

	m.RecordRequest("api.example.com", "10.0.1.5:8080", 200, 10*time.Millisecond)
	m.RecordRequest("web.example.com", "10.0.1.6:8080", 200, 20*time.Millisecond)
	m.RecordRequest("admin.example.com", "10.0.1.7:8080", 200, 30*time.Millisecond)

	domains := m.AllDomains()
	if len(domains) != 3 {
		t.Errorf("Expected 3 domains, got %d", len(domains))
	}

	// Check sorted order
	if domains[0] != "admin.example.com" || domains[1] != "api.example.com" || domains[2] != "web.example.com" {
		t.Errorf("Domains should be sorted alphabetically, got %v", domains)
	}
}

func TestMetricsNoSamples(t *testing.T) {
	m := New()

	// Percentile for non-existent domain/backend should return 0
	p50 := m.Percentile("nonexistent.com", "10.0.1.5:8080", 0.5)
	if p50 != 0 {
		t.Errorf("Expected 0 for non-existent domain, got %.6f", p50)
	}

	// Sample count should be 0
	count := m.SampleCount("nonexistent.com", "10.0.1.5:8080")
	if count != 0 {
		t.Errorf("Expected 0 samples, got %d", count)
	}
}

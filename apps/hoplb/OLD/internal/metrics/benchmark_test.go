package metrics

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"
)

func BenchmarkRecordRequest(b *testing.B) {
	m := New()

	domains := []string{"api.example.com", "web.example.com", "admin.example.com"}
	backends := []string{"10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080"}
	codes := []int{200, 200, 200, 200, 500, 502}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		m.RecordRequest(
			domains[i%len(domains)],
			backends[i%len(backends)],
			codes[i%len(codes)],
			time.Duration(i%100)*time.Millisecond,
		)
	}
}

func BenchmarkConcurrentRecordRequest(b *testing.B) {
	m := New()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.RecordRequest(
				fmt.Sprintf("domain-%d.example.com", i%10),
				fmt.Sprintf("10.0.0.%d:8080", i%5),
				200,
				time.Duration(i%100)*time.Millisecond,
			)
			i++
		}
	})
}

func BenchmarkPercentile(b *testing.B) {
	m := New()

	for i := 0; i < 10000; i++ {
		m.RecordRequest("api.example.com", "10.0.0.1:8080", 200,
			time.Duration(i%500)*time.Millisecond)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		p := m.Percentile("api.example.com", "10.0.0.1:8080", 0.99)
		if p == 0 {
			b.Fatal("expected non-zero percentile")
		}
	}
}

func BenchmarkPercentileScale(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("%d_samples", n), func(b *testing.B) {
			m := New()
			m.maxSamples = n

			for i := 0; i < n; i++ {
				m.RecordRequest("api.example.com", "10.0.0.1:8080", 200,
					time.Duration(i%500)*time.Millisecond)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				p := m.Percentile("api.example.com", "10.0.0.1:8080", 0.99)
				if p == 0 {
					b.Fatal("expected non-zero percentile")
				}
			}
		})
	}
}

func BenchmarkExporter(b *testing.B) {
	m := New()

	for d := 0; d < 10; d++ {
		domain := fmt.Sprintf("service-%d.example.com", d)
		for backend := 0; backend < 3; backend++ {
			addr := fmt.Sprintf("10.0.%d.%d:8080", d, backend)
			for s := 0; s < 1000; s++ {
				code := 200
				if s%20 == 0 {
					code = 500
				}
				m.RecordRequest(domain, addr, code,
					time.Duration(s%500)*time.Millisecond)
			}
		}
	}

	exporter := NewExporter(m)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/metrics", nil)
		w := httptest.NewRecorder()
		exporter.ServeHTTP(w, req)
		if w.Code != 200 {
			b.Fatalf("expected 200, got %d", w.Code)
		}
	}
}

func BenchmarkAllDomains(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("%d_domains", n), func(b *testing.B) {
			m := New()

			for d := 0; d < n; d++ {
				m.RecordRequest(fmt.Sprintf("service-%d.example.com", d),
					"10.0.0.1:8080", 200, time.Millisecond)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				domains := m.AllDomains()
				if len(domains) != n {
					b.Fatalf("expected %d domains, got %d", n, len(domains))
				}
			}
		})
	}
}

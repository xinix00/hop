package lb

import (
	"github.com/xinix00/hoplib"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xinix00/hoplb/internal/metrics"
)

func BenchmarkRouteMatch(b *testing.B) {
	rt := NewRouteTable()
	routes := make(map[string]*Route)

	// 70 exact-match routes
	for i := 0; i < 70; i++ {
		pattern := fmt.Sprintf("api-%d.example.com", i)
		routes[pattern] = &Route{
			Pattern: pattern,
			Backends: []*Backend{
				{Address: fmt.Sprintf("10.0.0.%d:8001", i%255), Healthy: true},
				{Address: fmt.Sprintf("10.0.1.%d:8001", i%255), Healthy: true},
				{Address: fmt.Sprintf("10.0.2.%d:8001", i%255), Healthy: true},
			},
		}
	}

	// 30 wildcard routes
	for i := 0; i < 30; i++ {
		pattern := fmt.Sprintf("*.domain%d.com", i)
		routes[pattern] = &Route{
			Pattern: pattern,
			Backends: []*Backend{
				{Address: fmt.Sprintf("10.1.0.%d:8001", i%255), Healthy: true},
				{Address: fmt.Sprintf("10.1.1.%d:8001", i%255), Healthy: true},
				{Address: fmt.Sprintf("10.1.2.%d:8001", i%255), Healthy: true},
			},
		}
	}

	rt.Update(routes)

	hosts := []string{"api-35.example.com", "app.domain15.com"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		route := rt.Match(hosts[i%2])
		if route == nil {
			b.Fatal("expected route match")
		}
	}
}

func BenchmarkRouteMatchScale(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("%d_routes", n), func(b *testing.B) {
			rt := NewRouteTable()
			routes := make(map[string]*Route)

			for i := 0; i < n/2; i++ {
				pattern := fmt.Sprintf("api-%d.example.com", i)
				routes[pattern] = &Route{
					Pattern:  pattern,
					Backends: []*Backend{{Address: fmt.Sprintf("10.0.0.%d:8001", i%255), Healthy: true}},
				}
			}
			for i := 0; i < n/2; i++ {
				pattern := fmt.Sprintf("*.domain%d.com", i)
				routes[pattern] = &Route{
					Pattern:  pattern,
					Backends: []*Backend{{Address: fmt.Sprintf("10.1.0.%d:8001", i%255), Healthy: true}},
				}
			}

			rt.Update(routes)

			// Wildcard match (worst case — scans all routes)
			host := fmt.Sprintf("app.domain%d.com", n/2-1)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				route := rt.Match(host)
				if route == nil {
					b.Fatal("expected route match")
				}
			}
		})
	}
}

func BenchmarkGetHealthyBackend(b *testing.B) {
	route := &Route{
		Pattern:  "test",
		Backends: make([]*Backend, 10),
	}
	for i := 0; i < 10; i++ {
		route.Backends[i] = &Backend{
			Address: fmt.Sprintf("10.0.0.%d:8080", i),
			Healthy: true,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		backend := route.GetHealthyBackend()
		if backend == nil {
			b.Fatal("expected healthy backend")
		}
	}
}

func BenchmarkConcurrentRouteMatch(b *testing.B) {
	rt := NewRouteTable()
	routes := make(map[string]*Route)

	for i := 0; i < 100; i++ {
		pattern := fmt.Sprintf("api-%d.example.com", i)
		routes[pattern] = &Route{
			Pattern: pattern,
			Backends: []*Backend{
				{Address: fmt.Sprintf("10.0.0.%d:8001", i%255), Healthy: true},
				{Address: fmt.Sprintf("10.0.1.%d:8001", i%255), Healthy: true},
			},
		}
	}

	rt.Update(routes)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			host := fmt.Sprintf("api-%d.example.com", i%100)
			route := rt.Match(host)
			if route == nil {
				b.Fatal("expected route match")
			}
			i++
		}
	})
}

func BenchmarkBuildRoutes(b *testing.B) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(nil)

	rt := NewRouteTable()
	w := &Watcher{
		routeTable: rt,
		agentHosts: make(map[string]string),
		jobs:       make(map[string]*hoplib.Job),
		relevant:   make(map[string]struct{}),
		tasks:      make(map[string]map[string][]*hoplib.Task),
	}

	for i := 0; i < 10; i++ {
		w.agentHosts[fmt.Sprintf("agent-%d", i)] = fmt.Sprintf("10.0.0.%d", i)
	}

	for i := 0; i < 100; i++ {
		jobName := fmt.Sprintf("job-%d", i)
		w.jobs[jobName] = &hoplib.Job{
			ID:   fmt.Sprintf("jobid-%d", i),
			Name: jobName,
			Tags: map[string]string{
				"hoplb-urlprefix": fmt.Sprintf("*.job%d.example.com", i),
				"hoplb-port":     "http",
			},
		}
		w.relevant[jobName] = struct{}{}

		w.tasks[jobName] = make(map[string][]*hoplib.Task)
		for a := 0; a < 5; a++ {
			agentID := fmt.Sprintf("agent-%d", a%10)
			w.tasks[jobName][agentID] = append(w.tasks[jobName][agentID], &hoplib.Task{
				ID:      fmt.Sprintf("task-%d-%d", i, a),
				JobName: jobName,
				State:   "running",
				Ports:   map[string]int{"http": 8080 + a},
			})
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w.buildRoutes()
	}
}

func BenchmarkProxyHandler(b *testing.B) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(nil)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	backendAddr := backend.Listener.Addr().String()

	rt := NewRouteTable()
	rt.Update(map[string]*Route{
		"api.example.com": {
			Pattern:  "api.example.com",
			Backends: []*Backend{{Address: backendAddr, Healthy: true}},
		},
	})

	m := metrics.New()
	proxy := NewProxy(rt, m)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "http://api.example.com/test", nil)
		w := httptest.NewRecorder()
		proxy.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("expected 200, got %d", w.Code)
		}
	}
}

func BenchmarkParseJobFromData(b *testing.B) {
	lines := []string{
		`data: {"name":"my-api","type":"task_started","task_id":"abc123"}`,
		`data: {"name":"nginx-proxy","type":"task_stopped"}`,
		`data: {"name":"redis-cache","type":"state_changed","state":"running"}`,
		`data: {"name":"worker-pool-long-name","type":"task_failed","error":"exit 1"}`,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		job := hoplib.ParseJobFromSSE(lines[i%len(lines)])
		if job == "" {
			b.Fatal("expected job name")
		}
	}
}

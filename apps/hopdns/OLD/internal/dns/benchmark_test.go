package dns

import (
	"fmt"
	"net"
	"testing"

	"github.com/xinix00/hoplib"
	"github.com/miekg/dns"
)

func BenchmarkCacheGet(b *testing.B) {
	cache := NewCache()

	// Populate with 50 jobs across 3 clusters, 3 IPs each
	for i := 0; i < 50; i++ {
		cache.Set("cluster-a", fmt.Sprintf("job-%d", i), []net.IP{
			net.ParseIP(fmt.Sprintf("10.0.0.%d", i%256)),
			net.ParseIP(fmt.Sprintf("10.0.1.%d", i%256)),
			net.ParseIP(fmt.Sprintf("10.0.2.%d", i%256)),
		})
	}

	jobs := make([]string, 50)
	for i := range jobs {
		jobs[i] = fmt.Sprintf("job-%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ips := cache.GetCluster("cluster-a",jobs[i%50])
		if len(ips) != 3 {
			b.Fatalf("expected 3 IPs, got %d", len(ips))
		}
	}
}

func BenchmarkCacheGetClusterSpecific(b *testing.B) {
	cache := NewCache()

	// Same job in 3 clusters with different IPs
	for c := 0; c < 3; c++ {
		cluster := fmt.Sprintf("cluster-%d", c)
		for i := 0; i < 50; i++ {
			cache.Set(cluster, fmt.Sprintf("job-%d", i), []net.IP{
				net.ParseIP(fmt.Sprintf("10.%d.0.%d", c, i%256)),
			})
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ips := cache.GetCluster("cluster-0", fmt.Sprintf("job-%d", i%50))
		if len(ips) != 1 {
			b.Fatalf("expected 1 IP, got %d", len(ips))
		}
	}
}

func BenchmarkConcurrentCacheGet(b *testing.B) {
	cache := NewCache()

	for i := 0; i < 50; i++ {
		cache.Set("cluster-a", fmt.Sprintf("job-%d", i), []net.IP{
			net.ParseIP(fmt.Sprintf("10.0.0.%d", i%256)),
			net.ParseIP(fmt.Sprintf("10.0.1.%d", i%256)),
			net.ParseIP(fmt.Sprintf("10.0.2.%d", i%256)),
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			ips := cache.GetCluster("cluster-a",fmt.Sprintf("job-%d", i%50))
			if len(ips) != 3 {
				b.Fatalf("expected 3 IPs, got %d", len(ips))
			}
			i++
		}
	})
}

func BenchmarkCacheSet(b *testing.B) {
	cache := NewCache()
	ips := []net.IP{
		net.ParseIP("10.0.0.1"),
		net.ParseIP("10.0.0.2"),
		net.ParseIP("10.0.0.3"),
	}

	jobs := make([]string, 50)
	for i := range jobs {
		jobs[i] = fmt.Sprintf("job-%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cache.Set("cluster-a", jobs[i%50], ips)
	}
}

func BenchmarkCacheUpdate(b *testing.B) {
	for _, n := range []int{10, 50, 200} {
		b.Run(fmt.Sprintf("%d_jobs", n), func(b *testing.B) {
			cache := NewCache()

			data := make(map[string][]net.IP, n)
			for i := 0; i < n; i++ {
				data[fmt.Sprintf("job-%d", i)] = []net.IP{
					net.ParseIP(fmt.Sprintf("10.0.0.%d", i%256)),
					net.ParseIP(fmt.Sprintf("10.0.1.%d", i%256)),
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				cache.Update("cluster-a", data)
			}
		})
	}
}

func BenchmarkHandleQuery(b *testing.B) {
	cache := NewCache()
	cache.Set("prod", "myapp", []net.IP{
		net.ParseIP("10.0.0.1"),
		net.ParseIP("10.0.0.2"),
		net.ParseIP("10.0.0.3"),
	})

	server := NewServer(cache, ":0", "internal")

	req := new(dns.Msg)
	req.SetQuestion("myapp.internal.", dns.TypeA)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rw := &mockResponseWriter{}
		server.handleQuery(rw, req)
		if len(rw.msg.Answer) != 3 {
			b.Fatalf("expected 3 answers, got %d", len(rw.msg.Answer))
		}
	}
}

func BenchmarkHandleQueryScale(b *testing.B) {
	for _, n := range []int{1, 5, 10} {
		b.Run(fmt.Sprintf("%d_ips", n), func(b *testing.B) {
			cache := NewCache()
			ips := make([]net.IP, n)
			for i := range ips {
				ips[i] = net.ParseIP(fmt.Sprintf("10.0.0.%d", i+1))
			}
			cache.Set("prod", "myapp", ips)

			server := NewServer(cache, ":0", "internal")
			req := new(dns.Msg)
			req.SetQuestion("myapp.internal.", dns.TypeA)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				rw := &mockResponseWriter{}
				server.handleQuery(rw, req)
				if len(rw.msg.Answer) != n {
					b.Fatalf("expected %d answers, got %d", n, len(rw.msg.Answer))
				}
			}
		})
	}
}

func BenchmarkConcurrentHandleQuery(b *testing.B) {
	cache := NewCache()
	for i := 0; i < 20; i++ {
		cache.Set("prod", fmt.Sprintf("job-%d", i), []net.IP{
			net.ParseIP(fmt.Sprintf("10.0.0.%d", i+1)),
			net.ParseIP(fmt.Sprintf("10.0.1.%d", i+1)),
		})
	}

	server := NewServer(cache, ":0", "internal")

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			req := new(dns.Msg)
			req.SetQuestion(fmt.Sprintf("job-%d.internal.", i%20), dns.TypeA)
			rw := &mockResponseWriter{}
			server.handleQuery(rw, req)
			if len(rw.msg.Answer) != 2 {
				b.Fatalf("expected 2 answers, got %d", len(rw.msg.Answer))
			}
			i++
		}
	})
}

func BenchmarkExtractIP(b *testing.B) {
	endpoints := []string{
		"http://10.0.0.1:8080",
		"http://192.168.1.50:8080",
		"http://172.16.0.100:8080",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ip := extractIP(endpoints[i%len(endpoints)])
		if ip == nil {
			b.Fatal("expected non-nil IP")
		}
	}
}

func BenchmarkParseJobFromData(b *testing.B) {
	lines := []string{
		`data: {"name":"my-api"}`,
		`data: {"name":"web-frontend"}`,
		`data: {"name":"worker-pool"}`,
		`data: {"name":"hopdns"}`,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		job := hoplib.ParseJobFromSSE(lines[i%len(lines)])
		if job == "" {
			b.Fatal("expected non-empty job")
		}
	}
}

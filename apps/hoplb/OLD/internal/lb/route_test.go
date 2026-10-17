package lb

import "testing"

func TestWildcardRouteMatch(t *testing.T) {
	rt := NewRouteTable()
	rt.Update(map[string]*Route{
		"*.haas.eu": {Pattern: "*.haas.eu", Backends: []*Backend{{Address: "10.0.0.1:80", Healthy: true}}},
		"*.example.com": {Pattern: "*.example.com", Backends: []*Backend{{Address: "10.0.0.2:80", Healthy: true}}},
	})

	tests := []struct {
		host    string
		want    string
		wantNil bool
	}{
		{"app.haas.eu", "*.haas.eu", false},
		{"api.haas.eu", "*.haas.eu", false},
		{"haas.eu", "", true},                   // No subdomain = no match
		{"sub.app.haas.eu", "", true},            // Multi-level = no match
		{"test.example.com", "*.example.com", false},
	}

	for _, tt := range tests {
		route := rt.Match(tt.host)
		if tt.wantNil {
			if route != nil {
				t.Errorf("Match(%q) = %q; want nil", tt.host, route.Pattern)
			}
		} else {
			if route == nil {
				t.Errorf("Match(%q) = nil; want %q", tt.host, tt.want)
			} else if route.Pattern != tt.want {
				t.Errorf("Match(%q) = %q; want %q", tt.host, route.Pattern, tt.want)
			}
		}
	}
}

func TestRouteTableMatch(t *testing.T) {
	rt := NewRouteTable()
	rt.Update(map[string]*Route{
		"api.example.com": {Pattern: "api.example.com", Backends: []*Backend{{Address: "127.0.0.1:8001", Healthy: true}}},
		"*.example.com":   {Pattern: "*.example.com", Backends: []*Backend{{Address: "127.0.0.1:8002", Healthy: true}}},
	})

	tests := []struct {
		host        string
		wantPattern string
		wantNil     bool
	}{
		{"api.example.com", "api.example.com", false},   // Exact match
		{"app.example.com", "*.example.com", false},     // Wildcard match
		{"other.example.com", "*.example.com", false},   // Wildcard match
		{"example.com", "", true},                       // No match
		{"api.example.com:443", "api.example.com", false}, // Strip port
	}

	for _, tt := range tests {
		route := rt.Match(tt.host)
		if tt.wantNil {
			if route != nil {
				t.Errorf("Match(%q) = %v; want nil", tt.host, route.Pattern)
			}
		} else {
			if route == nil {
				t.Errorf("Match(%q) = nil; want %q", tt.host, tt.wantPattern)
			} else if route.Pattern != tt.wantPattern {
				t.Errorf("Match(%q) = %q; want %q", tt.host, route.Pattern, tt.wantPattern)
			}
		}
	}
}

func TestRouteRoundRobin(t *testing.T) {
	route := &Route{
		Pattern: "test",
		Backends: []*Backend{
			{Address: "127.0.0.1:8001", Healthy: true},
			{Address: "127.0.0.1:8002", Healthy: true},
			{Address: "127.0.0.1:8003", Healthy: true},
		},
	}

	// Should round-robin through backends
	seen := make(map[string]int)
	for i := 0; i < 9; i++ {
		b := route.GetHealthyBackend()
		if b == nil {
			t.Fatal("GetHealthyBackend returned nil")
		}
		seen[b.Address]++
	}

	// Each backend should be hit 3 times
	for addr, count := range seen {
		if count != 3 {
			t.Errorf("Backend %s hit %d times, want 3", addr, count)
		}
	}
}

func TestRouteSkipsUnhealthy(t *testing.T) {
	route := &Route{
		Pattern: "test",
		Backends: []*Backend{
			{Address: "127.0.0.1:8001", Healthy: false},
			{Address: "127.0.0.1:8002", Healthy: true},
			{Address: "127.0.0.1:8003", Healthy: false},
		},
	}

	// Should only return healthy backend
	for i := 0; i < 5; i++ {
		b := route.GetHealthyBackend()
		if b == nil {
			t.Fatal("GetHealthyBackend returned nil")
		}
		if b.Address != "127.0.0.1:8002" {
			t.Errorf("Got %s, want 127.0.0.1:8002", b.Address)
		}
	}
}

func TestRouteNoHealthyBackends(t *testing.T) {
	route := &Route{
		Pattern: "test",
		Backends: []*Backend{
			{Address: "127.0.0.1:8001", Healthy: false},
			{Address: "127.0.0.1:8002", Healthy: false},
		},
	}

	b := route.GetHealthyBackend()
	if b != nil {
		t.Errorf("GetHealthyBackend = %v; want nil", b)
	}
}

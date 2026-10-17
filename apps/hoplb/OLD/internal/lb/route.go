package lb

import (
	"strings"
	"sync"
	"sync/atomic"
)

// Backend represents a single backend server
type Backend struct {
	Address string // host:port
	Healthy bool
}

// Route represents a routing rule
type Route struct {
	Pattern  string     // e.g., "*.haas.eu" or "api.haas.eu"
	Backends []*Backend
	next     uint64 // round-robin counter
}

// RouteTable manages all routes
type RouteTable struct {
	mu        sync.RWMutex
	exact     map[string]*Route // "api.example.com" -> route
	wildcards map[string]*Route // "*.example.com" -> route
}

// NewRouteTable creates a new route table
func NewRouteTable() *RouteTable {
	return &RouteTable{
		exact:     make(map[string]*Route),
		wildcards: make(map[string]*Route),
	}
}

// Update replaces all routes atomically
func (rt *RouteTable) Update(routes map[string]*Route) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	exact := make(map[string]*Route, len(routes))
	wildcards := make(map[string]*Route)
	for pattern, route := range routes {
		if strings.HasPrefix(pattern, "*.") {
			wildcards[pattern] = route
		} else {
			exact[pattern] = route
		}
	}
	rt.exact = exact
	rt.wildcards = wildcards
}

// Match finds a route for the given host
func (rt *RouteTable) Match(host string) *Route {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	// Strip port if present
	if idx := strings.LastIndex(host, ":"); idx != -1 {
		host = host[:idx]
	}

	// Exact match first (O(1))
	if route, ok := rt.exact[host]; ok {
		return route
	}

	// Wildcard match (O(1)): *.domain.com matches app.domain.com
	// The dot-count constraint means only first-level subdomain matches,
	// so we extract "*" + everything after the first dot.
	if idx := strings.Index(host, "."); idx != -1 {
		wildcard := "*" + host[idx:]
		if route, ok := rt.wildcards[wildcard]; ok {
			return route
		}
	}

	return nil
}

// GetHealthyBackend returns a healthy backend using round-robin
func (r *Route) GetHealthyBackend() *Backend {
	n := len(r.Backends)
	if n == 0 {
		return nil
	}

	// Round-robin through backends, skipping unhealthy ones
	start := atomic.AddUint64(&r.next, 1)
	for i := 0; i < n; i++ {
		idx := (int(start) + i) % n
		if r.Backends[idx].Healthy {
			return r.Backends[idx]
		}
	}
	return nil
}



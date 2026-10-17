package lb

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/xinix00/hoplb/internal/metrics"
)

// Proxy is a reverse proxy that routes based on the route table and tracks metrics
type Proxy struct {
	routeTable *RouteTable
	metrics    *metrics.Metrics
}

// NewProxy creates a new proxy with metrics tracking
func NewProxy(routeTable *RouteTable, m *metrics.Metrics) *Proxy {
	return &Proxy{
		routeTable: routeTable,
		metrics:    m,
	}
}

// ServeHTTP handles incoming requests and records metrics
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	domain := r.Host

	route := p.routeTable.Match(domain)
	if route == nil {
		p.recordMetrics(domain, "", http.StatusBadGateway, time.Since(start))
		http.Error(w, "no route for host", http.StatusBadGateway)
		return
	}

	backend := route.GetHealthyBackend()
	if backend == nil {
		p.recordMetrics(domain, "", http.StatusServiceUnavailable, time.Since(start))
		http.Error(w, "no healthy backend", http.StatusServiceUnavailable)
		return
	}

	target, err := url.Parse("http://" + backend.Address)
	if err != nil {
		p.recordMetrics(domain, backend.Address, http.StatusInternalServerError, time.Since(start))
		http.Error(w, "invalid backend", http.StatusInternalServerError)
		return
	}

	// Wrap ResponseWriter to capture status code
	wrappedWriter := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("Proxy error for %s -> %s: %v", r.Host, backend.Address, err)
		wrappedWriter.statusCode = http.StatusBadGateway
		http.Error(w, "backend error", http.StatusBadGateway)
	}

	log.Printf("%s %s -> %s", r.Method, r.Host+r.URL.Path, backend.Address)
	proxy.ServeHTTP(wrappedWriter, r)

	// Record metrics after request completes
	duration := time.Since(start)
	p.recordMetrics(domain, backend.Address, wrappedWriter.statusCode, duration)
}

// recordMetrics records request metrics (domain, backend, status code, latency)
func (p *Proxy) recordMetrics(domain, backend string, statusCode int, duration time.Duration) {
	if p.metrics != nil {
		p.metrics.RecordRequest(domain, backend, statusCode, duration)
	}
}

// statusWriter wraps http.ResponseWriter to capture the status code
type statusWriter struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader captures the status code before passing it through
func (w *statusWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// Unwrap exposes the underlying ResponseWriter so http.ResponseController (and
// httputil.ReverseProxy) can reach its Flusher and Hijacker. Without this,
// wrapping the writer hides those interfaces: streaming/SSE responses would be
// buffered instead of flushed in real time, and WebSocket upgrades would fail.
func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

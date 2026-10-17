package dns

import (
	"log"
	"net"
	"strings"

	"github.com/miekg/dns"
)

// Server handles DNS queries for service discovery
type Server struct {
	cache  *Cache
	cnames *CNAMEs
	domain string
	addr   string
	server *dns.Server
}

// NewServer creates a new DNS server
func NewServer(cache *Cache, addr, domain string) *Server {
	if domain == "" {
		domain = "internal."
	}
	if !strings.HasSuffix(domain, ".") {
		domain += "."
	}

	return &Server{
		cache:  cache,
		domain: domain,
		addr:   addr,
	}
}

// SetCNAMEs installs static CNAME mappings (matched before service lookup).
// Safe to call before Run; not safe to call concurrently with queries.
func (s *Server) SetCNAMEs(c *CNAMEs) {
	s.cnames = c
}

// Run starts the DNS server
func (s *Server) Run() error {
	dns.HandleFunc(s.domain, s.handleQuery)

	// CNAMEs can live in zones other than s.domain — register a handler
	// for each so queries reach handleQuery and our lookup table wins.
	for _, z := range s.cnames.Zones() {
		if z == s.domain {
			continue
		}
		dns.HandleFunc(z, s.handleQuery)
		log.Printf("DNS handler registered for CNAME zone: %s", z)
	}

	s.server = &dns.Server{
		Addr: s.addr,
		Net:  "udp",
	}

	log.Printf("DNS server listening on %s (domain: %s)", s.addr, s.domain)
	return s.server.ListenAndServe()
}

// Shutdown stops the DNS server
func (s *Server) Shutdown() error {
	if s.server != nil {
		return s.server.Shutdown()
	}
	return nil
}

// handleQuery handles DNS queries.
// Format: <service>.<cluster>.<domain> → IPs from that cluster
// Example: myapp.prod-eu.hop.local → IPs for "myapp" in cluster "prod-eu"
func (s *Server) handleQuery(w dns.ResponseWriter, r *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative = true

	for _, q := range r.Question {
		// Static CNAMEs take precedence and apply to any query type.
		// We return only the CNAME RR — no chasing — so the resolver
		// will issue a follow-up query for the target.
		if target, ok := s.cnames.Lookup(q.Name); ok {
			m.Answer = append(m.Answer, &dns.CNAME{
				Hdr: dns.RR_Header{
					Name:   q.Name,
					Rrtype: dns.TypeCNAME,
					Class:  dns.ClassINET,
					Ttl:    5,
				},
				Target: target,
			})
			continue
		}

		if q.Qtype != dns.TypeA {
			continue
		}

		// Strip domain suffix: "myapp.prod-eu.hop.local." → "myapp.prod-eu"
		prefix := strings.TrimSuffix(q.Name, "."+s.domain)
		if prefix == q.Name {
			continue // Query doesn't match our domain
		}

		// "<service>.<cluster>" → that cluster; bare "<service>" → merge all
		// peers (the documented federation default, e.g. myapp.hop.local).
		var ips []net.IP
		if service, cluster, ok := strings.Cut(prefix, "."); ok {
			ips = s.cache.GetCluster(cluster, service)
		} else {
			ips = s.cache.GetMerged(prefix)
		}

		for _, ip := range ips {
			m.Answer = append(m.Answer, &dns.A{
				Hdr: dns.RR_Header{
					Name:   q.Name,
					Rrtype: dns.TypeA,
					Class:  dns.ClassINET,
					Ttl:    5,
				},
				A: ip,
			})
		}
	}

	// RFC 2308: add SOA to Authority section for empty responses so resolvers
	// use our MinTtl (5s) as negative cache TTL instead of their own default.
	if len(m.Answer) == 0 {
		m.Ns = append(m.Ns, &dns.SOA{
			Hdr: dns.RR_Header{
				Name:   s.domain,
				Rrtype: dns.TypeSOA,
				Class:  dns.ClassINET,
				Ttl:    5,
			},
			Ns:      "ns." + s.domain,
			Mbox:    "hostmaster." + s.domain,
			Serial:  1,
			Refresh: 5,
			Retry:   5,
			Expire:  5,
			Minttl:  5, // Negative cache TTL — match our 5s polling interval
		})
	}

	_ = w.WriteMsg(m)
}

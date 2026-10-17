package dns

import (
	"fmt"
	"os"
	"strings"

	"github.com/miekg/dns"
	"gopkg.in/yaml.v3"
)

// Config is the optional YAML config file format for hopdns.
//
//	cnames:
//	  mail.hop.local:        mailserver.example.com
//	  git.hop.local:         gitea.prod-eu.hop.local
//	  "*.apps.hop.local":    ingress.prod-eu.hop.local
type Config struct {
	CNAMEs map[string]string `yaml:"cnames"`
}

// LoadConfig reads a YAML config from path.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &c, nil
}

// CNAMEs stores static CNAME mappings with optional wildcards.
// Lookup is safe on a nil receiver.
type CNAMEs struct {
	exact    map[string]string // "mail.hop.local." -> "mailserver.example.com."
	wildcard map[string]string // "apps.hop.local." -> "ingress.hop.local." (from "*.apps.hop.local")
}

// NewCNAMEs builds a lookup table from a name→target map. Keys
// beginning with "*." become wildcard entries that match any subdomain.
func NewCNAMEs(m map[string]string) *CNAMEs {
	c := &CNAMEs{
		exact:    make(map[string]string),
		wildcard: make(map[string]string),
	}
	for k, v := range m {
		k = strings.ToLower(strings.TrimSpace(k))
		v = strings.TrimSpace(v)
		if k == "" || v == "" {
			continue
		}
		target := dns.Fqdn(v)
		if strings.HasPrefix(k, "*.") {
			c.wildcard[dns.Fqdn(k[2:])] = target
		} else {
			c.exact[dns.Fqdn(k)] = target
		}
	}
	return c
}

// Lookup returns the CNAME target for a query name, if any.
// Wildcards match any number of leading labels, longest match wins.
func (c *CNAMEs) Lookup(name string) (string, bool) {
	if c == nil {
		return "", false
	}
	name = strings.ToLower(dns.Fqdn(name))
	if v, ok := c.exact[name]; ok {
		return v, true
	}
	// Strip leading labels and check each parent against the wildcard table.
	// "foo.bar.apps.hop.local." -> "bar.apps.hop.local." -> "apps.hop.local." (hit)
	parent := name
	for {
		i := strings.Index(parent, ".")
		if i < 0 || i == len(parent)-1 {
			return "", false
		}
		parent = parent[i+1:]
		if v, ok := c.wildcard[parent]; ok {
			return v, true
		}
	}
}

// Len returns the total number of configured CNAME entries.
func (c *CNAMEs) Len() int {
	if c == nil {
		return 0
	}
	return len(c.exact) + len(c.wildcard)
}

// Zones returns the FQDN zones that contain at least one CNAME entry,
// derived by stripping the leftmost label of each exact name and using
// each wildcard's parent directly. The server uses this to register
// handlers for every zone the CNAMEs cover, so a single hopdns process
// can serve CNAMEs across multiple domains.
func (c *CNAMEs) Zones() []string {
	if c == nil {
		return nil
	}
	set := make(map[string]struct{})
	for k := range c.exact {
		if i := strings.Index(k, "."); i >= 0 && i < len(k)-1 {
			set[k[i+1:]] = struct{}{}
		}
	}
	for k := range c.wildcard {
		set[k] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for z := range set {
		out = append(out, z)
	}
	return out
}

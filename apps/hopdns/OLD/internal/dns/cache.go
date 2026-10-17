package dns

import (
	"net"
	"sync"
)

// Cache stores cluster -> job name -> IPs mapping.
type Cache struct {
	mu       sync.RWMutex
	clusters map[string]map[string][]net.IP // cluster -> jobName -> IPs
}

// NewCache creates a new cache
func NewCache() *Cache {
	return &Cache{
		clusters: make(map[string]map[string][]net.IP),
	}
}

// GetCluster returns IPs for a job name in a specific cluster
func (c *Cache) GetCluster(cluster, jobName string) []net.IP {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if p := c.clusters[cluster]; p != nil {
		return p[jobName]
	}
	return nil
}

// GetMerged returns the union of IPs for a job name across all clusters,
// de-duplicated. It serves the cluster-less query "<service>.<domain>", which
// merges every peer (the documented federation default).
func (c *Cache) GetMerged(jobName string) []net.IP {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []net.IP
	seen := make(map[string]bool)
	for _, jobs := range c.clusters {
		for _, ip := range jobs[jobName] {
			if k := ip.String(); !seen[k] {
				seen[k] = true
				out = append(out, ip)
			}
		}
	}
	return out
}


// Set stores IPs for a job name in a cluster
func (c *Cache) Set(cluster, jobName string, ips []net.IP) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clusters[cluster] == nil {
		c.clusters[cluster] = make(map[string][]net.IP)
	}
	c.clusters[cluster][jobName] = ips
}

// Update replaces entire cache for a cluster
func (c *Cache) Update(cluster string, data map[string][]net.IP) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clusters[cluster] = data
}

// Clear removes all cached data for a cluster
func (c *Cache) Clear(cluster string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.clusters, cluster)
}

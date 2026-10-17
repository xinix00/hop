package dns

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/xinix00/hoplib"
)

// Watcher watches an hop cluster via SSE and keeps the cache updated.
// Every cluster (including your own) is a peer — there is no special "local" watcher.
type Watcher struct {
	agentAddr string
	cache     *Cache
	client    *hoplib.Client
	interval  time.Duration
	cluster   string // discovered from /v1/status → cluster_name
}

// NewWatcher creates a watcher for a cluster endpoint
func NewWatcher(agentAddr string, cache *Cache, apiKey string) *Watcher {
	return &Watcher{
		agentAddr: agentAddr,
		cache:     cache,
		client:    hoplib.NewClient(apiKey),
		interval:  5 * time.Second,
	}
}

// Run discovers the cluster name and then watches SSE for state changes.
// On disconnect it keeps the last-known-good cache (stale) and retries; the
// cache is replaced on reconnect. This matches hoplb — a failover must not
// black out DNS while tasks keep running (graceful degradation).
func (w *Watcher) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		// Discover cluster name if not known yet
		if w.cluster == "" {
			name, err := w.discoverCluster()
			if err != nil {
				log.Printf("[%s] failed to discover cluster name: %v", w.agentAddr, err)
				select {
				case <-time.After(w.interval):
					continue
				case <-ctx.Done():
					return
				}
			}
			w.cluster = name
			log.Printf("[%s] cluster: %s", w.agentAddr, w.cluster)
		}

		err := w.watchSSE(ctx)
		if ctx.Err() != nil {
			return
		}

		// Keep the last-known-good cache on disconnect (same as hoplb): a
		// dropped SSE stream — e.g. during a leader failover — must not black
		// out DNS while the tasks are still running. The cache is replaced on
		// reconnect, when watchSSE re-seeds via refresh(). The cluster name is
		// stable, so we keep it and skip re-discovery.
		log.Printf("[%s] (%s) SSE disconnected: %v, reconnecting in %v", w.agentAddr, w.cluster, err, w.interval)

		select {
		case <-time.After(w.interval):
		case <-ctx.Done():
			return
		}
	}
}

// discoverCluster fetches /v1/status and extracts cluster_name
func (w *Watcher) discoverCluster() (string, error) {
	status, err := hoplib.Fetch[struct {
		ClusterName string `json:"cluster_name"`
	}](w.client, w.agentAddr+"/v1/status")
	if err != nil {
		return "", err
	}
	if status.ClusterName == "" {
		return "", fmt.Errorf("remote cluster did not report cluster_name (upgrade hop?)")
	}
	return status.ClusterName, nil
}

// watchSSE connects to the SSE stream and triggers targeted refreshes per job.
// Does a full refresh on connect to seed the cache.
func (w *Watcher) watchSSE(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", w.agentAddr+"/v1/events", nil)
	if err != nil {
		return err
	}
	w.client.Sign(req, nil)

	resp, err := (&http.Client{}).Do(req) // no timeout — SSE is long-lived
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return &url.Error{Op: "GET", URL: w.agentAddr + "/v1/events", Err: http.ErrNotSupported}
	}

	log.Printf("[%s] (%s) SSE connected, seeding cache", w.agentAddr, w.cluster)
	w.refresh() // seed cache — SSE stream already open, events buffered

	lineCh := make(chan string)
	go func() {
		defer close(lineCh)
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			lineCh <- scanner.Text()
		}
	}()

	debounce := time.NewTimer(0)
	if !debounce.Stop() {
		<-debounce.C
	}
	pending := make(map[string]struct{})

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case line, ok := <-lineCh:
			if !ok {
				return nil // stream closed
			}
			if strings.HasPrefix(line, "data:") {
				if job := hoplib.ParseJobFromSSE(line); job != "" {
					if len(pending) == 0 {
						debounce.Reset(500 * time.Millisecond)
					}
					pending[job] = struct{}{}
				}
			}
		case <-debounce.C:
			for job := range pending {
				w.refreshJob(job)
			}
			pending = make(map[string]struct{})
		}
	}
}

// refreshJob fetches status for a single job and updates the cache.
func (w *Watcher) refreshJob(jobName string) {
	status, err := hoplib.Fetch[struct {
		Agents       []hoplib.Agent            `json:"agents"`
		TasksByAgent map[string][]hoplib.Task   `json:"tasks_by_agent"`
	}](w.client, fmt.Sprintf("%s/v1/jobs/%s/status", w.agentAddr, jobName))
	if err != nil {
		log.Printf("[%s] (%s) failed to fetch job %s: %v", w.agentAddr, w.cluster, jobName, err)
		w.refresh()
		return
	}

	agentIPs := buildAgentIPs(status.Agents)
	var ips []net.IP
	for agentID, tasks := range status.TasksByAgent {
		ip := agentIPs[agentID]
		if ip == nil {
			continue
		}
		for _, task := range tasks {
			if task.State == "running" {
				ips = appendUniqIP(ips, ip)
			}
		}
	}

	w.cache.Set(w.cluster, jobName, ips)
	log.Printf("[%s] (%s) cache updated job %s: %d IPs", w.agentAddr, w.cluster, jobName, len(ips))
}

// refresh fetches all jobs and their task status, then updates the cache.
func (w *Watcher) refresh() {
	jobs, err := hoplib.Fetch[[]hoplib.Job](w.client, w.agentAddr+"/v1/jobs")
	if err != nil {
		log.Printf("[%s] (%s) failed to fetch jobs: %v", w.agentAddr, w.cluster, err)
		return
	}

	data := make(map[string][]net.IP)
	for _, job := range jobs {
		status, err := hoplib.Fetch[struct {
			Agents       []hoplib.Agent          `json:"agents"`
			TasksByAgent map[string][]hoplib.Task `json:"tasks_by_agent"`
		}](w.client, fmt.Sprintf("%s/v1/jobs/%s/status", w.agentAddr, job.Name))
		if err != nil {
			log.Printf("[%s] (%s) failed to fetch job %s: %v", w.agentAddr, w.cluster, job.Name, err)
			continue
		}

		agentIPs := buildAgentIPs(status.Agents)
		for agentID, tasks := range status.TasksByAgent {
			ip := agentIPs[agentID]
			if ip == nil {
				continue
			}
			for _, task := range tasks {
				if task.State == "running" {
					data[job.Name] = appendUniqIP(data[job.Name], ip)
				}
			}
		}
	}

	w.cache.Update(w.cluster, data)
	log.Printf("[%s] (%s) cache updated: %d jobs", w.agentAddr, w.cluster, len(data))
}

func buildAgentIPs(agents []hoplib.Agent) map[string]net.IP {
	ips := make(map[string]net.IP)
	for _, agent := range agents {
		if ip := extractIP(agent.Endpoint); ip != nil {
			ips[agent.ID] = ip
		}
	}
	return ips
}

// appendUniqIP appends ip to list if not already present
func appendUniqIP(list []net.IP, ip net.IP) []net.IP {
	for _, existing := range list {
		if existing.Equal(ip) {
			return list
		}
	}
	return append(list, ip)
}

// extractIP extracts IP from endpoint (http://ip:port -> ip)
func extractIP(endpoint string) net.IP {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil
	}
	return net.ParseIP(u.Hostname())
}

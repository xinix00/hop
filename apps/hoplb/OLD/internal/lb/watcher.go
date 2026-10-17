package lb

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/xinix00/hoplib"
)

// Watcher watches local hop agent for task changes
type Watcher struct {
	agentAddr  string
	routeTable *RouteTable
	client     *hoplib.Client
	interval   time.Duration
	tagFilter  string // e.g., "lb:haas" means only jobs with tag lb=haas

	// Cached state for incremental updates
	agentHosts map[string]string                    // agentID → hostname
	jobs       map[string]*hoplib.Job               // jobName → job
	relevant   map[string]struct{}                  // job names that contribute routes
	tasks      map[string]map[string][]*hoplib.Task // jobName → agentID → tasks
}

// NewWatcher creates a new watcher
func NewWatcher(agentAddr string, routeTable *RouteTable, tagFilter string, apiKey string) *Watcher {
	return &Watcher{
		agentAddr:  agentAddr,
		routeTable: routeTable,
		client:     hoplib.NewClient(apiKey),
		interval:   5 * time.Second,
		tagFilter:  tagFilter,
	}
}

// Run connects to the SSE event stream and syncs on state changes.
// On disconnect, retries after a short delay.
func (w *Watcher) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		err := w.watchSSE(ctx)
		if ctx.Err() != nil {
			return
		}
		log.Printf("SSE disconnected: %v, reconnecting in %v", err, w.interval)
		select {
		case <-time.After(w.interval):
		case <-ctx.Done():
			return
		}
	}
}

// watchSSE connects to the agent's SSE stream and triggers debounced
// sync on any state change. Does a full sync on connect to seed routes.
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

	log.Printf("SSE connected to %s/v1/events, seeding routes", w.agentAddr)
	w.sync() // seed routes — SSE stream already open, events buffered

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
	pendingFull := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case line, ok := <-lineCh:
			if !ok {
				return nil // stream closed
			}
			if strings.HasPrefix(line, "data:") {
				job, full := classifyEvent(line, w.relevant, w.jobs)
				if job == "" {
					continue
				}
				if len(pending) == 0 {
					debounce.Reset(500 * time.Millisecond)
				}
				pending[job] = struct{}{}
				if full {
					pendingFull = true
				}
			}
		case <-debounce.C:
			if pendingFull {
				w.sync() // definition changed or new job: re-read the job list
			} else {
				for job := range pending {
					w.syncJob(job)
				}
			}
			pending = make(map[string]struct{})
			pendingFull = false
		}
	}
}

// classifyEvent decides what an SSE data line means for the route table.
// It returns the job the event is about ("" = ignore) and whether a full
// sync is needed. A job event ({"name":...}) means the DEFINITION changed —
// tags included — so it always earns a full sync, even for a job we had
// filed as irrelevant: that is exactly how a job becomes relevant (measured
// 2026-09-08 on traqqr: an easylb→hoplb tag rename was ignored until a
// restart). A task event ({"job":...}) only moves instances around, so it
// can be skipped for known irrelevant jobs and handled per job otherwise.
// An unknown job needs the job list too.
func classifyEvent(line string, relevant map[string]struct{}, known map[string]*hoplib.Job) (job string, full bool) {
	var ev struct {
		Name string `json:"name"`
		Job  string `json:"job"`
	}
	_ = json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &ev)
	if ev.Name != "" {
		return ev.Name, true // job event: definition (and tags) may have changed
	}
	if ev.Job == "" {
		return "", false
	}
	_, isRelevant := relevant[ev.Job]
	_, isKnown := known[ev.Job]
	if !isRelevant && isKnown {
		return "", false // known irrelevant job: nothing to route
	}
	return ev.Job, !isKnown
}

// sync does a full fetch of agents, jobs, and per-job task status for relevant jobs.
func (w *Watcher) sync() {
	agents, err := hoplib.Fetch[[]hoplib.Agent](w.client, w.agentAddr+"/v1/agents")
	if err != nil {
		log.Printf("Failed to fetch agents: %v", err)
		return
	}

	jobs, err := hoplib.Fetch[[]hoplib.Job](w.client, w.agentAddr+"/v1/jobs")
	if err != nil {
		log.Printf("Failed to fetch jobs: %v", err)
		return
	}

	// Cache agents
	w.agentHosts = make(map[string]string)
	for _, agent := range agents {
		if h := extractHost(agent.Endpoint); h != "" {
			w.agentHosts[agent.ID] = h
		}
	}

	// Cache jobs + determine relevant
	w.jobs = make(map[string]*hoplib.Job)
	w.relevant = make(map[string]struct{})
	for i := range jobs {
		w.jobs[jobs[i].Name] = &jobs[i]
		if w.jobMatchesFilter(&jobs[i]) && jobs[i].Tags["hoplb-urlprefix"] != "" {
			w.relevant[jobs[i].Name] = struct{}{}
		}
	}

	// Fetch tasks only for relevant jobs
	w.tasks = make(map[string]map[string][]*hoplib.Task)
	for jobName := range w.relevant {
		w.syncJob(jobName)
	}

	w.buildRoutes()
}

// syncJob fetches only the tasks for a single job and rebuilds routes from cache.
func (w *Watcher) syncJob(jobName string) {
	status, err := hoplib.Fetch[struct {
		Agents       []hoplib.Agent            `json:"agents"`
		TasksByAgent map[string][]*hoplib.Task `json:"tasks_by_agent"`
	}](w.client, fmt.Sprintf("%s/v1/jobs/%s/status", w.agentAddr, jobName))
	if err != nil {
		log.Printf("Failed to fetch job status for %s: %v", jobName, err)
		w.sync()
		return
	}

	// Update agent hosts from response
	for _, agent := range status.Agents {
		if h := extractHost(agent.Endpoint); h != "" {
			w.agentHosts[agent.ID] = h
		}
	}

	// Replace cached tasks for this job
	w.tasks[jobName] = status.TasksByAgent
	w.buildRoutes()
}

// buildRoutes rebuilds the route table from cached state.
func (w *Watcher) buildRoutes() {
	routes := make(map[string]*Route, len(w.relevant))

	for jobName := range w.relevant {
		job := w.jobs[jobName]
		if job == nil {
			continue
		}

		pattern := job.Tags["hoplb-urlprefix"]
		if pattern == "" {
			continue
		}

		portName := job.Tags["hoplb-port"]
		for agentID, tasks := range w.tasks[jobName] {
			host := w.agentHosts[agentID]
			if host == "" {
				continue
			}

			for _, task := range tasks {
				if task.State != "running" {
					continue
				}

				port := taskPort(task, portName)
				if port == 0 {
					continue
				}

				backend := &Backend{
					Address: host + ":" + strconv.Itoa(port),
					Healthy: true,
				}

				if route, ok := routes[pattern]; ok {
					route.Backends = append(route.Backends, backend)
				} else {
					routes[pattern] = &Route{
						Pattern:  pattern,
						Backends: []*Backend{backend},
					}
				}
			}
		}
	}

	// Debug: log what we're building
	for jobName := range w.relevant {
		job := w.jobs[jobName]
		if job == nil {
			continue
		}
		pattern := job.Tags["hoplb-urlprefix"]
		portName := job.Tags["hoplb-port"]
		tasksByAgent := w.tasks[jobName]
		log.Printf("[debug] job=%s pattern=%q portName=%q agents=%d", jobName, pattern, portName, len(tasksByAgent))
		for agentID, tasks := range tasksByAgent {
			host := w.agentHosts[agentID]
			log.Printf("[debug]   agent=%s host=%q tasks=%d", agentID, host, len(tasks))
			for _, task := range tasks {
				port := taskPort(task, portName)
				log.Printf("[debug]     task=%s state=%s port=%d", task.ID[:8], task.State, port)
			}
		}
	}

	w.routeTable.Update(routes)
	log.Printf("Updated routes: %d patterns, %d total backends",
		len(routes), func() int {
			n := 0
			for _, r := range routes {
				n += len(r.Backends)
			}
			return n
		}())
}

// taskPort returns the named port (from job's "port" tag) or first available.
func taskPort(task *hoplib.Task, portName string) int {
	if portName != "" {
		if p, ok := task.Ports[portName]; ok {
			return p
		}
	}
	for _, p := range task.Ports {
		return p
	}
	return 0
}

func extractHost(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// parseTagFilter parses "key:value" into key and value
func parseTagFilter(filter string) (string, string) {
	for i, c := range filter {
		if c == ':' {
			return filter[:i], filter[i+1:]
		}
	}
	return filter, ""
}

// jobMatchesFilter checks if job has the required tag
func (w *Watcher) jobMatchesFilter(job *hoplib.Job) bool {
	if w.tagFilter == "" {
		return true // no filter = match all
	}
	key, value := parseTagFilter(w.tagFilter)
	return job.Tags[key] == value
}

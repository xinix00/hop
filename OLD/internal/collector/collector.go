package collector

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/xinix00/hoplib"
)

const agentHealthyThreshold = 60 * time.Second

// Collector polls hop API and calculates metrics
type Collector struct {
	agentURL string
	client   *hoplib.Client

	mu             sync.RWMutex
	metrics        *Metrics
	lastTaskStates map[string]string // taskID -> state (for detecting transitions)
}

// New creates a new collector
func New(agentURL string, apiKey string) *Collector {
	return &Collector{
		agentURL:       agentURL,
		client:         hoplib.NewClient(apiKey),
		lastTaskStates: make(map[string]string),
		metrics: &Metrics{
			TasksByState:      make(map[string]int),
			TasksByJob:        make(map[string]int),
			TaskRestartsByJob: make(map[string]int),
			JobInstances:      make(map[string]*JobMetric),
			TaskStartsTotal:   make(map[string]int),
			TaskFailuresTotal: make(map[string]int),
			TaskRestartsTotal: make(map[string]int),
			AgentLastSeen:     make(map[string]time.Time),
			AgentCapacity:     make(map[string]*CapacityResponse),
			AgentCPUUsed:      make(map[string]float64),
			AgentMemoryUsed:   make(map[string]uint64),
		},
	}
}

// Collect fetches data from hop and calculates metrics
func (c *Collector) Collect() error {
	agents, err := hoplib.Fetch[[]*hoplib.Agent](c.client, c.agentURL+"/v1/agents")
	if err != nil {
		return fmt.Errorf("failed to fetch agents: %w", err)
	}

	jobs, err := hoplib.Fetch[[]*hoplib.Job](c.client, c.agentURL+"/v1/jobs")
	if err != nil {
		return fmt.Errorf("failed to fetch jobs: %w", err)
	}

	tasksByAgent := c.fetchAllJobTasks(jobs)
	c.fetchAgentCapacities(agents)
	c.calculateMetrics(agents, jobs, tasksByAgent)
	c.collectLeader()
	return nil
}

// collectLeader reads the local agent's /leader (address, and the lease
// expiry when this agent leads) and /v1/status (deploying, settling). Both
// are best-effort: a miss keeps the previous values.
func (c *Collector) collectLeader() {
	lead, err := hoplib.Fetch[struct {
		Leader         string    `json:"leader"`
		LeaseExpiresAt time.Time `json:"lease_expires_at"`
	}](c.client, c.agentURL+"/leader")
	if err != nil {
		log.Printf("leader: %v", err)
		return
	}
	status, err := hoplib.Fetch[struct {
		Deploying []string `json:"deploying"`
		Settling  bool     `json:"settling"`
	}](c.client, c.agentURL+"/v1/status")
	if err != nil {
		log.Printf("status: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if lead.Leader != c.metrics.Leader && c.metrics.Leader != "" {
		c.metrics.LeaderChanges++
	}
	c.metrics.Leader = lead.Leader
	c.metrics.LeaseExpiresAt = lead.LeaseExpiresAt
	if err == nil {
		c.metrics.JobsDeploying = status.Deploying
		c.metrics.Settling = status.Settling
	}
}

// copyMap copies a map
func copyMap[K comparable, V any](src map[K]V) map[K]V {
	dst := make(map[K]V, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// GetMetrics returns a copy of current metrics (thread-safe)
func (c *Collector) GetMetrics() *Metrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	m := &Metrics{
		AgentsTotal:       c.metrics.AgentsTotal,
		AgentsHealthy:     c.metrics.AgentsHealthy,
		JobsTotal:         c.metrics.JobsTotal,
		TasksByState:      copyMap(c.metrics.TasksByState),
		TasksByJob:        copyMap(c.metrics.TasksByJob),
		TaskRestartsByJob: copyMap(c.metrics.TaskRestartsByJob),
		TaskStartsTotal:   copyMap(c.metrics.TaskStartsTotal),
		TaskFailuresTotal: copyMap(c.metrics.TaskFailuresTotal),
		TaskRestartsTotal: copyMap(c.metrics.TaskRestartsTotal),
		AgentLastSeen:     copyMap(c.metrics.AgentLastSeen),
		AgentCapacity:     copyMap(c.metrics.AgentCapacity),
		AgentCPUUsed:      copyMap(c.metrics.AgentCPUUsed),
		AgentMemoryUsed:   copyMap(c.metrics.AgentMemoryUsed),
		JobInstances:      make(map[string]*JobMetric, len(c.metrics.JobInstances)),
		Leader:            c.metrics.Leader,
		LeaseExpiresAt:    c.metrics.LeaseExpiresAt,
		LeaderChanges:     c.metrics.LeaderChanges,
		JobsDeploying:     append([]string(nil), c.metrics.JobsDeploying...),
		Settling:          c.metrics.Settling,
	}
	for k, v := range c.metrics.JobInstances {
		m.JobInstances[k] = &JobMetric{
			Placed:     v.Placed,
			Expected:   v.Expected,
			Healthy:    v.Healthy,
			CPUPercent: v.CPUPercent,
			MemPercent: v.MemPercent,
		}
	}
	return m
}

func (c *Collector) calculateMetrics(agents []*hoplib.Agent, jobs []*hoplib.Job, tasksByAgent map[string][]*hoplib.Task) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.metrics.TasksByState = make(map[string]int)
	c.metrics.TasksByJob = make(map[string]int)
	c.metrics.TaskRestartsByJob = make(map[string]int)
	c.metrics.JobInstances = make(map[string]*JobMetric)
	c.metrics.AgentCPUUsed = make(map[string]float64)
	c.metrics.AgentMemoryUsed = make(map[string]uint64)

	c.metrics.AgentsTotal = len(agents)
	c.metrics.AgentsHealthy = 0
	for _, agent := range agents {
		c.metrics.AgentLastSeen[agent.ID] = agent.LastSeen
		if time.Since(agent.LastSeen) < agentHealthyThreshold {
			c.metrics.AgentsHealthy++
		}
	}

	jobCPUSum := make(map[string]float64)
	jobMemSum := make(map[string]float64)
	jobRunCount := make(map[string]int)

	for _, tasks := range tasksByAgent {
		for _, task := range tasks {
			c.metrics.TasksByState[task.State]++

			if task.State == "running" {
				c.metrics.TasksByJob[task.JobName]++
				jobCPUSum[task.JobName] += task.CPUPercent
				jobMemSum[task.JobName] += task.MemPercent
				jobRunCount[task.JobName]++
			}

			if task.RestartCount > 0 {
				c.metrics.TaskRestartsByJob[task.JobName] += task.RestartCount
			}

			oldState := c.lastTaskStates[task.ID]
			if oldState != task.State {
				if oldState == "" {
					c.metrics.TaskStartsTotal[task.JobName]++
				} else if task.State == "failed" {
					c.metrics.TaskFailuresTotal[task.JobName]++
				}
				if task.RestartCount > 0 {
					if prevTask, ok := c.lastTaskStates[task.ID]; ok && prevTask == "running" {
						c.metrics.TaskRestartsTotal[task.JobName]++
					}
				}
				c.lastTaskStates[task.ID] = task.State
			}
		}
	}

	for agentID, cap := range c.metrics.AgentCapacity {
		c.metrics.AgentCPUUsed[agentID] = float64(cap.CPUUsedShares) / 1024.0
		c.metrics.AgentMemoryUsed[agentID] = cap.MemoryUsedBytes
	}

	c.metrics.JobsTotal = len(jobs)
	for _, job := range jobs {
		placed := c.metrics.TasksByJob[job.Name]
		expected := job.Count
		if expected == -1 {
			expected = len(agents)
		} else if expected == 0 {
			expected = 1
		}

		var avgCPU, avgMem float64
		if cnt := jobRunCount[job.Name]; cnt > 0 {
			avgCPU = jobCPUSum[job.Name] / float64(cnt)
			avgMem = jobMemSum[job.Name] / float64(cnt)
		}

		c.metrics.JobInstances[job.Name] = &JobMetric{
			Placed:     placed,
			Expected:   expected,
			Healthy:    placed >= expected,
			CPUPercent: avgCPU,
			MemPercent: avgMem,
		}
	}
}

// fetchAllJobTasks fetches task details for each job in parallel via per-job status.
func (c *Collector) fetchAllJobTasks(jobs []*hoplib.Job) map[string][]*hoplib.Task {
	result := make(map[string][]*hoplib.Task)
	if len(jobs) == 0 {
		return result
	}

	type jobResult struct {
		tasks map[string][]*hoplib.Task
	}
	ch := make(chan jobResult, len(jobs))

	for _, job := range jobs {
		go func(j *hoplib.Job) {
			status, err := hoplib.Fetch[struct {
				TasksByAgent map[string][]*hoplib.Task `json:"tasks_by_agent"`
			}](c.client, fmt.Sprintf("%s/v1/jobs/%s/status", c.agentURL, j.Name))
			if err != nil {
				log.Printf("Failed to fetch status for job %s: %v", j.Name, err)
				ch <- jobResult{}
				return
			}
			ch <- jobResult{tasks: status.TasksByAgent}
		}(job)
	}

	for range jobs {
		r := <-ch
		for agentID, tasks := range r.tasks {
			result[agentID] = append(result[agentID], tasks...)
		}
	}

	return result
}

func (c *Collector) fetchAgentCapacities(agents []*hoplib.Agent) {
	type result struct {
		id  string
		cap *CapacityResponse
	}
	ch := make(chan result, len(agents))

	for _, agent := range agents {
		go func(a *hoplib.Agent) {
			cap, err := hoplib.Fetch[CapacityResponse](c.client, a.Endpoint+"/capacity")
			if err != nil {
				log.Printf("Failed to fetch capacity for %s: %v", a.ID, err)
				ch <- result{a.ID, nil}
				return
			}
			ch <- result{a.ID, &cap}
		}(agent)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for range agents {
		r := <-ch
		if r.cap != nil {
			c.metrics.AgentCapacity[r.id] = r.cap
		}
	}
}

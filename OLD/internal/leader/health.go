package leader

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
	"github.com/xinix00/lean/leanhttp"
)

// Run starts the leader's state loop and dead agent checker
func (l *Leader) Run(ctx context.Context) {
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		l.stateLoop(ctx)
	}()
	if l.persister != nil {
		workers.Add(1)
		go func() {
			defer workers.Done()
			l.persistLoop(ctx) // gecommitte staat (persist.go)
		}()
	}

	ticker := time.NewTicker(deadAgentCheckInterval)
	defer ticker.Stop()

	// Reconcile is verder event-gedreven (dispatch, updates, hand-backs, dode
	// agents) — dit is het vangnet erónder: een job die onderplaatst raakte
	// terwijl er even niets gebeurde (capaciteit kwam later vrij, een event
	// ging verloren) wordt anders nooit meer geprobeerd. Gemeten 01-08:
	// cloudflared bleef na een gefragmenteerde pool voorgoed op 0/1 staan
	// terwijl de ruimte allang terug was. Elke derde tik (~30s) is ruim
	// genoeg: reconcileJob doet niets bij een gezonde vloot.
	tick := 0
	for {
		select {
		case <-ctx.Done():
			workers.Wait()
			l.httpClient.CloseIdle()
			l.deleteClient.CloseIdle()
			return
		case <-ticker.C:
			l.checkDeadAgents()
			if tick++; tick%3 == 0 {
				if query(l, func(s *leaderState) bool { return s.settled }) {
					go l.reconcileJobs()
				}
			}
		}
	}
}

// checkDeadAgents removes agents that haven't sent heartbeat and reconciles jobs
func (l *Leader) checkDeadAgents() {
	settled := query(l, func(s *leaderState) bool { return s.settled })
	if !settled {
		return
	}

	var deadIDs []string
	query(l, func(s *leaderState) bool {
		for id, agent := range s.agents {
			if time.Since(agent.LastSeen) > l.agentTimeout {
				log.Printf("Agent %s is dead (no heartbeat for %v)", id, time.Since(agent.LastSeen))
				delete(s.agents, id)
				delete(s.placed, id)
				deadIDs = append(deadIDs, id)
			}
		}
		if len(deadIDs) > 0 {
			s.rebuildSortedAgents()
		}
		return false
	})

	if len(deadIDs) > 0 {
		go l.reconcileJobs()
		for _, id := range deadIDs {
			l.eventBus.Notify("agent:" + id)
		}
	}
}

// normalizePriorities ensures all jobs have unique sequential priorities 0..N-1.
// Called at the start of every reconcileJobs to fix any duplicates or gaps.
func (l *Leader) normalizePriorities(jobs []*types.Job) {
	sort.Slice(jobs, func(i, j int) bool {
		pi, pj := effectivePriority(jobs[i].Priority), effectivePriority(jobs[j].Priority)
		if pi != pj {
			return pi < pj
		}
		return jobs[i].Name < jobs[j].Name
	})
	for i, job := range jobs {
		if job.Priority == nil || *job.Priority != i {
			p := i
			// Alleen de prioriteit, in de store zelf. Deze lus werkt op een
			// snapshot: een upsert vanaf een snapshot herrees een job die
			// intussen gedeletet was (de delete-storm-zombies van 15-07), en
			// een hele-job-write vanaf een snapshot overschreef het
			// Deploying=false dat een update net had weggeschreven (traqqr,
			// 2026-09-08: server02/hoplb/cloudflared bleven "deploying").
			if !l.jobStore.SetJobPriority(job.Name, p) {
				continue // deleted since the snapshot: leave it dead
			}
			updated := *job
			updated.Priority = &p
			jobs[i] = &updated
		}
	}
}

// reconcileJobs ensures all jobs have the correct number of running instances.
func (l *Leader) reconcileJobs() {
	jobs := l.jobStore.GetJobs()
	if len(jobs) == 0 {
		return
	}

	agents := l.GetAgents()
	if len(agents) == 0 {
		return
	}

	// Normalize priorities first so preemption logic always sees unique 0..N-1 values.
	l.normalizePriorities(jobs)

	// Reset round-robin so high-priority jobs start from a consistent position.
	l.do(func(s *leaderState) { s.roundRobin = 0 })

	for _, job := range jobs {
		if job.Name == "" {
			continue
		}
		if err := l.reconcileJob(job, agents); err != nil {
			log.Printf("Failed to reconcile job %s: %v", job.Name, err)
		}
	}
}

// reconcileJob ensures a single job has the correct instances running.
func (l *Leader) reconcileJob(job *types.Job, agents []*types.Agent) error {
	// Skip jobs being actively dispatched (prevents double dispatch)
	dispatching := query(l, func(s *leaderState) bool { return s.dispatching[job.Name] })
	if dispatching {
		return nil
	}

	if job.Count == -1 {
		// Daemons bypass dispatchInstances, so they need the same per-job claim
		// here. Without it, two reconcile snapshots can both observe the same
		// missing node and each create a daemon task there.
		if !l.lockJob(job.Name) {
			return nil
		}
		defer l.unlockJob(job.Name)

		// Daemon: run on ALL agents, check placed to find who's missing
		missing := query(l, func(s *leaderState) []*types.Agent {
			var need []*types.Agent
			for _, agent := range agents {
				if s.placed[agent.ID] == nil || s.placed[agent.ID][job.Name] == 0 {
					need = append(need, agent)
				}
			}
			return need
		})

		dispatched := 0
		for _, agent := range missing {
			log.Printf("Reconciling daemon %s: dispatching to %s", job.Name, agent.ID)
			if err := l.sendJobToAgent(agent, job); err != nil {
				log.Printf("Failed to dispatch daemon %s to %s: %v", job.Name, agent.ID, err)
			} else {
				l.trackPlacement(agent.ID, job.Name)
				dispatched++
			}
		}
		if dispatched == 0 && len(missing) > 0 {
			return fmt.Errorf("all agents rejected daemon job %s", job.Name)
		}
		if dispatched > 0 {
			l.eventBus.Notify("job:" + job.Name)
		}
		return nil
	}

	// Regular: use sum of placed across live agents
	desired := job.Count
	if desired <= 0 {
		desired = 1
	}

	totalPlaced := query(l, func(s *leaderState) int {
		total := 0
		for agentID := range s.agents {
			if p := s.placed[agentID]; p != nil {
				total += p[job.Name]
			}
		}
		return total
	})

	missing := desired - totalPlaced
	if missing > 0 {
		log.Printf("Reconciling job %s: %d/%d placed, dispatching %d", job.Name, totalPlaced, desired, missing)
		err := l.dispatchInstances(job, missing)
		l.eventBus.Notify("job:" + job.Name)
		return err
	}
	return nil
}

// GetClusterStatus fetches status from all agents (parallel)
func (l *Leader) GetClusterStatus() map[string][]*types.Task {
	agents := l.GetAgents()
	result := make(map[string][]*types.Task)

	if len(agents) == 0 {
		return result
	}

	ctx, cancel := context.WithTimeout(l.context(), HTTPClientTimeout)
	defer cancel()

	type agentResult struct {
		agentID string
		tasks   []*types.Task
	}
	resultCh := make(chan agentResult, len(agents))

	for _, agent := range agents {
		go func(a *types.Agent) {
			tasks, err := l.fetchAgentTasks(ctx, a)
			if err != nil {
				log.Printf("Failed to fetch tasks from %s: %v", a.ID, err)
				resultCh <- agentResult{agentID: a.ID, tasks: nil}
				return
			}
			resultCh <- agentResult{agentID: a.ID, tasks: tasks}
		}(agent)
	}

	for i := 0; i < len(agents); i++ {
		select {
		case res := <-resultCh:
			if res.tasks != nil {
				result[res.agentID] = res.tasks
			}
		case <-ctx.Done():
			log.Printf("GetClusterStatus timeout after collecting %d/%d agents", len(result), len(agents))
			return result
		}
	}

	return result
}

// GetJobStatus fetches tasks for a specific job by only querying agents that have it placed.
func (l *Leader) GetJobStatus(jobName string) (map[string][]*types.Task, []*types.Agent) {
	job := l.jobStore.GetJob(jobName)
	if job == nil {
		return nil, nil
	}

	agents := query(l, func(s *leaderState) []*types.Agent {
		var result []*types.Agent
		for agentID, jobs := range s.placed {
			if jobs[jobName] > 0 {
				if a := s.agents[agentID]; a != nil {
					result = append(result, a)
				}
			}
		}
		return result
	})

	result := make(map[string][]*types.Task)
	if len(agents) == 0 {
		return result, agents
	}

	ctx, cancel := context.WithTimeout(l.context(), HTTPClientTimeout)
	defer cancel()

	type agentResult struct {
		agentID string
		tasks   []*types.Task
	}
	resultCh := make(chan agentResult, len(agents))

	for _, agent := range agents {
		go func(a *types.Agent) {
			tasks, err := l.fetchAgentTasks(ctx, a)
			if err != nil {
				resultCh <- agentResult{agentID: a.ID}
				return
			}
			var filtered []*types.Task
			for _, t := range tasks {
				if t.JobName == jobName {
					filtered = append(filtered, t)
				}
			}
			resultCh <- agentResult{agentID: a.ID, tasks: filtered}
		}(agent)
	}

	for i := 0; i < len(agents); i++ {
		select {
		case res := <-resultCh:
			if res.tasks != nil {
				result[res.agentID] = res.tasks
			}
		case <-ctx.Done():
			return result, agents
		}
	}

	return result, agents
}

// fetchAgentTasks gets the task list from an agent
func (l *Leader) fetchAgentTasks(ctx context.Context, agent *types.Agent) ([]*types.Task, error) {
	call := leanhttp.Call{Method: leanhttp.MethodGet, URL: fmt.Sprintf("%s/tasks", agent.Endpoint)}
	httputil.SignCall(&call, l.apiKey)

	resp, err := l.httpClient.DoContext(ctx, call)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var tasks []*types.Task
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, err
	}

	return tasks, nil
}

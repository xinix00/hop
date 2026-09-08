package leader

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
)

const (
	defaultAgentTimeout    = 30 * time.Second
	deadAgentCheckInterval = 10 * time.Second
	stateChannelBufferSize = 256
)

var (
	// HTTPClientTimeout can be overridden in tests for faster execution.
	HTTPClientTimeout = 5 * time.Second

	// DeleteClientTimeout is used for delete operations which may take longer
	// (Docker stop: ~10s SIGTERM + 10s SIGKILL worst case).
	DeleteClientTimeout = 60 * time.Second
)

// JobStore is the interface for accessing jobs (implemented by Agent)
type JobStore interface {
	GetJobs() []*types.Job
	GetJob(name string) *types.Job
	StoreJob(job *types.Job)
	// UpdateJob writes only if the job still exists (atomically in the
	// store's state loop) and reports whether it did. Reconcile-side
	// rewrites (priority renumbering) MUST use this instead of StoreJob:
	// a reconcile holds a snapshot, and upserting from a snapshot
	// resurrects jobs deleted since the snapshot was taken (measured
	// 15-07: delete-storm zombies on the Altra).
	UpdateJob(job *types.Job) bool
	DeleteJob(name string) // Remove job from store by name
	GetStateTime() time.Time
	SyncJobs(jobs []*types.Job, updated time.Time)
}

// leaderState holds all mutable state (owned by single goroutine)
type leaderState struct {
	agents       map[string]*types.Agent
	agentsSorted []*types.Agent            // cached sorted agent list, rebuilt on mutation
	placed       map[string]map[string]int // agentID -> jobName -> count
	dispatching  map[string]bool           // jobName -> true if actively being dispatched
	settled      bool                      // false during settle period after leader election
	roundRobin   int
	// tombstones marks recently deleted job names. Reconcile snapshots and
	// in-flight dispatches carry job COPIES; without this marker they can
	// resurrect a deleted job (re-dispatch → agent /run re-registers it —
	// measured 15-07: delete-storm zombies). Checked before every dispatch;
	// cleared when the same name is legitimately re-submitted; entries
	// expire after tombstoneTTL.
	tombstones map[string]time.Time
}

// tombstoneTTL: hoe lang een delete-grafsteen geldig blijft. Ruim langer dan
// elke reconcile-ronde + dispatch-vlucht; kort genoeg om de map klein te
// houden bij naam-hergebruik in stormen.
const tombstoneTTL = 2 * time.Minute

// rebuildSortedAgents rebuilds the cached sorted agent list from the agents map.
// Must be called inside a state operation whenever agents map is modified.
func (s *leaderState) rebuildSortedAgents() {
	s.agentsSorted = make([]*types.Agent, 0, len(s.agents))
	for _, a := range s.agents {
		s.agentsSorted = append(s.agentsSorted, a)
	}
	sort.Slice(s.agentsSorted, func(i, j int) bool {
		return s.agentsSorted[i].ID < s.agentsSorted[j].ID
	})
}

// Leader dispatches jobs to agents and monitors health
type Leader struct {
	localAgentID string
	jobStore     JobStore

	ops chan func(*leaderState) // all state access goes through here

	httpClient     *httputil.Client
	deleteClient   *httputil.Client
	agentTimeout   time.Duration
	persistTimeout time.Duration // 0 = defaultPersistTimeout; see SetLeaseTTL
	settleDelay    time.Duration // wait before first reconciliation (0 = settled immediately)
	eventBus       *EventBus
	apiKey         string

	// Committed cluster state (persist.go): extern gecommitte gewenste
	// staat, met de leader als enige auteur. nil = geen persistentie
	// (huidige gedrag).
	persister  StatePersister
	stateDirty chan struct{}

	// stateDone closes exactly once when the state owner exits. State
	// operations select on it so callers cannot remain parked on ops/result
	// after leadership has been cancelled.
	stateOnce sync.Once
	stateDone chan struct{}

	lifecycleMu sync.RWMutex
	runCtx      context.Context
}

// New creates a new leader with optional HTTP client (nil uses default)
func New(localAgentID string, jobStore JobStore, client *httputil.Client) *Leader {
	if client == nil {
		client = &httputil.Client{Timeout: HTTPClientTimeout}
	}
	return &Leader{
		localAgentID: localAgentID,
		jobStore:     jobStore,
		ops:          make(chan func(*leaderState), stateChannelBufferSize),
		agentTimeout: defaultAgentTimeout,
		httpClient:   client,
		deleteClient: &httputil.Client{Timeout: DeleteClientTimeout},
		eventBus:     NewEventBus(),
		stateDone:    make(chan struct{}),
	}
}

// SetAPIKey sets the API key used for leader→agent HTTP requests
func (l *Leader) SetAPIKey(key string) {
	l.apiKey = key
}

// SetAgentTimeout overrides the default agent timeout (for config wiring)
func (l *Leader) SetAgentTimeout(d time.Duration) {
	l.agentTimeout = d
}

// EnableSettle enables settle period (waits agentTimeout before first reconciliation)
func (l *Leader) EnableSettle() {
	l.settleDelay = l.agentTimeout
}

// SetSettleDelay sets a custom settle delay (for tests; production should use EnableSettle)
func (l *Leader) SetSettleDelay(d time.Duration) {
	l.settleDelay = d
}

// stateLoop is the single goroutine that owns all mutable state. Some older
// tests start it directly while production starts it through Run, so the once
// guard is here rather than in Run.
func (l *Leader) stateLoop(ctx context.Context) {
	l.stateOnce.Do(func() { l.runStateLoop(ctx) })
}

func (l *Leader) runStateLoop(ctx context.Context) {
	l.lifecycleMu.Lock()
	l.runCtx = ctx
	l.lifecycleMu.Unlock()

	state := &leaderState{
		agents:      make(map[string]*types.Agent),
		placed:      make(map[string]map[string]int),
		dispatching: make(map[string]bool),
		settled:     l.settleDelay == 0,
		tombstones:  make(map[string]time.Time),
	}

	var settleTimer <-chan time.Time
	if l.settleDelay > 0 {
		settleTimer = time.After(l.settleDelay)
	}
	defer close(l.stateDone)

	for {
		select {
		case <-ctx.Done():
			return
		case op := <-l.ops:
			op(state)
		case <-settleTimer:
			state.settled = true
			settleTimer = nil
			log.Printf("Leader settled after %v, reconciling jobs", l.settleDelay)
			l.eventBus.Notify("status")
			go l.reconcileJobs()
		}
	}
}

// do executes an operation on state (fire-and-forget)
func (l *Leader) do(op func(*leaderState)) {
	select {
	case <-l.stateDone:
		return
	default:
	}
	select {
	case l.ops <- op:
	case <-l.stateDone:
	}
}

// query executes an operation and waits for result
func query[T any](l *Leader, fn func(*leaderState) T) T {
	var zero T
	result := make(chan T, 1)
	select {
	case <-l.stateDone:
		return zero
	default:
	}
	select {
	case l.ops <- func(s *leaderState) {
		result <- fn(s)
	}:
	case <-l.stateDone:
		return zero
	}
	select {
	case value := <-result:
		return value
	case <-l.stateDone:
		return zero
	}
}

func (l *Leader) context() context.Context {
	l.lifecycleMu.RLock()
	ctx := l.runCtx
	l.lifecycleMu.RUnlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (l *Leader) stopped() bool {
	select {
	case <-l.stateDone:
		return true
	default:
		return false
	}
}

// Heartbeat registreert een levensteken van een agent; false = onbekende
// agent (caller hoort 404 te geven zodat die zich her-registreert). PUUR liveness —
// géén job-uitwisseling meer (gesloopt 16-07, Derek): de leader is de enige
// auteur van gewenste staat (gecommit naar S3, persist.go) en agents zijn
// uitvoerders. De oude bidirectionele job-sync hier was de kraamkamer van
// de delete-storm-zombies van 15-07.
func (l *Leader) Heartbeat(id, version string, tempMilliC int) bool {
	return query(l, func(s *leaderState) bool {
		agent, ok := s.agents[id]
		if !ok {
			return false
		}
		agent.LastSeen = time.Now()
		agent.Version = version
		// Telemetrie, geen scheduling-input: de CPU-temperatuur van de node
		// (heetste sensor, milligraden; 0 = onbekend) reist mee op de
		// heartbeat en is via /v1/agents zichtbaar.
		agent.TempMilliC = tempMilliC
		return true
	})
}

// RegisterAgent registers a (re)starting agent. Clears old state and triggers reconciliation.
// Returns false if the ID is already registered with a different endpoint (duplicate).
func (l *Leader) RegisterAgent(id, endpoint, version string, placed map[string]int) bool {
	type result struct {
		reconcile bool
		rejected  bool
	}
	res := query(l, func(s *leaderState) result {
		// Reject if another live agent has the same ID but different endpoint
		if existing, ok := s.agents[id]; ok && existing.Endpoint != endpoint {
			if time.Since(existing.LastSeen) < l.agentTimeout {
				log.Printf("Rejecting agent %s: already registered at %s (new: %s)", id, existing.Endpoint, endpoint)
				return result{rejected: true}
			}
		}

		// Clear stale state from previous incarnation
		delete(s.agents, id)
		delete(s.placed, id)

		s.agents[id] = &types.Agent{
			ID:       id,
			Endpoint: endpoint,
			Version:  version,
			LastSeen: time.Now(),
		}
		if placed != nil {
			s.placed[id] = placed
		}
		s.rebuildSortedAgents()
		return result{reconcile: s.settled}
	})

	if res.rejected {
		return false
	}
	if res.reconcile {
		log.Printf("Agent %s registered, reconciling jobs", id)
		// A returning agent may bring back tasks the cluster re-placed while it
		// was away. Stop that surplus on the returning agent (at worst a stale
		// version, and a replacement already exists) before reconciling.
		l.trimReturningAgentSurplus(id)
		l.reconcileJobs()
	}
	l.eventBus.Notify("agent:" + id)
	return true
}

// UnregisterAgent removes an agent and reconciles jobs
func (l *Leader) UnregisterAgent(id string) {
	l.do(func(s *leaderState) {
		delete(s.agents, id)
		delete(s.placed, id)
		s.rebuildSortedAgents()
	})
	l.reconcileJobs()
}

// GetAgents returns all registered agents
func (l *Leader) GetAgents() []*types.Agent {
	return query(l, func(s *leaderState) []*types.Agent {
		agents := make([]*types.Agent, 0, len(s.agents))
		for _, a := range s.agents {
			cp := *a // copy: callers marshal/read this off the state loop while Heartbeat mutates LastSeen/Version
			agents = append(agents, &cp)
		}
		return agents
	})
}

// GetAgent returns a registered agent by ID
func (l *Leader) GetAgent(id string) *types.Agent {
	return query(l, func(s *leaderState) *types.Agent {
		if a, ok := s.agents[id]; ok {
			cp := *a // full copy, like GetAgents: a hand-picked field list loses every field added later (it already dropped TempMilliC)
			return &cp
		}
		return nil
	})
}

// GetJobs returns all jobs
func (l *Leader) GetJobs() []*types.Job {
	return l.jobStore.GetJobs()
}

// GetJob returns a single job by name (nil if not found)
func (l *Leader) GetJob(name string) *types.Job {
	return l.jobStore.GetJob(name)
}

// NextPriority returns the next available priority index (= number of existing jobs).
func (l *Leader) NextPriority() int {
	return len(l.jobStore.GetJobs())
}

// PatchJobPriority moves a job to the given index position, renumbering all other
// jobs to keep a dense 0..N-1 sequence.
func (l *Leader) PatchJobPriority(name string, targetIdx int) error {
	job := l.jobStore.GetJob(name)
	if job == nil {
		return fmt.Errorf("job %s not found", name)
	}

	jobs := l.jobStore.GetJobs()
	sort.Slice(jobs, func(i, j int) bool {
		pi, pj := effectivePriority(jobs[i].Priority), effectivePriority(jobs[j].Priority)
		if pi != pj {
			return pi < pj
		}
		return jobs[i].Name < jobs[j].Name
	})

	for i := 0; i < len(jobs); i++ {
		if jobs[i].Name == name {
			jobs = append(jobs[:i], jobs[i+1:]...)
			break
		}
	}

	if targetIdx < 0 {
		targetIdx = 0
	}
	if targetIdx > len(jobs) {
		targetIdx = len(jobs)
	}
	jobs = append(jobs[:targetIdx], append([]*types.Job{job}, jobs[targetIdx:]...)...)

	for i, j := range jobs {
		p := i
		updated := *j
		updated.Priority = &p
		// Snapshot-herschrijf: UpdateJob zodat een concurrent gedeletete job
		// hier niet herrijst (zelfde les als normalizePriorities, 15-07).
		l.jobStore.UpdateJob(&updated)
	}

	l.eventBus.Notify("job:" + name)
	go l.reconcileJobs()
	return nil
}

// GetPlacedCounts returns placed counts aggregated by job name: jobName → total count.
func (l *Leader) GetPlacedCounts() map[string]int {
	return query(l, func(s *leaderState) map[string]int {
		result := make(map[string]int)
		for _, jobs := range s.placed {
			for jobName, count := range jobs {
				result[jobName] += count
			}
		}
		return result
	})
}

// GetPlaced returns which agents have a job placed and how many (for testing/debugging)
func (l *Leader) GetPlaced(jobName string) map[string]int {
	return query(l, func(s *leaderState) map[string]int {
		result := make(map[string]int)
		for agentID, jobs := range s.placed {
			if count := jobs[jobName]; count > 0 {
				result[agentID] = count
			}
		}
		return result
	})
}

// GetStateTime returns the job store's state time
func (l *Leader) GetStateTime() time.Time {
	return l.jobStore.GetStateTime()
}

// IsSettled returns whether the leader has finished its settle period
func (l *Leader) IsSettled() bool {
	return query(l, func(s *leaderState) bool { return s.settled })
}

// EventBus returns the leader's event bus for SSE subscribers
func (l *Leader) EventBus() *EventBus {
	return l.eventBus
}

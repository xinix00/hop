package agent

import (
	"context"
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/xinix00/hop/internal/runner"
	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/config"
	"github.com/xinix00/hop/pkg/httputil"
	"github.com/xinix00/lean/leanhttp"
)

const (
	// defaultMaxRestarts is unlimited: a task that keeps crashing keeps
	// coming back, with restartDelay's exponential backoff (capped at 30 s)
	// between attempts. A finite default (it was 5 in 5 minutes) turned a
	// dependency that started late into a permanent outage: the traqqr web
	// apps burnt their budget before RavenDB was up (2026-09-08) and stayed
	// "failed" until someone re-posted them. Jobs that must give up set
	// max_restarts explicitly.
	defaultMaxRestarts   = -1
	defaultRestartWindow = 5 * time.Minute
	proxyTimeout         = 10 * time.Second

	// proxyMaxBody caps a body the proxy forwards to the leader. leanhttp sends a
	// body as bytes, so the proxy has to read it before it can pass it on, and an
	// unbounded read on a route that has not been authenticated yet is a
	// pre-auth memory DoS. Same size as httputil's cap, for the same reason.
	proxyMaxBody           = 8 << 20
	stateChannelBufferSize = 256
)

// agentState holds all mutable state (owned by single goroutine)
type agentState struct {
	jobs       map[string]*types.Job  // job name → job
	tasks      map[string]*types.Task // task ID → task
	stateTime  time.Time
	leaderAddr string // leader API ("ip:port") the tick loop last confirmed; "" = none known
	// leaseExpiresAt is when OUR lease lapses unless renewed; zero unless
	// this node leads. Published by the tick loop for /leader and hopprom.
	leaseExpiresAt time.Time
}

// Agent runs jobs and reports status
type Agent struct {
	id           string
	endpoint     string
	config       *config.Config
	execRunner   runner.Runner
	dockerRunner runner.Runner
	hopRunner    runner.Runner     // set via WithHopRunner on HopOS nodes
	sysInfo      SystemInfo        // detected once at startup
	attributes   map[string]string // node attributes for affinity matching

	ops chan func(*agentState) // all state access goes through here

	server    *httputil.Server
	getLeader func() string // returns current leader address (for proxying)

	// Two clients, and the split is the timeout: httpClient carries
	// proxyTimeout, streamClient carries none. An SSE tail or a log stream must
	// not be cut off by the deadline that a buffered proxy call wants.
	httpClient   *httputil.Client
	streamClient *httputil.Client
	apiKey       string        // API key for authenticating with leader and protecting local endpoints
	shutdownCh   chan struct{} // closed by shutdown() — long-running goroutines select on this

	// onFlip vraagt HopOS zichzelf live te vervangen (de kern-flip): de node
	// haalt de bundel op url, controleert de sha256 en springt erin terwijl de
	// taken doordraaien. Gezet via SetFlipFunc (agentboot, alleen op HopOS-
	// nodes die het kunnen); nil = het /flip-endpoint zegt eerlijk 501.
	onFlip func(url, sha256 string) error

	// restartWait is a test seam. Nil uses the cancellable timer below.
	restartWait func(time.Duration) bool

	checkStates map[string]*checkState // health check state per task (monitor goroutine only)
}

// New creates a new agent with optional runner (nil uses default ExecRunner)
func New(cfg *config.Config, id string, r runner.Runner) *Agent {
	endpoint := fmt.Sprintf("http://%s:%d", cfg.Node.IP, cfg.Node.Port)
	sysInfo := GetSystemInfo() // detect once at startup

	// Build node attributes: auto-detected + user-configured (config overrides).
	// node.docker betekent: de daemon antwoordt op de socket — een node die
	// containers echt kan draaien, niet slechts een CLI-binary op het pad.
	hasDocker := "false"
	if runner.DockerPresent(cfg.Runner.DockerSocket) {
		hasDocker = "true"
	}

	attrs := map[string]string{
		"node.id":     id,
		"node.arch":   runtime.GOARCH,
		"node.os":     runtime.GOOS,
		"node.docker": hasDocker,
	}
	for k, v := range cfg.Node.Attributes {
		attrs[k] = v
	}

	logs := runner.LogPolicy{
		TailLines: cfg.Runner.LogTailLines,
		Keep:      time.Duration(cfg.Runner.LogKeepSeconds) * time.Second,
	}
	if r == nil {
		r = runner.NewExecRunner(&runner.Config{
			RootfsBase:   cfg.Paths.RootfsBase,
			MaxCPUShares: cfg.Capacity.CPUShares,
			Isolate:      cfg.Runner.Isolate,
			NodeAttrs:    attrs,
			Logs:         logs,
		})
	}
	dockerRunner := runner.NewDockerRunner(attrs, cfg.Runner.DockerSocket, sysInfo.CPUCores)
	dockerRunner.SetLogPolicy(logs)

	a := &Agent{
		id:           id,
		endpoint:     endpoint,
		config:       cfg,
		execRunner:   r,
		dockerRunner: dockerRunner,
		sysInfo:      sysInfo,
		attributes:   attrs,
		ops:          make(chan func(*agentState), stateChannelBufferSize),
		httpClient:   &httputil.Client{Timeout: proxyTimeout},
		streamClient: &httputil.Client{},
		apiKey:       cfg.APIKey,
		checkStates:  make(map[string]*checkState),
		getLeader:    nil, // nil = LeaderAddr (state loop); tests override via SetLeaderFunc
		shutdownCh:   make(chan struct{}),
	}
	// Runners met een zichtbare startfase (streamende download) melden hun
	// voortgang terug: de task gaat queued → downloading met bytes erbij.
	if pr, ok := r.(runner.ProgressReporter); ok {
		pr.SetProgressSink(progressSink{a})
	}
	return a
}

// progressSink is de runner→agent brug voor startfase-voortgang. Alle mutatie
// loopt door de state-loop, en alleen vooruit: een task die al Stopping,
// Failed of Running is wordt nooit teruggezet door een late voortgangsmelding.
type progressSink struct{ a *Agent }

func (p progressSink) TaskDownloading(taskID string, downloaded, total uint64) {
	p.a.do(func(s *agentState) {
		t := s.tasks[taskID]
		if t == nil || (t.State != types.TaskQueued && t.State != types.TaskDownloading) {
			return
		}
		t.State = types.TaskDownloading
		t.Downloaded, t.ImageSize = downloaded, total
	})
}

// WithHopRunner registers the HopOS slot runner; jobs with driver "hop" are
// dispatched to it. Only meaningful on HopOS nodes (node.os == "hopos").
// SetFlipFunc schakelt het /flip-endpoint in (de kern-flip van HopOS). Vóór
// Run aanroepen, net als de andere bedrading.
func (a *Agent) SetFlipFunc(fn func(url, sha256 string) error) { a.onFlip = fn }

func (a *Agent) WithHopRunner(r runner.Runner) *Agent {
	a.hopRunner = r
	if pr, ok := r.(runner.ProgressReporter); ok {
		pr.SetProgressSink(progressSink{a})
	}
	return a
}

// SetLeaderAddr records the leader the tick loop is registered with (or
// our own leader API while we hold the lease). The agent proxies cluster
// calls there. It goes through the state loop like every other agent
// fact — nobody asks the lock store "who leads?" per request.
func (a *Agent) SetLeaderAddr(addr string) {
	a.do(func(s *agentState) { s.leaderAddr = addr })
}

// LeaderAddr returns the leader API address to proxy to, or "" while no
// leader is known.
func (a *Agent) LeaderAddr() string {
	return query(a, func(s *agentState) string { return s.leaderAddr })
}

// SetLeaseExpiresAt records when this node's own lease lapses (zero when it
// does not lead). The lease is the leader's timer; /leader shows it and
// hopprom turns it into hop_leader_lease_seconds.
func (a *Agent) SetLeaseExpiresAt(t time.Time) {
	a.do(func(s *agentState) { s.leaseExpiresAt = t })
}

// LeaseExpiresAt returns the published lease expiry (zero unless leading).
func (a *Agent) LeaseExpiresAt() time.Time {
	return query(a, func(s *agentState) time.Time { return s.leaseExpiresAt })
}

// leaderAddr is what the handlers use: the test override if set, else the
// state-loop value.
func (a *Agent) leaderAddr() string {
	if a.getLeader != nil {
		return a.getLeader()
	}
	return a.LeaderAddr()
}

// SetLeaderFunc overrides where the leader address comes from (tests).
func (a *Agent) SetLeaderFunc(fn func() string) {
	a.getLeader = fn
}

// SetSysInfo overrides detected system info (for testing)
func (a *Agent) SetSysInfo(info SystemInfo) {
	a.sysInfo = info
}

// effectiveCPUShares returns the lower of (configured cap, detected hardware).
// Operators set Capacity.CPUShares > 0 when the node is shared with other
// workloads and hop should commit fewer resources than the box physically has.
func (a *Agent) effectiveCPUShares() int {
	detected := a.sysInfo.CPUCores * 1024
	if cap := a.config.Capacity.CPUShares; cap > 0 && cap < detected {
		return cap
	}
	return detected
}

// effectiveMemoryBytes mirrors effectiveCPUShares for memory: the configured
// Capacity.Memory caps usage when set (and not larger than the host).
func (a *Agent) effectiveMemoryBytes() uint64 {
	detected := a.sysInfo.MemoryBytes
	if cap := a.config.Capacity.Memory; cap > 0 && cap < detected {
		return cap
	}
	return detected
}

// monitorInterval returns the task monitor interval from config (default 5s).
func (a *Agent) monitorInterval() time.Duration {
	if d := a.config.Timeouts.HealthCheckInterval; d > 0 {
		return d
	}
	return 5 * time.Second
}

// healthTimeout returns the health check timeout from config (default 5s).
func (a *Agent) healthTimeout() time.Duration {
	if d := a.config.Timeouts.HealthCheckTimeout; d > 0 {
		return d
	}
	return 5 * time.Second
}

// ID returns the agent ID
func (a *Agent) ID() string {
	return a.id
}

// Endpoint returns the agent's HTTP endpoint
func (a *Agent) Endpoint() string {
	return a.endpoint
}

// Attributes returns the agent's node attributes
func (a *Agent) Attributes() map[string]string {
	return a.attributes
}

// matchesAffinity checks if this agent's attributes satisfy all job affinity constraints.
func (a *Agent) matchesAffinity(affinity map[string]string) bool {
	for k, v := range affinity {
		if a.attributes[k] != v {
			return false
		}
	}
	return true
}

// resolveArtifact picks the first artifact whose Match constraints are satisfied
// by this agent's attributes. Empty Match = catch-all (always matches).
// Returns nil if no artifact matches.
func (a *Agent) resolveArtifact(artifacts []types.Artifact) *types.Artifact {
	for i := range artifacts {
		if a.matchesAffinity(artifacts[i].Match) {
			return &artifacts[i]
		}
	}
	return nil
}

// resolveJobForRun returns a job copy with platform-specific artifact selected.
// Runners expect job.Artifacts to contain at most one entry (the matched one);
// every code path that calls runner.Run must funnel through this first.
func (a *Agent) resolveJobForRun(job *types.Job) (*types.Job, error) {
	if len(job.Artifacts) == 0 {
		return job, nil
	}
	resolved := a.resolveArtifact(job.Artifacts)
	if resolved == nil {
		return nil, fmt.Errorf("no matching artifact for this node's attributes")
	}
	copy := *job
	copy.Artifacts = []types.Artifact{*resolved}
	return &copy, nil
}

// Init performs startup cleanup (removes old task directories and containers)
func (a *Agent) Init() error {
	if err := a.execRunner.Cleanup(); err != nil {
		return err
	}
	// An exec-only host (and every Tamago node) has no Docker daemon to clean.
	// Docker cleanup is best effort: an unreachable daemon must not prevent
	// exec/Hop tasks from starting.
	if runner.DockerPresent(a.config.Runner.DockerSocket) {
		if err := a.dockerRunner.Cleanup(); err != nil {
			log.Printf("Warning: Docker startup cleanup failed: %v", err)
		}
	}
	return nil
}

// runnerFor returns the appropriate runner based on driver
func (a *Agent) runnerFor(driver string) runner.Runner {
	switch driver {
	case types.DriverDocker:
		return a.dockerRunner
	case types.DriverHop:
		if a.hopRunner != nil {
			return a.hopRunner
		}
		// Not a HopOS node: fall through to exec so the task fails with a
		// clear "command is required"-style error instead of a nil panic.
	}
	return a.execRunner
}

// stateLoop is the single goroutine that owns all mutable state. The agent has
// process lifetime, so this loop dies with the process; honoring ctx here would
// race shutdown's final task snapshot through query.
func (a *Agent) stateLoop(ctx context.Context) {
	state := &agentState{
		jobs:  make(map[string]*types.Job),
		tasks: make(map[string]*types.Task),
	}
	for op := range a.ops {
		op(state)
	}
	_ = ctx
}

// do executes an operation on state (fire-and-forget)
func (a *Agent) do(op func(*agentState)) {
	a.ops <- op
}

// query executes an operation and waits for result
func query[T any](a *Agent, fn func(*agentState) T) T {
	result := make(chan T, 1)
	a.ops <- func(s *agentState) {
		result <- fn(s)
	}
	return <-result
}

// Run starts the agent HTTP server and state loop
func (a *Agent) Run(ctx context.Context) error {
	go a.stateLoop(ctx)

	auth := func(h leanhttp.Handler) leanhttp.Handler {
		return httputil.RequireHMAC(a.apiKey, h)
	}

	mux := leanhttp.NewServeMux()
	mux.HandleFunc("/health", a.handleHealth)
	mux.HandleFunc("/capacity", auth(a.handleCapacity))
	mux.HandleFunc("/tasks", auth(a.handleTasks))
	mux.HandleFunc("/run", auth(a.handleRun))
	mux.HandleFunc("/delete/", auth(a.handleDelete))
	mux.HandleFunc("/stop/", auth(a.handleStop))
	mux.HandleFunc("/stop-task/", auth(a.handleStopTask))
	mux.HandleFunc("/logs/", auth(a.handleLogs))
	mux.HandleFunc("/flip", auth(a.handleFlip))
	mux.HandleFunc("/leader", a.handleLeader)

	// Proxy endpoints - forward to leader for cluster-wide operations.
	// Streaming endpoints (SSE, log tailing) route to proxyStreamToLeader
	// so the response is flushed chunk-by-chunk; everything else uses the
	// buffered proxy. ServeMux picks the most-specific pattern, so the
	// logs route wins over the generic /v1/agents/ prefix.
	mux.HandleFunc("/v1/agents", auth(a.proxyToLeader))
	mux.HandleFunc("/v1/agents/", auth(a.proxyToLeader))
	mux.HandleFunc("/v1/agents/{id}/logs/", auth(a.proxyStreamToLeader))
	mux.HandleFunc("/v1/jobs", auth(a.proxyToLeader))
	mux.HandleFunc("/v1/jobs/", auth(a.proxyToLeader))
	mux.HandleFunc("/v1/status", auth(a.proxyToLeader))
	mux.HandleFunc("/v1/tasks", auth(a.proxyToLeader))
	mux.HandleFunc("/v1/events", auth(a.proxyStreamToLeader))

	addr := fmt.Sprintf(":%d", a.config.Node.Port)
	// The slowloris guard (a header deadline, and deliberately no write timeout
	// because /v1/events is a long-lived SSE stream) lives in httputil.NewServer:
	// it is a property of the transport, and only the host transport has one to
	// set — a node's port sits on the node network behind the switch.
	a.server = httputil.NewServer(addr, corsMiddleware(mux.Handler()))

	go a.monitorTasks(ctx)

	// shutdown runs in a goroutine but Run must NOT return until it finishes,
	// otherwise main exits and our tasks orphan to PID 1 while still mid-SIGTERM.
	shutdownDone := make(chan struct{})
	go func() {
		<-ctx.Done()
		a.shutdown()
		close(shutdownDone)
	}()

	log.Printf("Agent listening on %s", addr)
	if err := a.server.ListenAndServe(); err != httputil.ErrServerClosed {
		return err
	}
	<-shutdownDone
	return nil
}

// stopAndRemove removes the logical task, then makes one best-effort attempt to
// release the runner resource. A failed physical cleanup is runner-owned
// quarantine; it must not keep a dead task alive in the scheduler forever.
func (a *Agent) stopAndRemove(task *types.Task) error {
	query(a, func(s *agentState) struct{} {
		delete(s.tasks, task.ID)
		return struct{}{}
	})
	return a.runnerFor(task.Driver).Stop(task)
}

// stopTasks stops each task once, in parallel. Every task ends in GONE.
func (a *Agent) stopTasks(tasks []*types.Task) {
	if len(tasks) == 0 {
		return
	}
	query(a, func(s *agentState) struct{} {
		for _, task := range tasks {
			delete(s.tasks, task.ID)
		}
		return struct{}{}
	})
	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		go func(t *types.Task) {
			defer wg.Done()
			if err := a.runnerFor(t.Driver).Stop(t); err != nil {
				log.Printf("Failed to stop task %s: %v", t.ID, err)
			}
		}(task)
	}
	wg.Wait()
}

// markAllStopping marks every present task, including failed or already
// stopping ones. stopTasks performs the single STOPPING -> GONE transition
// before starting physical cleanup.
func markAllStopping(s *agentState) []*types.Task {
	tasks := make([]*types.Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		// Presence means ownership, independent of state. There is no final
		// stopped state to filter out: after one stop attempt the record is gone.
		task.State = types.TaskStopping
		tasks = append(tasks, task)
	}
	return tasks
}

// StopAllTasks attempts to stop every task once (used when the agent is isolated).
func (a *Agent) StopAllTasks() {
	tasks := query(a, func(s *agentState) []*types.Task { return markAllStopping(s) })
	a.stopTasks(tasks)
	if len(tasks) > 0 {
		log.Printf("Isolation mode: requested cleanup of %d tasks", len(tasks))
	}
}

// shutdown stops all tasks and closes the HTTP server. Close is immediate
// (listener + every connection); the task stop is what bounds total time
// (~11s SIGTERM+SIGKILL worst case) and must not stack behind an HTTP drain —
// systemd's TimeoutStopSec is waiting.
func (a *Agent) shutdown() {
	close(a.shutdownCh)
	log.Println("Agent shutting down...")

	_ = a.server.Close()

	tasks := query(a, func(s *agentState) []*types.Task { return markAllStopping(s) })
	a.stopTasks(tasks)
}

// corsMiddleware adds CORS headers for browser access
func corsMiddleware(next leanhttp.Handler) leanhttp.Handler {
	return func(w leanhttp.ResponseWriter, r *leanhttp.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, PATCH, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Hop-Auth")

		// Chrome Private Network Access: when a public origin (e.g. the
		// hosted GUI at gui.gethop.org) fetches a private/LAN address,
		// Chrome sends this preflight header and blocks the request unless
		// the server explicitly allows it.
		if r.Header.Get("Access-Control-Request-Private-Network") == "true" {
			w.Header().Set("Access-Control-Allow-Private-Network", "true")
		}

		// Handle preflight
		if r.Method == "OPTIONS" {
			w.WriteHeader(leanhttp.StatusOK)
			return
		}

		next(w, r)
	}
}

// GetJob returns a specific job by name
func (a *Agent) GetJob(name string) *types.Job {
	return query(a, func(s *agentState) *types.Job {
		return s.jobs[name]
	})
}

// GetJobs returns all jobs this agent knows about (for JobStore interface)
func (a *Agent) GetJobs() []*types.Job {
	return query(a, func(s *agentState) []*types.Job {
		jobs := make([]*types.Job, 0, len(s.jobs))
		for _, j := range s.jobs {
			jobs = append(jobs, j)
		}
		return jobs
	})
}

// GetPlacedTaskCounts returns a map of jobName -> number of placed tasks on this agent.
// Counts ALL tasks (including failed) because failed tasks exhausted their restart
// counter and should NOT be re-dispatched by the leader.
func (a *Agent) GetPlacedTaskCounts() map[string]int {
	return query(a, func(s *agentState) map[string]int {
		counts := make(map[string]int)
		for _, task := range s.tasks {
			if task.JobName != "" {
				counts[task.JobName]++
			}
		}
		return counts
	})
}

// StoreJob stores a job (used by leader when it learns about remote jobs)
func (a *Agent) StoreJob(job *types.Job) {
	a.do(func(s *agentState) {
		s.jobs[job.Name] = job
		s.stateTime = time.Now()
	})
}

// UpdateJob writes only if the job still exists (JobStore interface): the
// existence check and the write happen in one state-loop op, so a delete
// can never interleave. Snapshot-based rewrites (reconcile's priority
// renumbering) use this so they cannot resurrect a deleted job.
func (a *Agent) UpdateJob(job *types.Job) bool {
	return query(a, func(s *agentState) bool {
		if _, ok := s.jobs[job.Name]; !ok {
			return false
		}
		s.jobs[job.Name] = job
		s.stateTime = time.Now()
		return true
	})
}

// SetJobPriority rewrites only the priority of a stored job (JobStore
// interface). A fresh copy replaces the stored pointer, so readers that
// hold the old pointer never see a field change under them.
func (a *Agent) SetJobPriority(name string, priority int) bool {
	return query(a, func(s *agentState) bool {
		cur, ok := s.jobs[name]
		if !ok {
			return false
		}
		cp := *cur
		p := priority
		cp.Priority = &p
		s.jobs[name] = &cp
		s.stateTime = time.Now()
		return true
	})
}

// keepRolloutFlag makes a job received from the leader carry the store's
// CURRENT Deploying flag, not the one in the payload. Deploying has one
// author, the leader's Update; on the leader node the agent's store IS the
// leader's job store, so a dispatch to ourselves that stored the payload
// as-is wrote "deploying" back over the "done" the update had just set
// (traqqr 2026-09-08: server02 and cloudflared stayed deploying after every
// rollout that ended in a self-dispatch or a reconcile). On a follower the
// flag means nothing, and stays false.
func keepRolloutFlag(s *agentState, job *types.Job) {
	if cur, ok := s.jobs[job.Name]; ok && cur != nil {
		job.Deploying = cur.Deploying
		return
	}
	job.Deploying = false
}

// SetJobDeploying rewrites only the Deploying flag of a stored job (JobStore
// interface); copy-on-write like SetJobPriority.
func (a *Agent) SetJobDeploying(name string, deploying bool) bool {
	return query(a, func(s *agentState) bool {
		cur, ok := s.jobs[name]
		if !ok {
			return false
		}
		cp := *cur
		cp.Deploying = deploying
		s.jobs[name] = &cp
		s.stateTime = time.Now()
		return true
	})
}

// DeleteJob removes a job from the store by name (for JobStore interface)
func (a *Agent) DeleteJob(name string) {
	a.do(func(s *agentState) {
		delete(s.jobs, name)
		s.stateTime = time.Now()
	})
}

// GetStateTime returns when state was last updated
func (a *Agent) GetStateTime() time.Time {
	return query(a, func(s *agentState) time.Time {
		return s.stateTime
	})
}

// SyncJobs updates local jobs from leader (used only while this node is leader;
// durable persistence is the leader's StatePersister, not a per-agent file).
func (a *Agent) SyncJobs(jobs []*types.Job, updated time.Time) {
	a.do(func(s *agentState) {
		for _, job := range jobs {
			s.jobs[job.Name] = job
		}
		s.stateTime = updated
	})
}

// resourceUsage totals what this node has handed out. The rule is the task map
// itself: **every task in s.tasks holds its reservation, whatever its state.**
// A task is inserted when it is admitted (the record IS the reservation) and
// deleted when stop, delete, preemption, shutdown, a restart swap or an
// unplaceable hand-back claims it. A runner that cannot confirm physical cleanup
// keeps that resource quarantined internally. So presence is the question;
// state is not.
//
// That is why there is no state filter here. Filtering on state is how this
// drifted before: Failed was treated as free, so a crashed task's core was
// handed to a new job while its own restart was about to reclaim it, and the two
// then fought over one core (26-07: an unplaceable app slipped in during a
// restart flap and stormed a 3-core node). A crashing task has not given
// anything back — it is about to be restarted right here, or it is waiting for
// an operator.
func (s *agentState) resourceUsage() (cpu int, mem uint64) {
	return s.resourceUsageExcluding("")
}

// resourceUsageExcluding telt als resourceUsage, maar zonder de taken van één
// job — voor de replace-toelating: de opvolger hoeft niet te passen NAAST zijn
// voorganger, die gaat immers direct na de toelating weg. De sharegroup-collapse
// blijft kloppen: draagt alleen de uitgesloten job de pool, dan telt die pool
// niet mee (en claimt de opvolger hem zo weer via zijn eigen tag).
func (s *agentState) resourceUsageExcluding(jobName string) (cpu int, mem uint64) {
	// CPU telt in HELE cores (CPUShares; 1024 = 1 core). Sharegroup-leden delen
	// één pool cores, dus dat pool-CPU telt ÉÉN keer — niet per lid. Zonder deze
	// collapse zou "2 apps in sharegroup web (pool 2)" als 4 cores tellen i.p.v.
	// 2, en zou de node onterecht "vol" melden (HopOS stapelt ze op 2 cores).
	// Geheugen telt WÉL per lid: elke app heeft z'n eigen partitie, sharegroups
	// delen cores, geen RAM.
	seenGroup := map[string]bool{}
	for _, task := range s.tasks {
		if jobName != "" && task.JobName == jobName {
			continue
		}
		mem += task.MemoryLimit
		if grp := s.sharegroupOf(task); grp != "" {
			if seenGroup[grp] {
				continue // pool al geteld — dit lid deelt de cores
			}
			seenGroup[grp] = true
		}
		cpu += task.CPUShares
	}
	return
}

// sharegroupOf geeft de sharegroup-tag van de job achter een task ("" = geen).
// De poolgrootte zit in de CPUShares van diezelfde job (hele cores).
func (s *agentState) sharegroupOf(task *types.Task) string {
	if job := s.jobs[task.JobName]; job != nil {
		return job.Tags["sharegroup"]
	}
	return ""
}

// sharegroupRunning meldt of er al een levende task in sharegroup grp draait —
// dan is de pool-CPU al gereserveerd en kost een nieuw lid er geen cores bij.
func (s *agentState) sharegroupRunning(grp string) bool {
	for _, task := range s.tasks {
		if s.sharegroupOf(task) == grp {
			return true
		}
	}
	return false
}

// allocatePortsForJob allocates host ports appropriate for the job type.
// For all jobs: 0 = dynamic (allocate free port), >0 = fixed (use as-is).
func (a *Agent) allocatePortsForJob(job *types.Job) (map[string]int, error) {
	return allocatePorts(job.Ports)
}

// getFreePort picks a free node port via a wildcard bind (no 127.0.0.1:
// HopOS' network stack has no loopback address).
func getFreePort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

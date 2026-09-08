package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/xinix00/hop/internal/runner"
	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
	"github.com/xinix00/lean/leanhttp"

	"github.com/xinix00/lean/leanrand"
)

// setAuth forwards the caller's X-Hop-Auth signature to the leader. The proxy
// relays the same method, path and body, so the caller's signature stays valid
// against the leader (the whole cluster shares one key). Nothing is forwarded
// when the caller didn't sign — the proxy endpoints sit behind RequireHMAC, so
// a missing signature only happens in empty-key (auth-off) mode.
func (a *Agent) setAuth(call *leanhttp.Call, incoming *leanhttp.Request) {
	if incoming == nil {
		return
	}
	if sig := incoming.Header.Get(httputil.AuthHeader); sig != "" {
		call.SetHeader(httputil.AuthHeader, sig)
	}
}

// proxyToLeader forwards requests to the current leader.
// For long-lived endpoints (SSE events, log tailing) use proxyStreamToLeader
// instead — io.Copy's buffering would delay chunk delivery here.
func (a *Agent) proxyToLeader(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	leaderAddr := a.leaderAddr()
	if leaderAddr == "" {
		httputil.WriteError(w, leanhttp.StatusServiceUnavailable, "no leader available")
		return
	}

	body, ok := proxyBody(w, r)
	if !ok {
		return
	}
	call := leanhttp.Call{
		Method: r.Method,
		URL:    fmt.Sprintf("http://%s%s", leaderAddr, r.Path),
		Body:   body,
	}
	call.SetHeader("Content-Type", r.Header.Get("Content-Type"))
	a.setAuth(&call, r)

	// Geen r.Context() hier: de request-lifetime claimen kost op leanhttp de
	// herbruikbaarheid van de verbinding (Done bezet de leeskant), en dit is
	// het pad dat elke satellite elke vijf seconden loopt. De call is al
	// begrensd door httpClient's proxyTimeout; een weggelopen caller kost dus
	// hooguit die tien seconden, geen handshake per poll.
	resp, err := a.httpClient.Do(call)
	if err != nil {
		// De transportfout gaat MEE. Zonder hem is een 502 niet te
		// diagnosticeren: hij zegt alleen dat de leader niet antwoordde, niet
		// of dat een timeout, een geweigerde verbinding of een kapotte
		// verbinding uit de pool was — drie storingen met drie andere oorzaken.
		httputil.WriteError(w, leanhttp.StatusBadGateway, "failed to contact leader: "+err.Error())
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// proxyStreamToLeader forwards a request to the leader and streams the
// response back chunk-by-chunk, flushing as data arrives. Used for SSE
// (/v1/events) and live log tailing where buffering would delay output.
func (a *Agent) proxyStreamToLeader(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	leaderAddr := a.leaderAddr()
	if leaderAddr == "" {
		httputil.WriteError(w, leanhttp.StatusServiceUnavailable, "no leader available")
		return
	}

	body, ok := proxyBody(w, r)
	if !ok {
		return
	}
	call := leanhttp.Call{
		Method: r.Method,
		URL:    fmt.Sprintf("http://%s%s", leaderAddr, r.Path),
		Body:   body,
	}
	a.setAuth(&call, r)

	// No timeout — these are long-lived streams. streamClient exists for exactly
	// that: a.httpClient carries proxyTimeout, which would cut an SSE tail off.
	resp, err := a.streamClient.DoContext(r.Context(), call)
	if err != nil {
		// De transportfout gaat MEE. Zonder hem is een 502 niet te
		// diagnosticeren: hij zegt alleen dat de leader niet antwoordde, niet
		// of dat een timeout, een geweigerde verbinding of een kapotte
		// verbinding uit de pool was — drie storingen met drie andere oorzaken.
		httputil.WriteError(w, leanhttp.StatusBadGateway, "failed to contact leader: "+err.Error())
		return
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(resp.StatusCode)

	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			// Flush per chunk: that is the whole difference with proxyToLeader.
			if ferr := w.Flush(); ferr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// proxyBody reads the request body the proxy is about to forward. leanhttp sends
// a body as bytes, so it has to be read here — and therefore bounded, because
// this runs before the leader ever sees the request. On the authenticated routes
// RequireHMAC has already read and capped it; this bound is for the ones that
// are not (and for the day one is added).
func proxyBody(w leanhttp.ResponseWriter, r *leanhttp.Request) ([]byte, bool) {
	if r.Body == nil {
		return nil, true
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, proxyMaxBody+1))
	switch {
	case err != nil:
		httputil.WriteError(w, leanhttp.StatusBadRequest, "failed to read body")
		return nil, false
	case len(body) > proxyMaxBody:
		httputil.WriteError(w, leanhttp.StatusRequestEntityTooLarge, "request body too large")
		return nil, false
	}
	return body, true
}

// notifyLeader sends a lightweight event to the leader's /v1/notify endpoint.
// Events: "start" (process started), "started" (healthy), "crash", "stop".
func (a *Agent) notifyLeader(jobName, event string) {
	addr := a.leaderAddr()
	if addr == "" {
		return
	}
	// Mét afzender: bij een hand-back ("unplaceable") moet de leader wéten bij
	// welke agent de plaatsing verviel, anders blijft zijn placed-teller staan
	// en denkt reconcile voorgoed "1/1" (gemeten 01-08: cloudflared voor eeuwig
	// pending terwijl de node ruimte had).
	payload := fmt.Sprintf(`{"job":%q,"event":%q,"agent":%q}`, jobName, event, a.id)
	call := leanhttp.Call{
		Method: leanhttp.MethodPost,
		URL:    fmt.Sprintf("http://%s/v1/notify", addr),
		Body:   []byte(payload),
	}
	call.SetHeader("Content-Type", "application/json")
	httputil.SignCall(&call, a.apiKey)
	resp, err := a.httpClient.Do(call)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// handleLeader returns the current leader address
func (a *Agent) handleLeader(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	resp := map[string]any{"leader": a.leaderAddr()}
	if exp := a.LeaseExpiresAt(); !exp.IsZero() {
		// Only the leader itself knows its lease; a follower reports the
		// leader address alone.
		resp["lease_expires_at"] = exp
	}
	httputil.WriteJSON(w, leanhttp.StatusOK, resp)
}

// handleHealth returns basic health status
func (a *Agent) handleHealth(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	httputil.WriteJSON(w, leanhttp.StatusOK, map[string]string{"status": "ok"})
}

// CapacityResponse shows system resources and actual usage
type CapacityResponse struct {
	CPUCores        int               `json:"cpu_cores"`
	MemoryBytes     uint64            `json:"memory_bytes"`
	CPUUsedShares   int               `json:"cpu_used_shares"`
	MemoryUsedBytes uint64            `json:"memory_used_bytes"`
	TasksRunning    int               `json:"tasks_running"`
	Attributes      map[string]string `json:"attributes,omitempty"`
}

// handleCapacity returns detected system capacity with actual usage from running tasks
func (a *Agent) handleCapacity(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	usage := query(a, func(s *agentState) CapacityResponse {
		cpuUsed, memUsed := s.resourceUsage()
		var running int
		for _, task := range s.tasks {
			if task.State == types.TaskRunning {
				running++
			}
		}
		// Report the effective cap so callers (hopprom, hoplb scheduling) see
		// what hop will actually schedule against, not raw hardware.
		cores := a.effectiveCPUShares() / 1024
		if cores == 0 {
			cores = a.sysInfo.CPUCores
		}
		return CapacityResponse{
			CPUCores:        cores,
			MemoryBytes:     a.effectiveMemoryBytes(),
			CPUUsedShares:   cpuUsed,
			MemoryUsedBytes: memUsed,
			TasksRunning:    running,
			Attributes:      a.attributes,
		}
	})
	httputil.WriteJSON(w, leanhttp.StatusOK, usage)
}

// handleTasks returns all running tasks
func (a *Agent) handleTasks(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	tasks := query(a, func(s *agentState) []*types.Task {
		result := make([]*types.Task, 0, len(s.tasks))
		for _, t := range s.tasks {
			cp := *t // copy: marshalled on this HTTP goroutine while the monitor writes State/CPU/Mem
			result = append(result, &cp)
		}
		return result
	})
	httputil.WriteJSON(w, leanhttp.StatusOK, tasks)
}

// handleRun starts a new job. Met ?replace=1 vervangt hij de lopende taken van
// dezelfde job: de toelating rekent hun reservering dan niet mee (de opvolger
// hoeft niet naast zijn voorganger te passen) en de oude taken worden pas ná
// een geslaagde toelating gestopt — een weigering laat ze dus ongemoeid
// draaien. Dat is het update-pad van de leader op een node zonder headroom:
// vóór deze vorm werd zo'n update via de preemptie-pas over een búúrman
// uitgevochten (gemeten 01-08, welcome-update offerde cloudflared).
func (a *Agent) handleRun(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	if r.Method != leanhttp.MethodPost {
		leanhttp.Error(w, "method not allowed", leanhttp.StatusMethodNotAllowed)
		return
	}
	replace := r.Query().Get("replace") == "1"

	var job types.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		httputil.WriteJSON(w, leanhttp.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	// Check affinity before capacity (agent-side: leader stays dumb)
	if !a.matchesAffinity(job.Affinity) {
		httputil.WriteJSON(w, leanhttp.StatusNotAcceptable, map[string]string{
			"error": "affinity mismatch",
		})
		return
	}

	// Ensure driver is set, then create the task.
	if job.Driver == "" {
		job.Driver = types.DriverFor(job.Image)
	}
	// A HopOS app runs on whole cores (slots): cpu_shares picks the SMP width and
	// a fractional request can't map onto a partial slot. Round up to the next
	// whole core (min one) so HOP's accounting matches exactly what HopOS
	// allocates — HOP helps HopOS. The capacity check below counts CORES (on a
	// HopOS node CPUCores == NumCores, agentboot), so a full node rejects HERE,
	// before a slot is allocated, instead of accepting and then failing in the
	// runner when no core is free.
	if job.Driver == types.DriverHop {
		cores := (job.CPUShares + 1023) / 1024
		if cores < 1 {
			cores = 1
		}
		job.CPUShares = cores * 1024
		// Zelfde principe voor geheugen: HopOS deelt partities uit in blokken
		// van 2MB (de kooi-map werkt per blok), dus de POOL-kosten van een
		// task zijn zijn memory_limit naar boven afgerond — een limit van 21MB
		// kost een partitie van 22. Zonder deze afronding zei de admissie ja
		// (som van limits ≤ pool) waar de runner nee moest zeggen (som van
		// partities > pool), en pingpongde een onplaatsbare job in een
		// hand-back-lus die de agent-poort verstikte en via de gemiste
		// watchdog-pets de node velde (gemeten 17/18-08, LicheeRV). De
		// afronding hier maakt de weigering wat hij hoort te zijn:
		// beschikbaar < kosten → 503, vóór er ook maar iets geplaatst is.
		const hopBlockBytes = 2 << 20
		if job.MemoryLimit > 0 {
			job.MemoryLimit = (job.MemoryLimit + hopBlockBytes - 1) / hopBlockBytes * hopBlockBytes
		}
	}
	task := newTask(&job)

	// Check capacity AND add task to state atomically.
	// The task in state IS the capacity reservation — no separate reservation needed.
	var oldTasks []*types.Task
	added := query(a, func(s *agentState) bool {
		exclude := ""
		if replace {
			exclude = job.Name
		}
		usedCPU, usedMem := s.resourceUsageExcluding(exclude)
		// Een sharegroup-lid dat een AL draaiende pool joint kost geen extra
		// cores: het deelt de al-gereserveerde pool (HopOS stapelt het erbij).
		// Alleen de eerste van een groep — of een dedicated/SMP-app — claimt
		// cores. Zo matcht de admissie wat PlaceCage op de node werkelijk doet.
		newCPU := job.CPUShares
		if grp := job.Tags["sharegroup"]; grp != "" && s.sharegroupRunning(grp) {
			newCPU = 0
		}
		if job.CPUShares > 0 && usedCPU+newCPU > a.effectiveCPUShares() {
			return false
		}
		if job.MemoryLimit > 0 && usedMem+job.MemoryLimit > a.effectiveMemoryBytes() {
			return false
		}
		// A sum is not a hole, and this is the other half of the 2 MB rounding
		// above. That rounding made the sum agree with what a partition costs;
		// it cannot see FRAGMENTATION. On a node whose pool is several regions
		// the bytes can be free while no single piece is large enough, so the
		// sum still says yes and the same hand-back loop returns — the loop that
		// chokes the agent port and fells the node through missed watchdog pets.
		//
		// MEASURED 19-08 on a LicheeRV: pool 222 MB with 126 and 36 placed, so
		// 60 MB free in holes of 28 and 32. A 36 MB job was admitted on the sum
		// and refused by the placer every five seconds; the reported capacity
		// flapped between 162 and 198 MB, and inside that window the node
		// refused a different 28 MB job that did fit. The node fell over three
		// times during that session.
		//
		// Not for a replace: there the predecessor still holds its partition at
		// this point (it is stopped after admission, on purpose), so the largest
		// hole is legitimately too small and the rolling update must proceed.
		if job.MemoryLimit > 0 && !replace && job.Driver == types.DriverHop {
			if pr, ok := a.hopRunner.(interface{ PoolLargest() uint64 }); ok {
				if largest := pr.PoolLargest(); largest > 0 && job.MemoryLimit > largest {
					return false
				}
			}
		}
		if replace {
			// De voorgangers gaan via Stopping naar GONE. De runner-release gebeurt
			// hieronder vóór de opvolger start; runner-quarantaine voorkomt hergebruik
			// wanneer die release niet bevestigd kan worden.
			for _, t := range s.tasks {
				if t.JobName == job.Name && t.ID != task.ID {
					t.State = types.TaskStopping
					oldTasks = append(oldTasks, t)
				}
			}
		}
		s.jobs[job.Name] = &job
		s.tasks[task.ID] = task
		return true
	})

	if !added {
		httputil.WriteJSON(w, leanhttp.StatusServiceUnavailable, map[string]string{
			"error": "insufficient capacity",
		})
		return
	}

	// Accept job immediately (fire-and-forget)
	httputil.WriteJSON(w, leanhttp.StatusAccepted, map[string]string{
		"status":  "accepted",
		"job":     job.Name,
		"message": "job accepted, starting in background",
	})

	// Start process in background (task already in state for capacity reservation)
	start := func() {
		// Replace: eerst de voorgangers écht weg — hun core en partitie moeten
		// vrij zijn vóór de opvolger plaatst (op HopOS is dat letterlijk
		// dezelfde partitie-pool; dit is ook wat fragmentatie voorkomt: de
		// opvolger krijgt de vrijgekomen regio terug).
		for _, old := range oldTasks {
			if err := a.stopAndRemove(old); err != nil {
				log.Printf("Replace of job %s: failed to stop predecessor %.8s: %v", job.Name, old.ID, err)
			}
		}
		if err := a.startJob(&job, task); err != nil {
			if errors.Is(err, runner.ErrNoCapacity) {
				a.releaseUnplaceable(job.Name, task, err)
				return
			}
			log.Printf("Failed to start job %s: %v", job.Name, err)
			a.do(func(s *agentState) {
				if t := s.tasks[task.ID]; t != nil {
					t.State = types.TaskFailed
				}
			})
			a.notifyLeader(job.Name, "crash")
			a.restartTask(task, false)
		}
	}
	go start()
}

// handleDelete deletes a job and cleans up all its tasks (by job name)
func (a *Agent) handleDelete(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	if r.Method != leanhttp.MethodDelete {
		leanhttp.Error(w, "method not allowed", leanhttp.StatusMethodNotAllowed)
		return
	}

	jobName := strings.TrimPrefix(r.Path, "/delete/")
	if jobName == "" {
		httputil.WriteJSON(w, leanhttp.StatusBadRequest, map[string]string{"error": "job name required"})
		return
	}

	deleted := a.deleteJob(jobName)
	httputil.WriteJSON(w, leanhttp.StatusOK, map[string]int{"deleted": deleted})
}

// handleStop stops all tasks for a job WITHOUT removing the job definition (by job name).
// Used by the leader for preemption — the job definition must remain for rescheduling.
func (a *Agent) handleStop(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	if r.Method != leanhttp.MethodPost {
		leanhttp.Error(w, "method not allowed", leanhttp.StatusMethodNotAllowed)
		return
	}

	jobName := strings.TrimPrefix(r.Path, "/stop/")
	if jobName == "" {
		httputil.WriteJSON(w, leanhttp.StatusBadRequest, map[string]string{"error": "job name required"})
		return
	}

	stopped := a.stopJobTasks(jobName)
	httputil.WriteJSON(w, leanhttp.StatusOK, map[string]int{"stopped": stopped})
}

// handleStopTask stops a single specific task by task ID.
// Used by rolling and blue-green updates to stop precise old instances.
func (a *Agent) handleStopTask(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	if r.Method != leanhttp.MethodPost {
		leanhttp.Error(w, "method not allowed", leanhttp.StatusMethodNotAllowed)
		return
	}

	taskID := strings.TrimPrefix(r.Path, "/stop-task/")
	if taskID == "" {
		httputil.WriteJSON(w, leanhttp.StatusBadRequest, map[string]string{"error": "task id required"})
		return
	}

	task := query(a, func(s *agentState) *types.Task {
		if t := s.tasks[taskID]; t != nil {
			t.State = types.TaskStopping
			return t
		}
		return nil
	})

	if task == nil {
		httputil.WriteJSON(w, leanhttp.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}

	stop := func() {
		err := a.stopAndRemove(task)
		if err != nil {
			log.Printf("Failed to stop task %s: %v", taskID, err)
		}
	}
	go stop()

	httputil.WriteJSON(w, leanhttp.StatusOK, map[string]string{"stopped": taskID})
}

// stopJobTasks stops all tasks for a job WITHOUT removing the job definition.
// Used for preemption so the job remains in the store for future rescheduling.
func (a *Agent) stopJobTasks(jobName string) int {
	tasks := query(a, func(s *agentState) []*types.Task {
		var tasks []*types.Task
		for _, task := range s.tasks {
			if task.JobName == jobName {
				task.State = types.TaskStopping
				tasks = append(tasks, task)
			}
		}
		return tasks
	})

	a.stopTasks(tasks)
	log.Printf("Stopped tasks for job %s: %d tasks (job definition preserved)", jobName, len(tasks))
	return len(tasks)
}

// newTask creates a Task from a Job.
func newTask(job *types.Job) *types.Task {
	return &types.Task{
		ID:      leanrand.ID(16),
		JobName: job.Name,
		Driver:  job.Driver,
		Image:   job.Image,
		// Geboren als Queued, niet Running: tussen aanname en echte start zit
		// de hele artifact-download, en die kan op een klein board minuten
		// duren. De capaciteit telt vanaf hier (aanwezigheid, niet state);
		// Running wordt het pas als de runner de app echt draaiend heeft.
		State:       types.TaskQueued,
		CPUShares:   job.CPUShares,
		MemoryLimit: job.MemoryLimit,
		StartedAt:   time.Now(),
	}
}

// startJob prepares and starts a job process. The task must be pre-created
// (via newTask). startJob allocates ports, runs the process, and stores in state.
func (a *Agent) startJob(job *types.Job, task *types.Task) error {
	if job.Driver == "" {
		job.Driver = types.DriverFor(job.Image)
	}

	// Registreer de job vóór de (trage) runner-start; de post-run-guard
	// hieronder weigert dan precies het geval "job verdween ONDERWEG" (een
	// delete die deze start kruiste) zonder directe aanroepers (restarts,
	// tests) te breken die niet via de /run-accept binnenkwamen.
	// NIET onvoorwaardelijk: een delete die tussen de /run-accept en deze
	// goroutine viel, heeft de accepted task al op Stopping gezet — het
	// job-record dan terugzetten liet het als spook op de agent achter (en
	// op een leader-node kon het via de store zelfs cluster-breed
	// herrijzen). task.State lezen kan alleen ín de state-op (het muteert
	// in de state-loop); nieuwe tasks staan op TaskRunning, dus directe
	// aanroepers passeren gewoon.
	fresh := query(a, func(s *agentState) bool {
		if task.State == types.TaskStopping {
			return false
		}
		s.jobs[job.Name] = job
		return true
	})
	if !fresh {
		log.Printf("job %s deleted before start of task %.8s — skipping", job.Name, task.ID)
		return nil
	}

	// Resolve platform-specific artifact (runtime only — don't modify stored job)
	runJob, err := a.resolveJobForRun(job)
	if err != nil {
		return err
	}

	ports, err := a.allocatePortsForJob(runJob)
	if err != nil {
		return fmt.Errorf("failed to allocate ports: %w", err)
	}
	task.Ports = ports

	// Runner fills in Pid and registers internal state
	// ER_ATTR_* env vars are injected by the runner itself (via Config.NodeAttrs)
	if err := a.runnerFor(runJob.Driver).Run(runJob, task); err != nil {
		return fmt.Errorf("failed to start: %w", err)
	}
	// Vanaf hier draait de app pas — zie de stempel in restartTask voor waarom
	// de aanmaaktijd geen uptime is.
	task.StartedAt = time.Now()

	// Store in state. If /stop marked the task as Stopping while we were
	// starting, don't re-add it (prevents ghost tasks after preemption race).
	// Same guard for the JOB record: the accept step registered it, but if a
	// delete swept it away while the runner was starting, re-adding it here
	// would resurrect a deleted job (the last zombie of the 15-07 hunt) —
	// treat that exactly like the ghost-task case.
	alive := query(a, func(s *agentState) bool {
		if _, ok := s.jobs[job.Name]; !ok {
			return false // job deleted mid-start → ghost: stop it again below
		}
		s.jobs[job.Name] = job
		// Een /stop die de start kruiste heeft de task op Stopping gezet — dan
		// is dit een ghost. Anders is dit HET moment waarop de task echt
		// draait: Queued/Downloading (de startfase) wordt hier Running.
		if task.State == types.TaskStopping {
			return false
		}
		task.State = types.TaskRunning
		s.tasks[task.ID] = task
		return true
	})
	if !alive {
		log.Printf("ghost task %.8s (job %s): deleted mid-start — stopping again", task.ID, job.Name) // freeze-forensiek
		if err := a.stopAndRemove(task); err != nil {
			log.Printf("ghost task %.8s cleanup failed: %v", task.ID, err)
		}
		return nil
	}

	log.Printf("Started task %s (job %s) with ports %v, pid %d", task.ID, job.Name, ports, task.Pid)
	if job.HealthCheck != nil {
		go a.notifyLeader(job.Name, "start")
	} else {
		go a.notifyLeader(job.Name, "started")
	}
	return nil
}

// allocatePorts allocates ports based on job port config
func allocatePorts(portConfig map[string]int) (map[string]int, error) {
	ports := make(map[string]int)
	for name, fixed := range portConfig {
		if fixed > 0 {
			if !isPortAvailable(fixed) {
				return nil, fmt.Errorf("port %d for %s is already in use", fixed, name)
			}
			ports[name] = fixed
		} else {
			port, err := getFreePort()
			if err != nil {
				return nil, fmt.Errorf("failed to get port for %s: %w", name, err)
			}
			ports[name] = port
		}
	}
	return ports, nil
}

// isPortAvailable checks if a port is available for binding. It probes the
// wildcard address: the agent is asking "is this node port free", and on
// HopOS the network stack has no loopback address at all.
func isPortAvailable(port int) bool {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return false
	}
	listener.Close()
	return true
}

// releaseUnplaceable handles a task the node accepted but cannot PLACE
// (runner.ErrNoCapacity: no free core, or the sharegroup pool doesn't fit).
// That is not a crash, so restarting is exactly wrong — every retry
// re-downloads the image and fails again, and the churn starves the tasks that
// DO run (measured 26-07: one unplaceable app pegged a whole 3-core node while
// the leader kept re-dispatching it).
//
// The task never ran and the runner already released its cage, so we drop it
// from state. That also restores the capacity accounting, which is what stops
// the storm: the next attempt is refused up front by the admission check (503),
// so the leader either places the job on another node or leaves it pending for
// reconciliation — "ask everyone, drop it where one says yes".
func (a *Agent) releaseUnplaceable(jobName string, task *types.Task, err error) {
	log.Printf("Job %s cannot be placed on this node (%v) — task %.8s handed back to the leader, not restarted", jobName, err, task.ID)
	a.do(func(s *agentState) { delete(s.tasks, task.ID) })
	a.notifyLeader(jobName, "unplaceable")
}

// restartTask restarts a failed task. ran zegt of de vorige poging de app ook
// écht aan de praat had (een crash of een gezakte health-check) of dat het
// starten zélf mislukte — alleen het eerste kan een schone lei verdienen.
func (a *Agent) restartTask(task *types.Task, ran bool) {
	needsCleanup := true
	for {
		// Release the previous attempt before deciding whether it may be retried.
		// In particular, the terminal max-restarts path must not strand runner
		// bookkeeping, cgroups, containers or HopOS slots.
		if needsCleanup {
			if err := a.runnerFor(task.Driver).Stop(task); err != nil {
				log.Printf("Task %s cleanup failed: %v", task.ID, err)
			}
			needsCleanup = false
		}

		job := a.GetJob(task.JobName)
		if job == nil {
			log.Printf("Cannot restart task %s: job %s not found", task.ID, task.JobName)
			query(a, func(s *agentState) struct{} {
				delete(s.tasks, task.ID)
				return struct{}{}
			})
			return
		}

		maxRestarts := defaultMaxRestarts
		if job.MaxRestarts != nil {
			maxRestarts = *job.MaxRestarts
		}
		restartWindow := job.RestartWindow
		if restartWindow == 0 {
			restartWindow = defaultRestartWindow
		}

		restartCount := query(a, func(s *agentState) int {
			if t := s.tasks[task.ID]; t != nil {
				// Only genuine uptime earns a clean restart budget. A slow failed
				// start must never reset its own counter.
				if ran && !t.StartedAt.IsZero() && time.Since(t.StartedAt) > restartWindow {
					t.RestartCount = 0
				}
				t.LastFailedAt = time.Now()
				return t.RestartCount
			}
			return 0
		})

		// -1 = unlimited restarts; 0 = no restarts at all. Cleanup above has
		// already attempted to return the runner resource before TaskFailed
		// becomes final.
		if maxRestarts >= 0 && restartCount >= maxRestarts {
			log.Printf("Task %s exceeded max restarts (%d within %s), giving up", task.ID, maxRestarts, restartWindow)
			query(a, func(s *agentState) struct{} {
				if t := s.tasks[task.ID]; t != nil {
					t.State = types.TaskFailed
				}
				return struct{}{}
			})
			return
		}

		if restartCount > 0 {
			delay := restartDelay(restartCount)
			log.Printf("Task %s restart #%d, waiting %s before retry", task.ID, restartCount, delay)
			next := time.Now().Add(delay)
			query(a, func(s *agentState) struct{} {
				if t := s.tasks[task.ID]; t != nil {
					t.NextRestartAt = next
				}
				return struct{}{}
			})
			if !a.waitRestart(delay) {
				return
			}
		}

		// Resolve platform-specific artifact (same invariant as startJob).
		runJob, err := a.resolveJobForRun(job)
		if err != nil {
			log.Printf("Cannot restart task %s: %v", task.ID, err)
			query(a, func(s *agentState) struct{} {
				if t := s.tasks[task.ID]; t != nil {
					t.State = types.TaskFailed
				}
				return struct{}{}
			})
			return
		}

		ports, err := a.allocatePortsForJob(runJob)
		if err != nil {
			log.Printf("Failed to allocate ports for restart: %v", err)
			query(a, func(s *agentState) struct{} {
				if t := s.tasks[task.ID]; t != nil {
					t.RestartCount++
				}
				return struct{}{}
			})
			ran = false
			continue
		}

		// Don't sneak a replacement past shutdown's snapshot.
		select {
		case <-a.shutdownCh:
			return
		default:
		}

		// Atomic swap: preserve the reservation without a scheduling gap.
		replacement := newTask(job)
		replacement.Ports = ports
		swapped := query(a, func(s *agentState) bool {
			old := s.tasks[task.ID]
			if old == nil {
				return false
			}
			replacement.RestartCount = old.RestartCount + 1
			replacement.LastFailedAt = old.LastFailedAt
			delete(s.tasks, task.ID)
			s.tasks[replacement.ID] = replacement
			return true
		})
		if !swapped {
			log.Printf("Task %s disappeared from state during restart", task.ID)
			return
		}
		// Close the gap between the pre-swap check and shutdown's snapshot. If
		// shutdown won before the publication, no runner resource exists yet and
		// this fresh reservation can be dropped directly. If it wins after this
		// check, the replacement is visible to its snapshot and the post-Run ghost
		// guard below handles a Stop that races the driver's start.
		select {
		case <-a.shutdownCh:
			query(a, func(s *agentState) struct{} {
				delete(s.tasks, replacement.ID)
				return struct{}{}
			})
			return
		default:
		}

		if err := a.runnerFor(runJob.Driver).Run(runJob, replacement); err != nil {
			if errors.Is(err, runner.ErrNoCapacity) {
				a.releaseUnplaceable(job.Name, replacement, err)
				return
			}
			log.Printf("Failed to restart task %s: %v", task.ID, err)
			query(a, func(s *agentState) struct{} {
				if t := s.tasks[replacement.ID]; t != nil {
					// Keep failed starts observable while the next retry backs off.
					// The next loop iteration makes one cleanup attempt for any
					// partially acquired runner resource before it retries.
					t.State = types.TaskFailed
				}
				return struct{}{}
			})
			task, ran, needsCleanup = replacement, false, true
			continue
		}

		alive := query(a, func(s *agentState) bool {
			t := s.tasks[replacement.ID]
			if t == nil || t.State == types.TaskStopping {
				return false
			}
			t.State = types.TaskRunning
			t.StartedAt = time.Now()
			return true
		})
		if !alive {
			// Shutdown/delete may have removed the reservation just before Run
			// published driver ownership. Make one cleanup attempt now that ownership
			// exists; the logical task remains gone even if the runner quarantines it.
			log.Printf("ghost restart task %.8s (job %s): removed during start — stopping again", replacement.ID, job.Name)
			if err := a.stopAndRemove(replacement); err != nil {
				log.Printf("ghost restart task %.8s cleanup failed: %v", replacement.ID, err)
			}
			return
		}

		log.Printf("Restarted task %s -> %s (job %s), restart #%d", task.ID, replacement.ID, job.Name, replacement.RestartCount)
		go a.notifyLeader(job.Name, "started")
		return
	}
}

const maxRestartDelay = 30 * time.Second

// restartDelay saturates before shifting, so large/unlimited restart counts can
// never overflow a duration into zero or negative tight-loop delays.
func restartDelay(restartCount int) time.Duration {
	if restartCount <= 0 {
		return 0
	}
	base := maxRestartDelay
	if restartCount <= 5 {
		base = time.Second << uint(restartCount-1)
	}
	delay := leanrand.Jitter(base)
	if delay < 0 {
		return 0
	}
	if delay > maxRestartDelay {
		return maxRestartDelay
	}
	return delay
}

func (a *Agent) waitRestart(delay time.Duration) bool {
	select {
	case <-a.shutdownCh:
		return false
	default:
	}
	if a.restartWait != nil {
		return a.restartWait(delay)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-a.shutdownCh:
		return false
	case <-timer.C:
		return true
	}
}

// deleteJob removes job definition AND cleans up all tasks by job name
func (a *Agent) deleteJob(jobName string) int {
	tasks := query(a, func(s *agentState) []*types.Task {
		delete(s.jobs, jobName)
		// De klok mee: zonder deze bump geldt een sync-payload van vóór deze
		// delete nog als "nieuwer" en her-importeert hij de job (15-07).
		s.stateTime = time.Now()
		var tasks []*types.Task
		for _, task := range s.tasks {
			if task.JobName == jobName {
				task.State = types.TaskStopping
				tasks = append(tasks, task)
			}
		}
		return tasks
	})

	a.stopTasks(tasks)
	log.Printf("Deleted job %s: cleanup requested for %d tasks", jobName, len(tasks))

	go a.notifyLeader(jobName, "stop")

	return len(tasks)
}

// handleLogs streams task logs (stdout or stderr)
func (a *Agent) handleLogs(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	parts := strings.Split(strings.TrimPrefix(r.Path, "/logs/"), "/")
	if len(parts) != 2 {
		leanhttp.Error(w, "usage: /logs/{taskID}/stdout or /logs/{taskID}/stderr", leanhttp.StatusBadRequest)
		return
	}

	taskID := parts[0]
	stream := parts[1]

	var get func(runner.Runner) *runner.LogBroadcaster
	switch stream {
	case "stdout":
		get = func(r runner.Runner) *runner.LogBroadcaster { return r.GetStdout(taskID) }
	case "stderr":
		get = func(r runner.Runner) *runner.LogBroadcaster { return r.GetStderr(taskID) }
	default:
		leanhttp.Error(w, "stream must be stdout or stderr", leanhttp.StatusBadRequest)
		return
	}
	broadcaster := get(a.execRunner)
	if broadcaster == nil {
		broadcaster = get(a.dockerRunner)
	}
	if broadcaster == nil && a.hopRunner != nil {
		broadcaster = get(a.hopRunner)
	}

	if broadcaster == nil {
		leanhttp.Error(w, "task not found or not running", leanhttp.StatusNotFound)
		return
	}

	// De lifetime VÓÓR de eerste write claimen (zelfde regel als /v1/events):
	// leanhttp's Request.Done is na de eerste Flush terecht te laat.
	done := r.Context().Done()

	sse := httputil.SSEWriter(w)

	logCh := broadcaster.Subscribe()
	defer broadcaster.Unsubscribe(logCh)

	for {
		select {
		case line, ok := <-logCh:
			if !ok {
				return
			}
			sse.WriteData(line)
		case <-done:
			return
		}
	}
}

// handleFlip vraagt de node zijn eigen kern live te vervangen (de kern-flip
// van HopOS, docs/kern-flip.md in hop-os): POST {"url","sha256"} — de node
// haalt de bundel op, controleert de som en springt erin terwijl de taken
// doordraaien. Dít is de plek voor dat verzoek en niet een open poort: het
// endpoint zit achter dezelfde HMAC als job-dispatch, want wie een kern mag
// aanleveren mag alles.
//
// Het antwoord is een 202, niet het resultaat: een geslaagde flip keert per
// definitie nooit terug (de kern die antwoordt bestaat dan niet meer). De
// aanroeper ziet het vervolg op de console van de node en aan de her-
// registratie bij de leader; een mislukte flip logt hier en de node draait
// gewoon door.
func (a *Agent) handleFlip(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	if r.Method != leanhttp.MethodPost {
		leanhttp.Error(w, "method not allowed", leanhttp.StatusMethodNotAllowed)
		return
	}
	if a.onFlip == nil {
		httputil.WriteJSON(w, leanhttp.StatusNotImplemented, map[string]string{
			"error": "this node cannot flip its kernel (not a HopOS node, or flipping is disabled)",
		})
		return
	}
	var req struct {
		URL    string `json:"url"`
		SHA256 string `json:"sha256"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteJSON(w, leanhttp.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if !strings.Contains(req.URL, "://") {
		httputil.WriteJSON(w, leanhttp.StatusBadRequest, map[string]string{"error": "url must be absolute"})
		return
	}
	if len(req.SHA256) != 64 {
		httputil.WriteJSON(w, leanhttp.StatusBadRequest, map[string]string{"error": "sha256 must be 64 hex characters"})
		return
	}
	for _, c := range req.SHA256 {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			httputil.WriteJSON(w, leanhttp.StatusBadRequest, map[string]string{"error": "sha256 must be 64 hex characters"})
			return
		}
	}
	log.Printf("agent: kernel flip requested: %s (sha256 %s…)", req.URL, req.SHA256[:12])
	// In een eigen goroutine: een geslaagde flip keert nooit terug, en dit
	// antwoord moet de deur nog uit. De fetch geeft ruimte genoeg.
	go func() {
		if err := a.onFlip(req.URL, req.SHA256); err != nil {
			log.Printf("agent: kernel flip failed: %v", err)
		}
	}()
	httputil.WriteJSON(w, leanhttp.StatusAccepted, map[string]string{
		"status": "flip requested — the node fetches, verifies and replaces its kernel; watch its console and its re-registration",
	})
}

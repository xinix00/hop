package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/xinix00/hop/internal/leader"
	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/httputil"
	"github.com/xinix00/lean/leanhttp"
)

// Server provides the HTTP API for the leader
type Server struct {
	leader *leader.Leader
	// mux is kept next to the server so handler tests route through the real
	// route table instead of a copy of it — precedence is part of the API.
	mux         *leanhttp.Mux
	server      *httputil.Server
	clusterName string
	apiKey      string

	// client proxies to agents. One per server, not one per request: it pools
	// connections, and it deliberately has no timeout because a log tail is a
	// stream that must be allowed to stay open.
	client *httputil.Client

	// lifecycleCtx is narrower than the process context: it ends as soon as
	// this leader server stops. Long-lived handlers merge it with the caller's
	// request context so an old leader cannot retain SSE subscriptions, agent
	// log bodies or pooled connections after leadership moves elsewhere.
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	stopOnce        sync.Once
}

// NewServer creates a new API server
func NewServer(l *leader.Leader, addr string, apiKey string, clusterName string) *Server {
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	s := &Server{
		leader:          l,
		clusterName:     clusterName,
		apiKey:          apiKey,
		client:          &httputil.Client{},
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
	}

	auth := func(h leanhttp.Handler) leanhttp.Handler {
		return httputil.RequireHMAC(apiKey, h)
	}

	mux := leanhttp.NewServeMux()

	// Health (public - needed for discovery)
	mux.HandleFunc("/health", s.handleHealth)

	// Agents (authenticated)
	mux.HandleFunc("GET /v1/agents", auth(s.handleGetAgents))
	mux.HandleFunc("POST /v1/agents", auth(s.handleRegisterAgent))
	mux.HandleFunc("POST /v1/heartbeat", auth(s.handleHeartbeat))
	mux.HandleFunc("DELETE /v1/agents/", auth(s.handleUnregisterAgent))
	mux.HandleFunc("GET /v1/agents/{agent_id}/capacity", auth(s.handleAgentCapacity))
	mux.HandleFunc("GET /v1/agents/{agent_id}/logs/{task_id}/{stream}", auth(s.withLifecycle(s.handleAgentLogs)))

	// Jobs (authenticated)
	mux.HandleFunc("GET /v1/jobs", auth(s.handleGetJobs))
	mux.HandleFunc("POST /v1/jobs", auth(s.handleApplyJob))
	mux.HandleFunc("DELETE /v1/jobs/", auth(s.handleDeleteJob))

	// Status (authenticated)
	mux.HandleFunc("GET /v1/status", auth(s.handleStatus))
	mux.HandleFunc("GET /v1/tasks", auth(s.handleTasks))

	// Events (authenticated)
	mux.HandleFunc("GET /v1/events", auth(s.withLifecycle(s.handleEvents)))
	mux.HandleFunc("POST /v1/notify", auth(s.handleNotify))

	// Per-job endpoints (authenticated)
	mux.HandleFunc("GET /v1/jobs/{name}/status", auth(s.handleJobStatus))
	mux.HandleFunc("PATCH /v1/jobs/{name}/priority", auth(s.handlePatchJobPriority))

	// The slowloris guard lives in httputil.NewServer: headers (incl. the
	// X-Hop-Auth signature) must arrive promptly, and there is no WriteTimeout
	// because /v1/events is a long-lived SSE stream.
	s.mux = mux
	s.server = httputil.NewServer(addr, mux.Handler())

	return s
}

// Run starts the HTTP server
func (s *Server) Run(ctx context.Context) error {
	runDone := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-ctx.Done():
			s.Stop()
		case <-runDone:
		}
	}()

	log.Printf("API server listening on %s", s.server.Addr())
	err := s.server.ListenAndServe()
	close(runDone)
	<-watchDone
	if err != httputil.ErrServerClosed {
		// Bind/accept failures bypass the context watcher. Retire client pools and
		// any connection that was accepted before the listener failed.
		s.Stop()
		return err
	}
	return nil
}

// Stop shuts down the server: first end every stream (the lifecycle ctx), then
// close listener and connections. Close is immediate — in-flight buffered
// handlers are milliseconds and every caller retries, so an ex-leader must
// never sit out a drain timer on each leadership change.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		// Cancel streams first. In particular this closes the upstream body
		// read in handleAgentLogs, which may otherwise have no data and no
		// natural return point.
		s.lifecycleCancel()
		s.client.CloseIdle()
		_ = s.server.Close()
		s.client.CloseIdle()
	})
}

// withLifecycle binds a long-lived request to both ends of its ownership: the
// client connection and this particular leader server. It is deliberately used
// only for streaming routes; asking for Request.Context claims the
// connection's read side (leanhttp's Done watcher), which is correct for a
// stream but would cost every ordinary request/response its keep-alive.
func (s *Server) withLifecycle(next leanhttp.Handler) leanhttp.Handler {
	return func(w leanhttp.ResponseWriter, r *leanhttp.Request) {
		ctx, cancel := context.WithCancel(r.Context())
		stopLifecycleCancel := context.AfterFunc(s.lifecycleCtx, cancel)
		if s.lifecycleCtx.Err() != nil {
			cancel()
		}
		defer func() {
			stopLifecycleCancel()
			cancel()
		}()
		next(w, r.WithContext(ctx))
	}
}

func (s *Server) handleHealth(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	httputil.WriteJSON(w, leanhttp.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleGetAgents(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	httputil.WriteJSON(w, leanhttp.StatusOK, s.leader.GetAgents())
}

// handleHeartbeat is puur liveness: id/endpoint/version in, "ok" uit. De
// job-lijsten die hier vroeger heen en weer reisden zijn gesloopt (16-07):
// gewenste staat heeft één auteur (de leader, gecommit naar S3) en agents
// zijn uitvoerders. Onbekende velden in oudere agents worden genegeerd.
func (s *Server) handleHeartbeat(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	var req struct {
		ID         string `json:"id"`
		Endpoint   string `json:"endpoint"`
		Version    string `json:"version,omitempty"`
		TempMilliC int    `json:"temp_milli_c,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "invalid json")
		return
	}
	if req.ID == "" || req.Endpoint == "" {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "id and endpoint required")
		return
	}

	if !s.leader.Heartbeat(req.ID, req.Version, req.TempMilliC) {
		httputil.WriteError(w, leanhttp.StatusNotFound, "not registered")
		return
	}
	httputil.WriteJSON(w, leanhttp.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleRegisterAgent(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	var req struct {
		ID       string         `json:"id"`
		Endpoint string         `json:"endpoint"`
		Version  string         `json:"version,omitempty"`
		Placed   map[string]int `json:"placed,omitempty"` // jobName -> count
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "invalid json")
		return
	}
	if req.ID == "" || req.Endpoint == "" {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "id and endpoint required")
		return
	}

	if !s.leader.RegisterAgent(req.ID, req.Endpoint, req.Version, req.Placed) {
		httputil.WriteError(w, leanhttp.StatusConflict, fmt.Sprintf("agent %s already registered with different endpoint", req.ID))
		return
	}
	httputil.WriteJSON(w, leanhttp.StatusOK, map[string]any{
		"status":     "registered",
		"jobs":       s.leader.GetJobs(),
		"state_time": s.leader.GetStateTime(),
	})
}

func (s *Server) handleUnregisterAgent(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	id := strings.TrimPrefix(r.Path, "/v1/agents/")
	if id == "" {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "agent id required")
		return
	}
	s.leader.UnregisterAgent(id)
	w.WriteHeader(leanhttp.StatusNoContent)
}

func (s *Server) handleGetJobs(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	httputil.WriteJSON(w, leanhttp.StatusOK, s.leader.GetJobs())
}

// handleApplyJob creates or updates a job by name (upsert).
// Name exists → UpdateJob with update_policy. Name unknown → DispatchJob.
func (s *Server) handleApplyJob(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	var job types.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "invalid json")
		return
	}
	if job.Name == "" {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "name required")
		return
	}
	// hop driver: the artifact IS the program (a native app image); there is
	// no command (exec) or container image (docker). One artifact and no
	// command/image can only mean the hop driver — default it, the same
	// shorthand DecodeInitJobs accepts (a launcher POSTs catalog entries
	// verbatim, so both paths must agree).
	if job.Driver == "" && job.Command == "" && job.Image == "" && len(job.Artifacts) == 1 {
		job.Driver = types.DriverHop
	}
	// Several artifacts is how one job spans architectures: each carries a
	// `match` on node attributes and the agent resolves it per node
	// (resolveJobForRun), so the runner still sees exactly one.
	hopImage := job.Driver == types.DriverHop && len(job.Artifacts) >= 1
	if job.Command == "" && job.Image == "" && !hopImage {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "command or image required (or driver \"hop\" with at least one artifact)")
		return
	}

	// UPDATE — job already exists. Run synchronously so callers get real status codes:
	// 200 OK on success, 409 Conflict if another update is in flight, 500 on dispatch failure.
	if s.leader.GetJob(job.Name) != nil {
		policy := job.UpdatePolicy
		if policy == "" {
			policy = "rolling"
		}
		if err := s.leader.UpdateJob(&job); err != nil {
			if errors.Is(err, leader.ErrJobLocked) {
				httputil.WriteError(w, leanhttp.StatusConflict, err.Error())
				return
			}
			httputil.WriteError(w, leanhttp.StatusInternalServerError, err.Error())
			return
		}
		httputil.WriteJSON(w, leanhttp.StatusOK, map[string]string{
			"name":   job.Name,
			"status": "updated",
			"policy": string(policy),
		})
		return
	}

	// CREATE — new job
	explicitPriority := job.Priority
	if job.Priority == nil {
		n := s.leader.NextPriority()
		job.Priority = &n
	}
	if err := s.leader.DispatchJob(&job); err != nil {
		httputil.WriteJSON(w, leanhttp.StatusCreated, map[string]string{
			"name":   job.Name,
			"status": "pending",
			"error":  err.Error(),
		})
		return
	}
	if explicitPriority != nil {
		_ = s.leader.PatchJobPriority(job.Name, *explicitPriority)
	}
	httputil.WriteJSON(w, leanhttp.StatusCreated, map[string]string{
		"name":   job.Name,
		"status": "dispatched",
	})
}

// handleDeleteJob deletes a job and cleans up all its tasks
func (s *Server) handleDeleteJob(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	name := strings.TrimPrefix(r.Path, "/v1/jobs/")
	if name == "" {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "job name required")
		return
	}
	s.leader.DeleteJobByName(name)
	w.WriteHeader(leanhttp.StatusNoContent)
}

// handleStatus returns cluster overview from placed data (no HTTP calls to agents).
func (s *Server) handleStatus(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	agents := s.leader.GetAgents()
	jobs := s.leader.GetJobs()
	placed := s.leader.GetPlacedCounts()

	totalPlaced := 0
	for _, count := range placed {
		totalPlaced += count
	}

	// Jobs whose last rollout did not finish: honest "not healthy yet".
	deploying := []string{}
	for _, j := range jobs {
		if j.Deploying {
			deploying = append(deploying, j.Name)
		}
	}
	httputil.WriteJSON(w, leanhttp.StatusOK, map[string]any{
		"cluster_name": s.clusterName,
		"agents":       len(agents),
		"jobs":         len(jobs),
		"total_placed": totalPlaced,
		"settling":     !s.leader.IsSettled(),
		"placed":       placed,
		"deploying":    deploying,
	})
}

// handleTasks returns every task in the cluster, keyed by agent ID. One
// parallel, time-bounded fan-out over the agents — for callers (CLI status,
// log lookup) that would otherwise ask per job and multiply the fan-out by
// the number of jobs.
func (s *Server) handleTasks(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	httputil.WriteJSON(w, leanhttp.StatusOK, map[string]any{
		"tasks_by_agent": s.leader.GetClusterStatus(),
	})
}

// handleEvents streams SSE notifications when cluster state changes.
func (s *Server) handleEvents(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	sse := httputil.SSEWriter(w)

	// De levensduur VÓÓR de eerste write claimen: op de node draagt
	// r.Context() leanhttp's Request.Done, en die is na de eerste Flush
	// terecht te laat (de kop heeft dan al keep-alive beloofd; fail-fast
	// sinds lean-review 13-08). De ping hieronder flusht — GEMETEN 14-08 op
	// QEMU: één /v1/events-verzoek legde anders de héle node om.
	done := r.Context().Done()

	ch := s.leader.EventBus().Subscribe()
	defer s.leader.EventBus().Unsubscribe(ch)

	sse.WriteEvent("ping", "{}")

	for {
		select {
		case <-done:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			switch {
			case strings.HasPrefix(msg, "agent:"):
				sse.WriteEvent("agent", fmt.Sprintf(`{"id":%q}`, strings.TrimPrefix(msg, "agent:")))
			case strings.HasPrefix(msg, "job:"):
				rest := strings.TrimPrefix(msg, "job:")
				if name, event, ok := strings.Cut(rest, ":"); ok {
					sse.WriteEvent("task", fmt.Sprintf(`{"job":%q,"event":%q}`, name, event))
				} else {
					sse.WriteEvent("job", fmt.Sprintf(`{"name":%q}`, rest))
				}
			default:
				sse.WriteEvent("status", "{}")
			}
		}
	}
}

// handleNotify receives agent task-change notifications and fires the event bus.
func (s *Server) handleNotify(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	var req struct {
		Job   string `json:"job"`
		Event string `json:"event"`
		Agent string `json:"agent"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	// Een hand-back is méér dan een event: de agent heeft de taak verwijderd
	// (onplaatsbaar — geen core, geen partitie), dus de plaatsing bestaat niet
	// meer. Zonder deze afboeking bleef placed op 1 staan en zag reconcile een
	// gezonde job waar in werkelijkheid niets draaide.
	if req.Event == "unplaceable" && req.Job != "" && req.Agent != "" {
		s.leader.MarkUnplaced(req.Agent, req.Job)
	}
	if req.Job != "" {
		topic := "job:" + req.Job
		if req.Event != "" {
			topic += ":" + req.Event
		}
		s.leader.EventBus().Notify(topic)
	} else {
		s.leader.EventBus().Notify("")
	}
	w.WriteHeader(leanhttp.StatusNoContent)
}

// handlePatchJobPriority updates only the priority of a job.
func (s *Server) handlePatchJobPriority(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "job name required")
		return
	}
	var body struct {
		Priority int `json:"priority"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "invalid json")
		return
	}
	if err := s.leader.PatchJobPriority(name, body.Priority); err != nil {
		httputil.WriteError(w, leanhttp.StatusNotFound, err.Error())
		return
	}
	w.WriteHeader(leanhttp.StatusNoContent)
}

// handleJobStatus returns tasks and agents for a specific job.
func (s *Server) handleJobStatus(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "job name required")
		return
	}

	tasks, agents := s.leader.GetJobStatus(name)
	httputil.WriteJSON(w, leanhttp.StatusOK, map[string]any{
		"agents":         agents,
		"tasks_by_agent": tasks,
	})
}

// handleAgentCapacity proxies capacity query to specific agent
func (s *Server) handleAgentCapacity(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	agentID := r.PathValue("agent_id")
	if agentID == "" {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "agent id required")
		return
	}

	// Begrensd en ZONDER r.Context(): dit is verzoek/antwoord, en de
	// request-lifetime claimen kost op leanhttp de herbruikbaarheid van de
	// inkomende verbinding. s.client draagt bewust geen timeout (log-tails),
	// dus de grens komt hier van de context.
	ctx, cancel := context.WithTimeout(s.lifecycleCtx, leader.HTTPClientTimeout)
	defer cancel()
	resp := s.proxyToAgent(ctx, w, r, agentID, "/capacity")
	if resp == nil {
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	_, _ = io.Copy(w, resp.Body)
}

// handleAgentLogs proxies SSE log streaming from specific agent
func (s *Server) handleAgentLogs(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	agentID := r.PathValue("agent_id")
	taskID := r.PathValue("task_id")
	stream := r.PathValue("stream")

	if agentID == "" || taskID == "" || (stream != "stdout" && stream != "stderr") {
		httputil.WriteError(w, leanhttp.StatusBadRequest, "invalid request parameters")
		return
	}

	// Een log-tail is een stream: hier hoort de request-lifetime WEL bij de
	// call (withLifecycle heeft hem al aan deze leader-generatie geknoopt).
	path := fmt.Sprintf("/logs/%s/%s", taskID, stream)
	resp := s.proxyToAgent(r.Context(), w, r, agentID, path)
	if resp == nil {
		return
	}
	defer resp.Body.Close()

	sse := httputil.SSEWriter(w)

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		fmt.Fprintf(w, "%s\n", scanner.Text())
		if scanner.Text() == "" {
			sse.Flush()
		}
	}
}

// proxyToAgent forwards an HTTP request to an agent, checking existence and
// setting API headers. The caller picks the lifetime: a stream passes its
// request context (the tail ends when the client walks away), request/response
// passes a bounded one so it never claims the inbound connection's read side.
func (s *Server) proxyToAgent(ctx context.Context, w leanhttp.ResponseWriter, r *leanhttp.Request, agentID string, path string) *leanhttp.Response {
	agent := s.leader.GetAgent(agentID)
	if agent == nil {
		httputil.WriteError(w, leanhttp.StatusNotFound, "agent not found")
		return nil
	}

	call := leanhttp.Call{Method: r.Method, URL: agent.Endpoint + path}
	if accept := r.Header.Get("Accept"); accept != "" {
		call.SetHeader("Accept", accept)
	}

	httputil.SignCall(&call, s.apiKey)

	resp, err := s.client.DoContext(ctx, call)
	if err != nil {
		httputil.WriteError(w, leanhttp.StatusBadGateway, "failed to contact agent")
		return nil
	}

	if resp.StatusCode != leanhttp.StatusOK {
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		resp.Body.Close()
		return nil
	}

	return resp
}

# Data Structures

## Job

What the user wants to run.

```go
type Job struct {
    Name          string            // UNIQUE KEY — no separate UUID
    Affinity      map[string]string // Node attribute constraints (optional, AND logic)
    Driver        string            // "exec" (default), "docker", or "hop" (HopOS slot)
    Image         string            // Docker image (only for driver=docker)
    Artifacts     []Artifact        // Platform-specific binaries (optional, agent picks first match)
    User          string            // Run as this user (default: inherit from agent)
    Command       string            // Command to execute (required for process, optional for Docker)
    Count         int               // Number of instances (see below)
    Ports         map[string]int    // port name → host port (0=dynamic, >0=fixed); Docker maps host=container=same port. App reads ER_PORT_<NAME>
    CPUShares     int               // Relative CPU priority (0 = no limiting)
    MemoryLimit   uint64            // Bytes (0 = no limiting)
    Env           map[string]string // Extra environment variables
    Tags          map[string]string // Labels for service discovery/grouping
    Volumes       map[string]string // host_path → task_path (bind-mounted on Linux, symlinked on macOS)
    HealthCheck   *HealthCheck      // Health check config (optional, http/tcp/file)
    MaxRestarts   *int              // nil = default (5), 0 = no restarts, -1 = unlimited
    RestartWindow time.Duration     // 0 = default (5m) — reset restart count if last crash was longer ago
    UpdatePolicy  UpdatePolicy      // How to update: rolling | recreate | blue-green
    Priority      *int              // nil = auto (append at end), 0 = top, N = Nth position
}
```

### Image (Docker Support)

Run a Docker container instead of a process:

```json
{
  "name": "redis",
  "image": "redis:7",
  "count": 3,
  "ports": {"redis": 6379}
}
```

- If `image` is set, the agent uses the DockerRunner instead of ExecRunner
- `command` is optional for Docker (overrides the image's CMD if provided)
- `ports` behave identically to exec: the value is the **host port** (`>0` fixed, `0` dynamic) and Docker maps host = container = the same port (no remapping). The app reads `ER_PORT_<NAME>`.
- All other fields (env, volumes, cpu_shares, memory_limit, tags, health_check) work identically

### Affinity (Node Targeting)

Target jobs to specific nodes based on attributes:

```json
{
  "name": "gpu-training",
  "command": "./train",
  "count": 1,
  "affinity": {"node.arch": "arm64"}
}
```

Pin to a specific node:
```json
{
  "name": "monitoring",
  "command": "./monitor",
  "count": 1,
  "affinity": {"node.id": "node-1"}
}
```

All constraints must match (AND logic). The agent checks affinity and rejects with 406 if no match — the leader stays unaware of attributes.

**Auto-detected attributes:** `node.id`, `node.arch` (arm64/amd64), `node.os` (linux/darwin/windows), `node.docker` (true/false).
**Custom attributes:** configurable via `node.attributes` in the config file.

### UpdatePolicy

How job updates are rolled out when POST /v1/jobs is called with existing job name:

```go
type UpdatePolicy string

const (
    UpdateRolling   UpdatePolicy = "rolling"    // Replace 1 at a time (default)
    UpdateRecreate  UpdatePolicy = "recreate"   // Stop all, then start new
    UpdateBlueGreen UpdatePolicy = "blue-green" // Start new alongside old
)
```

| Policy | Downtime | Resources | Use Case |
|--------|----------|-----------|----------|
| `rolling` | None | Normal | Standard deployments |
| `recreate` | Yes | Minimal | Breaking changes, DB migrations |
| `blue-green` | None | 2x (temporary) | Canary testing, instant rollback |

### Count

| Value | Behavior |
|-------|----------|
| `count: 3` | Run 3 instances, spread via round-robin |
| `count: 0` or omitted | Default to 1 instance |
| `count: -1` | **Run on ALL agents** |

`count: -1` is useful for node-level services like hopdns or monitoring agents. New agents automatically receive all `count: -1` jobs on registration.

### Ports

Ports can be dynamic (assigned at runtime) or fixed:

```json
{
  "ports": {
    "http": 0,      // Dynamic - system assigns a free port
    "grpc": 0,      // Dynamic
    "metrics": 9090 // Fixed - must use port 9090
  }
}
```

**Fixed ports (process jobs):** If the specified port is already in use, the job will be rejected with an error.

**Docker jobs:** Ports behave exactly like process jobs — the value is the host port, and Docker maps it to the same container port. Example: `{"http": 8080}` → `-p 8080:8080`; `{"http": 0}` → `-p <dynamic>:<dynamic>`. The container app reads `ER_PORT_HTTP` and listens on it (a stock image that listens on a fixed internal port should use that port as the value).

**Environment variables:** Task gets `ER_PORT_HTTP`, `ER_PORT_GRPC`, etc. for all ports (host ports), and `ER_ATTR_NODE_OS`, `ER_ATTR_NODE_ARCH`, etc. for all node attributes (dots/dashes become underscores, uppercased).

### Volumes

Mount host directories into the task's working directory:

```json
{
  "volumes": {
    "/data/shared": "data",
    "/etc/ssl/certs": "certs"
  }
}
```

- Host paths must exist (validation at task start)
- Target paths are relative to task directory
- Bind-mounted on Linux, symlinked on macOS; unmounted on task cleanup (Docker: `-v hostPath:containerPath`)

### Artifact

```go
type Artifact struct {
    URL      string            // Download URL (http://, https://, s3://)
    Match    map[string]string // Node attribute constraints (agent picks first match, empty = catch-all)
    Headers  map[string]string // HTTP headers (Authorization, X-API-Key, etc.)
    Auth     map[string]string // Other credentials (S3, helpers)
    Extract  string            // "tar.gz", "tar.bz2", "zip", "" (empty = raw file)
    Filename string            // Override filename for raw downloads (default: basename from URL)
}
```

**Platform-specific artifacts:** Jobs have an `artifacts` array. The agent picks the first entry whose `match` constraints match its node attributes (same AND logic as affinity). Empty `match` = catch-all.

**URL scheme determines which downloader to use.**

**Extract field:**
- `"tar.gz"` or `"tgz"` — extract tar.gz archive
- `"tar.bz2"` or `"tbz2"` — extract tar.bz2 archive
- `"zip"` — extract zip archive
- `""` (empty) — raw file, automatically `chmod +x`

**HTTP/HTTPS downloaders:**
- Use `headers` for custom HTTP headers (direct pass-through)
- Or use `auth` helpers: `username`/`password` → generates Basic Auth header

**S3 downloader:**
- Use `auth` for S3 credentials: `access_key`, `secret_key`, `region`

**Examples:**

Custom headers:
```json
{
  "url": "https://artifacts.example.com/app.tar.gz",
  "headers": {
    "Authorization": "Bearer token123",
    "X-API-Key": "secret"
  },
  "extract": "tar.gz"
}
```

Raw binary (no extraction):
```json
{
  "url": "https://releases.example.com/myapp-v1.0",
  "headers": { "Authorization": "Bearer token123" }
}
```
File is downloaded, `chmod +x`, ready to run.

S3:
```json
{
  "url": "s3://bucket/key.tar.gz",
  "auth": {
    "access_key": "AKIA...",
    "secret_key": "...",
    "region": "eu-west-1"
  },
  "extract": "tar.gz"
}
```

### HealthCheck

```go
type HealthCheck struct {
    Type             string        // "http" (default), "tcp", "file"
    Path             string        // HTTP: endpoint path, File: absolute file path
    Port             string        // HTTP/TCP: named port (default "http")
    Interval         time.Duration // Check interval (default 10s)
    Timeout          time.Duration // HTTP/TCP: request/connect timeout (default 5s)
    InitialTimeout   time.Duration // Max time after start to become healthy (default 30s)
    FailureThreshold int           // Consecutive failures before unhealthy (default 3)
}
```

**Check types:**

| Type | Check | Fields used |
|------|-------|-------------|
| `http` (default) | HTTP GET, 200-399 = healthy | `Path`, `Port`, `Timeout` |
| `tcp` | TCP connect, success = healthy | `Port`, `Timeout` |
| `file` | File mtime since last check = healthy | `Path` |

**FailureThreshold:** Task must fail N consecutive checks before being marked unhealthy and restarted. Default 3 (= 15s with 5s monitor interval).

**InitialTimeout:** Allows slow-starting services time to initialize before health checks begin.

## Task

A running instance of a Job.

```go
type Task struct {
    ID           string         // Unique identifier (regenerated on every restart)
    JobName      string         // Job name (which job this task belongs to)
    Driver       string         // "exec", "docker", or "hop"
    Image        string         // Docker image (only for driver=docker)
    Ports        map[string]int // Named port -> host port number
    Pid          int            // Process ID (Docker: 0; HopOS: primary slot index)
    State        TaskState      // queued, downloading, running, stopping, failed
    StartedAt    time.Time
    RestartCount int            // Number of times restarted
    LastFailedAt time.Time      // Last crash time (drives the restart window)
    NextRestartAt time.Time     // When the next restart attempt runs (backoff in progress); zero while running or given up
    CPUShares    int            // Copied from the job (capacity accounting)
    MemoryLimit  uint64         // Copied from the job (capacity accounting)
    CPUPercent   float64        // Live usage, measured by the agent monitor (5s)
    MemPercent   float64        // Live usage, measured by the agent monitor (5s)

    Downloaded   uint64         // Bytes in so far   (state "downloading" only)
    ImageSize    uint64         // Total image size  (state "downloading" only)
}
```

**Note:** Task has **only `JobName`** — there is no JobID (jobs have no separate ID). Always use `task.JobName` for job lookups.

**Note:** `task.Driver` determines which runner manages this task: `"exec"` = ExecRunner, `"docker"` = DockerRunner, `"hop"` = HopRunner (HopOS).

**Ports:** Task gets ENV vars `ER_PORT_HTTP`, `ER_PORT_GRPC`, etc. for all allocated ports.

**Node attributes:** Task gets ENV vars `ER_ATTR_NODE_OS`, `ER_ATTR_NODE_ARCH`, `ER_ATTR_NODE_ID`, etc. for all node attributes. User-defined `env` on the job takes priority over attribute env vars with the same name.

### Task States

| State | Meaning |
|-------|---------|
| `queued` | Accepted, capacity already reserved, waiting for its turn to download |
| `downloading` | The image is streaming in — progress in `Downloaded` / `ImageSize` |
| `running` | Process is running |
| `stopping` | Being stopped (shutdown, restart swap, preemption); then removed |
| `failed` | Crashed, OOM killed, exceeded max restarts, etc |

`queued` and `downloading` exist because a task used to be called `running`
from birth, and on HopOS the image download before it can take minutes — ten
minutes of "running, 0% cpu" while nothing runs at all. Only runners that
stream fill the progress fields.

**Every state counts against capacity.** Presence is the measure, never the
state: a `queued` or `failed` task still occupies its share, and capacity is
freed by *deleting the record* — never by filtering on state. There is no
`stopped` state: an intentionally stopped task is absent.

## Agent

A registered agent with the leader.

```go
type Agent struct {
    ID         string    // Unique identifier
    Endpoint   string    // HTTP endpoint (http://ip:port)
    Version    string    // Agent version (injected at build time)
    LastSeen   time.Time // Last heartbeat
    TempMilliC int       // Node CPU temperature in milli-°C, 0 = no sensor
}
```

`TempMilliC` rides along on every heartbeat — one number per node, on purpose:
a node with several sensors reports the hottest one, because that is the number
you act on.

## Leader State (in-memory)

```go
type leaderState struct {
    agents      map[string]*Agent           // Registered agents
    placed      map[string]map[string]int   // agentID → jobName → count
    dispatching map[string]bool             // jobName → true if being dispatched
    settled     bool                        // false during settle period
    roundRobin  int                         // Counter for round-robin
}
```

Jobs are stored in the shared `JobStore` (owned by Agent, referenced by Leader).

All state access goes through a single goroutine via the `ops` channel, using `do()` (fire-and-forget) and `query()` (blocking with result) helpers.

The leader tracks:
- Which agents are online (via heartbeats)
- Which job instances run on which agents (`placed`: agentID → jobName → count)
- Which jobs are being actively dispatched (`dispatching`: prevents double dispatch)
- Whether the settle period has elapsed (`settled`: defers reconciliation until agents register)
- Round-robin counter for deterministic agent selection (agents sorted by ID)

**Settle period:** After becoming leader, the leader waits for `agentTimeout` (30s) before reconciling. This allows agents to register with their `placed` counts, preventing duplicate dispatches.

**Placement tracking:** `placed[agentID][jobName] = count` tracks how many instances of each job are on each agent. Updated on dispatch, cleared on agent death/unregister.

**Reconciliation:** After agent changes, `reconcileJob` compares desired vs actual state and dispatches the difference. Single code path for daemon (count=-1) and regular jobs. Skips jobs that are actively being dispatched.

**Delete:** `DeleteJobByID` uses two-phase approach (placement + cluster status) to catch orphaned tasks.

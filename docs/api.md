# HTTP API

There are two APIs: the **Leader API** (port+1000) and the **Agent API** (port).

## Authentication (X-Hop-Auth)

All endpoints — leader and agent — **except `GET /health` and `GET /leader`**
require a valid HMAC signature in the `X-Hop-Auth` header. The shared key
(`api_key` in config, `--api-key` flag) never travels on the wire; an empty
key disables auth (dev/standalone).

Canonical string that gets signed:

```
METHOD \n PATH \n hex(sha256(body))
```

- `METHOD` = HTTP method, `PATH` = URL path (no query string), `body` = exact
  request-body bytes (empty body → `sha256("")`)
- Signature = `hex(HMAC-SHA256(key, canonical))`, sent as `X-Hop-Auth`

Raw curl (the CLI, satellites and GUI sign automatically):

```bash
KEY="your-secret-key"; M=POST; P=/v1/jobs; BODY='{"name":"api","count":3}'
BH=$(printf '%s' "$BODY" | openssl dgst -sha256 | awk '{print $2}')
SIG=$(printf '%s\n%s\n%s' "$M" "$P" "$BH" | openssl dgst -sha256 -hmac "$KEY" | awk '{print $2}')
curl -X $M "http://127.0.0.1:9080$P" -H "X-Hop-Auth: $SIG" -d "$BODY"
```

Properties: the key can't be sniffed, requests can't be forged or tampered with
(method+path+body are bound into the signature), and there is no clock/nonce/
server state — failover-safe. A verbatim replay of a captured request remains
possible; see SECURITY.md for the threat model. The agent's proxy forwards the
caller's signature unchanged (same method/path/body + shared key ⇒ still valid
at the leader).

## Leader API

Runs on whichever node currently holds the leader lease (via hoplock — CAS
over hoplockserver or any S3-compatible store). Default port: 9080 (agent
port + 1000).

### Health

```
GET /health
```

Returns `{"status": "ok"}`.

### Cluster Status

```
GET /v1/status
```

Returns cluster overview (from placed data, no HTTP calls to agents):
```json
{
  "cluster_name": "prod-eu",
  "agents": 3,
  "jobs": 5,
  "total_placed": 12,
  "settling": false,
  "placed": {
    "my-api": 3,
    "worker": 2
  },
  "deploying": []
}
```

- **cluster_name:** Cluster name from config — used by hopdns for federation discovery.
- **deploying:** Jobs whose last update did not complete (a rollout failed or a leader died mid-rollout). Empty when every rollout finished.
- **settling:** `true` during the settle period after leader election (30s). During this period, jobs are stored but not dispatched until agents have registered with their placed counts.
- **placed:** Job name → total placed count across all agents.

For per-job task details (state, pid, restarts), use `GET /v1/jobs/{name}/status`.

### Cluster Tasks

```
GET /v1/tasks
```

Returns every task in the cluster, keyed by agent ID — one parallel,
time-bounded fan-out over the agents:

```json
{
  "tasks_by_agent": {
    "node-a": [ { "id": "…", "job_name": "my-api", "state": "running", … } ],
    "node-b": [ … ]
  }
}
```

For callers that need the whole picture (CLI status, finding which agent runs
a task). Asking `GET /v1/jobs/{name}/status` per job would multiply the agent
fan-out by the number of jobs.

### Agents

```
GET    /v1/agents                                   # All registered agents
POST   /v1/agents                                   # Register agent (with placed counts)
DELETE /v1/agents/{id}                              # Unregister agent (triggers reconciliation)
GET    /v1/agents/{agent_id}/capacity               # Proxy an agent's /capacity through the leader
GET    /v1/agents/{agent_id}/logs/{task_id}/{stream} # Proxy an agent's log stream through the leader (SSE)
```

The capacity/logs proxies let clients (like the GUI) reach every agent via a
single connection point, even when only the leader is routable.

#### Register Agent

```
POST /v1/agents
```

Called on agent startup and on leader change:
```json
{
  "id": "agent-1",
  "endpoint": "http://10.0.0.5:8080",
  "version": "dev",
  "placed": {
    "api": 2,
    "worker": 1
  }
}
```

**placed:** Map of jobName → count, telling the leader what this agent is already running. This prevents duplicate dispatches during leader failover.

**Response:**
```json
{
  "status": "registered",
  "jobs": [...],
  "state_time": "2025-01-31T12:00:00Z"
}
```

### Heartbeat

```
POST /v1/heartbeat
```

Agents send this every 10s to stay registered:
```json
{
  "id": "agent-1",
  "endpoint": "http://10.0.0.5:8080",
  "version": "dev",
  "temp_milli_c": 45000
}
```

`temp_milli_c` is the node's CPU temperature in milli-°C, or absent when the
node has no sensor. One number per node: a node with several sensors sends the
hottest, because that is the one you act on.

**Response (known agent):**
```json
{
  "status": "ok"
}
```

**Response (unknown agent):** `404 Not Found` — agent should re-register via POST /v1/agents.

**Pure liveness:** the heartbeat only refreshes the agent's `LastSeen` (and version). The old bidirectional job sync (`jobs` + `state_time` in request and response) was removed: desired state has a single author — the leader — which commits it as a snapshot to S3 next to the election lease (see `internal/leader/persist.go`). A new leader loads that committed state instead of learning jobs from agent heartbeats. Unknown fields sent by older agents are ignored.

### Jobs

```
GET    /v1/jobs                     # All jobs
POST   /v1/jobs                     # Run or update job (upsert based on name)
DELETE /v1/jobs/{name}              # Delete job and all its tasks
GET    /v1/jobs/{name}/status       # Per-job task details (see below)
PATCH  /v1/jobs/{name}/priority     # Update only the job's priority: {"priority": N}
```

#### Run or Update Job (Upsert)

**POST /v1/jobs performs upsert:**
- If job with this `name` exists → **UPDATE** (according to `update_policy`)
- If job doesn't exist → **INSERT** (dispatch new job)

**Full example:**
```bash
curl -X POST http://localhost:9080/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "api",
    "affinity": {"node.arch": "amd64"},
    "artifacts": [
      {
        "url": "s3://mybucket/api-v2.0-amd64.tar.gz",
        "match": {"node.arch": "amd64"},
        "auth": {
          "access_key": "AKIAIOSFODNN7EXAMPLE",
          "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          "region": "eu-west-1"
        },
        "extract": "tar.gz"
      },
      {
        "url": "s3://mybucket/api-v2.0-arm64.tar.gz",
        "match": {"node.arch": "arm64"},
        "auth": {
          "access_key": "AKIAIOSFODNN7EXAMPLE",
          "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
          "region": "eu-west-1"
        },
        "extract": "tar.gz"
      }
    ],
    "command": "./server --http=$ER_PORT_HTTP --grpc=$ER_PORT_GRPC",
    "count": 3,
    "ports": {"http": 0, "grpc": 0, "metrics": 9090},
    "cpu_shares": 2048,
    "memory_limit": 536870912,
    "env": {
      "DB_HOST": "postgres.internal",
      "LOG_LEVEL": "info"
    },
    "tags": {
      "service": "api",
      "loadbalancer_domain": "*.example.com"
    },
    "volumes": {
      "/data/shared": "data"
    },
    "health_check": {
      "type": "http",
      "path": "/health",
      "port": "http",
      "timeout": "5s",
      "initial_timeout": "30s",
      "failure_threshold": 3
    },
    "max_restarts": 5,
    "update_policy": "rolling"
  }'
```

**Fields:**
- `name` (string, **required**): Job name — **the unique key** (jobs have no separate ID)
- `driver` (string): `"exec"` (default), `"docker"`, or `"hop"` (HopOS slot) — auto-derived from `image` if not set
- `image` (string): Docker image (sets driver to `"docker"` automatically)
- `affinity` (map): Node attribute constraints, AND logic (optional). Example: `{"node.arch": "arm64"}`. Agent rejects with 406 if no match.
- `artifacts` (array): Platform-specific binaries/assets (optional). Agent picks first matching entry.
  - `url` (string): URL with scheme — determines downloader (http://, s3://)
  - `match` (map): Node attribute constraints — agent picks first artifact where all match (empty = catch-all)
  - `headers` (map): HTTP headers (Authorization, X-API-Key, etc.)
  - `auth` (map): Credentials (S3: access_key/secret_key/region, HTTP: username/password)
  - `extract` (string): Archive type — `tar.gz`, `tar.bz2`, `zip`, or `""` (raw binary, auto chmod +x)
  - `filename` (string): Override filename for raw downloads (default: basename from URL)
- `command` (string, **required**): Command to execute (for Docker, overrides image CMD)
- `user` (string): Run as this user (default: inherit from the agent)
- `count` (int): Number of instances (default 1, -1 = all agents)
- `ports` (map): port name → host port (0=dynamic, >0=fixed); same for process and Docker (Docker maps host=container=same port). App reads ENV vars `ER_PORT_<NAME>`
- `cpu_shares` (int): CPU priority (nice-based)
- `memory_limit` (uint64): Memory limit in bytes
- `env` (map): Environment variables (note: node attributes are auto-injected as `ER_ATTR_<KEY>`, user env takes priority)
- `tags` (map): Labels for service discovery
- `volumes` (map): Host path → task path (symlinked)
- `health_check`: Health monitoring (optional)
  - `type` (string): `"http"` (default), `"tcp"`, or `"file"`
  - `path` (string): HTTP endpoint path (http) or absolute file path (file)
  - `port` (string): Named port to check (http/tcp, default "http")
  - `timeout` (duration): Request/connect timeout (http/tcp, default 5s)
  - `initial_timeout` (duration): Grace period for slow-starting services (default 30s)
  - `failure_threshold` (int): Consecutive failures before restart (default 3)
- `max_restarts` (int): Max restart attempts — omit for the default of 5, `0` = no restarts (first crash is final), `-1` = unlimited
- `restart_window` (duration): Reset the restart count when the last crash is longer ago than this (default 5m)
- `update_policy` (string): `rolling` (default), `recreate`, or `blue-green`
- `priority` (int): Scheduling priority — 0 = highest/top, N = Nth position (omit to append at the end)

**Artifact Downloaders:**

URL scheme → downloader:
- `http://`, `https://` → HTTP downloader
  - Uses `headers` for custom headers
  - Or `auth.username`/`auth.password` → generates Basic Auth header
- `s3://bucket/key` → S3 downloader
  - Uses `auth.access_key`, `auth.secret_key`, `auth.region`

**Response (INSERT):**
```json
{
  "name": "api",
  "status": "dispatched"
}
```

**Response (UPDATE):**
```json
{
  "name": "api",
  "status": "updated",
  "policy": "rolling"
}
```

**Note:** Job **name** is the unique key for upsert — jobs have no separate ID. During an update, old and new instances coexist temporarily according to the update policy.

**Update Policies:**

| Policy | Downtime | Behavior |
|--------|----------|----------|
| `rolling` (default) | None | Start new → stop old, 1 at a time with 2s delay |
| `recreate` | Yes | Stop all → start new version |
| `blue-green` | None | Start all new → stop all old (2x resources during switch) |

## Agent API

Runs on each node. Default port: 8080.

Agents also proxy `/v1/*` endpoints to the leader for cluster-wide operations.

CORS is enabled for browser access (the GUI talks to agents directly). The
agent also answers Chrome's Private Network Access preflight
(`Access-Control-Allow-Private-Network: true`) so a hosted GUI on a public
origin can reach agents on private addresses.

### Health

```
GET /health
```

### Leader

```
GET /leader
```

Returns the current leader address as the agent's tick loop knows it — no lock-store read per request:
```json
{"leader": "10.0.0.5:9080"}
```

On the leader itself the response also carries when its lease lapses unless renewed (the lease is the leader's timer; hopprom exposes it as `hop_leader_lease_seconds`):
```json
{"leader": "10.0.0.5:9080", "lease_expires_at": "2026-09-08T17:01:48+02:00"}
```

### Capacity

```
GET /capacity
```

Returns capacity, live usage and node attributes:
```json
{
  "cpu_cores": 14,
  "memory_bytes": 51539607552,
  "cpu_used_shares": 3072,
  "memory_used_bytes": 402653184,
  "tasks_running": 3,
  "attributes": {
    "node.id": "agent-1",
    "node.arch": "arm64",
    "node.os": "linux"
  }
}
```

- `cpu_cores` / `memory_bytes` are the **effective** cap this agent schedules
  against (the configured `capacity.*` where set), not raw hardware — that is
  what a caller like hopprom or hoplb needs to see.
- `cpu_used_shares` and `memory_used_bytes` are what the tasks on this node
  claim, counted from **every** task present regardless of its state. Members
  of one sharegroup share a pool of cores, so that pool's CPU counts **once**;
  memory counts per member, because every app keeps its own partition.
- `tasks_running` is a display count of tasks in state `running` — it is not
  what capacity is computed from.

For HopOS jobs, `core-class` selects physical cores, including the cores of a
`sharegroup` pool. Shared cage IDs are independent of that class and start above
the physical core range, leaving low IDs available for dedicated SMP. The node
confirms placement; unavailable physical capacity leaves the job pending.

### Tasks

```
GET /tasks                 # All tasks on this agent
```

### Run (internal, called by leader)

```
POST /run
```

Start a job. Returns 202 Accepted (fire-and-forget, artifact download + start happens async):
```json
{
  "status": "accepted",
  "job": "api",
  "message": "job accepted, starting in background"
}
```

Returns 406 if affinity mismatch. Returns 503 if no capacity.

### Delete (internal, called by leader)

```
DELETE /delete/{job_name}
```

Deletes a job by name and cleans up all its tasks on this agent.

### Stop (internal, called by leader)

```
POST /stop/{job_name}        # Stop all tasks for a job, keep the job definition
POST /stop-task/{task_id}    # Stop one specific task by ID
```

`/stop/` is used for preemption (the definition must remain for rescheduling);
`/stop-task/` lets rolling and blue-green updates stop precise old instances.

### Logs (streaming)

```
GET /logs/{task_id}/stdout   # Stream stdout (SSE)
GET /logs/{task_id}/stderr   # Stream stderr (SSE)
```

Live stream of task output. Server-Sent Events (SSE) format.

**Example:**
```bash
curl http://agent:8080/logs/abc123/stdout

# SSE output:
data: [2025-01-31 12:00:00] Server starting...
data: [2025-01-31 12:00:01] Listening on port 8080
```

**No persistence** — logs are streamed live only, with one exception: when a task
ends (crash, stop, delete) its last 50 lines stay retrievable for 5 minutes on
every driver (exec, docker, hop). Asking a finished task for its logs returns
that history and then closes the stream, instead of the `404` it used to — a
crashed task's last words are exactly what you need right after it fell over.
After the 5 minutes it is `404 task not found or not running`.

### Proxy Endpoints

The agent proxies these paths to the current leader:
- `/v1/agents`
- `/v1/jobs`
- `/v1/jobs/{name}`
- `/v1/status`
- `/v1/events` (SSE proxy)

This means hopdns/hoplb/hopprom can query their local agent and automatically get cluster-wide data.

### Events (SSE)

```
GET /v1/events
```

Server-Sent Events stream that notifies on cluster state changes (job dispatches, agent registrations, etc.). SSE event types map directly to the resource that changed:

```
event: ping
data: {}

event: agent
data: {"id": "agent-1"}

event: job
data: {"name": "api"}

event: task
data: {"job": "api", "event": "started"}
```

Task events: `start` (process started), `started` (healthy), `crash`, `stop`.

### Notify (internal, called by agents)

```
POST /v1/notify
```

Agents call this when a task changes state (starts, crashes, etc.) to trigger an SSE notification to subscribers. Body: `{"job": "api"}` or empty.

### Per-Job Status

```
GET /v1/jobs/{name}/status
```

Returns tasks and agents for a specific job. Only queries agents that have this job placed (more efficient than full cluster status).

```json
{
  "agents": [...],
  "tasks_by_agent": {
    "agent-1": [{"id": "abc", "job_name": "api", "state": "running", ...}]
  }
}
```

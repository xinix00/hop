# HTTP API

Every Hop node serves two HTTP APIs:

| API | Port | Served by | Purpose |
| --- | --- | --- | --- |
| Agent API | `P` (default `8080`) | every node | run/stop tasks on this node, logs, capacity; proxies `/v1/*` to the leader |
| Leader API | `P + 1000` (default `9080`) | the node holding the lease | cluster state: agents, jobs, status, events |

`P` is `node.port` in the host config (`agentd`) or `HOPOS_PORT` on HopOS
(see [configuration.md](configuration.md)). Both APIs are pure handlers in
`api/` (`NodeApi`, `LeaderApi`); the host daemon (`agentd/src/http.rs`) and
the HopOS resident (`hop-http/`, `agentd-hopos/src/forward.rs`) are thin
adapters that send the same bytes. Background on the roles is in
[architecture.md](architecture.md); the JSON shapes of jobs, tasks and agents
are in [data-structures.md](data-structures.md).

All bodies are JSON (`Content-Type: application/json`) unless the response is
a stream (`text/event-stream`). Errors are always:

```json
{"error": "human readable reason"}
```

## Authentication (`X-Hop-Auth`)

The whole cluster shares one key (`api_key` in the host config,
`hopos.apikey` / `HOPOS_APIKEY` on HopOS). An **empty key disables
authentication** (dev, standalone).

With a key, every request (except the few listed below) must carry:

```
X-Hop-Auth: hex(HMAC-SHA256(key, METHOD + "\n" + PATH + "\n" + hex(SHA256(body))))
```

- `METHOD` is the upper-case HTTP method (`GET`, `POST`, ...).
- `PATH` is the decoded URL path **without the query string** (the query is
  deliberately not signed: `/v1/jobs?x=1` signs as `/v1/jobs`).
- `body` is the exact request body; an empty body hashes as `SHA256("")`.
- Hex is lower case, 64 characters; the comparison is constant-time.

The key never travels over the wire. There is no timestamp or nonce: a
signature survives a failover and an agent can forward a request unchanged to
the leader, but a captured request can be replayed verbatim.

Test vector (from `auth/src/lib.rs`): key `your-secret-key`, `POST /v1/jobs`,
body `{"name":"api","count":3}` gives
`54f6f8899ba8af25f69825f54c0c52a61f3ea224b6dc5542f4646d936f746f59`.

```sh
KEY=your-secret-key
BODY='{"name":"api","count":3}'
SIG=$(printf 'POST\n/v1/jobs\n%s' "$(printf %s "$BODY" | sha256sum | cut -d' ' -f1)" \
      | openssl dgst -sha256 -hmac "$KEY" | awk '{print $NF}')
curl -s -X POST -H "X-Hop-Auth: $SIG" -H 'Content-Type: application/json' \
     -d "$BODY" http://leader:9080/v1/jobs
```

Rejections:

| Status | Body | When |
| --- | --- | --- |
| `401` | `{"error":"unauthorized"}` | missing or wrong signature |
| `413` | `{"error":"request body too large (N > 524288 bytes)"}` | body over 512 KiB; checked **before** the signature |
| `413` | (from the HTTP server) | body over 1 MiB; refused by `leanhttp` before any handler runs |

### Unauthenticated routes

| Port | Route |
| --- | --- |
| agent | `OPTIONS *` (CORS preflight, always `200`) |
| agent | `GET /health` |
| agent | `GET /leader` |
| leader | `GET /health` |

Everything else, including streams, is signed.

## CORS (agent port only)

The dashboard (`hop-gui`) talks to the agent port from the browser, so every
response on that port, including proxied answers and streams, carries:

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, DELETE, PATCH, OPTIONS
Access-Control-Allow-Headers: Content-Type, X-Hop-Auth
```

plus `Access-Control-Allow-Private-Network: true` when the request carries
`Access-Control-Request-Private-Network: true`. The leader port sends no CORS
headers.

## Leader API (`P + 1000`)

A node that does not lead answers every leader-port request with `503`:

- `GET /health`: `{"error":"not the leader"}` (so a load balancer can find the leader);
- anything else: `this node is not the leader; the leader is <ip:port>`, or
  `this node is not the leader, and no leader is known`; on HopOS also
  `this node is becoming the leader; the cluster state is loading` while the
  committed state loads.

### Overview

| Method | Path | Response | Notes |
| --- | --- | --- | --- |
| `GET` | `/health` | `200 {"status":"ok"}` | unauthenticated |
| `GET` | `/v1/status` | `200` | cluster summary from the leader's books |
| `GET` | `/v1/agents` | `200 [agent, ...]` | registered agents |
| `GET` | `/v1/agents/{id}/capacity` | agent's answer | proxied to that agent's `/capacity` |
| `GET` | `/v1/agents/{id}/logs/{task}/{stdout\|stderr}` | SSE | proxied to that agent's `/logs/...`; follows by default |
| `DELETE` | `/v1/agents/{id}` | `204` | unregister an agent |
| `POST` | `/v1/agents` | `200` | register (agent to leader, internal) |
| `POST` | `/v1/heartbeat` | `200` | heartbeat (agent to leader, internal) |
| `POST` | `/v1/notify` | `204` | task event (agent to leader, internal) |
| `GET` | `/v1/jobs` | `200 [job, ...]` | all jobs |
| `POST` | `/v1/jobs` | `201` / `200` | create or update a job (upsert by name) |
| `DELETE` | `/v1/jobs/{name}` | `204` | delete a job and its tasks |
| `PATCH` | `/v1/jobs/{name}/priority` | `204` | set placement priority |
| `GET` | `/v1/jobs/{name}/status` | `200` | the job's tasks, asked from the agents it is placed on |
| `GET` | `/v1/tasks` | `200` | every agent's tasks, asked from every agent |
| `GET` | `/v1/events` | SSE | cluster events |

Any other path or method: `404 {"error":"not found"}`.

### `GET /v1/status`

Computed from the leader's placement counters; no agent is called.

```json
{
  "cluster_name": "home",
  "agents": 3,
  "jobs": 2,
  "total_placed": 4,
  "settling": false,
  "placed": {"web": 3, "dns": 1},
  "deploying": ["web"]
}
```

`settling` is true until the leader's settle period after taking over has
passed; `deploying` lists jobs whose last rollout did not finish (see
[lifecycles.md](lifecycles.md)).

### `GET /v1/agents`

```json
[
  {
    "id": "pi-kitchen",
    "endpoint": "http://192.168.1.20:8080",
    "version": "3.0.11",
    "last_seen": "2026-10-17T12:00:00Z",
    "temp_milli_c": 48000,
    "hop_cpu_percent": 2.5,
    "hop_mem_bytes": 12582912
  }
]
```

The telemetry fields (`temp_milli_c`, `kern_cpu_percent`, `kern_mem_bytes`,
`kern_ram_bytes`, `kern_core`, and the same with `hop_`) appear only when the
last heartbeat reported them.

### `POST /v1/jobs`

The body is a job spec ([data-structures.md](data-structures.md)); unknown
fields are ignored. Example:

```json
{"name": "web", "command": "./web", "count": 2}
```

Admission:

- `name` is required: `400 {"error":"name required"}`;
- one of `command`, `image`, or driver `hop` with at least one artifact:
  `400 {"error":"command or image required (or driver \"hop\" with at least one artifact)"}`.
  A job with exactly one artifact and no `driver`, `command` or `image` is
  read as `"driver": "hop"`;
- a fixed (non-zero) port with an explicit `update_policy` of `rolling` or
  `blue-green` is refused with `400` (`job <name>: a fixed port (<port> <n>)
  cannot roll; use update_policy recreate or a dynamic port`), and the same
  line is logged with marker `HOP_JOB_REFUSED`. Without a policy, a fixed
  port means `recreate`;
- malformed JSON: `400 {"error":"invalid json"}`.

Responses:

| Status | Body | Meaning |
| --- | --- | --- |
| `201` | `{"name":"web","status":"dispatched"}` | new job, placed |
| `201` | `{"name":"web","status":"pending","error":"..."}` | new job, stored but not (yet) placed |
| `200` | `{"name":"web","status":"updated","policy":"rolling"}` | existing job, update rolled out with this policy |
| `409` | `{"error":"job is locked by another operation"}` | an update of this job is already running |
| `500` | `{"error":"..."}` | the update failed |

A new job without `priority` goes to the back (`next_priority`). An update
call can take long (a rolling update waits for its steps); proxies allow
120 s.

### `DELETE /v1/jobs/{name}`

`204`, also for an unknown name. `400 {"error":"job name required"}` for an
empty name.

### `PATCH /v1/jobs/{name}/priority`

Body `{"priority": 0}` (0 is first; a missing field counts as 0). `204` on
success, `404 {"error":"job <name> not found"}`, `400` for an empty name or
bad JSON.

### `GET /v1/tasks`

The leader asks every registered agent `GET /tasks` (signed with the cluster
key), with 2 s per agent (and on `agentd` a total budget of 5 s). Agents that do not answer
in time are listed under `unreachable`. Each answering agent's list ends with
its system tasks (`kern`, `hop`, state `system`) built from its last
heartbeat; these exist only here.

```json
{
  "tasks_by_agent": {
    "pi-kitchen": [
      {"id": "web-1a2b", "job_name": "web", "driver": "exec", "ports": {"http": 31002},
       "pid": 4711, "state": "running", "started_at": "2026-10-17T12:00:00Z",
       "restart_count": 0, "last_failed_at": "0001-01-01T00:00:00Z",
       "next_restart_at": "0001-01-01T00:00:00Z"}
    ]
  },
  "unreachable": ["pi-garage"]
}
```

Task states: `queued`, `downloading`, `running`, `stopping`, `failed`,
`system` (see [data-structures.md](data-structures.md)).

### `GET /v1/jobs/{name}/status`

Like `/v1/tasks`, but only the agents the job is placed on and only that
job's tasks (the dashboard's task table):

```json
{"agents": [agent, ...], "tasks_by_agent": {"pi-kitchen": [task, ...]}}
```

An unknown job gives `200 {"agents":null,"tasks_by_agent":null}`; a job placed
nowhere `200 {"agents":null,"tasks_by_agent":{}}`. An agent that did not
answer, or has no task of this job, is left out of `tasks_by_agent`.

### Proxying to an agent

`hop logs`, `hop agents <id>` and the dashboard only need to reach the leader;
the leader reaches every agent (including ones on a HopOS slot LAN or a
private network).

- `GET /v1/agents/{id}/capacity` becomes a signed `GET {endpoint}/capacity`
  on that agent (10 s timeout), and its answer is passed back.
- `GET /v1/agents/{id}/logs/{task}/{stream}` becomes a signed
  `GET {endpoint}/logs/{task}/{stream}` with the caller's query. Without a
  `follow` parameter the leader adds `follow=1`: this route is the **live
  tail** by default. Pass `?follow=0` for a snapshot (what `hop logs` without
  `-f` does).

Errors: `404 {"error":"agent not found"}` for an unknown id;
`400 {"error":"invalid request parameters"}` for a malformed log path (empty
task, a stream other than `stdout`/`stderr`, extra segments);
`502 {"error":"failed to contact agent <endpoint>: ..."}` (or
`<path>: ...` for a stream) when the agent cannot be reached. A non-`200`
from the agent (for example `404 no logs for task ...`) is passed through
with its status and body.

The log route always counts as a stream on the leader (see
[Stream limits](#stream-limits)), also with `follow=0`.

### `GET /v1/events` (SSE)

`Content-Type: text/event-stream`, `Cache-Control: no-cache`. The stream
opens with a ping and then sends one event per change:

```
event: ping
data: {}

event: agent
data: {"id":"pi-kitchen"}

event: job
data: {"name":"web"}

event: task
data: {"job":"web","event":"crash"}

event: status
data: {}

: keepalive
```

| Event | Data | Meaning |
| --- | --- | --- |
| `ping` | `{}` | first event of every stream: the stream is up |
| `agent` | `{"id"}` | an agent registered, or was dropped after missing its heartbeats |
| `job` | `{"name"}` | a job was created, updated, placed, moved or deleted |
| `task` | `{"job","event"}` | a task event reported by an agent (`POST /v1/notify`): `start`, `started`, `crash`, `stop`, `unplaceable` |
| `status` | `{}` | refresh everything: a general change, or the reader missed events |

Events are not a log: a client should treat any event as "re-read the
affected resource". The node keeps the last 64 events; a reader that falls
further behind (it polls every 500 ms) gets a single `status`. A
`: keepalive` comment is written after 15 s of silence. When the node stops
leading, it ends the stream; reconnect to the new leader.

### Internal routes (agent to leader)

These are called by agents, not by users. They are documented for
completeness and for plugin authors.

**`POST /v1/agents`** (register):

```json
{"id": "pi-kitchen", "endpoint": "http://192.168.1.20:8080", "version": "3.0.11",
 "placed": {"web": 1}}
```

`placed` reports the instances already running on that agent per job.
`200 {"status":"registered","jobs":[job, ...],"state_time":"<RFC 3339>"}`;
`400 {"error":"id and endpoint required"}`;
`409 {"error":"agent <id> already registered with different endpoint"}`.

**`POST /v1/heartbeat`**: `{"id", "endpoint", "version"}` plus optional
telemetry fields (as in `GET /v1/agents`); unreadable telemetry is ignored.
`200 {"status":"ok"}`; `404 {"error":"not registered"}` tells the agent to
register again; `400` as above.

**`POST /v1/notify`**: `{"job": "web", "event": "crash", "agent": "pi-kitchen"}`.
Always `204`. The event goes to `/v1/events` as a `task` event (a `status`
event if `job` is empty). `unplaceable` with both `job` and `agent` also books
off that placement so the leader places the instance elsewhere.

**`DELETE /v1/agents/{id}`**: `204`; `400 {"error":"agent id required"}`.

## Agent API (`P`)

| Method | Path | Response | Notes |
| --- | --- | --- | --- |
| `OPTIONS` | any | `200` | CORS preflight, unauthenticated |
| `GET` | `/health` | `200 {"status":"ok"}` | unauthenticated |
| `GET` | `/leader` | `200` | unauthenticated |
| `GET` | `/capacity` | `200` | this node's capacity |
| `GET` | `/tasks` | `200 [task, ...]` | this node's tasks |
| `GET` | `/logs/{task}/{stdout\|stderr}` | SSE | snapshot, or live with `?follow=1` |
| `POST` | `/run[?replace=1]` | `202` | start a job's task here (leader to agent) |
| `POST` | `/stop/{job}` | `200 {"stopped": n}` | stop the job's tasks, keep the job (preemption) |
| `POST` | `/stop-task/{id}` | `200 {"stopped": "<id>"}` | stop one task |
| `DELETE` | `/delete/{job}` | `200 {"deleted": n}` | delete the job's tasks here |
| `POST` | `/flip` | `202` | replace the HopOS kernel |
| any | `/v1/agents`, `/v1/agents/*`, `/v1/jobs`, `/v1/jobs/*`, `/v1/status`, `/v1/tasks`, `/v1/events` | leader's answer | proxied to the leader |

Unknown paths: `404 {"error":"not found"}`. A wrong method on `/run`,
`/stop/`, `/stop-task/`, `/delete/` or `/flip`:
`405 {"error":"method not allowed"}`. `/v1/heartbeat` and `/v1/notify` are
not proxied; agents call the leader port directly.

### `GET /leader`

```json
{"leader": "192.168.1.10:9080", "lease_expires_at": "2026-10-17T12:00:30Z"}
```

`leader` is the leader API address this node knows (empty if none).
`lease_expires_at` is present only on the leader itself.

### `GET /capacity`

```json
{
  "cpu_cores": 4,
  "memory_bytes": 4294967296,
  "cpu_used_shares": 1536,
  "memory_used_bytes": 805306368,
  "tasks_running": 3,
  "attributes": {"zone": "kitchen"}
}
```

`attributes` (the node's affinity attributes) is present only when the node
has any. CPU is accounted in shares, 1024 per core.

### `GET /logs/{task}/{stream}`

`stream` is `stdout` or `stderr`. Each line is one SSE `data:` event (a line
with embedded newlines becomes several `data:` lines of one event).

- Without `follow` (or with anything but `follow=1` / `follow=true`): the
  retained lines (default the last 50, kept 5 minutes after the task ends;
  see `runner.log_tail_lines` and `runner.log_keep_seconds` in
  [configuration.md](configuration.md)), then the response ends.
- With `follow=1`: the retained lines, then new lines as they come, until the
  task stops or the reader goes away; `: keepalive` after 15 s of silence.
  Counts as a stream.

Errors: `404 {"error":"no logs for task <id>"}`;
`400 {"error":"usage: /logs/{taskID}/stdout or /logs/{taskID}/stderr"}`;
`400 {"error":"stream must be stdout or stderr"}`.

### `POST /run`

Body: a job spec (as for `POST /v1/jobs`). The leader calls this when it
places an instance; `?replace=1` replaces the job's existing task here.

| Status | Body |
| --- | --- |
| `202` | `{"status":"accepted","job":"web","message":"job accepted, starting in background"}` |
| `400` | `{"error":"invalid json"}` |
| `406` | `{"error":"affinity mismatch"}` |
| `503` | `{"error":"insufficient capacity"}` (no capacity, too many tasks or jobs; on HopOS also no slot partition large enough) |
| `500` | `{"error":"internal error"}` |

### `POST /flip` (HopOS)

Replaces the node's kernel while (warm) or after stopping (cold) its tasks.
See [hopos.md](hopos.md).

```json
{"url": "http://10.0.0.5/hopos-o6n-headless.flip", "sha256": "<64 hex>", "cold": false}
```

- `url` must be absolute (`400 url must be absolute`); `sha256` 64 hex
  characters (`400 sha256 must be 64 hex characters`); `cold` optional, must
  be a boolean (`400 cold must be true or false`).
- `202 {"status":"flip requested: ..."}` (or `cold flip requested: ...`). The
  answer is not the outcome: a successful flip never returns. Watch the
  node's console and its re-registration.
- On HopOS, if the kernel refuses the bundle before the jump:
  `502 {"error":"..."}` (logged with `HOP_FLIP_FAIL`).
- On a host (`agentd`): `501 {"error":"this node cannot flip its kernel (not a HopOS node, or flipping is disabled)"}`.

### Proxy to the leader

Requests to the `/v1/*` routes above are forwarded unchanged (method, path,
query, body, `Content-Type` and the caller's `X-Hop-Auth`) to the leader this
node knows. Because the signature covers only method, path and body and the
cluster shares one key, it stays valid. If this node is the leader, the
request is handled in-process.

- No leader known: `503 {"error":"no leader available"}`.
- Body over 8 MiB: `413 {"error":"request body too large"}` (in practice the
  1 MiB server limit applies first).
- Leader unreachable: `502 {"error":"leader <addr>: ..."}`.
- Timeout 120 s for buffered calls; `/v1/events` and
  `/v1/agents/{id}/logs/...` are relayed chunk by chunk as streams (60 s
  idle limit on the source).

## Stream limits

A stream holds a connection worker for as long as it runs, and the worker
pools are fixed. Each node therefore admits only a few open streams at once,
counted per node across both ports:

| Node | Workers per port | Max open streams |
| --- | --- | --- |
| host (`agentd`) | 8 | 4 |
| HopOS | 4 | 3 |

Streams are `/v1/events`, a followed `/logs/...?follow=1`, any
`/v1/agents/{id}/logs/...` on the leader (also `follow=0`), and any proxied
stream on an agent port. Over the limit:

```
503 {"error":"too many open streams (4); try again later"}
```

A stream frees its slot when it ends or when the reader goes away (detected
within a poll, 500 ms). When every worker of a port is busy, the response
gets `Connection: close` so a waiting connection gets the worker next.

## Status codes

| Code | Meaning |
| --- | --- |
| `200` | OK |
| `201` | job created (`dispatched` or `pending`) |
| `202` | accepted, runs in the background (`/run`, `/flip`) |
| `204` | done, no body |
| `400` | malformed request (`invalid json`, missing name/id, bad path) |
| `401` | missing or wrong `X-Hop-Auth` |
| `404` | unknown route, agent, job, task or logs; `not registered` on heartbeat |
| `405` | wrong method on an agent route |
| `406` | affinity mismatch on `/run` |
| `409` | agent id registered with another endpoint; job update already running |
| `413` | body too large (512 KiB signed limit, 1 MiB server limit, 8 MiB proxy limit) |
| `500` | internal error (also: out of memory building a response) |
| `501` | `/flip` on a node that cannot flip |
| `502` | leader or agent unreachable through a proxy; HopOS flip refused |
| `503` | not the leader, no leader known, too many open streams, insufficient capacity on `/run`, node shutting down |
| `504` | `agentd` only: the node did not answer in time (300 s) |

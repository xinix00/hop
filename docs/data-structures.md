# Data structures

The vocabulary of Hop lives in the `types` crate (`types/src`): the job spec,
the task and its states, the agent (node identity), times and durations, and
the JSON reader/writer they all use. Every other crate speaks these types, so a
job that enters the API is byte for byte the job the leader sends to an agent
and the job in the committed snapshot. The JSON shape follows the earlier Go
generation of Hop field for field, so the GUI, the CLI and old snapshots keep
working.

See [api.md](api.md) for the endpoints that carry these types and
[lifecycles.md](lifecycles.md) for how jobs and tasks move through the cluster.

## JSON conventions

- **Parser limits** (`types/src/json.rs`): at most 1 MiB of input and 32 levels
  of nesting; duplicate keys, invalid UTF-8 and trailing content after the
  document are rejected. Integers are kept exact (no `f64` round trip), so a
  `memory_limit` above 2^53 survives.
- **`null`** for any field means "not set" (the default stays).
- **Unknown keys**: the API and the CLI are lenient and ignore them. Jobs in
  `cluster.init_jobs` (see [configuration.md](configuration.md#cluster)) are
  read strictly: an unknown key is an error.
- **Writing** follows Go's `omitempty`: empty strings, zero numbers and empty
  maps are left out (exceptions are noted below).
- **Maps** (`env`, `tags`, `affinity`, ...) are string-keyed and written with
  sorted keys.
- **Durations** in a job are either an integer number of nanoseconds or a Go
  duration string (`"5s"`, `"1m30s"`, `"500ms"`; units `ns`, `us`/`µs`, `ms`,
  `s`, `m`, `h`; `"0"` is allowed without a unit; negative is an error). Hop
  writes them back as integer nanoseconds. (In the node config, durations must
  be strings; see [configuration.md](configuration.md#timeouts).)
- **Timestamps** are RFC 3339 strings (`"2026-09-29T10:00:00.5Z"`), internally
  Unix nanoseconds. The zero time is written as Go's zero time.

## Job spec

A job is what the user wants to run. A job has no id: **the name is the key**,
and submitting a job with an existing name updates it (upsert).

| JSON field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | string | required | Unique name. |
| `affinity` | map string to string | `{}` | Node attributes that must all equal the given values (AND, exact match). |
| `driver` | `"exec"`, `"docker"`, `"hop"` | derived | The runner. Not set (or `""`): `docker` if `image` is set, else `exec`; see also the [hop shorthand](#validation). Any other value is an error. |
| `image` | string | `""` | Docker image (Docker only). |
| `artifacts` | array of [artifacts](#artifact) | `[]` | Downloads, at most 16. Each agent uses the first one whose `match` fits the node. |
| `user` | string | `""` | Run as this user (default: the agent's user). |
| `command` | string | `""` | The command (run via `/bin/sh -c` on a host). |
| `count` | integer | `0` | Number of instances. `0` (or less) means 1; `-1` means a daemon job: one instance on every agent. |
| `ports` | map string to integer 0..65535 | `{}` | Port name to port, at most 64. `0` is dynamic (assigned by the node). A fixed port changes the default `update_policy`, see below. |
| `cpu_shares` | integer | `0` | Relative CPU weight, 1024 per core. `0` is unlimited and not counted against capacity. |
| `memory_limit` | integer (bytes) | `0` | Memory limit. `0` is unlimited and not counted. Must be non-negative. |
| `env` | map string to string | `{}` | Extra environment variables. |
| `tags` | map string to string | `{}` | Labels for discovery and grouping. Some tags have meaning on HopOS, see [Runners](#runners). |
| `volumes` | map string to string | `{}` | Host path to task path. |
| `health_check` | [health check](#health-check) | none | Optional health check. |
| `max_restarts` | integer | not set | Not set: unlimited with backoff. `0`: never restart. `-1`: unlimited. `N`: give up after N restarts within `restart_window`. |
| `restart_window` | duration | `0` (= 5 min) | Window in which restarts count. |
| `update_policy` | `"rolling"`, `"recreate"`, `"blue-green"` | derived | How an update rolls out. Not set: `recreate` if any port is fixed (non-zero), else `rolling`. |
| `priority` | integer | not set | Lower is more important. Not set: appended at the end. `0` is the top. The leader renumbers all jobs to unique values `0..N-1`. |
| `deploying` | bool | `false` | Set by the leader while an update rolls out (written only when `true`). It stays set if the rollout fails or the leader dies halfway; applying the job again clears it. Not meant to be set by users. |

Maps (`affinity`, `env`, `tags`, `volumes`, artifact `match`/`headers`/`auth`)
hold at most 256 keys each.

### Artifact

| JSON field | Type | Meaning |
| --- | --- | --- |
| `url` | string | Download URL; the scheme picks the downloader: `http://`, `https://`, `s3://`. |
| `match` | map string to string | Node attributes that must match; empty fits every node. |
| `headers` | map string to string | HTTP headers for the download. |
| `auth` | map string to string | Other credentials; for S3: `access_key`, `secret_key`, `region`. |
| `extract` | string | `"tar.gz"` (or `"tgz"`), `"tar.bz2"` (or `"tbz2"`), `"zip"`, or empty for a plain file. |
| `filename` | string | File name for a plain download (default: the base name of the URL). |

Several artifacts are how one job spans architectures, e.g. one per
`node.arch`.

### Health check

| JSON field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `type` | `"http"`, `"tcp"`, `"file"` | `"http"` | HTTP: GET, status 200..399 is healthy. TCP: a successful connect is healthy. File: the file's mtime changed since the previous check (the first check only needs it to exist). |
| `path` | string | `""` | HTTP: the request path. File: the absolute file path. Always written. |
| `port` | string | `"http"` | HTTP/TCP: the **name** of a port from `ports`. A missing port counts as a failed check. |
| `interval` | duration | `0` | Accepted and stored, but **not used by the current agent**: checks run every `timeouts.health_check_interval` of the node config (default 5 s). |
| `timeout` | duration | `0` | HTTP/TCP timeout per probe. `0`: the node's `timeouts.health_check_timeout` (default 5 s). |
| `initial_timeout` | duration | `0` | Grace period after a task starts before checks begin. `0`: no grace period. |
| `failure_threshold` | integer | `0` (= 3) | Consecutive failures before the task is unhealthy. |

Note: the doc comments in `types/src/job.rs` say `interval` defaults to 10 s and
`initial_timeout` to 30 s; the agent code (`agent/src/node.rs`,
`agent/src/health.rs`) does not apply either default. The table above
describes the code.

### Validation

When a job is submitted (`POST /v1/jobs`, `api/src/leader.rs`):

1. The body must parse as a job; otherwise `400 invalid json`.
2. `name` must be non-empty; otherwise `400 name required`.
3. **Hop shorthand**: no `driver`, no `command`, no `image` and exactly one
   artifact means `"driver": "hop"` (the artifact is the program).
4. There must be something to run: a `command`, an `image`, or `driver` `hop`
   with at least one artifact; otherwise `400 command or image required (or
   driver "hop" with at least one artifact)`.
5. **Fixed ports cannot roll**: an explicit `rolling` or `blue-green` with any
   non-zero port is refused with `400 job <name>: a fixed port (<port>
   <number>) cannot roll; use update_policy recreate or a dynamic port` (and
   the log line `HOP_JOB_REFUSED`). Without a policy, a fixed port means
   `recreate`.

Type errors (a string for `count`, `1.5` for `count`, a port above 65535, a
negative `memory_limit`, an unknown enum value) fail step 1. `init_jobs` apply
steps 2 to 4 plus strict unknown-key checking at startup.

### Update strategies

When a job with an existing name is applied (`leader/src/update.rs`), the
leader stores the new definition with `deploying: true`, rolls it out, and
clears `deploying` on success. A job without its own `priority` keeps the old
one.

| Policy | Rollout |
| --- | --- |
| `rolling` | Per old task: place a new one (up to the desired count), then stop the old one, with a 2 s pause between steps. Reconcile fills a scale-up afterwards. Never preempts other jobs. |
| `recreate` | Stop all old tasks, then start all new ones. The only policy that works with fixed ports. |
| `blue-green` | Start the full new set next to the old one, then stop the old set. Needs twice the capacity; never preempts. |

### Placement and preemption

The leader places instances round-robin over the agents; each agent decides
itself whether a job fits (`agent/src/node.rs`):

- `affinity` must match the node attributes, else the agent refuses.
- `cpu_shares` and `memory_limit` must fit next to what is already reserved
  (every task counts, whatever its state). Zero means not counted.
- For a new job, if every agent is full, the leader may stop the least
  important job on a full agent, provided it is strictly less important than
  the new job (a higher `priority` number; unset counts as last), and place
  the new job there. Updates never preempt.

### Examples

A minimal process job:

```json
{"name": "sleeper", "command": "sleep 30"}
```

A service on several nodes with a dynamic port and a health check:

```json
{
  "name": "api-service",
  "command": "./server --http=$ER_PORT_HTTP",
  "count": 3,
  "affinity": {"node.arch": "arm64"},
  "artifacts": [
    {
      "url": "s3://bucket/app-arm64.tar.gz",
      "match": {"node.arch": "arm64"},
      "auth": {"access_key": "...", "secret_key": "...", "region": "eu-west-1"},
      "extract": "tar.gz"
    }
  ],
  "ports": {"http": 0, "grpc": 0},
  "cpu_shares": 2048,
  "memory_limit": 536870912,
  "env": {"DB_HOST": "postgres.internal"},
  "tags": {"service": "api"},
  "volumes": {"/data/shared": "data"},
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
}
```

A Docker job:

```json
{
  "name": "counter-docker",
  "image": "alpine:latest",
  "command": "sh -c 'i=0; while true; do echo counter: $i; i=$((i+1)); sleep 1; done'",
  "count": 10,
  "cpu_shares": 1024,
  "memory_limit": 1073741824
}
```

A HopOS app on every node (daemon), using the hop shorthand (no `driver`
needed), with a fixed port (so `update_policy` defaults to `recreate`):

```json
{
  "name": "welcome",
  "count": -1,
  "artifacts": [{"url": "https://example.com/welcome-arm64.elf"}],
  "ports": {"http": 80},
  "memory_limit": 8388608,
  "tags": {"core-class": "small"}
}
```

## Runners

The `driver` picks the backend (`runner/src`):

| Driver | Where | What a task is |
| --- | --- | --- |
| `exec` | host (`agentd`) | A process run with `/bin/sh -c <command>` in a task directory under `paths.rootfs_base`. With `runner.isolate`: chroot plus namespaces and a cgroup v2 memory limit on Linux, `sandbox-exec` on macOS. `cpu_shares` becomes a `nice` level. Artifacts are downloaded and extracted into the task directory. |
| `docker` | host (`agentd`) | A container via the Docker socket (`runner.docker_socket`). |
| `hop` | HopOS | A native app image in its own slot (cage). The kernel owns slots, cores and partitions and enforces isolation and the memory limit in hardware. |

The host does not run `hop` jobs, and HopOS does not run `exec` or `docker`
jobs (`containers are not supported on HopOS`).

HopOS specifics:

- The job needs exactly one matching artifact, a raw app image (no `extract`).
- Cores: `cpu_shares / 1024`, at least 1 (the agent rounds `cpu_shares` up to
  whole cores). `memory_limit` is rounded up to 2 MiB blocks and is the
  partition size.
- Tag `sharegroup`: the app runs on one core in a cooperative pool of
  `cpu_shares / 1024` cores shared with apps carrying the same tag value.
- Tag `core-class`: the physical core class (`big`, `mid`, `small`).
- At most 4 images download at once; the others stay `queued`.
- A stop that the kernel does not confirm within 3 s escalates; an
  unconfirmed stop quarantines the slot.
- Each task has its own network stack; the node publishes every assigned port
  with stateless DNAT and the app binds the same number.

Environment every task gets (all drivers): its ports as `ER_PORT_<NAME>` and the
node attributes as `ER_ATTR_<KEY>`, where the name is upper-cased and any
character other than `A-Z`/`0-9` becomes `_` (`node.os` becomes
`ER_ATTR_NODE_OS`). On a host the order is: platform base (`HOME`, `TMPDIR`,
`PATH`), `ER_PORT_*`, the job's `env`, `ER_ATTR_*`; later entries win.

## Task

A task is one running instance of a job on one agent (`types/src/task.rs`).

| JSON field | Type | Meaning |
| --- | --- | --- |
| `id` | string | Unique; new on every restart. |
| `job_name` | string | The job it belongs to. |
| `driver` | string | `exec`, `docker` or `hop`. |
| `image` | string | Docker image (omitted when empty). |
| `ports` | map string to integer | Assigned ports (always written). |
| `pid` | integer | Process id; Docker: 0; HopOS: index of the primary slot. |
| `state` | string | See [Task states](#task-states). |
| `started_at` | timestamp | When the app actually started running. Always written. |
| `restart_count` | integer | Restarts so far. |
| `last_failed_at` | timestamp | Last crash (drives the restart window). Always written. |
| `next_restart_at` | timestamp | When the next restart attempt runs; zero while running or after giving up. Always written. |
| `cpu_shares`, `memory_limit` | integer | Copied from the job, for capacity accounting (omitted when 0). |
| `cpu_percent` | number | Measured CPU use, relative to `cores`. |
| `cores` | integer | Cores `cpu_percent` refers to; omitted when unknown (0). |
| `core` | integer | Logical core the task runs on (primary core of its slot); `0` is the OS core and valid; omitted when unknown. |
| `mem_percent` | number | Measured memory use. |
| `downloaded_bytes` | integer | Bytes received while `downloading`. |
| `image_size_bytes` | integer | Total image size while `downloading`. |

Example (`GET /v1/tasks`, one element):

```json
{
  "id": "k3x9p2aa",
  "job_name": "api-service",
  "driver": "exec",
  "ports": {"grpc": 41233, "http": 41231},
  "pid": 12345,
  "state": "running",
  "started_at": "2026-10-17T09:12:03.25Z",
  "restart_count": 0,
  "last_failed_at": "0001-01-01T00:00:00Z",
  "next_restart_at": "0001-01-01T00:00:00Z",
  "cpu_shares": 2048,
  "memory_limit": 536870912,
  "cpu_percent": 1.5,
  "mem_percent": 3.2
}
```

### Task states

| State | Meaning |
| --- | --- |
| `queued` | Accepted, capacity reserved, waiting for a download slot. Every new task starts here. |
| `downloading` | The image is streaming in; progress in `downloaded_bytes` / `image_size_bytes`. |
| `running` | The app really runs. |
| `stopping` | Being stopped; also the state of a crashed task that is waiting to be restarted (its reservation is kept). |
| `failed` | Crashed, unhealthy, failed to start, or gave up after too many restarts. |
| `system` | A node resident (the kernel, slot 0, or Hop itself, slot 1). Only synthesised in `/v1/tasks` from the heartbeat; never placed, restarted, stopped or counted. |

There is no `stopped` state: a deliberately stopped task is removed. Every
present task counts toward capacity, whatever its state.

Transitions (`agent/src/node.rs`):

```
            accept
              |
              v
  queued --progress--> downloading
     |                      |
     +-------start ok-------+-----> running
     |                      |         |  \
     | start failed         |   exit  |   \ unhealthy (failure_threshold)
     v                      v         v    v
   failed <-----------------+     stopping  failed
     |                                |      |
     +------- restart path -----------+------+
              (new task: new id, queued, restart_count + 1)

  any state --stop / delete / update--> removed (runner releases it)
  start with no capacity on the node  --> removed, handed back to the leader ("unplaceable")
```

Restart path:

- `max_restarts` not set or `-1`: unlimited; `0`: never; `N`: once
  `restart_count` reaches N the task stays `failed`.
- If the task ran longer than `restart_window` (default 5 min) before failing,
  `restart_count` is reset to 0 first.
- The first restart is immediate; restart number `n` waits a backoff of
  1 s, 2 s, 4 s, 8 s, 16 s, then 30 s, with jitter taken from the upper half of
  that base. `next_restart_at` shows when.
- The replacement is a new task with a new id, born `queued`.

## Agent (node identity)

An agent is a node registered with the leader (`types::Agent`).

| JSON field | Type | Meaning |
| --- | --- | --- |
| `id` | string | Unique, persistent across restarts (`data/node-id` on a host). |
| `endpoint` | string | Agent HTTP address, `http://ip:port`. |
| `version` | string | Agent version. |
| `last_seen` | timestamp | Last heartbeat. |
| `temp_milli_c` | integer | Hottest CPU temperature in milli-degrees Celsius; omitted when unknown. |
| `kern_cpu_percent`, `kern_mem_bytes`, `kern_ram_bytes`, `kern_core` | number | Usage of the kernel (slot 0) on HopOS; each omitted when not measured. |
| `hop_cpu_percent`, `hop_mem_bytes`, `hop_ram_bytes`, `hop_core` | number | Usage of Hop itself (slot 1) on HopOS; each omitted when not measured. |

The telemetry fields are reported flat in the heartbeat; they are not
scheduling input. An agent whose last heartbeat is older than
`timeouts.node_dead_threshold` is considered dead. Node attributes (used by
`affinity` and artifact `match`) are not part of this record; on a host they
are `node.id`, `node.arch`, `node.os`, `node.docker` plus `node.attributes`
from the config (see [configuration.md](configuration.md#node-identity)).

```json
{
  "id": "pi-kitchen",
  "endpoint": "http://192.168.1.21:8080",
  "version": "3.0.11",
  "last_seen": "2026-10-17T09:12:10Z",
  "temp_milli_c": 59800,
  "kern_cpu_percent": 0,
  "kern_mem_bytes": 3145728,
  "kern_ram_bytes": 67108864,
  "kern_core": 0,
  "hop_mem_bytes": 5242880
}
```

## Events

The leader's event stream (`GET /v1/events`, Server-Sent Events, see
[api.md](api.md)) says *what* changed, not how; clients re-read the resource.
Each frame is `event: <kind>\ndata: <json>\n\n`:

| Kind | Data | When |
| --- | --- | --- |
| `ping` | `{}` | First frame of every stream. |
| `status` | `{}` | The cluster as a whole changed (settle period over), or too many changes to list: re-read everything. |
| `agent` | `{"id": "<agent id>"}` | An agent registered, died or left. |
| `job` | `{"name": "<job>"}` | A job was placed, updated or deleted. |
| `task` | `{"job": "<job>", "event": "<event>"}` | A task-level notification from an agent. |

Task events: `start` (process runs, health check not yet passed), `started`
(ready for traffic: started without a health check, or first passing check),
`crash` (crashed or unhealthy), `stop` (tasks of a deleted job stopped),
`unplaceable` (accepted but could not be placed; handed back to the leader).

Identical pending events are coalesced. At most 64 distinct events wait; on
overflow, or when a reader falls behind the buffer, it gets a single `status`.
Idle streams receive `: keepalive` comments.

## Limits

| Limit | Value | Source |
| --- | --- | --- |
| JSON input | 1 MiB | `types::json::MAX_INPUT` |
| JSON nesting | 32 | `types::json::MAX_DEPTH` |
| Keys per map in a job | 256 | `types::job::MAX_MAP_ENTRIES` |
| Artifacts per job | 16 | `types::job::MAX_ARTIFACTS` |
| Ports per job | 64 | `types::job::MAX_PORTS` |
| Jobs per agent / tasks per agent | 1024 / 1024 | `agent::MAX_JOBS`, `agent::MAX_TASKS` |
| Jobs / agents per leader | 1024 / 256 | `leader::MAX_JOBS`, `leader::MAX_AGENTS` |

## See also

- [api.md](api.md), [cli.md](cli.md), [lifecycles.md](lifecycles.md),
  [architecture.md](architecture.md), [hopos.md](hopos.md), [apps.md](apps.md),
  [development.md](development.md), [index.md](index.md)

# Configuration

This page covers the configuration of `agentd`, the Hop daemon for Linux and
macOS: its JSON config file, its command-line flags, the lock backends, where
the cluster state lives, ports, timeouts, capacity and paths.

A HopOS node is configured through `hopos.cfg` instead; see
[hopos.md](hopos.md) for those keys. Both kinds of node can join the same
cluster (see [architecture.md](architecture.md)).

Source of truth: `config/src/lib.rs` (fields and defaults), `agentd/src/boot.rs`
and `agentd/src/main.rs` (flags and startup), `store/src/lib.rs` (lock and
state backends).

## Starting agentd

```
agentd [--config FILE] [--node ID] [--cluster NAME] [--lock URL] [--standalone] [--api-key KEY]
```

| Flag | Effect |
| --- | --- |
| `--config FILE` | Read the JSON config from `FILE`. A missing file is not an error: agentd logs `config FILE not found; using defaults` and continues with the defaults. Without `--config` the defaults are used. |
| `--node ID` | Overrides `node.id`. |
| `--cluster NAME` | Overrides `cluster.name`. |
| `--lock URL` | Sets `cluster.lock.type` to `hoplockserver` and `cluster.lock.url` to `URL`. Ignored together with `--standalone`. |
| `--standalone` | Forces standalone mode (in-memory lock, single node). Also accepts `--standalone=true` / `--standalone=1`; any other inline value turns it off. |
| `--api-key KEY` | Overrides `api_key`. |
| `--help`, `-h` | Prints the usage line. |

Flags accept `--name value`, `--name=value` and the single-dash form
(`-name value`). An unknown flag or a positional argument is an error.
Flags are applied on top of the config file; an empty flag value leaves the
config value alone.

`agentd` reads **no environment variables**. (The `HOPOS_*` variables belong
to the HopOS resident, see [hopos.md](hopos.md); `HOP_LEADER` belongs to the
`hop` CLI, see [cli.md](cli.md).)

Quick start (standalone, defaults, state in `./data`):

```
agentd --cluster demo
```

### Startup order

1. Parse flags, read the config file, apply flags.
2. Decide standalone: `--standalone`, or no usable lock section (see
   [Lock backends](#lock-backends)).
3. Validate `cluster.init_jobs` strictly; a bad init job stops the daemon.
4. Resolve the node id and the advertised IP (see [Node identity](#node-identity)).
5. Warn `HOP_API_NO_AUTH` if `api_key` is empty.
6. Clean up task directories and containers left over from a previous run.
7. Make one synchronous lease claim: if the lock is free, this node leads
   immediately.
8. Bind the agent port and the leader port, then log `... HOP_UP`.

## The config file

The format is JSON (not YAML). Rules enforced by `Config::from_json`:

- Anything not mentioned keeps its default, also inside a block that is
  mentioned (`{"timeouts": {"leader_lease": "45s"}}` keeps the other three
  timeouts).
- **Unknown keys are an error**, at every level (a typo would otherwise be a
  setting that silently does nothing).
- `null` values are skipped (the default stays).
- An empty file (or only whitespace) is the default config.
- A file that does not start with `{` is rejected with a hint that the config
  is JSON now (old YAML configs convert one-to-one: same keys, durations
  quoted).
- Durations in `timeouts` must be strings in Go duration syntax (`"30s"`,
  `"1m30s"`, `"1500ms"`, `"0"`). A number is an error, a number without a
  unit (`"30"`) is an error, a negative duration is an error.
- One JSON document only; trailing content is an error. The JSON parser also
  rejects duplicate keys, input over 1 MiB and nesting deeper than 32.

### Top level

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `node` | object | see below | This node. |
| `cluster` | object | see below | The cluster it belongs to. |
| `capacity` | object | see below | Ceiling on what Hop may hand out on this node. |
| `paths` | object | see below | File system paths. |
| `runner` | object | see below | Task runner settings. |
| `timeouts` | object | see below | Timeouts. |
| `api_key` | string | `""` | Shared HMAC key for request authentication (`X-Hop-Auth`, see [api.md](api.md)). Empty means no authentication: every request is accepted and agentd logs `HOP_API_NO_AUTH`. Must be the same on every node of a cluster. |

### `node`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `id` | string | `""` | Node id. Empty: read `node-id` from the data directory, or generate one and store it there (see [Node identity](#node-identity)). |
| `ip` | string | `""` | IP to advertise. Empty: chosen automatically. |
| `port` | integer (0..65535) | `8080` | Agent port. The leader port is this plus 1000; startup fails if that overflows 65535. |
| `network` | string | `""` | A CIDR (`"10.0.0.0/24"`). When `ip` is empty, the advertised IP is picked from the interfaces inside this network. Startup fails if no interface matches or the CIDR is invalid. |
| `attributes` | object of string to string | `{}` | Extra node attributes (at most 64), used for job `affinity` and artifact `match`. They override the detected attributes of the same name. |

### `cluster`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | string | `"default"` | Cluster name. Must not be empty. The lease key and state key derive from it. |
| `lock` | object | see below | Backend for the leader lease. |
| `init_jobs` | array of job objects | `[]` | Jobs seeded once on a clean boot (at most 64). |

`init_jobs` entries use exactly the job JSON of `POST /v1/jobs` (see
[data-structures.md](data-structures.md#job-spec)), but are parsed
**strictly**: unknown fields, a missing `name`, or a job with nothing to run are
startup errors. The hop shorthand applies (one artifact, no `command`, no
`image` means `"driver": "hop"`).

A clean boot is: no committed snapshot in the state store and an empty job
store. An unreachable state store is not a clean boot; init jobs are never
seeded on a storage error. An init job whose name already exists is skipped.

### `cluster.lock`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `type` | string | `""` | `""` or `"hoplockserver"`, `"s3"`, or `"mem"`. Any other value is a startup error. |
| `url` | string | `""` | Base URL of the hoplockserver (`http://host:port`). |
| `key` | string | `""` | Key of the lease object. Empty: `leases/<cluster.name>`. |
| `api_key` | string | `""` | `X-API-Key` header for the hoplockserver (omitted when empty). |
| `s3` | object | see below | S3 settings, used when `type` is `"s3"`. |

### `cluster.lock.s3`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `endpoint` | string | `""` | S3 endpoint URL. Required for `type: "s3"`. |
| `bucket` | string | `""` | Bucket. Required for `type: "s3"`. |
| `region` | string | `""` | Region. |
| `access_key_id` | string | `""` | Access key. |
| `secret_access_key` | string | `""` | Secret key. |
| `session_token` | string | `""` | Session token, if any. |
| `use_path_style` | bool | `false` | Path-style addressing (MinIO and similar). |

Secrets (S3 keys, session token, hoplockserver key) never appear in log lines;
user info in URLs is redacted.

### `capacity`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `cpu_shares` | integer | `0` | CPU ceiling in shares (1024 per core). |
| `memory` | integer | `0` | Memory ceiling in bytes. |

`0` means "use what is detected". A ceiling only ever lowers capacity: the node
plans against the lower of the ceiling and the hardware (detected cores x 1024,
and `MemTotal` from `/proc/meminfo` on Linux or `sysctl hw.memsize` on macOS).

### `paths`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `state_file` | string | `"./data/state.json"` | Local cluster-state file, used when the state lives on disk (see [State storage](#state-storage)). If left at the default, agentd rewrites it to `./data/state-<cluster>.json` so two clusters on one host do not overwrite each other. Its directory is the **data directory**, which also holds `node-id`. |
| `rootfs_base` | string | `"/tmp/hop"` | Where per-task directories are created. |

### `runner`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `isolate` | bool | `true` | Process isolation: chroot with namespaces on Linux, `sandbox-exec` on macOS. |
| `docker_socket` | string | `"/var/run/docker.sock"` | Docker daemon socket. |
| `log_tail_lines` | integer | `50` | Lines of stdout/stderr kept in memory per task (small on purpose). |
| `log_keep_seconds` | integer | `300` | How long the tail of a stopped task stays readable (so a crash can be read afterwards). |

### `timeouts`

All values are duration strings.

| Key | Default | Meaning |
| --- | --- | --- |
| `health_check_interval` | `"5s"` | How often the agent polls its running tasks and runs their health checks. `0` falls back to 5 s. |
| `health_check_timeout` | `"5s"` | Default timeout of one HTTP/TCP health probe, when the job's `health_check.timeout` is 0. `0` falls back to 5 s. |
| `node_dead_threshold` | `"30s"` | After how long without a heartbeat the leader considers an agent dead. |
| `leader_lease` | `"30s"` | Lease TTL. Also drives the backend call deadline (see below). |

Keep `leader_lease` at **15 s or more**. The election runs every 10 s; a lease
shorter than that tick means the cached leader has expired at every tick and a
follower never registers. agentd does not enforce this minimum (even `"0s"` is
accepted); the HopOS side does (`hopos.lease_ttl`, see [hopos.md](hopos.md)).

Each call to the lease or state backend (S3, hoplockserver) has one total
deadline covering connect, headers, body and any retry: `max(leader_lease / 3,
5 s)`. Loading the committed state on becoming leader waits at most twice that.

## Ports

| Port | Default | Serves |
| --- | --- | --- |
| `node.port` | 8080 | Agent API (node-local endpoints; on a follower, `/v1/*` is passed on to the leader). |
| `node.port + 1000` | 9080 | Leader API. The lease records this address (`<ip>:<port+1000>`) as the owner. |

Both listen on `0.0.0.0`. Each port has a fixed pool of 8 connection threads;
long-lived streams (`/v1/events`, `logs -f`) may use at most 4 at once, beyond
that the node answers 503. See [api.md](api.md).

Task ports: a job port of `0` gets a free port from the kernel on a host; a
fixed port is used as given.

## Node identity

- **Id**: `node.id` (or `--node`); else the trimmed contents of
  `<data dir>/node-id`; else a new random 8-character `[a-z0-9]` id, written to
  `<data dir>/node-id` (a write failure is a warning, not fatal).
- **IP**: `node.ip`; else the first interface IP inside `node.network`; else
  the source address of an outbound UDP route (no packet is sent); else
  `127.0.0.1`.
- **Endpoint**: `http://<ip>:<node.port>`.
- **Attributes** detected by agentd: `node.id`, `node.arch` (Go names: `amd64`,
  `arm64`, `riscv64`), `node.os` (`linux`, `darwin`), `node.docker`
  (`true`/`false`). `node.attributes` from the config override these. Tasks
  see attributes as `ER_ATTR_<KEY>` environment variables (see
  [data-structures.md](data-structures.md#runners)).

## Lock backends

The lock holds the leader lease; whoever holds it leads (see
[lifecycles.md](lifecycles.md) for election and failover).

| `cluster.lock.type` | Required | Backend |
| --- | --- | --- |
| `"mem"` | nothing | In-process lease. Single node; effectively standalone. |
| `""` or `"hoplockserver"` | `url` | hoplockserver (compare-and-swap lease store, see [apps.md](apps.md)), with optional `api_key`. |
| `"s3"` | `s3.endpoint`, `s3.bucket` | Any S3-compatible bucket. A "ghost" lease object (GET 404, HEAD 200, create 412) is taken over after it stays unchanged for one lease TTL. |

**Standalone** mode applies when `--standalone` is given or the lock section is
not usable: for `s3`, endpoint or bucket missing; for `""`/`hoplockserver`,
`url` empty. So a config without any lock section, or `agentd --cluster demo`,
runs standalone with an in-memory lease. An unknown `type` with a URL is a
startup error.

The lease object is at `cluster.lock.key`, default `leases/<cluster>`. Its JSON
is the same as the Go `hoplock.State`, so host agents, HopOS nodes and older Go
nodes can share one bucket or one hoplockserver.

## State storage

The leaseholder writes the committed cluster state (the desired jobs) as one
snapshot, overwritten unconditionally (one writer: the current leader). A new
leader loads it before opening its leader API. The store is chosen by one rule
shared with HopOS:

1. Standalone: the local file `paths.state_file`.
2. Else, if `cluster.lock.s3.endpoint` and `cluster.lock.s3.bucket` are both
   set: object `state/<cluster>` in that bucket. Note this wins regardless of
   `lock.type`.
3. Else, if `lock.type` is `""` or `"hoplockserver"` and `url` is set: object
   `state/<cluster>` on that hoplockserver.
4. Else (`mem`): the local file `paths.state_file`.

The local file is written crash-safely (temp file, fsync, atomic rename, fsync
of the directory). Snapshots and leases are read up to 4 MiB; a hoplockserver
accepts at most 1 MiB per PUT. Deleting or renaming the state object is the
operator's "start clean" switch.

## Example config

A host agent joining a cluster on a hoplockserver:

```json
{
  "node": {
    "id": "node-1",
    "ip": "",
    "port": 8080,
    "network": "192.168.1.0/24",
    "attributes": {"rack": "a", "core-class": "big"}
  },
  "cluster": {
    "name": "home",
    "lock": {
      "type": "hoplockserver",
      "url": "http://192.168.1.10:8090",
      "key": "",
      "api_key": "<hoplockserver key>",
      "s3": {
        "endpoint": "",
        "bucket": "",
        "region": "",
        "access_key_id": "",
        "secret_access_key": "",
        "session_token": "",
        "use_path_style": false
      }
    },
    "init_jobs": [
      {"name": "hello", "command": "sleep 3600"}
    ]
  },
  "capacity": {"cpu_shares": 0, "memory": 0},
  "paths": {
    "state_file": "./data/state.json",
    "rootfs_base": "/tmp/hop"
  },
  "runner": {
    "isolate": true,
    "docker_socket": "/var/run/docker.sock",
    "log_tail_lines": 50,
    "log_keep_seconds": 300
  },
  "timeouts": {
    "health_check_interval": "5s",
    "health_check_timeout": "5s",
    "node_dead_threshold": "30s",
    "leader_lease": "30s"
  },
  "api_key": "<shared secret>"
}
```

The same with the lease and state in S3 (only the lock block differs):

```json
{
  "cluster": {
    "name": "home",
    "lock": {
      "type": "s3",
      "s3": {
        "endpoint": "https://s3.example.com",
        "bucket": "hop-leases",
        "region": "eu-west-1",
        "access_key_id": "...",
        "secret_access_key": "...",
        "use_path_style": true
      }
    }
  },
  "api_key": "<shared secret>"
}
```

A minimal config is `{}` (or no file): cluster `default`, standalone, port 8080.

## See also

- [hopos.md](hopos.md): `hopos.cfg` keys and how a HopOS node joins.
- [architecture.md](architecture.md): threads, leader and agent roles.
- [lifecycles.md](lifecycles.md): election, failover, state loading.
- [data-structures.md](data-structures.md): the job spec used in `init_jobs`.
- [index.md](index.md)

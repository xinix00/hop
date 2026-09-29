# Configuration

## Config File

The config file is **JSON** — the same format as a job spec, the API and the
state file. Two rules that make a config file behave:

- **An unknown key is an error**, not a silently ignored line. A typo in a key
  used to look like a setting and do nothing.
- **Durations are strings**: `"30s"`, `"1m30s"`, `"500ms"`. A bare number is an
  error, because a `leader_lease` of `30` silently meaning 30 *nanoseconds* is a
  cluster that loses its leader thousands of times a second.

Everything is optional; what you leave out keeps its default. A missing file is
not an error either — then the defaults *are* the configuration.

```json
{
  "node": {
    "id": "",
    "ip": "",
    "network": "",
    "port": 8080,
    "attributes": {"region": "eu-west-1", "gpu": "true"}
  },
  "cluster": {
    "name": "my-cluster",
    "lock": {
      "type": "hoplockserver",
      "url": "http://10.0.0.1:8090",
      "api_key": "",
      "key": ""
    }
  },
  "api_key": "",
  "capacity": {"cpu_shares": 14000, "memory": 8589934592},
  "paths": {
    "state_file": "/var/lib/hop/state.json",
    "rootfs_base": "/var/lib/hop/rootfs"
  },
  "runner": {"isolate": true, "docker_socket": "/var/run/docker.sock"},
  "timeouts": {
    "health_check_interval": "10s",
    "health_check_timeout": "5s",
    "node_dead_threshold": "30s",
    "leader_lease": "30s"
  }
}
```

| Key | Default | Meaning |
|-----|---------|---------|
| `node.id` | auto | Node ID. Empty = generated at startup |
| `node.ip` | auto | The IP the node advertises to the cluster. Empty = detected from the outbound interface |
| `node.network` | `""` | Optional CIDR (e.g. `"10.0.0.0/24"`). When `ip` is empty, hop takes the first interface IP inside this range. Handy on multi-homed nodes: HTTP still listens on `0.0.0.0`, only the advertised IP is pinned to the LAN/VPN. Ignored when `ip` is set |
| `node.port` | `8080` | Agent port. The leader runs on `port+1000` |
| `node.attributes` | `{}` | Custom node attributes, merged with the auto-detected ones (config wins). Used for placement constraints |
| `cluster.name` | `"default"` | Cluster name. Also the default lease key and state-file suffix |
| `cluster.lock.type` | `""` | Leader-election backend: `""`/`"hoplockserver"`, `"s3"`, or `"mem"`. No lock config at all = standalone |
| `cluster.lock.url` | `""` | hoplockserver base URL (`type=hoplockserver`) |
| `cluster.lock.api_key` | `""` | `X-API-Key` for hoplockserver — a *separate* key from `api_key` below |
| `cluster.lock.key` | auto | Lease object key. Default `clusters/<cluster.name>/lease.json` |
| `cluster.lock.s3` | — | `type=s3`: see the block below (AWS, Cloudflare R2, MinIO, B2, Ceph RGW) |
| `cluster.init_jobs` | `[]` | The baseline an empty cluster gets on a clean boot — see [Init jobs](#init-jobs) |
| `api_key` | `""` | Shared secret for HMAC request auth (`X-Hop-Auth`) on every hop endpoint. Empty = auth off (dev) |
| `capacity.cpu_shares` | `0` | Relative CPU capacity. `0` = auto-detect from the hardware |
| `capacity.memory` | `0` | Memory to commit, in bytes. `0` = auto-detect |
| `paths.state_file` | `./data/state.json` | Local state snapshot |
| `paths.rootfs_base` | `/tmp/hop` | Base directory for task rootfs/working dirs |
| `runner.isolate` | `true` | Process isolation (chroot on Linux, sandbox on macOS) |
| `runner.docker_socket` | `/var/run/docker.sock` | Docker daemon socket for the docker driver |
| `runner.log_tail_lines` | `50` | Lines of stdout/stderr kept per task in memory; `/logs/{id}/{stream}` serves them first. Small on purpose — hop runs on boards with a few hundred MB |
| `runner.log_keep_seconds` | `300` | How long the tail of a stopped task stays retrievable, so a crash can be read after the fact |
| `timeouts.health_check_interval` | `"5s"` | How often a task's health check runs |
| `timeouts.health_check_timeout` | `"5s"` | Deadline for one health check |
| `timeouts.node_dead_threshold` | `"30s"` | No heartbeat for this long = the node is dead |
| `timeouts.leader_lease` | `"30s"` | Leader lease duration |

An S3 lock backend instead of hoplockserver:

```json
{
  "cluster": {
    "name": "my-cluster",
    "lock": {
      "type": "s3",
      "s3": {
        "endpoint": "https://s3.eu-west-1.amazonaws.com",
        "bucket": "hop-cluster",
        "region": "eu-west-1",
        "access_key_id": "...",
        "secret_access_key": "",
        "session_token": "",
        "use_path_style": false
      }
    }
  }
}
```

`use_path_style` is `true` for MinIO and B2, `false` (default) for AWS and R2.
`session_token` is for STS temporary credentials.

**Coming from a YAML config?** It converts one-to-one: the same keys, in braces,
durations quoted. hop tells you so if you hand it the old file.

## Init jobs

`cluster.init_jobs` is the baseline an **empty** cluster gets by itself. A
leader that starts without a committed snapshot *and* without local jobs (a
"clean boot") seeds these jobs once through the normal upsert path — exactly as
if an operator had submitted them with `run apply`. That is how a blank node
(a Pi, a HopOS node) comes up with its work already on it, with nobody
deploying anything.

```json
{
  "cluster": {
    "name": "my-cluster",
    "init_jobs": [
      {
        "name": "hopdns",
        "command": "/usr/local/bin/hopdns",
        "count": -1,
        "ports": {"dns": 5353}
      },
      {"name": "my-app", "image": "myapp:v1", "count": 2}
    ]
  }
}
```

(`count: -1` = one on every node.)

**Semantics:**

- Field names are the **job JSON schema** (the same as `POST /v1/jobs` /
  [data-structures.md](data-structures.md)) — a spec is copy-pastable between
  config and API.
- **Clean boot only**: no snapshot in the state store (or no store configured)
  *and* an empty job store. Init jobs are not continuous enforcement — delete a
  seeded job and it stays deleted until the next clean boot (deletion is
  absence).
- **An outage is not an empty cluster**: if the state store is unreachable,
  nothing is ever seeded — otherwise an S3 outage would reset the cluster to
  the baseline.
- An existing job name is skipped; a seed never overwrites operator state.
- Typos are boot errors: unknown fields, a missing `name`, or a job without
  `command`/`image` stop the agent at startup.
- **Factory reset**: delete the `state/<cluster>` object from the bucket (and
  the node's local state) → the next leader start is a clean boot → the
  baseline comes back.

## CLI Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--config` | Path to config file | (none, uses defaults) |
| `--node` | Node name/ID (overrides config) | (from config) |
| `--cluster` | Cluster name | (from config) |
| `--lock` | hoplockserver URL (overrides config) | (from config) |
| `--standalone` | Run without a lock backend (single-node, in-memory) | false |
| `--api-key` | Shared secret for HMAC request auth (X-Hop-Auth); overrides config | (from config) |

No lock backend configured → hop runs standalone automatically.

## Development Config

```json
{
  "node": {"id": "dev-node", "ip": "127.0.0.1", "port": 8080},
  "cluster": {"name": "dev"},
  "capacity": {"cpu_shares": 14000, "memory": 8589934592},
  "paths": {"state_file": "./data/state.json", "rootfs_base": "./data/rootfs"},
  "runner": {"isolate": false}
}
```

That is `dev-config.json` in the repo root, minus `runner.isolate` (isolation
stays on there). No lock config = standalone, in-memory. Multi-node locally:
add `"cluster": {"lock": {"url": "http://127.0.0.1:8090"}}` and run
hoplockserver.

**Note:** State file path is automatically adjusted per cluster name (`./data/state-{cluster}.json`) when using the default path.

## Resource Limiting

### CPU Shares

A relative value: more shares means higher priority.

- `0` = no limiting (default nice value)
- `1000` = low priority
- `14000` = highest priority

Internally this is translated to nice values (0-19).

Capacity check: agents have `cpu_cores * 1024` total shares. Requests exceeding available shares are rejected (503).

### Memory Limit

In bytes.

- `0` = no limiting
- `536870912` = 512MB
- `1073741824` = 1GB

Per-platform implementation:
- **Linux**: cgroups v2 (after process start, OOM killer integration)
- **macOS**: an `ulimit -v` wrapper (before exec)

Capacity check: agents check total system memory. Requests exceeding available memory are rejected (503).

## Process Isolation

```json
{"runner": {"isolate": true}}
```

With isolation enabled:
- Every task runs in a chroot jail (Linux)
- A minimal shell environment is linked in automatically (`/bin/sh`, libraries)
- The command is relative to the chroot root (e.g. `/app/mybin`)

Without isolation (the dev default):
- Tasks run in their own working directory, but are not isolated

In both modes:
- The command may use shell syntax
- Memory limiting via ulimit (macOS) of cgroups (Linux)
- CPU limiting via nice
- Volume mounts via symlinks

**Default:** `isolate: true` (security by default in production)

## Auto-Detection

| Setting | Auto-Detected | Override |
|---------|---------------|----------|
| Node IP | Outbound interface | `node.ip` in config |
| Node Port | 8080 | `node.port` in config |
| Node ID | UUID (persisted in data/node-id) | `node.id` in config |
| Node Attributes | `node.id`, `node.arch`, `node.os`, `node.docker` | `node.attributes` in config (merges) |
| Capacity | System CPU/RAM | `capacity.*` in config |
| Paths | ./data/* | `paths.*` in config |
| Leader Port | node.port + 1000 | N/A |
| Timeouts | Smart defaults | `timeouts.*` in config |
| State File | ./data/state-{cluster}.json | `paths.state_file` in config |

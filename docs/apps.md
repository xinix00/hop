# Apps

Hop ships five apps in `apps/` and one client crate, `hoplib`. They are
workspace members and are released with Hop under one version tag. The apps
stand on Hop, never the other way round: no core crate depends on `hoplib`
or `apps/` (`tools/gate.sh` checks this).

| App | What it does | Host binary | HopOS resident |
| --- | --- | --- | --- |
| [hopdns](#hopdns) | DNS service discovery with federation | `hopdns` | `hopdns-hopos` |
| [hoplb](#hoplb) | HTTP load balancer on the `Host` header, with Prometheus metrics | `hoplb` | `hoplb-hopos` |
| [hopprom](#hopprom) | Prometheus exporter for the cluster | `hopprom` | `hopprom-hopos` |
| [hoplockserver](#hoplockserver) | compare-and-swap lease store for Hop's leader election | `hoplockserver` | `hoplockserver-hopos` |
| [hop-gui](#hop-gui) | static dashboard and its server | `hop-gui` | `hop-gui-hopos` |

See also: [hopos.md](hopos.md) for how residents are placed,
[api.md](api.md) for the routes the apps call, and
[development.md](development.md) for the gates.

## Two forms from one crate

Every app has a sans-I/O core (`no_std` with `alloc`) and two binaries
chosen by a Cargo feature:

- **`std`**: a host daemon for Linux and macOS, on std sockets and threads
  (through Hop's `hostnet`). Every thread owns its state; they exchange
  messages, with no mutex.
- **`hopos`**: a resident for a HopOS slot (`aarch64-unknown-none-softfloat`),
  on `applib`'s executor and the slot's network stack. Hop places it like
  any other job: `"driver": "hop"`, an ELF artifact, and `ports`. The kernel
  publishes those ports on the node's uplink, and Hop passes the port
  numbers as `ER_PORT_<NAME>` env.

The default feature differs per crate:

| Crate | Default | Host build | Resident build |
| --- | --- | --- | --- |
| `hopdns` | none | `--features std` | `--features hopos` |
| `hoplb` | `std` | (default) | `--no-default-features --features hopos` |
| `hopprom` | none | `--features std` | `--features hopos` |
| `hoplockserver` | `std` | (default) | `--no-default-features --features hopos` |
| `hop-gui` | none | `--features std` | `--features hopos` |

From the repository root (the binaries land in the workspace `target/`):

```
cargo build --release -p hopdns --features std --bin hopdns
cargo build --release -p hopdns --features hopos \
  --target aarch64-unknown-none-softfloat --bin hopdns-hopos
```

A resident should ship without debug info; the symbols must stay (the
kernel reads them when it places the image). Either build with
`CARGO_PROFILE_RELEASE_DEBUG=false` (what the release workflow does) or
strip afterwards with `rust-objcopy --strip-debug`. Serve the ELF from any
HTTP(S) URL; tagged releases publish it as `<app>-arm64.elf`, and the host
binaries as `<app>-<os>-<arch>.tar.gz`.

### Reaching Hop from a resident

In a resident, the host name `HOP` in an agent URL means Hop's own slot on
the slot LAN (slot 1, `10.100.0.2`); no DNS is involved. The defaults differ:
hopdns and hoplb use `http://HOP:9080` (Hop's leader API), hopprom uses
`http://HOP:8080` (the agent API).

In a cluster, the leader port of a follower node answers 503 (see
[hopos.md](hopos.md#forwarding)); only the agent port forwards `/v1/*` to
the leader. On a node that may not lead, point the resident at
`http://HOP:8080`.

### Common job spec

```json
{"name": "hoplb", "driver": "hop",
 "artifacts": [{"url": "https://example.com/hoplb-arm64.elf"}],
 "memory_limit": 67108864,
 "ports": {"http": 80, "admin": 9091},
 "env": {"HOPLB_API_KEY": "..."}}
```

`"count": -1` runs one instance per agent. In a mixed cluster add
`"affinity": {"node.os": "hopos"}`: a host agent cannot run a `hop` job.

## hoplib

`hoplib/` is the client the apps use to talk to a Hop agent: signed
requests, typed replies, and the `/v1/events` stream. README:
[../hoplib/README.md](../hoplib/README.md).

| Feature | Module | Transport |
| --- | --- | --- |
| (none) | `hoplib` | sans-I/O: `Client` builds a `Request`, `Call<T>::parse` reads a `Response`; `Events` parses SSE; `Backoff` |
| `std` | `hoplib::host` | blocking, over `hostnet` |
| `hopos` | `hoplib::hopos` | async on the app executor, over `applib`'s `TcpConn` and leanhttp |

- `Client::new(base, api_key)`; calls: `status`, `agents`, `jobs`,
  `job(name)`, `job_status(name)`, `tasks`, `agent_tasks`, `capacity`,
  `agent_capacity`, `leader`, `apply`, `delete`, and `events()` for the
  stream request.
- Signing uses Hop's own `auth` crate:
  `X-Hop-Auth = hex(HMAC-SHA256(key, METHOD \n PATH \n hex(sha256(body))))`.
  No key, no header.
- `Event::topic()` (`Ping`, `Status`, `Job`, `Task`, `Agent`, `Other`),
  `Event::job()`, `Event::is_resync()` (a `ping` or `status` event: reread
  everything).
- `Backoff`: 1 s doubling to 30 s, reset after a `ping`.
- Point it at the **local agent**; the agent forwards `/v1/*` to the
  leader. Older leaders without `/v1/jobs/{name}/status` or `/v1/tasks` are
  handled by asking the agents directly.

## hopdns

DNS service discovery: `myapp.hop.local` answers with the address of every
`running` task of job `myapp`, across federated clusters. README:
[../apps/hopdns/README.md](../apps/hopdns/README.md).

- Every peer is a Hop agent (your own cluster included). Per peer a watcher
  reads `GET /v1/status` (`cluster_name`), then `/v1/events`, and rebuilds
  from `/v1/agents` + `/v1/tasks` or `/v1/jobs/{name}/status`; events are
  coalesced for 500 ms. A peer that drops its stream keeps its last
  addresses.
- `myapp.hop.local` merges all clusters; `myapp.<cluster>.hop.local` asks one.
  A records with TTL 5 s, NXDOMAIN or NODATA with a SOA (negative TTL 5 s),
  REFUSED outside the zone, EDNS up to 1232 bytes. UDP only: answers that do
  not fit are truncated to what fits, without the TC bit.
- Static CNAMEs (with `*.` wildcards) from a YAML file.
- A task's address is the host of its agent's endpoint. A resident that
  knows its node (`ER_ATTR_NODE_ID`) answers its own node's tasks with their
  slot address.

**Host** (`hopdns`; Go-style flags, `-name value` or `-name=value`):

| Flag | Default | Meaning |
| --- | --- | --- |
| `-listen` | `:5353` | UDP listen address |
| `-peer` | required, repeatable | a Hop agent URL; `http://key@host:port` carries its own key |
| `-domain` | `hop.local` | the zone |
| `-api-key` | empty | key for peers without `key@` |
| `-config` | none | YAML with `cnames:` |

**HopOS** (`hopdns-hopos`, port `"ports": {"dns": 5353}`):

| Env | Default | Meaning |
| --- | --- | --- |
| `ER_PORT_DNS` | `5353` | UDP port, set by Hop |
| `HOPDNS_PEER` (or `HOPDNS_PEERS`) | `http://HOP:9080` | comma-separated peers |
| `HOPDNS_DOMAIN` | `hop.local` | the zone |
| `HOPDNS_API_KEY` (or `HOP_API_KEY`) | empty | key for peers without `key@` |
| `HOPDNS_SELFTEST` | none | a job name to resolve from inside and log |
| `ER_ATTR_NODE_ID` | set by Hop | this node's id |

Markers: `HOPOS_HOPDNS_UP port=`, `HOPOS_HOPDNS_CACHE jobs=`,
`HOPOS_HOPDNS_SELFTEST ok ip=` (or `fail`), and the failures
`HOPOS_HOPDNS_FAIL`, `HOPOS_HOPDNS_PEER_FAIL`, `HOPOS_HOPDNS_PORT`,
`HOPOS_HOPDNS_RECV`, `HOPOS_HOPDNS_CACHE_FAIL`.

## hoplb

Reverse proxy that routes on the `Host` header to the running tasks of Hop
jobs, round-robin, with Prometheus metrics on a separate admin port. README:
[../apps/hoplb/README.md](../apps/hoplb/README.md).

Job tags drive the routes:

| Tag | Meaning |
| --- | --- |
| `hoplb-urlprefix` | the host name, e.g. `app.example.com` or `*.example.com` (one level deep) |
| `hoplb-port` | which named port gets traffic; default the task's first port |
| any `key: value` | matched by the tag filter (`-tag lb:haas`) |

A backend is `<host of the agent endpoint>:<task port>`. Route changes come
from `/v1/events`, debounced by 500 ms. Bodies stream both ways (chunked
passes through, SSE is flushed per event); `X-Forwarded-For` is set and
hop-by-hop headers are stripped. Metrics: `hoplb_requests_total`,
`hoplb_request_duration_seconds` (p50/p90/p95/p99, `_count`, `_sum`) per
domain and backend, with a series cap (512 on a host, 128 on HopOS) that
folds new series into domain `_other` (`hoplb_series_folded_total`).

**Host** (`hoplb`): 64 traffic and 2 admin worker threads.

| Flag | Default | Meaning |
| --- | --- | --- |
| `-listen` | `:80` | traffic |
| `-admin-listen` | `:9091` | `/health` and `/metrics` |
| `-agent` | `http://127.0.0.1:8080` | the local Hop agent |
| `-tag` | none | only jobs with this `key:value` tag |
| `-api-key` | none | cluster key for `X-Hop-Auth` |

**HopOS** (`hoplb-hopos`, `"ports": {"http": 80, "admin": 9091}`): 8
traffic and 2 admin workers.

| Env | Default | Meaning |
| --- | --- | --- |
| `ER_PORT_HTTP` | `80` | traffic port |
| `ER_PORT_ADMIN` | `9091` | admin port |
| `HOPLB_AGENT` | `http://HOP:9080` | the Hop API to watch |
| `HOPLB_TAG` | none | tag filter |
| `HOPLB_API_KEY` | none | cluster key |
| `HOPLB_VERBOSE` | off | `1`: one log line per request (`HOPOS_HOPLB_REQUEST`) |

Markers: `HOPOS_HOPLB_UP port= admin=`, `HOPOS_HOPLB_ROUTES n= backends=`.

## hopprom

Polls a Hop agent and serves the cluster as Prometheus metrics on
`/metrics` (`/health` answers `ok`). `/metrics` is 503 until the first
successful poll. Run one per cluster (`"count": 1`). README:
[../apps/hopprom/README.md](../apps/hopprom/README.md).

One poll round: `GET /v1/agents`, `/v1/jobs`, `/v1/tasks` (required: a
failure keeps the previous metrics), then per agent
`/v1/agents/{id}/capacity`, `/v1/status` and `/leader` (best effort).
Metric families: `hop_agents_*`, `hop_agent_*` (cores, memory, temperature),
`hop_tasks_*`, `hop_jobs_total`, `hop_job_*` (placed, expected, healthy,
cpu and memory percent, deploying), the counters `hop_task_starts_total`,
`hop_task_failures_total`, `hop_task_restarts_total` (deltas between two
polls), and `hop_leader_info`, `hop_leader_lease_seconds`,
`hop_leader_changes_total`, `hop_cluster_settling`, `hop_jobs_deploying`.

**Host** (`hopprom`):

| Flag | Default | Meaning |
| --- | --- | --- |
| `-listen` | `:9090` | `/metrics` and `/health` |
| `-agent` | `http://127.0.0.1:8080` | the agent to poll |
| `-interval` | `5s` | poll interval (Go duration) |
| `-api-key` | none | cluster key; without it requests are unsigned |

**HopOS** (`hopprom-hopos`, `"ports": {"metrics": 9090}`):

| Env | Default | Meaning |
| --- | --- | --- |
| `ER_PORT_METRICS` | `9090` | listen port |
| `HOPPROM_AGENT` | `http://HOP:8080` | the agent to poll |
| `HOPPROM_INTERVAL` | `5s` | poll interval |
| `HOPPROM_API_KEY` (or `HOP_API_KEY`) | none | cluster key |

The leader port (9080) has no `/leader` route, so with
`HOPPROM_AGENT=http://HOP:9080` the leader metrics stay empty. Markers:
`HOPOS_HOPPROM_UP port=`, `HOPOS_HOPPROM_POLL agents= tasks=`,
`HOPOS_HOPPROM_POLL_FAIL`, `HOPOS_HOPPROM_MISS`, `HOPOS_HOPPROM_PORT`,
`HOPOS_HOPPROM_INTERVAL`.

## hoplockserver

A small HTTP server with the compare-and-swap API behind Hop's
`hoplockserver` lock type: `GET`, `PUT` and `DELETE` on keys with `If-Match`
/ `If-None-Match: *`, ETag = `"<hex sha256 of body>"`, bodies up to 1 MiB,
one shared `X-API-Key`, and `GET /health` without auth. Hop stores the lease
at `leases/<cluster>` and, without an S3 bucket, the committed state at
`state/<cluster>`. README:
[../apps/hoplockserver/README.md](../apps/hoplockserver/README.md).

**Host** (`hoplockserver`): one file per key, written crash-safe (temp
file, fsync, rename, fsync of the directory). One store thread owns the
files and 16 connection threads send it messages, so one instance is
linearisable. Never point two instances at one data directory.

| Flag | Default | Meaning |
| --- | --- | --- |
| `-listen` | `:8090` | listen address |
| `-data` | `./data` | data directory |
| `-api-key` | empty (`HOPLOCK_API_KEY` if set) | required `X-API-Key`; empty disables auth |

Markers on stderr: `HOPLOCK_UP keys=`, `HOPLOCK_NO_AUTH`, `HOPLOCK_FAIL`.

**HopOS** (`hoplockserver-hopos`, `"ports": {"http": 8090}`): the store
lives on hopfs through the system API's file calls. hopfs has no rename or
fsync, so every record carries a header with the body length and SHA-256: a
torn record is treated as absent. The record format differs from the host's.

| Env | Default | Meaning |
| --- | --- | --- |
| `ER_PORT_HTTP` | `8090` | listen port |
| `HOPLOCK_API_KEY` | empty | required `X-API-Key` |
| `HOPLOCK_DATA` | `/data` | store directory; mount a job volume there (`"volumes": {"/volumes/hoplock": "/data"}`), or it lives in the slot's root, which the kernel empties at every start |

Markers: `HOPOS_HOPLOCK_UP keys=`, `HOPOS_HOPLOCK_NO_AUTH`,
`HOPOS_HOPLOCK_VOLUME`, `HOPOS_HOPLOCK_CORRUPT`, `HOPOS_HOPLOCK_FAIL`,
`HOPOS_HOPLOCK_REQUESTS` (every 1000 requests).

Using it from Hop: `"cluster": {"lock": {"type": "hoplockserver", "url": ...,
"api_key": ...}}` on a host ([configuration.md](configuration.md)), or
`hopos.lock.url` and `hopos.lock.apikey` on HopOS ([hopos.md](hopos.md)).

## hop-gui

A static cluster dashboard (`index.html`, `app.js`, `appearance.js`,
`style.css`, `vendor/`; no build step) and the crate in
`apps/hop-gui/server/` that bakes those files into one binary with
`include_bytes!` and serves them with `ETag` and `Cache-Control: no-cache`.
README: [../apps/hop-gui/README.md](../apps/hop-gui/README.md).

- **Host**: `cargo build --release -p hop-gui --features std --bin hop-gui`,
  then `target/release/hop-gui -listen :3000` (`-listen` defaults to `:3000`;
  8 worker threads). Marker on stderr: `HOPGUI_UP listen= files= bytes=`.
  Any static file server works too.
- **HopOS**: `hop-gui-hopos` with `"ports": {"http": 80}` (`ER_PORT_HTTP`,
  default 80). Marker: `HOPOS_HOPGUI_UP files= bytes= port=`. To keep it on a
  fresh node, put the job spec on one line behind `hopos.init[]=` in
  `hopos.cfg`.

The browser talks to the **agent** directly (`http://NODE:8080`), not to
the server that served the page. The agent port answers every request,
forwarded ones and streams included, with
`Access-Control-Allow-Origin: *` and the Private Network Access header; the
leader port (9080) sends no CORS headers. With a key, `app.js` signs each
request with Web Crypto, which needs a secure context (`localhost` or
HTTPS). The routes it calls are listed in its README;
`api/src/tests_gui.rs` checks them against `app.js`.

## Testing the apps

Each app has `tools/gate.sh` (host tests, clippy `-D warnings`, rustfmt,
the `no_std` core and the resident for the target) and `tools/qemu-test.sh`
(boots HopOS on QEMU with Hop in slot 1 and places the app). The QEMU tests
need the HopOS repository (`HOPOS_DIR`, default `../../hop-os` from the
repository root) and `qemu-system-aarch64`; `HOP_DIR` defaults to this
repository. `hoplockserver/tools/e2e-host.sh` runs two `agentd`s electing
through the server, and `hop-gui/tools/host-test.sh` runs the dashboard's
API sequence against `agentd`. The root `tools/gate.sh` runs every app's
gate after the core.

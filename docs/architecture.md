# Architecture

How the pieces of Hop v3 fit together. For the step-by-step behaviour of a
cluster over time see [lifecycles.md](lifecycles.md); for the wire shapes see
[api.md](api.md) and [data-structures.md](data-structures.md). Back to the
[index](index.md).

## In one paragraph

Hop is a cluster orchestrator written as a set of `no_std` + `alloc`
*logic* crates (vocabulary, config, auth, leader, agent, API handlers, lease
logic, runner) that do no I/O and read no clock, plus a few *edge* crates
that bind them to a runtime. The same logic runs in two binaries: `agentd`, a
threaded `std` daemon for Linux and macOS, and `agentd-hopos`, a `no_std`
async resident that HopOS starts in its own slot with the privilege to place,
stop and replace apps. Every node runs an agent; the node that holds the
leader lease also runs the leader, which is the only author of desired state
and the only placer.

## Crate map

Workspace members (root `Cargo.toml`), grouped by role. "no_std" means the
crate builds with `#![no_std]` outside tests and is part of the
`aarch64-unknown-none-softfloat` step of `tools/gate.sh`.

| Crate | Kind | Owns | Depends on (in-repo) |
| --- | --- | --- | --- |
| `types` | no_std | The vocabulary: `Job`, `Task`, `TaskState`, `Agent`, `Telemetry`, `Time`/`Nanos`, the JSON parser/writer (`MAX_INPUT` 1 MiB, `MAX_DEPTH` 32). Every allocation is fallible. JSON shape follows the Go generation (tag v1.0.7) field for field. | none |
| `config` | no_std | The node/cluster `Config`, its defaults and strict JSON reading (unknown keys are errors). Reads no file. | `types` |
| `auth` | no_std | HMAC-SHA256 request signing (`X-Hop-Auth`) over `METHOD\nPATH\nhex(sha256(body))`; own SHA-256/HMAC. No time window (by design). | none |
| `leader` | no_std | The cluster brain: registration, liveness, placement, preemption, reconcile, updates, delete, settle, snapshot, init jobs, `/v1/events` queue. Talks to agents only through the `Transport` trait. | `types`, `config` |
| `agent` | no_std | One node's state machine: admission/capacity, tasks, restarts with backoff, health verdicts, the election loop (`Election`) and the lease cache (`Elector`). Emits `Action`s; does no I/O. | `types`, `config` |
| `api` | no_std | Pure HTTP handlers: `NodeApi` (agent port P) and `LeaderApi` (leader port P+1000), `LeaderCluster` (bridge to `Leader` + `Transport`), SSE framing and `EventLog`. Returns `Response` + `Effect`/`LeaderEffect`. | `types`, `config`, `auth`, `leader`, `agent` |
| `discovery` | no_std | The lease protocol actually used in production: `Discovery` (handle, generation, `plan_claim`, `renew_op`, `on_write`), `leader_state`, `backend_timeout_for`, `MemBackend`, the lease wire format (`wire`, Go's `hoplock.State`), and `state_store_for` (where committed state lives). | `types` |
| `runner` | no_std (+ `std` feature) | Task backends. `HopRunner` (HopOS slots over the `SystemApi` trait), `LogStore`/`LogRing`, env helpers. With feature `std`: `runner::host` (`HostRunner`: processes and Docker, downloads, extraction, isolation). | `types` (+ `hostnet` with `std`) |
| `hopos-runner` | no_std | `KernSys`: `runner::SystemApi` as applib system-client frames (privileged ops `0x40`..`0x4A`). | `runner`, `agent`, `applib`, `abi` |
| `hop-http` | no_std | The HopOS HTTP adapter: leanhttp over an applib `TcpStream`, `api::Request` in, `Reply` out, SSE streams polled every `STREAM_POLL` (500 ms). | `api`, `applib`, `leanhttp` |
| `agentd-hopos` | no_std lib + bin | Hop as the HopOS resident: `Node` (agent + leader + `HopRunner` + both APIs), `Hub`, download task, lock backends (hoplockserver, S3), cluster tasks (`mail`, `lease`, `link`, `relay`), forwarding, SNTP, kernel flip, object store. | most of the above, `discovery`, lean, applib |
| `hostnet` | std | Host networking: `StdConn` (blocking sockets as leanhttp conns), `block_on`, the HTTP(S) client `Http` (one total deadline per call), S3 transports. | lean crates |
| `store` | std | Host lease backends (`S3Lease`, `HoplockLease`, `Lease::Mem`) and committed-state stores (`S3StateStore`, `HoplockStateStore`, `FileStateStore`). Lease key `leases/<cluster>` (or `lock.key`), state key `state/<cluster>`. | `types`, `config`, `discovery`, `hostnet` |
| `agentd` | std bin | The host daemon: one owner thread (`node::Node`) plus fixed worker threads. | everything core + `runner/std`, `hostnet`, `store` |
| `cli` | std bin | The `hop` command (talks to the leader, default `localhost:9080`) and the HopOS imager (`hop image`). | `types`, `auth`, `hostnet` |
| `hoplib` | client lib | Signed client for plugins/apps (`std` or `hopos` feature). | `auth`, `types` (+ `hostnet`/`applib`) |
| `apps/*` | apps | `hopdns`, `hoplb`, `hopprom`, `hoplockserver`, `hop-gui/server` (see [apps.md](apps.md)). | stand on Hop |

### Dependency direction

```
                    types            auth
                   /  |  \             |
             config   |   discovery    |
             /    \   |       |        |
        leader   agent|       |        |
            \     /   |       |        |
              api ----+-------+--------'
               |              |
   runner -----+---------+    |             (runner: types only;
     |         |         |    |              host feature adds hostnet)
hopos-runner  hop-http   |    |
     \         |         |    |
      '---- agentd-hopos-+----'             no_std resident
                         |
   hostnet --- store ----+---- agentd       std daemon
       \                         
        '--- cli                            std CLI

   hoplib, apps/*  --->  types, auth, hostnet   (never the other way)
```

Rules that hold in the manifests and are enforced by `tools/gate.sh`:

- `leader` and `agent` do not depend on each other. The leader reaches
  agents only through `leader::Transport`; the agent never calls the leader,
  it emits `Request`s/`Action`s that the binary executes.
- `agent` does not depend on `runner` (and vice versa). The binary glues an
  `Action::Start` to a runner call.
- No core crate depends on `hoplib` or `apps/` (gate checks `cargo tree`).
- lean (`leanhttp`, `leanhttps`, `leantls`, `leans3`, `leans3http`, tag
  `v3.1.12`) and HopOS (`applib`, `sync`, `abi`, tag `v3.0.13`) come in on one
  git tag each; never by path.

## No-std, sans-I/O design

- **Logic is a value machine.** `Leader`, `Agent`, `Election`, `Elector`,
  `Discovery`, `HopRunner` take "now" as a parameter (`types::Time`, Unix
  nanoseconds; milliseconds in runner/lease code) and return what should
  happen. Tests drive them by hand (`leader/src/testkit.rs` fakes agents with
  a `FakeNet`, `agent/src/tests.rs` feeds outcomes in a chosen order).
- **One owner per state** (handbook section 1). `Leader<S>` and `Agent` are
  plain structs behind `&mut self`; there are no locks. Other actors send
  messages and get values back.
- **Fallible allocation.** Inputs from outside are bounded
  (`leader::MAX_AGENTS` 256, `leader::MAX_JOBS` 1024, `agent::MAX_TASKS` 1024,
  `agent::MAX_JOBS` 1024, `MAX_EVENTS` 64, `config::MAX_INIT_JOBS` 64) and an
  out-of-memory is an error value, not a panic. Panics, `unwrap`, `expect`
  are denied by workspace lints; release builds use `panic = "abort"`.
- **The binaries choose the runtime.**
  - `agentd`: blocking std sockets, one owner thread fed by `mpsc` channels,
    fixed thread pools, `hostnet::block_on` per thread.
  - `agentd-hopos`: one applib executor on the app core; the owner task
    `await`s kernel calls (no nested executor rounds, handbook section 4);
    network calls of the cluster run in separate tasks and answer via
    mailboxes.

## Runtime picture

```
                    hop CLI / hop-gui / hoplib
                               |  X-Hop-Auth (HMAC)
                               v
  +--------------------------- node (agentd or agentd-hopos) -------------------------+
  |  port P (agent API)                         port P+1000 (leader API)              |
  |  NodeApi: /run /stop /stop-task /delete     LeaderApi: /v1/jobs /v1/agents         |
  |   /tasks /capacity /logs /flip /health      /v1/heartbeat /v1/notify /v1/tasks     |
  |   /v1/jobs,agents,.. -> proxy to leader     /v1/events (SSE) /v1/status ...        |
  |        \                                    /                                      |
  |         '---------- owner (one &mut) ------'                                       |
  |            Agent  <--Actions--  tick/inputs        Leader<MemStore> (only if lease) |
  |              |                                       |  Transport                   |
  |              v                                       |  own agent: in-process        |
  |           runner                                     |  others: HTTP /run,/stop,..   |
  |   HostRunner (proc, docker)  or  HopRunner->KernSys  v                              |
  |                                                  other nodes' agent API            |
  |   Election + Elector  --LeaseOp-->  lease worker  --CAS-->  lock store              |
  |   Leader::poll_snapshot --bytes-->  state worker  ------->  state/<cluster>          |
  +------------------------------------------------------------------------------------+
                                     |
                lock store: hoplockserver | S3 bucket | in-memory (standalone)
                leases/<cluster> (CAS lease)   state/<cluster> (committed jobs)
```

## The leader (`leader/`)

`Leader<S: JobStore>` is "the only author of desired state and the only one
that places" (`leader/src/state.rs`). Both binaries instantiate it with
`MemStore`; durability comes from the committed snapshot, not from the store.

| Module | Responsibility |
| --- | --- |
| `state.rs` | Agents (sorted by id: the deterministic round-robin order), `placed` (agent -> job -> instance count), `register_agent`, `heartbeat` (pure liveness + telemetry, unknown id returns `false` -> HTTP 404), settle state, dirty tracking. |
| `dispatch.rs` | `dispatch_job`, `place` (round-robin over agents; second pass preempts the lowest-priority job that is strictly less important, only when `allow_preempt`), `trim_returning_agent_surplus`, `mark_unplaced`, `delete_job`, `patch_job_priority`, `normalize_priorities` (dense 0..N-1; lower is more important; unset sorts last). |
| `health.rs` | `tick` (end settle, `check_dead_agents`, safety-net reconcile every `RECONCILE_EVERY_TICKS` = 3 ticks), `reconcile_jobs` (in priority order, from `placed`), daemons (`count == -1`: one per agent), `cluster_status`, `job_status`. |
| `update.rs` | `update_job`: rolling, recreate, blue-green; `deploying` flag. |
| `persist.rs` | `poll_snapshot` with `PERSIST_DEBOUNCE` (1 s), `snapshot_failed`, `load_committed_state` (snapshot is the only truth: local jobs absent from it are removed). |
| `init.rs` | `decode_init_jobs` (strict), `seed_init_jobs` (never overwrites an existing name). |
| `events.rs` | Bounded de-duplicated queue of `status` / `agent:<id>` / `job:<name>`; overflow collapses to one `status`. |
| `transport.rs` | `Transport` trait (`run`, `stop_job`, `stop_task`, `delete_job`, `tasks`, `pause`) and `RunReply::from_status` (200-202 accepted, 406 affinity, 503 full). |
| `lease.rs` | A self-contained CAS lease model (`Claim`, `LeaseStore`, `MemLease`) exercised by its own tests. The binaries do **not** use it; they use `discovery::Discovery` (see below). |

Key constants (`leader/src/lib.rs`): `DEFAULT_AGENT_TIMEOUT` 30 s,
`ROLLING_UPDATE_DELAY` 2 s, `RECONCILE_EVERY_TICKS` 3. The binaries tick the
leader every `LEADER_TICK` = 10 s.

Placement is computed from `placed` (what agents reported at registration plus
what this leader booked), not from `/tasks` replies; a briefly unreachable
agent therefore does not cause duplicates (`tests/phantom_node.rs`).

## The agent (`agent/`)

`Agent` owns the node's jobs and tasks. Invariant (`node.rs`): *every task
record holds its capacity reservation whatever its state; capacity is freed
only by removing the record.* Inputs: `run`, `on_progress`, `on_started`,
`on_status`, `on_probe`, `stop_task`, `stop_job_tasks`, `delete_job`,
`stop_all`, `tick(now)`. Outputs (`action.rs`), executed in order by the
binary: `Start`, `Stop`, `Poll`, `Probe`, `Refused`, `Notify`
(`start`/`started`/`crash`/`stop`/`unplaceable`).

- Admission (`Agent::run`): affinity before capacity (406), then CPU shares
  (sharegroup members counted once; groups on cores the node does not hand
  out are free), memory, and on HopOS the largest free partition.
  `driver: hop` jobs are rounded up to whole cores (1024 shares) and 2 MiB
  memory blocks.
- Restarts and health: `health.rs` (backoff, thresholds), defaults in
  `lib.rs`: `DEFAULT_MAX_RESTARTS` -1 (unlimited), `DEFAULT_RESTART_WINDOW`
  5 min, `MAX_RESTART_DELAY` 30 s, `DEFAULT_FAILURE_THRESHOLD` 3. Monitor
  and probe timeout default to 5 s (`Settings`).
- Election: `Election` (`election.rs`) is the per-node loop (register,
  heartbeat, take over, fail-safe) with `TAKEOVER_AFTER` 4 and
  `ISOLATED_AFTER` 7 failed ticks. `Elector` (`elector.rs`) answers the loop
  instantly from the last completed lease-store call and keeps the lease
  timer (renew every TTL/3); the store call itself runs on another
  thread/task. Both binaries share this code.

**State persistence.** The agent crate holds no persistent state. A
restarted `agentd` starts clean (`HostRunner::init` removes leftover task
directories and containers) and re-registers with empty `placed` counts; the
leader refills the gap. A restarted HopOS resident starts empty and stops
stray residents (`Node::sweep_strays`). What survives is the leader's
committed cluster state (below).

## Committed cluster state

- The leader marks itself dirty on every store mutation (`store_put`,
  `store_remove`, `store_set_priority`, `store_set_deploying`).
- `Leader::poll_snapshot(now)` returns `{"updated": <RFC3339>, "jobs": [...]}`
  once the state has been dirty for `PERSIST_DEBOUNCE` (1 s). The binary hands
  the bytes to a worker (host: `agentd/src/persist.rs` thread; HopOS: the state
  task via `STATE_Q`), which overwrites the object without CAS (the writer is
  the leaseholder). A failed write calls `snapshot_failed` and is retried;
  `HOP_STATE_SAVE_FAIL` is logged on failures 1-3 and every 100th.
- Where it lives (`discovery::state_store_for`, the single decision point for
  both binaries): an S3 section with endpoint and bucket wins (`state/<cluster>`
  in the lease bucket), else a hoplockserver URL (`state/<cluster>` there),
  else a local file (`paths.state_file`, default `./data/state.json`; also for
  standalone). A standalone HopOS node keeps no snapshot at all; its jobs come
  from init jobs.
- Deleting or renaming the object is the operator's "clean boot" switch.

## Discovery and the lease (`discovery/`, `store/`, `agentd-hopos/src/lock.rs`)

"Finding nodes" means finding the leader through the lease. One object per
cluster (`leases/<cluster>` by default) holds
`{"generation", "expires_at", "owner": "ip:leaderport"}` (Go `hoplock.State`;
Go and Rust nodes, host and HopOS, can share one lease). Mutual exclusion is
entirely the backend's conditional write on the last seen handle (ETag):

- claim: no record -> create (generation 1); our record -> refresh; expired ->
  take over with generation + 1; otherwise `LeaseHeld`.
- renew: one conditional PUT on the held handle, no read; 412 means displaced.
- every backend call has one total deadline `backend_timeout_for(ttl)` =
  max(TTL/3, 5 s).

Backends: in-memory (`MemBackend`, standalone or `mem`), hoplockserver, S3
(host: `store`; HopOS: `agentd-hopos::{hoplock, s3}`). Default TTL 30 s on
both; HopOS refuses less than 15 s (`MIN_LEASE_TTL_MS`), the host does not
enforce it (see [configuration.md](configuration.md)).

## Runners (`runner/`, `hopos-runner/`)

| Backend | Where | Model |
| --- | --- | --- |
| `HopRunner<S: SystemApi>` | HopOS (`runner/src/hopos.rs`) | Each task is a native app image in its own kernel cage (slot). Phases `Queued` -> `Streaming` (image bytes streamed into the partition) -> `Armed`. At most `MAX_CONCURRENT_DOWNLOADS` (4) streams. Stop is cooperative with `HOP_STOP_TIMEOUT_MS` (3 s) before the kernel's stage-2 revocation; an unconfirmed release keeps the slot quarantined. Ports: one network stack per task, published with stateless DNAT; dynamic ports start at 20000. Implements the `runner::Runner` trait. |
| `KernSys` | `hopos-runner` | The `SystemApi` impl: privileged ops as applib frames; `fake` feature for host tests. |
| `HostRunner` | host (`runner::host`, feature `std`) | Processes (own process group; on Linux chroot + namespaces + cgroup via `unshare`/`chroot`/`mount`, on macOS `sandbox-exec`, when `runner.isolate`) and Docker over its Unix socket (containers prefixed `hop-`). Stop: SIGTERM, SIGKILL after `GRACE_MS` 10 s, quarantine if the group survives `KILL_TIMEOUT_MS` 1 s more. Slow preparation (task dir, artifact download/extract, `docker pull`) runs on `agentd`'s 4 prep workers. Has its own API (`launch`, `stop`, `status`, `tick`), not the `Runner` trait. |

Both keep logs in a `LogStore` (see [lifecycles.md](lifecycles.md#9-log-retention)).

## The HTTP layer

- `api` is pure: `NodeApi::handle(&mut Agent, now, pool_largest, &Request)`
  and `LeaderApi::handle(&mut impl Cluster, now, &Request)` return a
  `Response` plus an effect the adapter must perform: `Effect::{Proxy, Logs,
  Flip}` on the agent port, `LeaderEffect::{Tasks, Agent, Events, Log}` on the
  leader port. HMAC checks happen inside the handlers (empty key =
  unauthenticated mode). The agent port carries CORS headers on every
  response because the dashboard talks to it directly.
- On the agent port, `/v1/agents`, `/v1/jobs`, `/v1/status`, `/v1/tasks`,
  `/v1/events` and the sub-paths of `/v1/agents/` and `/v1/jobs/` are
  proxied unchanged (signature included) to the known leader (503 if none,
  413 above `PROXY_MAX_BODY` = 8 MiB); when the leader is this node the
  request is handled in-process.
- Adapters: `hop-http` (HopOS, leanhttp over applib TCP) and
  `agentd/src/http.rs` (host, leanhttp over `hostnet::StdConn`). Both serve a
  stream by asking the owner every 500 ms for new data and sending a
  keepalive every 15 s.
- `hostnet` is the host's network edge (client with one total deadline per
  call, S3 transports); HopOS has its equivalents in `agentd-hopos`
  (`client`, `fetch`, `forward`).

See [api.md](api.md) for routes and status codes.

## Host daemon vs HopOS resident

| | `agentd` (host) | `agentd-hopos` (HopOS resident) |
| --- | --- | --- |
| Runtime | std threads, `mpsc` | one applib executor, async tasks, static mailboxes |
| Owner | `agentd/src/node.rs` `Node`, ticks every 1 s | `agentd-hopos/src/node.rs` `Node`, ticks every 1 s |
| Connection pool | `WORKERS` = 8 threads per port | `WORKERS` = 4 tasks per port |
| Open streams | `MAX_STREAMS` = 4 | `MAX_STREAMS` = 3 |
| Runner | `HostRunner` (exec, docker) | `HopRunner` over `KernSys` (`driver: hop`) |
| Leader -> remote agent | synchronous HTTP from the owner thread (5 s `/run` and `/tasks`, 60 s stop/delete) | `relay`: answers from its books (refusals cached `REFUSAL_TTL` 30 s), real call made by the dispatch task; a refusal is booked off via `mark_unplaced` (`HOP_RELAY_REFUSED`) |
| Rolling pause | sleeps `ROLLING_UPDATE_DELAY` | no pause (`Transport::pause` default no-op) |
| Health probes | probe thread (HTTP/TCP/file) | not wired: `HOP_PROBE_SKIPPED`, tasks stay unprobed |
| Lease | lease thread with `discovery::Discovery` + `store::Lease` | lease task, hoplockserver or S3; joins only after SNTP |
| Becoming leader | loads state synchronously (wait 2x backend timeout) | state task loads; leader API answers 503 until loaded |
| Standalone | `--standalone` or no lock: in-memory lease, file state | no `HOPOS_LOCK_*`: standalone leader, no settle, no snapshot |
| Config | JSON file + flags ([configuration.md](configuration.md)) | `HOPOS_*` env from `hopos.cfg` ([hopos.md](hopos.md)) |
| Extra | | kernel flip (`/flip`, warm and cold), SNTP, object store for apps |

See [development.md](development.md) for building and the gate.

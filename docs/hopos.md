# Hop on HopOS

On [HopOS](https://github.com/xinix00/HopOS), Hop is the **first resident**: an
app in slot 1 with one privilege no other app has, the right to place, stop
and replace apps (and to set the clock, serve the object store and flip the
kernel). The binary is `agentd-hopos`, built from the same crates as the host
daemon (`agent`, `leader`, `api`, `discovery`, `runner`) plus the HopOS
glue (`hopos-runner`, `hop-http`, `applib`).

See also: [architecture.md](architecture.md) for the crate layout,
[configuration.md](configuration.md) for the host config,
[cli.md](cli.md) for `hop image` and `hop flip`, [api.md](api.md) for the
HTTP API, and [lifecycles.md](lifecycles.md) for elections and task states.

## Kernel versus Hop

| The kernel (mechanism) | Hop (policy) |
| --- | --- |
| Slots: `START_SLOT`, `STREAM_IMAGE` (raw ELF bytes in), stop, status, usage | Which job runs where: placement, capacity, affinity, restarts, updates |
| Port publishing: a job's `ports` are DNAT'ed from the node's uplink to the slot (TCP and UDP) | Port assignment (`ER_PORT_*` env); dynamic ports start at 20000 |
| `hopfs` and the volumes in `START_SLOT` (mounts) | The job spec and its `volumes` |
| The node config window (`hopos.cfg`), handed to Hop as `HOPOS_*` env | Validating it; refusing to start on a bad config |
| A wall clock pinned to a fixed date until someone sets it (`SET_CLOCK`) | SNTP: Hop is the only resident that may set the clock |
| A queue of app store calls (`NEXT_STORE`, `STORE_READ`, `STORE_WRITE`, `STORE_DONE`); no S3, no keys, no TLS | The S3 side of the object store |
| `FLIP`: verify a kernel bundle and jump to it | Downloading the bundle, deciding warm or cold, stopping tasks first on a cold flip |
| An entropy seed on the control page | A DRBG for TLS (`applib::rand::Rng`) |

Hop keeps **no state on hopfs**. What it knows comes from the committed
cluster state in the lock store (clustered) or from the init jobs
(standalone). At boot it sweeps any resident the kernel still holds that it
does not know (`HOP_STRAY_STOPPED`). Health probes are not wired on HopOS
yet: a job with a `health_check` runs unprobed (`HOP_PROBE_SKIPPED`).

Inside the slot (`agentd-hopos/src/main.rs`), everything runs on one
executor:

- the **owner task** holds the `Node` (agent, leader, runner, both APIs)
  and ticks every second;
- the **download task** fetches artifacts over `http://` and `https://`
  (TLS against the Mozilla roots, redirects followed, `https` refused until
  the clock is synced) and hands the bytes to the owner piece by piece, so
  a large download never blocks the API;
- **per port** an acceptor and a fixed pool of 4 workers: the agent API on
  port P (`HOPOS_PORT`, default 8080) and the leader API on P + 1000 (9080);
- the **clock task** (SNTP), the **store task** (object store), and in a
  cluster the **lease**, **state**, **link** and **dispatch** tasks.

At most 3 streams (`/v1/events`, a log tail) are open per node; the next
one gets a 503. With 4 workers per port, that keeps one worker free for the
CLI and the dashboard.

Every job placed on HopOS uses `"driver": "hop"` and an ELF artifact. The
agent advertises `node.os=hopos`, `node.id` and `node.arch` (`arm64` or
`riscv64`, used to match artifacts).

## Configuration: `hopos.cfg` and `HOPOS_*`

The kernel reads `hopos.cfg` (from the config window in the image, see
[Imaging nodes](#imaging-nodes)) and passes Hop's settings as an env blob on
the control page. Hop reads them in `agentd-hopos/src/env.rs`,
`lock.rs` and `sntp.rs`.

### Node

| `hopos.cfg` | Env | Default | Meaning |
| --- | --- | --- | --- |
| `hopos.node` | `HOPOS_NODE` | `hopos-<slot>` | the node id |
| `hopos.apikey` | `HOPOS_APIKEY` | none: the API does not start | the HMAC key for `X-Hop-Auth`; the same on every node of a cluster |
| `hopos.insecure` | `HOPOS_INSECURE` | off | `1`: run without a key on purpose. Ignored (with a log line) when a key is set |
| `hopos.cluster` | `HOPOS_CLUSTER` | `hopos` | the cluster name; the lease is `leases/<name>`, the state `state/<name>` |
| `hopos.hop.sharegroup` | `HOPOS_HOP_GROUP` | `hop` | Hop's own sharegroup: jobs tagged `sharegroup=<it>` share Hop's core and do not count against `HOPOS_CORES` |
| `hopos.init[]` | `HOPOS_INIT_JOBS` | `/hop/init-jobs.json` in Hop's volume, if present (max 64 KiB) | init jobs as one JSON array. The kernel moves them to the file when they do not fit in the env |
| `hopos.s3.endpoint`, `.bucket`, `.region`, `.key`, `.secret`, `.pathstyle` | `HOPOS_S3_ENDPOINT`, `_BUCKET`, `_REGION`, `_KEY`, `_SECRET`, `_PATHSTYLE` | none | the node's bucket: the [object store](#object-store) for apps, and with `hopos.lock.type=s3` the cluster lock. Used only when both endpoint and bucket are set; `_PATHSTYLE=1` for path-style URLs |
| `hopos.ntp` | `HOPOS_NTP` | `pool.ntp.org:123` | the SNTP server, `host` or `host:port` |

The kernel also sets these from the board and its own configuration:

| Env | Default | Meaning |
| --- | --- | --- |
| `HOPOS_NODE_IP` | the slot IP | the address in the agent endpoint |
| `HOPOS_PORT` | `8080` | the agent port; the leader listens on port + 1000 |
| `HOPOS_CORES` | `1` | app cores Hop hands out; `0` only with `HOPOS_SYSTEM_CORE=1` |
| `HOPOS_SYSTEM_CORE` | off | `1`: the kernel shares its core as sharegroup `system`. A `system` job always fits, and a one-core job without a group may go onto the OS core on a full node |
| `HOPOS_COLD_FLIP` | `yes` | can the board flip cold: `yes`, `no` (no PSCI), or `fresh` (only while no app core has run yet) |
| `HOPOS_MEMORY` | 256 MiB | app memory in bytes that Hop plans against (`HOP_MEMORY_DEFAULT` when missing) |
| `DNS` | none | the resolver for host names (artifacts, SNTP, S3) |

Secrets (`HOPOS_APIKEY`, `HOPOS_S3_SECRET`, `HOPOS_LOCK_APIKEY`) are never
logged; `Debug` shows only their length.

### Cluster lock

| `hopos.cfg` | Env | Default | Meaning |
| --- | --- | --- | --- |
| `hopos.lock.type` | `HOPOS_LOCK_TYPE` | `hoplockserver` when a URL is set | `hoplockserver`, `s3`, or `mem` (standalone). Anything else is refused |
| `hopos.lock.url` | `HOPOS_LOCK_URL` | none | the hoplockserver, `http://` or `https://` |
| `hopos.lock.apikey` | `HOPOS_LOCK_APIKEY` | none | its `X-API-Key` |
| `hopos.lock.key` | `HOPOS_LOCK_KEY` | `leases/<cluster>` | the lease object |
| `hopos.lease_ttl` | `HOPOS_LEASE_TTL` | `30` | lease in seconds, at least 15 (the election ticks every 10 s) |
| `hopos.advertise` | `HOPOS_ADVERTISE` | `HOPOS_NODE_IP` and `HOPOS_PORT` | `ip` or `ip:port` as other nodes reach this one (behind a NAT, e.g. QEMU slirp). The listeners stay on `HOPOS_PORT` and port + 1000 |

The rules (`ClusterConfig::from_env`, the same choice as the host's
`store::open_lease`):

- no type and no URL, or `mem`: **standalone**;
- no type with a URL, or `hoplockserver`: the lease on a hoplockserver;
- `s3`: the lease in the `hopos.s3.*` bucket (refused without endpoint and
  bucket). A bucket alone does **not** make a cluster: it is also the
  apps' object store;
- the committed state (`state/<cluster>`) goes where
  `discovery::state_store_for` puts it: **in the S3 bucket whenever
  `hopos.s3.*` is set**, otherwise on the hoplockserver of the lease. A
  host agent of the same cluster makes the same choice, so both read the
  same state.

A bad lock config (bad URL, unknown type, TTL under 15 s, bad
`HOPOS_ADVERTISE`) is a refusal (`HOP_LOCK_BAD`), like a missing key: a node
that silently ran standalone next to its cluster would be a second leader.

## Standalone and clustered

**Standalone** (no lock): Hop is the leader of its own one-node cluster
from the first tick (`HOP_LEADER`), its leader has no settle period, and on
a clean boot it seeds the init jobs (`HOP_INIT_SEEDED`).

**Clustered**: HopOS nodes and host agents (`agentd`) share one lease on a
hoplockserver or in an S3 bucket. The lease bytes are the host's
(`discovery::wire`), so they interoperate.

1. **Clock first.** A lease is a time on the writer's wall clock, and the
   kernel pins its clock to a fixed date until SNTP succeeds
   (`HOPOS_CLOCK_FIXED` on the kernel side). A node with that clock would
   either see every lease as alive or write one that has already expired.
   So a clustered node joins only after SNTP (`HOP_CLOCK_SYNCED`, then
   `HOP_CLUSTER_JOIN`); until then it says `HOP_CLUSTER_NO_CLOCK` once and
   does not claim or vote.
2. **Boot claim.** The first claim after the clock is set: a free lock makes
   the node leader at once, without the takeover wait.
3. **Leading.** The election says "lead", the state task loads the committed
   state (`HOP_LEADER_LOADING`, `HOP_STATE_LOADED`), and only then the
   leader API opens (`HOP_LEADER`); until then it answers 503. A store that
   does not answer is not an empty store: init jobs are seeded only on a
   clean cluster state that was read successfully (`HOP_INIT_SEEDED`), by
   whichever node becomes leader, not by every boot.
4. **Following.** The node registers with the leader named in the lease and
   heartbeats every 10 s over the LAN (link task); `HOP_LINK_FAIL` says once
   why a call failed. Task events go to the leader as `POST /v1/notify`.
5. **Stepping down.** The leader half goes first, then the lease is released
   (`HOP_LEADER_STOPPED`). A renew that finds another owner steps down at
   once (`HOP_LEASE_DISPLACED`).
6. **Failover.** When a leader stops answering, the others take the lease
   after it expires, exactly as on hosts.

A minimal clustered `hopos.cfg`:

```
hopos.node=pi-kitchen
hopos.cluster=home
hopos.apikey=<shared secret>
hopos.lock.url=http://192.168.1.10:8090
hopos.lock.apikey=<hoplockserver key>
```

### Leader dispatch over the LAN

`leader::Transport` is synchronous, but the network on HopOS is not, and the
owner task never waits on another node. So for agents on other nodes the
leader answers from the books of a `Relay` (`agentd-hopos/src/relay.rs`),
and the **dispatch task** makes the real call afterwards:

- `run`: "accepted", and `POST /run` is queued, unless a recent refusal
  from that agent for that job is remembered. If the agent refuses (full,
  affinity, unreachable), the placement is booked off
  (`Leader::mark_unplaced`) and remembered, so the next reconcile picks
  another agent (`HOP_RELAY_REFUSED`);
- `stop_job`, `stop_task`, `delete_job`: queued, reported done; a stop that
  never arrives is corrected at that agent's next registration;
- `tasks`: the last list the dispatch task fetched, refreshed every leader
  tick (10 s).

A full dispatch queue drops calls and counts them (`HOP_RELAY_FULL`); the
next reconcile retries. The agent on the leader's own node is called
in-process.

### Forwarding

When a request must go to another node, the owner answers with a forward
and the connection worker that brought the request carries it out
(`agentd-hopos/src/forward.rs`):

- **Follower, agent port**: `/v1/*` goes unchanged to the leader (method,
  path, query, body and the caller's `X-Hop-Auth`; the whole cluster shares
  one key). Streams (`hop events`, `hop logs -f`) are flushed chunk by
  chunk.
- **Leader**: `/v1/agents/{id}/logs/...` and `/capacity` for an agent on
  another node become a signed `GET` there, buffered or streamed.
- **Leader**: `/v1/tasks` and `/v1/jobs/{name}/status` walk the agents on
  other nodes and merge their tasks with the local ones.

The **leader port** of a follower does not forward: it answers 503 with
the address of the leader. Clients (and apps) should use the agent port
(8080) to reach the cluster from any node.

Agent-port answers carry CORS headers for the dashboard
([apps.md](apps.md#hop-gui)); the leader port does not.

## Mixing HopOS nodes and host agents

A host agent joins the same cluster with the same name, key and lock:

```json
{"api_key": "<shared secret>",
 "cluster": {"name": "home",
             "lock": {"type": "hoplockserver",
                      "url": "http://192.168.1.10:8090",
                      "api_key": "<hoplockserver key>"}}}
```

Keep `timeouts.leader_lease` at 15 s or more on the host too: a lease
shorter than the 10 s election tick means the cached leader has expired at
every tick, and a follower never registers. See
[configuration.md](configuration.md).

Either kind of node can lead. A host agent cannot run a `"driver": "hop"`
job (it fails with `HOP_JOB_FAILED`), and a HopOS node only runs ELF
residents, so pin jobs with affinity:

```json
{"name": "dns", "driver": "hop", "affinity": {"node.os": "hopos"}, ...}
{"name": "api", "command": "./api", "affinity": {"node.id": "host-1"}, ...}
```

## Clock sync

The clock task runs SNTP (RFC 4330) at start and every hour, retrying every
5 minutes after a failure, and sets the kernel clock with `SET_CLOCK`. It is
stricter than the Go version: the reply must come from a server (mode 4),
be synchronised (not LI 3, stratum 1 to 15), echo our transmit timestamp,
and lie after 2026. Until the first success, `https` downloads wait and a
clustered node does not join. Without a resolver or internet the step fails
with `HOP_SNTP_FAIL` and Hop keeps running; `http://` to an IP address still
works.

## Object store

The kernel queues apps' store calls (`applib::store`): `pull`, `push`,
`list`, `drop` between an app's hopfs view and its folder in the bucket.
Hop's store task (`agentd-hopos/src/objstore.rs`, `objstore/bucket.rs`)
takes them with `NEXT_STORE`, does the S3 side, moves the bytes with
`STORE_READ` / `STORE_WRITE` and reports with `STORE_DONE`.

- Bucket: `hopos.s3.*`. The key prefix is `apps/<cluster>/<job>/`, next to
  `leases/<cluster>` and `state/<cluster>`. The job name comes from the
  kernel (the app's slot), and the kernel has already checked the object
  name (no `..`, not empty), so an app only chooses names inside its own
  folder.
- Streaming: pulls and pushes go in 256 KiB chunks straight between the
  connection and the kernel; Hop never holds a whole object. A list returns
  at most 4096 keys (8 KiB of names to the app).
- Without `hopos.s3.*`, every call is refused with "no object store on this
  node (boot with hopos.s3.*)" (`HOP_STORE_NONE`). A plain `http://`
  endpoint works but is logged (`HOP_STORE_PLAIN_HTTP`).

## Console markers

Hop logs one line per event to the kernel console, ending in a marker that
tests and operators grep for. Markers from `agentd-hopos` and the shared
crates it runs:

### Startup

| Marker | Meaning |
| --- | --- |
| `HOP_BOOT` | the resident started in its slot |
| `HOP_NET_FAIL` | no network stack: no API, the resident stops |
| `HOPOS_API_NO_AUTH` | no `HOPOS_APIKEY` and no `HOPOS_INSECURE=1`, or an invalid `HOPOS_*` value: the resident stays alive but opens no API |
| `HOPOS_API_INSECURE` | running without authentication on purpose |
| `HOP_LOCK_BAD` | the lock config (or `HOPOS_ADVERTISE`) is invalid: refused, no API |
| `HOP_MEMORY_DEFAULT` | `HOPOS_MEMORY` missing; planning against 256 MiB |
| `HOP_NO_CLOCK` | the kernel offers no wall clock; task times count from boot |
| `HOP_TLS_ENTROPY_HW` | TLS randomness seeded from a hardware RNG (`source=`) |
| `HOP_TLS_ENTROPY_WEAK` | TLS randomness from timer jitter, or a kernel seed without a hardware RNG |
| `HOP_SPAWN_FAIL` | a task (download, listeners, cluster) could not be spawned |
| `HOP_LISTEN_FAIL` | a port could not be bound; only that API is missing |
| `HOP_STRAY_STOPPED` | a resident Hop did not know was stopped at boot (`slot=`) |
| `HOP_INIT_SEEDED` | init jobs seeded on a clean boot or clean cluster state |
| `HOP_INIT_FAIL` | init jobs unreadable (file error, over 64 KiB, not UTF-8, bad JSON) |
| `HOP_UP` | both listeners are up (`node=`, `cluster=`, ports, `cores=`, `system=`) |
| `HOP_LEADER` | this node leads (standalone: its agent registered; clustered: became leader) |
| `HOP_LEADER_FAIL` | the leader refused the node's own agent |

### Clock

| Marker | Meaning |
| --- | --- |
| `HOP_CLOCK_SYNCED` | SNTP succeeded and the kernel clock was set |
| `HOP_SNTP_FAIL` | SNTP failed (logged the first 3 times, then every 12th) |
| `HOP_CLOCK_FAIL` | the kernel refused `SET_CLOCK` |

### Jobs and tasks

| Marker | Meaning |
| --- | --- |
| `HOP_JOB_PLACED` | a task started (`slot=N`) |
| `HOP_JOB_FAILED` | a task did not start (download, image or kernel refusal) |
| `HOP_JOB_REFUSED` | the leader API refused a job spec (e.g. a fixed port that would have to roll) |
| `HOP_NO_CAPACITY` | this agent refused a job: no CPU shares or memory left |
| `HOP_STOP_FAILED` | stopping a task failed |
| `HOP_PROBE_SKIPPED` | a job has a `health_check`; probes are not wired on HopOS |
| `HOP_DOWNLOAD_QUEUE_FULL` | a download waits; the download queue is full |

### Cluster

| Marker | Meaning |
| --- | --- |
| `HOP_CLUSTER` | clustered: the lock and lease TTL in use |
| `HOP_STATE_STORE` | where the committed state lives |
| `HOP_CLUSTER_NO_CLOCK` | waiting for SNTP before joining |
| `HOP_CLUSTER_JOIN` | clock synced; the boot claim is sent |
| `HOP_LEADER_LOADING` | won the lease; loading the committed state |
| `HOP_STATE_LOADED` | committed state loaded (job count); the leader API opens next |
| `HOP_STATE_FAIL` | state not loaded or unreadable (or the state queue is full); becoming leader waits |
| `HOP_STATE_SAVE_FAIL` | a state snapshot was not saved (count, reason) |
| `HOP_LEADER_STOPPED` | stepped down as leader |
| `HOP_LEASE_DISPLACED` | a renew found another owner; stepping down |
| `HOP_LEASE_RENEW_FAIL` | a renew failed; the lease is still valid for N seconds |
| `HOP_LEASE_STORE` | the lease store returned an error (once per new reason) |
| `HOP_LINK_FAIL` | register, heartbeat or notify to the leader failed (once per reason) |
| `HOP_LINK_FULL` | the link queue is full; a call counts as failed or a notify was dropped |
| `HOP_RELAY_REFUSED` | an agent on another node refused a job; placing it elsewhere |
| `HOP_RELAY_FULL` | calls to agents on other nodes did not fit the dispatch queue |

### Object store

| Marker | Meaning |
| --- | --- |
| `HOP_STORE_UP` | the object store serves apps (endpoint, bucket, prefix) |
| `HOP_STORE_NONE` | no `hopos.s3.*`; app store calls are refused |
| `HOP_STORE_PLAIN_HTTP` | the bucket endpoint is `http://`: signed but readable |
| `HOP_STORE_FAIL` | a store call failed (op, key, slot, reason) |
| `HOP_STORE_GONE` | the app was gone before the result arrived |
| `HOP_STORE_KERNEL` | `NEXT_STORE` failed (no queue or connection in the kernel) |

### Kernel flip

| Marker | Meaning |
| --- | --- |
| `HOP_FLIP` | a flip was requested (`POST /flip`, `hop flip`) |
| `HOP_FLIP_ACCEPTED` | the kernel took the bundle and flips now |
| `HOP_FLIP_FAIL` | the flip failed or was refused (502 to the caller) |
| `HOP_FLIP_COLD_STOP` | cold flip: N tasks on this node stopped first |
| `HOP_FLIP_COLD_BACK` | the cold flip did not happen; the stopped tasks restart |

Markers starting with `HOPOS_` without `API` (`HOPOS_CLOCK_FIXED`,
`HOPOS_HOP_RESUMED`, `HOPOS_SLOT_PUBLISH`, `HOPOS_PANIC`, ...) come from the
kernel; apps log their own (`HOPOS_HOPDNS_UP`, ..., see [apps.md](apps.md)).

## Building Hop's resident

`agentd-hopos` is a `no_std` binary for `aarch64-unknown-none-softfloat`
(the toolchain also carries `riscv64gc-unknown-none-elf`), linked with
applib's `hopapp.ld` (`agentd-hopos/build.rs`). On the host it builds as an
empty `main` so clippy can type-check it.

```
cargo build --release --target aarch64-unknown-none-softfloat -p agentd-hopos
```

The release artifacts `hop-arm64.elf` and `hop-riscv64.elf` are **not**
built in this repository's release workflow: `hop-os/tools/hop-build.sh` in
the HopOS repository builds them against the applib of the HopOS working
tree. This repository's release ships the apps' residents
(`<app>-arm64.elf`). See [development.md](development.md).

## QEMU cluster test

`sh tools/qemu-test-cluster.sh` runs one HopOS node on QEMU next to a host
`agentd`, with a hoplockserver and an SNTP server on the host. It builds
the kernel (`board-qemuvirt`) and `appspike` in `$HOPOS_DIR` (default
`../../hop-os` from the repository root), `agentd-hopos`, `agentd`, `hop`
and `hoplockserver` here. It checks, in order:

1. the clock (`HOP_CLOCK_SYNCED`, then `HOP_CLUSTER_JOIN`) and the lock;
2. the host agent leads; the HopOS node registers across the LAN
   (`hop agents` shows both);
3. the HopOS agent port forwards `/v1/*` to the host leader, buffered and as
   a stream (`hop events`);
4. a job with `"affinity": {"node.os": "hopos"}` lands on the HopOS node
   (`HOP_JOB_PLACED slot=2`) and `hop logs` works through the leader;
5. failover: the host agent is killed, the HopOS node takes the lease with
   the committed state (`HOP_LEADER`, `HOP_STATE_LOADED`);
6. the restarted host agent registers with the HopOS leader;
7. a job with `"affinity": {"node.id": "host-1"}` goes from the HopOS leader
   to the host agent; `hop jobs`, `hop logs` and `hop logs -f` work through
   the HopOS leader.

The node uses `hopos.advertise=127.0.0.1:<port>` (QEMU slirp is a NAT) and
the host agent `node.ip = 10.0.2.2`. `HOPOS_PANIC`, `HOPOS_EXCEPTION`,
`HOPOS_HOP_FAULT`, `HOPOS_HOP_EXIT`, `HOPOS_HOP_FAIL`, `HOP_LOCK_BAD` or
`HOP_SPAWN_FAIL` fail the test at once. The kernel must pass the lock,
lease, advertise and NTP keys to Hop and read them from the QEMU bootargs;
the script checks that first. `TIMEOUT` (per phase, default 120 s) and
`KEEP_LOG=<dir>` are optional.

## Imaging nodes

Every HopOS kernel carries its `hopos.cfg` in a 16 KiB config window.
`hop image` reads or rewrites it in an image, a flip bundle or on a card,
and writes cards with verification:

```
hop image hopos-rpi4-headless.img                                         # show the config
hop image hopos-rpi4-headless.img --config my-node.cfg --write /dev/rdisk4
hop image hopos-rpi4-headless.img --keep --write /dev/rdisk4              # keep the card's config
```

Details, including `--force` and flip bundles: [cli.md](cli.md).

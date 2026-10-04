# Hop

Lightweight cluster orchestrator. Simple alternative to Nomad.

This is Hop v3, written in Rust.

On HopOS, Hop is the first resident: an app with one privilege no other app
has, the right to place, stop and replace apps. The kernel owns the mechanism;
Hop owns the policy: jobs, placement, health, updates, the cluster state and
the API. The same crates also run as an ordinary daemon on Linux and macOS.

How the code is written: the haas.software Rust handbook (`rustdoc/README.md`
next to this repository).

## Layout

```
types/      the job spec, task state, node identity: the vocabulary
config/     node and cluster configuration
auth/       the HMAC request authentication (X-Hop-Auth)
leader/     scheduling, placement, updates, failover: the cluster brain
agent/      the node: tasks, heartbeats, restarts, state persistence
api/        the HTTP API as pure handlers over request/response types
discovery/  finding nodes on the LAN
runner/     the task backends: HopOS slots, processes, docker
hopos-runner/  runner::SystemApi over applib's system client (the kernel frames)
hop-http/   the HTTP adapter: leanhttp over an applib TcpStream, api in and out
agentd-hopos/  Hop as the HopOS resident: agent + leader in a slot (lib + no_std bin)
hostnet/    the host side of the net (std): std sockets for leanhttp, block_on, HTTP(S) client, S3 transport
store/      the lease and the committed cluster state on the host: S3, hoplockserver, a file
agentd/     the daemon binary (Linux, macOS): agent, election, leader, both APIs, processes and docker
cli/        the `hop` command: apply, jobs, status, agents, logs, events, delete, flip, image
```

## On a host

```
cargo build --release -p agentd -p cli
target/release/agentd --cluster demo            # standalone: in-memory lock, state in ./data
target/release/hop apply job.json               # {"name": "sleeper", "command": "sleep 30"}
target/release/hop jobs
target/release/hop logs sleeper                 # the latest lines; -f follows live until the task stops
target/release/hop events                       # the cluster events as they happen (SSE /v1/events)
target/release/hop delete sleeper
```

Every command talks to the leader only (`--leader`, or `HOP_LEADER`): the
tasks come from `GET /v1/tasks`, and logs and an agent's capacity go through
the leader to that agent (`/v1/agents/{id}/logs/...`, `/capacity`). An agent
may sit on an address the CLI cannot reach (a HopOS slot LAN, a private
network behind the leader); the leader reaches them all. Only `flip` goes to
an agent itself (`--agent`).

`hop image` talks to no one: it is the HopOS imager. Every HopOS kernel
carries its node config (`hopos.cfg`) in a 16 KiB config window (a
`#HOPCFG1 window=16384 len=...` head line, the config, `#` padding: HopOS
`board/src/cfgwin.rs`); `hop image` finds it in an image, a flip bundle or
on a card and rewrites it in place, with the Sophgo FIP checksums of the
LicheeRV fixed up:

```
hop image hopos-rpi4-headless.img                                   # show the config
hop image hopos-rpi4-headless.img --config my-node.cfg              # put mine in
hop image hopos-rpi4-headless.img --config my-node.cfg --write /dev/rdisk4   # and write the card, verified
hop image hopos-rpi4-headless.img --keep --write /dev/rdisk4        # new image, the card keeps its config
hop image hopos-o6n-headless.flip --config my-node.cfg              # a bundle: prints the new sha256
```

Streams (`hop events`, `hop logs -f`) hold a connection thread on the node
while they run, so a node allows only a few at once (4 on a host, 2 on
HopOS) and says so with a 503. Calls to the lease and state backends (S3,
hoplockserver) have one total deadline per call, not one per phase.

SIGTERM is not handled yet: std has no signal API. A killed daemon lets its
lease expire (TTL) instead of releasing it.

`sh tools/e2e-host.sh` runs apply, jobs, status, agents and delete and checks
the process comes and goes; `cargo test -p agentd --test streams` does the
same for events, `/v1/tasks` and a live log through the leader.
Cross-build for Linux: `CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld
cargo build --release -p agentd -p cli --target aarch64-unknown-linux-musl`.

## Een cluster van HopOS-nodes

A HopOS node is a standalone leader until its `hopos.cfg` names a lock. With a
lock, HopOS nodes and host agents (`agentd`) form one cluster: they share one
lease on a hoplockserver or in an S3 bucket (the same lease bytes as the host
writes, `discovery::wire`), one node holds it and leads, and the others
register and heartbeat with that leader over the LAN. The committed cluster
state (`state/<cluster>`) lives next to the lease, so a new leader carries on
with the jobs of the old one.

The kernel hands these `hopos.cfg` keys to Hop as `HOPOS_*` env:

| `hopos.cfg` | Env | Meaning |
| --- | --- | --- |
| `hopos.cluster` | `HOPOS_CLUSTER` | the cluster name; the lease is `leases/<name>`, the state `state/<name>` |
| `hopos.apikey` | `HOPOS_APIKEY` | the API key; the same on every node of the cluster (requests and proxies are signed with it) |
| `hopos.lock.type` | `HOPOS_LOCK_TYPE` | `hoplockserver` (the default when a URL is set), `s3`, or `mem` (standalone) |
| `hopos.lock.url` | `HOPOS_LOCK_URL` | the hoplockserver, `http://host:port` |
| `hopos.lock.apikey` | `HOPOS_LOCK_APIKEY` | its `X-API-Key` (a secret: never on the console) |
| `hopos.lock.key` | `HOPOS_LOCK_KEY` | the lease object, default `leases/<cluster>` |
| `hopos.s3.endpoint`, `.bucket`, `.region`, `.key`, `.secret`, `.pathstyle` | `HOPOS_S3_*` | the bucket; the lock only with `hopos.lock.type=s3` (the bucket is also the object store of the apps) |
| `hopos.lease_ttl` | `HOPOS_LEASE_TTL` | the lease in seconds, default 30, at least 15 (the election asks every 10 s) |
| `hopos.advertise` | `HOPOS_ADVERTISE` | `ip` or `ip:port` as the other nodes reach this one, when the uplink sits behind a NAT; the leader port is port + 1000 |
| `hopos.ntp` | `HOPOS_NTP` | the time server, `host` or `host:port`, default `pool.ntp.org` |

A lease is a time on the writer's wall clock, and HopOS pins its clock to a
fixed date until SNTP succeeds. A clustered node therefore joins only once
its clock is synced (`HOP_CLUSTER_JOIN`; until then `HOP_CLUSTER_NO_CLOCK`).
Its first claim is the boot claim: a free lock makes it leader at once. Then:

- leading: it loads the committed state (`HOP_LEADER_LOADING`,
  `HOP_STATE_LOADED`), and only then opens its leader API (`HOP_LEADER`). The
  leader reaches the agents on other nodes through a dispatch task: `/run`,
  stop, delete and `/tasks` go over the LAN, and an agent that refuses a job
  (full, affinity, unreachable) gets it booked off and placed elsewhere
  (`HOP_RELAY_REFUSED`). `hop logs` (also `-f`), `hop agents <id>` and
  `/v1/tasks` go through the leader to whichever node holds the task;
- following: it registers with the leader named in the lease and heartbeats
  every 10 s (`HOP_LINK_FAIL` says once why a call failed), and its agent
  port passes `/v1/*` on to the leader, streams included (`hop events`,
  `hop logs -f`);
- failover: when the leader stops answering, the others take the lease after
  it expires, exactly as on hosts.

```
hopos.node=pi-kitchen
hopos.cluster=home
hopos.apikey=<shared secret>
hopos.lock.url=http://192.168.1.10:8090
hopos.lock.apikey=<hoplockserver key>
```

A host agent joins the same cluster with the same name, key and lock
(`"cluster": {"name": "home", "lock": {"type": "hoplockserver", "url": ...,
"api_key": ...}}` and `"api_key"`). Keep `timeouts.leader_lease` at 15 s or
more there too: a lease shorter than the election tick means the cached
leader has expired by every tick, and a follower never registers.

`sh tools/qemu-test-cluster.sh` runs one HopOS node on QEMU next to a host
agent, with a hoplockserver and an SNTP server on the host: registration
across nodes, a job placed on the HopOS node through the host leader, its
logs, a failover to the HopOS node, the host agent rejoining it, and a job,
`hop jobs`, `hop logs` and `hop logs -f` through the HopOS leader to the
host. It needs a kernel that passes the keys above to Hop and reads them from
the QEMU bootargs (`HOPOS_DIR`, default `../../hop-os`).

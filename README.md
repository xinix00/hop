# Hop

Lightweight cluster orchestrator. Simple alternative to Nomad.

This is Hop v3, written in Rust. The Go generation (v1.x) lives in [OLD/](OLD/)
with its own README, docs and tests; its releases stay tagged. The Go tree is
the specification the Rust tree is written from, test for test.

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
cli/        the `hop` command: apply, jobs, status, agents, logs, events, delete, flip
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

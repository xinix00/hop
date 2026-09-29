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
cli/        the `hop` command: apply, jobs, status, agents, logs, delete, flip
```

## On a host

```
cargo build --release -p agentd -p cli
target/release/agentd --cluster demo            # standalone: in-memory lock, state in ./data
target/release/hop apply job.json               # {"name": "sleeper", "command": "sleep 30"}
target/release/hop jobs
target/release/hop delete sleeper
```

`sh tools/e2e-host.sh` runs exactly that and checks the process comes and goes.
Cross-build for Linux: `CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld
cargo build --release -p agentd -p cli --target aarch64-unknown-linux-musl`.

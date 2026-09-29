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
agentd/     the daemon binary (Linux, macOS)
cli/        the `hop` command
```

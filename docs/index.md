# Hop Documentation

Hop is a lightweight cluster orchestrator, a simple alternative to Nomad.
Version 3 is written in Rust. The logic crates are `no_std` and sans-I/O, so
the same code runs in two places:

- **`agentd`**, an ordinary daemon on Linux and macOS. It runs tasks as plain
  processes (`exec`) or Docker containers (`docker`).
- **`agentd-hopos`**, the first resident of a HopOS node. It places, stops and
  replaces apps in HopOS slots (`hop`). The kernel owns the mechanism and Hop
  owns the policy.

Every node holds an agent. One node also holds the cluster lease and leads:
it schedules jobs, watches health, rolls out updates and keeps the committed
cluster state. HopOS nodes and host agents can share one cluster.

## Quick start

```sh
cargo build --release -p agentd -p cli

# A single-node cluster: in-memory lock, state in ./data
target/release/agentd --cluster demo

# Deploy something
echo '{"name": "sleeper", "command": "sleep 30"}' > job.json
target/release/hop apply job.json
target/release/hop jobs
target/release/hop logs sleeper -f
target/release/hop events
target/release/hop delete sleeper
```

To add nodes, point the agents at a shared lock (a hoplockserver or an S3
bucket) and set the same `api_key` on all of them; see
[configuration.md](configuration.md). To add HopOS nodes, see
[hopos.md](hopos.md).

## Guides

| Doc | What it covers |
| --- | --- |
| [architecture.md](architecture.md) | The crate map and dependency rules, the sans-I/O design, leader, agent, discovery and lease, runners, the HTTP layer, `agentd` vs `agentd-hopos` |
| [api.md](api.md) | The HTTP API: HMAC authentication (`X-Hop-Auth`), leader endpoints (`/v1/*`), agent endpoints, proxying through the leader, the `/v1/events` SSE stream, stream limits, status codes |
| [cli.md](cli.md) | The `hop` command (`apply`, `jobs`, `status`, `agents`, `logs`, `events`, `delete`, `flip`) and the HopOS imager `hop image` |
| [configuration.md](configuration.md) | `agentd` flags and the JSON config file: node, cluster, lock backends, state storage, ports, capacity, timeouts |
| [data-structures.md](data-structures.md) | The job spec, runners, tasks and their states, agents, events and limits |
| [lifecycles.md](lifecycles.md) | Step-by-step lifecycles with their invariants and timings: election, settle, registration, placement, restarts, health checks, updates, failover |
| [hopos.md](hopos.md) | Hop as the HopOS resident: `hopos.cfg` keys, clustering, console markers, the object store, the QEMU cluster test |
| [apps.md](apps.md) | The apps Hop ships (hopdns, hoplb, hopprom, hoplockserver, hop-gui) and the `hoplib` client |
| [development.md](development.md) | Toolchain, lints, building and cross-building, the gate (`tools/gate.sh`), tests, CI and releases |

## Quick pointers

- **Auth:** one shared key per cluster. Every request except a few public
  routes carries an HMAC signature (`X-Hop-Auth`); the key never travels on
  the wire. An empty key disables auth (dev only).
  → [api.md](api.md#authentication-x-hop-auth)
- **Ports:** agent API on `8080`, leader API on agent port + 1000 (`9080`),
  hoplockserver on `8090`. → [configuration.md](configuration.md#ports)
- **The CLI talks to the leader:** `--leader` or `HOP_LEADER`. Logs and agent
  capacity are proxied by the leader to the node that holds the task.
  → [cli.md](cli.md)
- **Job upsert:** the job name is the unique key. Applying an existing name
  updates the job with its `update_policy` (`rolling`, `recreate` or
  `blue-green`). → [data-structures.md](data-structures.md#update-strategies)
- **Failover:** when the leader stops answering, a follower starts claiming
  the lease after four failed election ticks (10 s each) and takes over once
  the lease has expired; the new leader loads the committed state.
  → [lifecycles.md](lifecycles.md)
- **Streams:** `hop events` and `hop logs -f` hold a connection on the node;
  at most 4 at once on a host and 3 on HopOS, a 503 beyond that.
  → [api.md](api.md#stream-limits)
- **Not yet:** SIGTERM is not handled (std has no signal API), so a killed
  daemon lets its lease expire instead of releasing it.

## Repository layout

```
types/         the job spec, task state, node identity
config/        node and cluster configuration
auth/          the HMAC request authentication
leader/        scheduling, placement, updates, failover
agent/         the node: tasks, heartbeats, restarts, health, election
api/           the HTTP API as pure handlers
discovery/     the leader lease: finding the leader through it
runner/        the task backends: HopOS slots, processes, docker
hopos-runner/  the runner over the HopOS system client
hop-http/      the HTTP adapter on HopOS
agentd-hopos/  Hop as the HopOS resident
hostnet/       the host side of the network (std)
store/         lease and state backends on the host: S3, hoplockserver, file
agentd/        the host daemon
cli/           the `hop` command
hoplib/        the client for plugins
apps/          hopdns, hoplb, hopprom, hoplockserver, hop-gui
tools/         the gate, end-to-end and QEMU test scripts
```

The code follows the haas.software Rust handbook (`rustdoc/README.md`, a
sibling repository).

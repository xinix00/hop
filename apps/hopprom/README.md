# hopprom

Prometheus exporter for Hop clusters. hopprom polls a Hop agent and serves
the cluster as Prometheus metrics on `/metrics`. It runs as a host binary
next to `agentd`, or as a resident in a HopOS slot next to Hop itself.

hopprom v3 is Rust. The Go generation lives in `OLD/` as the specification
it was ported from; every metric kept its name.

## Layout

One crate, three faces:

| Part | Feature | What it is |
| --- | --- | --- |
| `hopprom` (lib) | none | The sans-I/O core, `no_std` with `alloc`: one poll round over a `Source`, the `Collector` (metrics and the counters as deltas between two polls) and `render` (the Prometheus text format). |
| `hopprom` (bin) | `std` | The host binary: a poll thread that owns the collector, an accept thread, and a server thread that owns the latest snapshot. Messages between them, no mutex. |
| `hopprom-hopos` (bin) | `hopos` | The HopOS resident (`aarch64-unknown-none-softfloat`, on `applib`): a poll task and a server task on the app executor. |

Talking to Hop (signing with `X-Hop-Auth`, the routes, reading the replies)
is [hoplib](../../hoplib), the client shared by the Hop plugins. hopprom
lives in the hop workspace: hop's crates and hoplib are path dependencies,
HopOS and lean come in on the workspace's tags (the root `Cargo.toml`).

## Host binary

```sh
cargo build --release --features std --bin hopprom

# Defaults: polls http://127.0.0.1:8080, serves :9090/metrics every 5s.
./target/release/hopprom

./target/release/hopprom \
  -listen :9090 \
  -agent http://127.0.0.1:8080 \
  -interval 5s \
  -api-key "$HOP_API_KEY"
```

`-interval` takes a Go duration (`5s`, `1m30s`). `-api-key` is the cluster
key; without it requests go unsigned (a cluster in insecure mode). Point
`-agent` at the local agent: it proxies `/v1/*` to the leader, so one
hopprom sees the whole cluster.

`GET /metrics` answers 503 until the first successful poll, so Prometheus
sees `up 0` instead of a cluster of zeros. `GET /health` answers `ok`.

## HopOS resident

```sh
cargo build --release --target aarch64-unknown-none-softfloat --features hopos --bin hopprom-hopos
rust-objcopy --strip-debug target/aarch64-unknown-none-softfloat/release/hopprom-hopos hopprom-hopos.elf
```

Place it with Hop; the kernel publishes the jobspec port on the node's
uplink (DNAT) and Hop passes it as `ER_PORT_METRICS`:

```json
{
  "name": "hopprom",
  "driver": "hop",
  "artifacts": [{"url": "http://LAPTOP:8000/hopprom-hopos.elf"}],
  "memory_limit": 33554432,
  "ports": {"metrics": 9090},
  "env": {"HOPPROM_API_KEY": "..."}
}
```

| Env | Default | Meaning |
| --- | --- | --- |
| `HOPPROM_AGENT` | `http://HOP:8080` | The agent to poll. The host name `HOP` is Hop's own slot (slot 1 on the slot LAN, `10.100.0.2`), no DNS involved. |
| `HOPPROM_INTERVAL` | `5s` | Poll interval, a Go duration. |
| `HOPPROM_API_KEY` | none | Cluster key (`HOP_API_KEY` from the Go generation also works). |
| `ER_PORT_METRICS` | `9090` | Set by Hop from `ports`. |

Port 9080 is Hop's leader API, which has no `/leader` route: with the
default the leader metrics stay empty (`hop_leader_info{leader=""} 0`) and
the console says so once. `HOPPROM_AGENT=http://HOP:8080` polls the agent
API instead, which proxies `/v1/*` to the leader and does serve `/leader`.

Console markers: `HOPOS_HOPPROM_UP port=<n>` when the listener is up;
`HOPOS_HOPPROM_POLL agents=<n> tasks=<n>` after the first successful poll
and whenever either number changes; `HOPOS_HOPPROM_POLL_FAIL` when a round
fails (once per new reason); `HOPOS_HOPPROM_MISS` when a best-effort call
misses (once per new reason).

## Metrics

### Agents

```prometheus
hop_agents_total                                 # Registered agents
hop_agents_healthy                               # Agents with a heartbeat in the last 60s
hop_agent_cpu_cores{agent="..."}                 # CPU cores per agent
hop_agent_cpu_used_cores{agent="..."}            # Reserved CPU (cpu_used_shares / 1024)
hop_agent_memory_bytes{agent="..."}              # Memory per agent
hop_agent_memory_used_bytes{agent="..."}         # Reserved memory per agent
hop_agent_temperature_celsius{agent="..."}       # CPU temperature from the heartbeat (new in v3; only agents that report one)
```

### Tasks

```prometheus
hop_tasks_total{state="queued|downloading|running|stopping|failed"}  # Tasks by the state the node reports
hop_tasks_placed{job="..."}                      # Running tasks per job
hop_task_restarts{job="..."}                     # Sum of the current restart_count per job
```

### Jobs

```prometheus
hop_jobs_total                                   # Jobs
hop_job_instances_placed{job="..."}              # Running instances
hop_job_instances_expected{job="..."}            # count; 0 means 1, -1 means one per agent
hop_job_healthy{job="..."}                       # 1 if placed >= expected
hop_job_cpu_percent{job="..."}                   # Average CPU of the running tasks, percent of allocation
hop_job_mem_percent{job="..."}                   # Average memory of the running tasks, percent of allocation
```

### Counters

```prometheus
hop_task_starts_total{job="..."}                 # Task ids not seen in the previous poll
hop_task_failures_total{job="..."}               # Known tasks that went to failed
hop_task_restarts_total{job="..."}               # Restarts: a restart_count that grew
```

The counters are deltas between two polls; hopprom keeps nothing but the
previous poll and the counters. The first poll counts every task it finds
as a start, and a restart of hopprom resets the counters (Prometheus'
`rate()` handles both). Hop v3 gives a restarted task a new id with its
predecessor's `restart_count` plus one, so a new id with a `restart_count`
above zero counts as one restart, and on the same id the increase counts.
The tasks of an agent the leader could not reach are kept from the previous
poll, so they are not counted as new starts when the agent returns.

### Leader and rollout

```prometheus
hop_leader_info{leader="ip:port"}                # 1 when a leader is known
hop_leader_lease_seconds                         # Until the local leader's lease lapses (0 on a follower)
hop_leader_changes_total                         # Leader switches seen since hopprom started
hop_cluster_settling                             # 1 while a new leader waits before its first reconcile
hop_jobs_deploying                               # Jobs whose last rollout did not finish
hop_job_deploying{job="..."}                     # 1 per such job
```

Every family has a `# HELP` and `# TYPE` line, series are sorted by label so
two scrapes of the same state are byte-identical, and label values are
escaped as the format says (`\\`, `\"`, `\n`). A family with no series
(no capacities read yet, no restarts, no failures) is left out, as in Go.

## Polling

One round asks the agent, in order: `GET /v1/agents`, `GET /v1/jobs`,
`GET /v1/tasks`, `GET /v1/agents/{id}/capacity` per agent, `GET /v1/status`
and `GET /leader`. The first three are required: if one fails, the round
fails and the previous metrics stay. The rest are best effort: a miss keeps
the previous value (a capacity is dropped when its agent is gone).

## Prometheus

```yaml
scrape_configs:
  - job_name: 'hop'
    scrape_interval: 10s
    static_configs:
      - targets: ['node1:9090']
```

Run one hopprom per cluster (`count: 1`): the agent proxies the cluster
routes to the leader, and Hop reschedules hopprom if its node goes away.

## Alerting examples

```yaml
groups:
  - name: hop
    rules:
      - alert: HopJobDegraded
        expr: hop_job_instances_placed < hop_job_instances_expected
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Job {{ $labels.job }} is degraded"

      - alert: HopAgentDown
        expr: hop_agents_healthy < hop_agents_total
        for: 2m
        labels:
          severity: critical

      - alert: HopHighFailureRate
        expr: rate(hop_task_failures_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning

      - alert: HopAgentCPUHigh
        expr: (hop_agent_cpu_used_cores / hop_agent_cpu_cores) > 0.9
        for: 10m
        labels:
          severity: warning
```

## Example queries

```promql
sum(hop_tasks_total)                                      # tasks across the cluster
hop_job_healthy == 0                                      # unhealthy jobs
rate(hop_task_failures_total[5m])                         # failure rate per job
sum(hop_agent_cpu_used_cores) / sum(hop_agent_cpu_cores)  # cluster CPU reservation
(hop_agent_memory_used_bytes / hop_agent_memory_bytes) * 100
```

## Development

```sh
sh tools/gate.sh       # tests, clippy -D warnings, rustfmt, the no_std core and the resident for the target, the host bin
sh tools/qemu-test.sh  # Hop on QEMU, hopprom placed with ports 9090, /metrics from outside
```

`tools/qemu-test.sh` uses the hop-os and hop repositories next to this one
(`HOPOS_DIR`, default `../../hop-os`; `HOP_DIR`, default `../hop`) for their
build and QEMU scripts only. It boots the kernel with Hop in slot 1, posts
the hopprom jobspec to the leader, adds the host forward to guest port 9090
over QMP, and checks `/metrics` from outside: `hop_agents_total 1` and
hopprom's own task metrics.

## License

MIT

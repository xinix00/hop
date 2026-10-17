# hoplb

Hostname-based load balancer for [Hop](https://github.com/xinix00/hop), with
Prometheus metrics. One Rust crate, two forms:

| Form | Build | Runs on |
| --- | --- | --- |
| `hoplb` (feature `std`, default) | `cargo build --release` | Linux, macOS: a host daemon next to a Hop agent |
| `hoplb-hopos` (feature `hopos`) | `cargo build --release --no-default-features --features hopos --target aarch64-unknown-none-softfloat --bin hoplb-hopos` | HopOS: a resident in its own slot, placed by Hop like any app |

Both run the same core (`no_std` + `alloc`, no I/O): the route table, the
reverse proxy, the metrics and the admin routes. What differs is who owns
the sockets and the clock.

## Features

- Routes traffic by the `Host` header, from the `hoplb-urlprefix` tag of Hop jobs
- Wildcards one level deep (`*.domain.com` matches `app.domain.com`, not `domain.com` or `a.b.domain.com`)
- Round-robin over the tasks of a job that are `running`
- Tag filter (`-tag lb:haas`): several hoplb instances, each with its own jobs
- Live updates from the agent's SSE stream (`/v1/events`), debounced by 500 ms, with reconnect and backoff
- Streaming in both directions: uploads of any length, chunked bodies passed through verbatim, SSE responses flushed event by event
- `X-Forwarded-For`, hop-by-hop headers stripped, `Expect: 100-continue` answered by the proxy
- Prometheus metrics: request counts per domain, backend and status code, latency quantiles (p50, p90, p95, p99), `_count` and `_sum`
- A separate admin port for `/health` and `/metrics`, so a firewall can keep it internal

## Job tags

```yaml
tags:
  hoplb-urlprefix: "app.example.com"   # or "*.example.com"
  hoplb-port: "http"                   # optional: which named port of the task gets traffic (default: the first)
  lb: "haas"                           # optional: only for a hoplb started with -tag lb:haas
```

A backend is `<host of the agent's endpoint>:<task port>`, so a job needs a
port (`"ports": {"http": 8081}`).

## On a host

```bash
# Defaults: traffic on :80, admin on :9091, the agent on 127.0.0.1:8080
./hoplb -listen :80 -admin-listen :9091 -agent http://127.0.0.1:8080

# Only jobs with tag lb=haas, with an API key for X-Hop-Auth
./hoplb -listen :80 -tag lb:haas -api-key "$HOP_API_KEY"

curl http://localhost:9091/health
curl http://localhost:9091/metrics
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `-listen` | `:80` | traffic (user requests) |
| `-admin-listen` | `:9091` | `/health` and `/metrics`: keep it internal |
| `-agent` | `http://127.0.0.1:8080` | the local Hop agent; it forwards `/v1/*` to the leader |
| `-tag` | none | only route jobs with this tag (`key:value`) |
| `-api-key` | none | the cluster key for `X-Hop-Auth` |

The daemon is a set of threads, each with one owner and no mutex: an owner
thread holds the metrics and the current route table, a stream thread holds
the SSE connection, a watcher thread holds the cluster cache, and a fixed
pool of 64 traffic threads and 2 admin threads each accept on a clone of
their listener. The route table reaches the workers as a published
snapshot: the owner bumps a generation, a worker that sees a newer one on
its next request fetches its own copy.

## On HopOS

hoplb-hopos is placed by Hop like any other app. Its configuration comes
from the job spec:

| Env | Meaning | Default |
| --- | --- | --- |
| `ER_PORT_HTTP` | traffic port, set by Hop from `"ports":{"http":80}` | 80 |
| `ER_PORT_ADMIN` | admin port, from `"ports":{"admin":9091}` | 9091 |
| `HOPLB_AGENT` | the agent; the host `HOP` means Hop's own slot (slot 1, `10.100.0.2`) | `http://HOP:9080` |
| `HOPLB_TAG` | tag filter, as `-tag` | none |
| `HOPLB_API_KEY` | key for `X-Hop-Auth` | none |
| `HOPLB_VERBOSE` | `1`: one log line per request | off |

Job spec (POST it to the leader, port 9080 of any node):

```json
{"name": "hoplb", "driver": "hop",
 "artifacts": [{"url": "http://LAPTOP:8000/hoplb.elf"}],
 "memory_limit": 67108864,
 "ports": {"http": 80, "admin": 9091},
 "env": {"HOPLB_API_KEY": "..."}}
```

Build the artifact without debug info (the symbols stay, placement reads them):

```bash
cargo build --release --no-default-features --features hopos \
  --target aarch64-unknown-none-softfloat --bin hoplb-hopos
rust-objcopy --strip-debug target/aarch64-unknown-none-softfloat/release/hoplb-hopos hoplb.elf
```

The node publishes ports 80 and 9091 on its uplink to hoplb's slot. A
backend on the same node is reached through its published port on the
node address (the switch turns that around internally, no byte leaves the
NIC). On the kernel console: `HOPOS_HOPLB_UP port=80 admin=9091` once both
listeners are up, and `HOPOS_HOPLB_ROUTES n=<patterns> backends=<n>` after
every new route table.

Inside the slot, one executor runs fixed tasks: an acceptor per port, 8
traffic workers and 2 admin workers from a fixed pool, a metrics task that
owns the metrics (workers post to its mailbox), a stream task for SSE and a
watcher task that replaces the route table (a table with one writer and
many short readers).

## Prometheus metrics

```prometheus
# HELP hoplb_requests_total Total HTTP requests
# TYPE hoplb_requests_total counter
hoplb_requests_total{domain="api.example.com",backend="10.0.1.5:8080",code="200"} 15234
hoplb_requests_total{domain="api.example.com",backend="10.0.1.5:8080",code="500"} 12

# HELP hoplb_request_duration_seconds Request duration percentiles
# TYPE hoplb_request_duration_seconds summary
hoplb_request_duration_seconds{domain="api.example.com",backend="10.0.1.5:8080",quantile="0.50"} 0.023000
hoplb_request_duration_seconds{domain="api.example.com",backend="10.0.1.5:8080",quantile="0.99"} 0.234000
hoplb_request_duration_seconds_count{domain="api.example.com",backend="10.0.1.5:8080"} 15234
hoplb_request_duration_seconds_sum{domain="api.example.com",backend="10.0.1.5:8080"} 350.234000
```

The domain is the client's `Host` header, the backend is empty for 502 (no
route) and 503 (no healthy backend). The latency window is the last 10,000
requests per series on a host (1,000 on HopOS). Series are capped (512 on a
host, 128 on HopOS) so random `Host` headers cannot grow memory; new series
beyond the cap fold into domain `_other`, counted by
`hoplb_series_folded_total`.

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'hoplb'
    static_configs:
      - targets: ['hoplb:9091']   # the admin port, not the traffic port
```

## Development

```bash
sh tools/gate.sh       # tests (core, host, resident), clippy -D warnings, rustfmt, the resident for its target
sh tools/qemu-test.sh  # HopOS on QEMU: Hop places welcome and hoplb, the bunny page through hoplb
```

`tools/qemu-test.sh` needs the sibling repositories: `HOPOS_DIR` (default
`../../hop-os`) and `HOP_DIR` (default `../hop`). It boots HopOS with Hop,
places welcome (`ports` 8081, tag `hoplb-urlprefix=welcome.local`) and hoplb
(`ports` 80 and 9091), fetches `http://127.0.0.1:$WEBPORT/` with
`Host: welcome.local` through hoplb, checks that `/metrics` counts it, then
deletes welcome and checks that the SSE stream takes the route away.

The Go generation lives in `OLD/` and is the specification: every Go test is
here under its own name, every Go benchmark is a test that prints a line
`bench <name>: <ns> ns/op`. Dependencies come from git tags only (lean
v3.1.3, hop v3.0.0, HopOS v3.0.0, hoplib v3.0.1), never
from a path across repositories.

## License

See [LICENSE](LICENSE).

# hopdns

DNS service discovery for [Hop](https://github.com/xinix00/hop), with
federation. A job name becomes an A record: `myapp.hop.local` answers with
the address of every running task of `myapp`, across all clusters.

hopdns v3 is written in Rust and comes in two shapes built from one core:

- **`hopdns`**, the host daemon (Linux, macOS): a UDP server on std
  sockets, one watcher thread per peer.
- **`hopdns-hopos`**, a resident of [HopOS](https://github.com/xinix00/HopOS):
  the same core on the app executor and the slot's own network stack,
  placed by Hop like any other job.

The Go generation lives in `OLD/`; it is the specification this port was
tested against, test by test.

## Features

- Resolves job names to task addresses, only tasks in state `running`.
- Real-time updates over Server-Sent Events (`/v1/events`) from every peer.
- Federation: every cluster is a peer, including your own. The cluster
  name comes from the peer's `GET /v1/status` (`cluster_name`).
- Multiple A records per job, TTL 5 s, and a SOA with a 5 s negative TTL
  on empty answers (RFC 2308).
- NXDOMAIN for unknown names, NODATA for known names without an A record,
  REFUSED outside the zone. A cluster name as an intermediate label
  (`prod-eu.hop.local`) is NODATA, not NXDOMAIN, so resolvers with QNAME
  minimisation keep working.
- Static CNAMEs (with wildcards) from an optional YAML file.
- EDNS: answers up to 1232 bytes when the client offers it.
- Graceful degradation: a peer that drops its stream keeps its last known
  addresses (stale but usable) until it reconnects.
- No allocation and no lock on the query path.

## Usage (host)

```bash
cargo build --release --features std --bin hopdns

# Single cluster
./target/release/hopdns -listen :5353 -peer http://127.0.0.1:8080

# Federation: local and remote clusters (over a VPN)
./target/release/hopdns -listen :5353 \
  -peer http://127.0.0.1:8080 \
  -peer http://key@10.0.1.100:8080 \
  -peer http://10.0.2.100:8080
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `-listen` | `:5353` | UDP address to listen on; `:53` for standard DNS |
| `-peer` | required, repeatable | a Hop agent endpoint; `http://key@host:port` carries its own API key |
| `-domain` | `hop.local` | DNS domain suffix |
| `-api-key` | empty | HMAC key for peers without their own `key@` |
| `-config` | none | YAML file with static CNAMEs |

Flags follow Go's `flag` package: `-name value`, `-name=value`, and the
same with two dashes.

```yaml
# -config hopdns.yaml
cnames:
  mail.hop.local: mailserver.example.com
  "*.apps.hop.local": ingress.prod-eu.hop.local
```

## Usage (HopOS)

`hopdns-hopos` is an ELF for `aarch64-unknown-none-softfloat`:

```bash
cargo build --release --features hopos --target aarch64-unknown-none-softfloat --bin hopdns-hopos
```

Serve it from any HTTP server and hand Hop a job spec. The kernel
publishes the job's ports on the node's uplink (TCP and UDP), so a
resolver on the LAN asks the node's address.

```json
{"name": "hopdns", "driver": "hop", "count": -1,
 "artifacts": [{"url": "https://example.com/hopdns-hopos.elf"}],
 "memory_limit": 33554432,
 "ports": {"dns": 5353},
 "env": {"HOPDNS_DOMAIN": "hop.local"}}
```

| Env | Default | Meaning |
| --- | --- | --- |
| `ER_PORT_DNS` | `5353` | UDP port (Hop sets it from `ports`) |
| `HOPDNS_PEER` | `http://HOP:9080` | comma-separated peers; `HOP` is Hop's slot address on this node (`HOPDNS_PEERS` also read) |
| `HOPDNS_DOMAIN` | `hop.local` | DNS domain suffix |
| `HOPDNS_API_KEY` | empty | HMAC key for peers without their own `key@` (`HOP_API_KEY` also read) |
| `HOPDNS_SELFTEST` | none | a job name: query it over UDP from inside and log the answer |
| `ER_ATTR_NODE_ID` | set by Hop | the own node: its tasks resolve to their slot address |

Console markers: `HOPOS_HOPDNS_UP port=<n>`, `HOPOS_HOPDNS_CACHE jobs=<n>`,
`HOPOS_HOPDNS_SELFTEST ok ip=<a.b.c.d>`.

## DNS resolution

```bash
dig @localhost -p 5353 myapp.hop.local          # all clusters merged
dig @localhost -p 5353 myapp.prod-eu.hop.local  # cluster prod-eu only
```

| Query | Answer |
| --- | --- |
| `myapp.hop.local` | addresses from **all** peers, merged and de-duplicated |
| `myapp.prod-eu.hop.local` | addresses from cluster `prod-eu` only |
| a static CNAME | the CNAME record only, for any query type (the resolver follows it) |
| unknown name in the zone | NXDOMAIN with SOA |
| known name, no running task | NOERROR, no answer, SOA |
| outside the zone | REFUSED |

The address of a task is the address of its agent (the host in its
endpoint), as in Go. A HopOS resident that knows its own node
(`ER_ATTR_NODE_ID`) answers the node's own tasks with their slot address
(`10.100.0.<slot+1>`), which a neighbouring slot reaches directly.

If the addresses do not fit the answer (about 30 at 512 bytes, about 74
with EDNS, depending on the length of the name),
the first ones that fit are sent without the TC bit: hopdns has no TCP
listener, and a resolver that retries over TCP would get nothing instead
of a subset.

## How it works

Per peer, a watcher state machine (`src/watcher.rs`, no I/O):

1. **Discover**: `GET /v1/status`, `cluster_name`. Retried every 5 s.
2. **Listen**: `GET /v1/events` through hoplib's stream, which reconnects
   with a fixed 5 s wait (Go's interval).
3. **Reload**: the `ping` that opens every connection seeds a full
   refresh. A `job` or `task` event queues that job; `status` (the reader
   missed events) or `agent` queues a full refresh. Events are coalesced
   for 500 ms.
4. **Rebuild**: a full refresh reads `/v1/agents` and `/v1/tasks` and
   replaces the cluster's map; a job refresh reads
   `/v1/jobs/{name}/status` (hoplib falls back to agents plus tasks on a
   leader without that route) and sets that job. A failed job refresh
   falls back to a full one; a failed full refresh retries after 5 s.

Ownership follows the Rust handbook: the cache has exactly one owner, the
task or thread that answers queries. Watchers send whole cluster maps (or
one job) as messages; nothing is shared behind a mutex.

- Host: one server thread (owns the cache), per peer a watcher thread and
  an event-reader thread, `std::sync::mpsc` between them.
- HopOS: one server task (owns the cache, `select` over the socket and
  its mailbox), per peer a watcher task and a reader task, fixed
  `sync::mpsc::Mailbox`es between them.

## Layout

| Path | What |
| --- | --- |
| `src/wire.rs` | the DNS wire format: parse a query, write A/CNAME/SOA/OPT, decode an answer |
| `src/cache.rs` | cluster to job to addresses |
| `src/server.rs` | one datagram in, one answer out |
| `src/cnames.rs` | static CNAMEs with wildcards |
| `src/config.rs` | flags, peers (`key@host`), the YAML subset |
| `src/watcher.rs` | the per-peer state machine and the cluster map builder |
| `src/host.rs` | the host daemon (feature `std`) |
| `src/bin/hopdns.rs` | the host binary |
| `src/bin/hopdns-hopos.rs` | the HopOS resident (feature `hopos`) |
| `tests/host.rs` | end to end: a fake Hop over real HTTP, queries over real UDP |
| `tests/bench.rs` | Go's benchmarks as tests with a measurement line |
| `tools/gate.sh` | the gate: tests, clippy `-D warnings`, rustfmt, the target build, the numbers |
| `tools/qemu-test.sh` | Hop on QEMU places welcome and hopdns; hopdns resolves welcome |
| `OLD/` | the Go generation (specification) |

Dependencies come from tags only: `hoplib` v3.0.1 (the shared plugin
client), and for the resident `applib` and `sync` from HopOS
v3.0.0.

## Testing

```bash
tools/gate.sh        # everything that runs on the host, plus the target build
tools/qemu-test.sh   # needs ../../hop-os, ../hop and qemu-system-aarch64
```

`tools/qemu-test.sh` boots HopOS with Hop in slot 1, posts welcome and
then hopdns (with `HOPDNS_SELFTEST=welcome`) to Hop's leader, and is green
when hopdns logs `HOPOS_HOPDNS_SELFTEST ok ip=<welcome's slot address>`.
The query comes from inside the node because HopOS's `image/qemu-run.sh`
only forwards TCP; with a UDP forward (`QEMU_RUN=<patched script>
DNSPORT=<port>`) the script also asks from the host.

## Performance

`cargo test --release --test bench -- --nocapture`, Apple M4 Pro,
30-09-2026 (Go numbers from `OLD/BENCHMARKS.md`, M4 Pro, Go 1.24):

| Benchmark | Rust ns/op | allocs/op | Go ns/op | Go allocs/op |
| --- | --- | --- | --- | --- |
| cache get (50 jobs) | 25 | 0 | 10 | 0 |
| cache set | 55 | 1 | 17 | 0 |
| cache update (swap) | 4 | 0 | 6 | 0 |
| handle query (3 IPs) | 58 | 0 | 144 | 9 |
| handle query (1 / 5 / 10 IPs) | 53 / 72 / 97 | 0 | 85 / 200 / 307 | 5 / 12 / 18 |
| extract IP | 38 | 0 | 128 | 2 |
| parse job from SSE | 239 | 6 | 208 | 7 |

Rust's "handle query" is the whole datagram path (parse, lookup, write
the answer into a buffer); Go's used a mock writer after miekg/dns had
parsed the message. "cache set" moves an owned address list in, so the
copy counts.

## Requirements

- A VPN between clusters (your responsibility).
- Hop with `cluster_name` in `/v1/status`.
- Matching API keys if peers use authentication.

## License

MIT, see `LICENSE`.

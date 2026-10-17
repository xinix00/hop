# hoplockserver

A tiny HTTP server that exposes the minimum compare-and-swap API needed to
back [hoplock](https://github.com/xinix00/hoplock) leader election, the
lease that Hop's leaders hold. Think of it as a fake S3 for lease files:
GET, PUT and DELETE on opaque keys, with `If-Match` and `If-None-Match`
preconditions, and one shared API key. No buckets, no SigV4, no AWS account.

It exists so anyone can run Hop (or any hoplock user) with leader election
without standing up an S3-compatible object store first.

Version 3 is a Rust port of the Go server, which now lives in `OLD/` as the
specification. It speaks the same protocol byte for byte where a client
reads it (status, `ETag`, body), and ships in two forms:

| Form | Binary | Runs on |
|------|--------|---------|
| Host | `hoplockserver` | Linux, macOS: std sockets, files under `-data` |
| HopOS resident | `hoplockserver-hopos` | a HopOS slot, placed by Hop as a job with a published port |

## Run on a host

```sh
cargo build --release
./target/release/hoplockserver \
  -listen :8090 \
  -data ./data \
  -api-key "$(openssl rand -hex 32)"
```

| Flag       | Default  | Notes                                            |
|------------|----------|--------------------------------------------------|
| `-listen`  | `:8090`  | TCP listen address (`:port` is every interface)  |
| `-data`    | `./data` | Directory where lease files are persisted        |
| `-api-key` | empty    | Required `X-API-Key` header; empty disables auth |

`HOPLOCK_API_KEY` is read when `-api-key` is empty. Flags parse like Go's
`flag` package (`-x v`, `-x=v`, `--x v`).

On stderr: `HOPLOCK_UP keys=<n>` once the port is open (n is the number of
keys already in the data directory), `HOPLOCK_NO_AUTH` when there is no key,
`HOPLOCK_FAIL` when the start fails.

## Run on HopOS

```sh
cargo build --release --target aarch64-unknown-none-softfloat \
  --no-default-features --features hopos --bin hoplockserver-hopos
rust-objcopy --strip-debug \
  target/aarch64-unknown-none-softfloat/release/hoplockserver-hopos hoplockserver.elf
```

Serve `hoplockserver.elf` over HTTP where the node can reach it and hand Hop
a jobspec (`hop apply`, or a POST to the leader's `/v1/jobs`):

```json
{"name": "hoplock", "driver": "hop",
 "artifacts": [{"url": "http://laptop:8000/hoplockserver.elf"}],
 "memory_limit": 33554432,
 "ports": {"http": 8090},
 "env": {"HOPLOCK_API_KEY": "..."}}
```

The kernel publishes port 8090 on the node's uplink and forwards it to the
slot. The resident reads from its environment:

| Variable          | Default | Notes                                   |
|-------------------|---------|-----------------------------------------|
| `ER_PORT_HTTP`    | `8090`  | Set by Hop from `"ports": {"http": N}`  |
| `HOPLOCK_API_KEY` | empty   | Required `X-API-Key`; empty disables auth |
| `HOPLOCK_DATA`    | `/data` | Where the store lives on hopfs          |

Console markers: `HOPOS_HOPLOCK_UP keys=<n>`, `HOPOS_HOPLOCK_NO_AUTH`,
`HOPOS_HOPLOCK_VOLUME` (the data directory could not be listed),
`HOPOS_HOPLOCK_CORRUPT` (a torn record, treated as absent),
`HOPOS_HOPLOCK_FAIL`, and every 1000 requests `HOPOS_HOPLOCK_REQUESTS`.

**Persistence on HopOS is not there yet.** A job's data belongs on a job
volume (`"volumes": {"/volumes/hoplock": "/data"}`), but Hop v3.0.0-alpha.10
refuses a job with volumes on HopOS ("persistent volumes require START_SLOT
mount support"): `START_SLOT` does not carry mounts yet. Without a volume,
`/data` is in the slot's own root, which the kernel empties at every start.
Keys survive everything except a restart of the job; once Hop and HopOS pass
volumes, the same image persists across restarts with no change here.

## Use it from Hop

```json
{"cluster": {"name": "prod",
             "lock": {"type": "hoplockserver",
                      "url": "http://lock.internal:8090",
                      "api_key": "..."}}}
```

The lease lives at `leases/<cluster>` (or `lock.key`), the committed cluster
state next to it at `state/<cluster>`. The Go client (`OLD/client`,
`hoplock.Backend`) talks to this server unchanged.

## HTTP API

| Method | Path      | Preconditions                            | Notes                        |
|--------|-----------|------------------------------------------|------------------------------|
| GET    | `/<key>`  | none                                     | Body plus `ETag`             |
| PUT    | `/<key>`  | `If-None-Match: *` or `If-Match: <etag>` | Body is opaque (max 1 MiB)   |
| DELETE | `/<key>`  | optional `If-Match: <etag>`              | 204                          |
| GET    | `/health` | none                                     | Always 200 `ok`, no auth     |

| Outcome                                              | Status |
|------------------------------------------------------|--------|
| GET found / PUT written                              | 200    |
| DELETE done                                          | 204    |
| missing or wrong `X-API-Key`                         | 401    |
| empty key, invalid key (PUT, DELETE), `If-None-Match` other than `*` | 400 |
| key not found (GET, DELETE)                          | 404    |
| method other than GET, PUT, DELETE (with `Allow`)    | 405    |
| `If-None-Match: *` on an existing key, stale `If-Match` | 412 |
| storage error, and an invalid key on GET (as in Go)  | 500    |

A PUT without a precondition overwrites. `If-None-Match: *` wins over
`If-Match` when both are sent. Errors are Go's `http.Error`: `text/plain;
charset=utf-8`, `X-Content-Type-Options: nosniff`, the message and a newline.

ETag = `"<hex sha256 of body>"`. It is deterministic on the body, so callers
can validate a write by recomputing it locally.

Keys are paths under the data directory, cleaned like Go's `filepath.Clean`:
`a//b`, `a/./b` and `a/b/` are the key `a/b`. A key with a NUL byte, a
leading `/`, or anything that climbs out (`..`) is invalid.

### Where the Rust server differs from the Go one

The HTTP layer is [leanhttp](https://github.com/xinix00/lean) instead of
`net/http`. None of these reach Hop's client or the Go client:

- a non-canonical path (`//a`, `/a/../b`) is 400, where Go redirected (301);
- a body over 1 MiB is 413, where Go answered 400 (the limit is the same);
- a chunked request body is 501 and any `Expect` header 417;
- the reason phrase of 412 and 504 is `Status` (leanhttp v3.1.1 does not
  name them); the status code is the same;
- there is no `Date` header.

## Persistence and consistency

**Host.** One file per key under `-data`, the same layout as the Go server,
so a Go data directory keeps working. Writes are crash-safe: a temporary
file next to the target, `fsync`, an atomic rename, and an `fsync` of the
directory. A killed server never leaves a half-written lease, and a PUT that
answered 200 survives a power cut.

One store thread owns the files; a fixed pool of 16 connection threads sends
it messages. Every compare-and-swap therefore runs in a single order and a
single instance is linearisable, with no lock anywhere. **Don't point two
hoplockserver instances at the same data directory**: they will race.

**HopOS.** hopfs has no rename and no fsync. A write truncates and rewrites
the file, and durability is hopfs's commit (every 10 s and when a slot
stops). Every record therefore carries a header with the body length and
its SHA-256 (`HLK1`, 40 bytes): a torn record is detected, logged, and
treated as absent, never served as a lease with the wrong body. For
hoplock, a lost lease is a new election; a wrong one would not be. The
record format is not the host's: a host data directory and a HopOS volume
are not interchangeable.

For HA, run a real S3-compatible store and use hoplock's S3 backend
instead. This server is the small, free option.

## Layout

| Path | What |
|------|------|
| `src/lib.rs` | The library, `no_std` + `alloc`, no I/O |
| `src/key.rs`, `src/etag.rs` | Keys (Go's `filepath.Clean`), ETags |
| `src/store.rs` | The `Store` trait (`get`, `put_if`, `delete`), the preconditions, the in-memory store |
| `src/server.rs` | The HTTP handling as a state machine: head, body, store op, response |
| `src/http.rs` | The seam to leanhttp, over any connection |
| `src/volume.rs` | The store on a file-call-only volume (hopfs), with the sealed records |
| `src/host.rs` | Feature `std`: the file store, the store thread, the connection pool, the flags |
| `src/bin/hoplockserver.rs` | The host binary |
| `src/bin/hoplockserver-hopos.rs` | Feature `hopos`: the HopOS resident (applib, leanhttp over `applib::tcp`) |
| `OLD/` | The Go server: the specification |

Dependencies come from git tags only: lean v3.1.3 (`leanhttp`), hop
v3.0.0 (`auth` for SHA-256, `hostnet` for the host sockets; `store`
and `discovery` for the client tests), HopOS v3.0.0 (`applib`,
`sync`).

## Tests

```sh
sh tools/gate.sh      # fmt, clippy -D warnings, tests, docs, the resident for aarch64
sh tools/e2e-host.sh  # two real Hop agentd's elect a leader through this server, fail over, survive a server restart
sh tools/qemu-test.sh # Hop on HopOS in QEMU places the resident; curl speaks the protocol from outside
```

`cargo test` runs every Go test by name: `store_test.go` against the
in-memory store, the volume store and the file store; `server_test.go`
against the state machine and over real sockets; `client_test.go` and
`object_test.go` with Hop's own Rust client (`store::HoplockLease`,
`store::HoplockStateStore`) against the running server.

`tools/e2e-host.sh` and `tools/qemu-test.sh` build Hop and HopOS from the
sibling checkouts (`HOP_DIR`, default `../hop/hop`; `HOPOS_DIR`, default
`../hop-os`) into `target/ext` of this repository, with `--locked`: nothing
in those repositories changes.

## Status

Part of the [Hop infrastructure suite](https://github.com/xinix00/hop).

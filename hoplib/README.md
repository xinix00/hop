# hoplib

The shared client for Hop plugins (hoplb, hopdns, hopprom): signed requests
to a Hop agent, typed replies, and the `/v1/events` stream.

Version 3 is a Rust crate. The Go library it replaces lives in `OLD/` as the
specification until the plugins have moved.

## Shape

The core is `no_std` with `alloc` and does no I/O. A call builds a
`Request` (method, URL, headers, body) and parses a `Response` (status,
body); the transport is chosen by a feature:

| Feature | Module | Transport |
| --- | --- | --- |
| (none) | `hoplib` | sans-I/O: `Client`, `Call<T>`, `Events`, `Backoff` |
| `std` | `hoplib::host` | blocking, over `hostnet::Http` (one executor per thread) |
| `hopos` | `hoplib::hopos` | async on the app executor, over `applib::tcp::TcpConn` and leanhttp |

Point the client at the LOCAL agent (`127.0.0.1:8080`, Hop's default agent port, or the slot address
on HopOS). The agent forwards `/v1/...` to whoever leads, so a plugin never
has to know the leader.

Signing is hop's own `auth` crate: `X-Hop-Auth` is
`hex(HMAC-SHA256(key, METHOD \n PATH \n hex(sha256(body))))`. The key never
travels. An empty or missing key sends no header (unauthenticated mode).

## Use

```toml
[dependencies]
hoplib = { git = "https://github.com/xinix00/hop.git", tag = "<a hop tag>", features = ["std"] }
```

On the host:

```rust
use core::ops::ControlFlow;
use hoplib::{Client, host::Agent};

let agent = Agent::new(Client::new("127.0.0.1:8080", Some(&key))?);
let status = agent.status()?;          // cluster_name, placed, deploying, ...
let jobs = agent.jobs()?;
let tasks = agent.tasks()?;            // per agent; falls back to the agents themselves
let one = agent.job_status("api")?;    // agents + tasks of one job

agent.events(
    |e| {
        if e.is_resync() {
            // ping (a fresh connection) or status: re-read everything
        } else if let Some(job) = e.job() {
            // a job or task event about `job`
        }
        ControlFlow::Continue(())
    },
    |err, retry_in| eprintln!("events: {err}, retrying in {retry_in:?}"),
);
```

On HopOS (feature `hopos`) the same calls are `async`:

```rust
let agent = hoplib::hopos::Agent::new(client, applib::EXEC.get());
let jobs = agent.jobs().await?;
agent.events(async |e| { /* may call agent.jobs().await */ ControlFlow::Continue(()) },
             |err, retry_in| log!("events: {err} HOPLIB_EVENTS_LOST")).await;
```

Without a transport, drive a `Call` yourself:

```rust
let call = client.status()?;
let resp: hoplib::Response = my_transport(call.request());
let status = call.parse(&resp)?;
```

## API

- `Client::new(base, api_key)`, `Client::sign(method, path, body)`.
- Calls, each a `Call<T>`: `status`, `agents`, `jobs`, `job(name)`,
  `job_status(name)`, `tasks`, `agent_tasks(agent)`, `capacity(agent)`,
  `agent_capacity(agent)`, `leader`, `apply(job)`, `delete(name)`; and
  `events()` for the stream request.
- Types: `Job`, `Task`, `TaskState`, `AgentInfo` (hop's `types`), plus
  `Status`, `Capacity`, `JobStatus`, `AgentTasks`, `Leader`, `Applied`.
- SSE: `Events::push(bytes)` / `Events::next()`, `Event { kind, data }`,
  `Event::topic()` (`Ping`, `Status`, `Job`, `Task`, `Agent`, `Other`),
  `Event::job()` (Go's `ParseJobFromSSE`), `Event::is_resync()`.
- Reconnecting: `Backoff` (1 s doubling to 30 s, reset after a `ping`). The
  host and hopos streams (`agent.stream().next()`) reconnect on the call
  after a break; giving up is the caller's choice.
- Errors: one `Copy` enum `Error` (`Status { code, reason }` carries the
  server's `{"error": ...}` text).

Hop up to v3.0.0-alpha.10 has no `/v1/jobs/{name}/status`; the agents fall
back to `/v1/tasks` filtered by job. A leader without `/v1/tasks` or the
capacity proxy is handled the same way the `hop` CLI does it: ask the agents
directly.

## Gate

```sh
tools/gate.sh   # host tests (all features), clippy -D warnings, fmt, no_std build for aarch64 with hopos
```

The tests run requests through hop's real `api::LeaderApi` (HMAC check
included) and read SSE bytes produced by `api::EventLog`. hoplib is a
member of the hop workspace and carries its version (3.0.11): hop's crates
are path dependencies, HopOS v3.0.13 and lean v3.1.12 come from git tags
through `[workspace.dependencies]` in the root `Cargo.toml`.

## License

MIT

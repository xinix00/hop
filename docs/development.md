# Development

## Build

```bash
# Build the binaries
go build -o bin/agent ./cmd/agent
go build -o bin/run ./cmd/cli

# Cross compile for Linux
GOOS=linux GOARCH=amd64 go build -o bin/agent-linux ./cmd/agent
```

## Testing locally

1. Start an agent (standalone = in-memory lock backend, no other services):
```bash
./bin/agent --cluster=dev --standalone
# or with a config file:
./bin/agent --config ./dev-config.json
```

Multi-node locally? Run hoplockserver and point every agent at it with `--lock`:
```bash
../bin/hoplockserver -listen :8090 -data ./data/lock   # from the monorepo root
./bin/agent --cluster=dev --lock http://127.0.0.1:8090 --config dev-node1.json
```

2. Test CLI:
```bash
./bin/run --leader localhost:9080 status
./bin/run --leader localhost:9080 apply --name test --command "sleep 60"
./bin/run --leader localhost:9080 delete test
./bin/run --leader localhost:9080 agents
./bin/run --leader localhost:9080 logs <task-id>
```

## Project structure

```
/cmd
    /agent/main.go         # Agent + leader binary (version injected at build time)
    /cli/main.go           # CLI tool
/internal
    /agent/
        agent.go           # Agent state loop, JobStore, state persistence
        handlers.go        # Agent HTTP handlers, proxy to leader, CORS
        monitor.go         # Task monitoring (health checks, restarts)
        sysinfo.go         # System info interface
        sysinfo_darwin.go  # macOS: CPU/memory detection
        sysinfo_linux.go   # Linux: CPU/memory detection
    /leader/
        leader.go          # State management, heartbeat, settle period, registration
        dispatch.go        # Job dispatch, round-robin, agent pinning, placement, delete
        health.go          # Reconciliation (reconcileJob/reconcileJobs), dead agent check, cluster status
        update.go          # Update policies: rolling, recreate, blue-green
    /leader/
        events.go          # EventBus for SSE notifications
    /runner/
        runner.go          # Runner interface + Config + ENV var helpers
        process.go         # Process runner (start, stop, status, limits, volumes)
        process_linux.go   # Linux: cgroups, chroot
        process_darwin.go  # macOS: ulimit, sandbox
        docker.go          # Docker runner (Docker API over the socket, no CLI/SDK)
        hopos.go           # HopRunner: jobs onto HopOS slots (driver=hop)
        hopos_stream.go    # The one-phase start: stream the image into the
                           # slot, queued → downloading → running
        download.go        # Artifact download router + extraction (tar.gz, tar.bz2, zip)
        download_http.go   # HTTP/HTTPS downloader
        download_s3.go     # S3 downloader
        logs.go            # Log broadcasting (SSE)
    /api/
        server.go          # Leader HTTP API
    /discovery/
        discovery.go       # hoplock backends (hoplockserver / S3 / mem) + leader discovery
    /agentloop/
        loop.go            # The shared election + heartbeat loop: become
                           # leader via the lock backend, or find the leader
                           # and register/heartbeat there. cmd/agent (Linux)
                           # and pkg/agentboot (HopOS) run the same loop
    /types/
        types.go           # Core data types (Job, Task, Agent, etc.)
/pkg
    /agentboot/            # Boots a whole single-node agent for HopOS —
                           # the public entry point hop-os links against
    /hopos/                # The HOP↔HopOS contract: the SlotManager
                           # interface HopOS implements, HopRunner consumes
    /config/
        config.go          # Loading the config file
    /httputil/             # hop's HTTP glue on top of leanhttp (the one
                           # transport on host AND node, see architecture.md)
        auth.go            # HMAC request auth (X-Hop-Auth): RequireHMAC + SignCall
        client.go          # The outbound client: plain + TLS keep-alive pools
        server.go          # The stop-lifecycle around leanhttp.Serve
        response.go        # JSON + SSE response helpers
/docs                      # Documentation
```

Also: `internal/leader/persist.go` (committed cluster state to S3).

The HopOS app images (`welcome`, `vitals`, `cloudflared`) used to live here under
`apps/`. They moved to [HopOS](https://github.com/xinix00/HopOS) under `apps/`,
next to the kernel they link against: an app image is a metal artifact — it links
`applib`, the app netstack and the slot ABI, and its link address comes from
HopOS' partition layout. This repo does not depend on metal at all, so it should
never have to be re-released because metal moved. `hopdns`, `hoplb` and `hopprom`
keep their own repos; only their `cmd/<name>-hopos` main is built over there.

## Ports

| Service | Port | Description |
|---------|------|-------------|
| Agent | 8080 | Agent HTTP API |
| Leader | 9080 | Leader HTTP API (port+1000) |
| hoplockserver | 8090 | CAS lease store (default lock backend) |

## Tests

```bash
# All tests
go test ./...

# With the race detector
go test -race ./...

# A single package
go test -v ./internal/leader
go test -v ./internal/agent
go test -v ./internal/api
go test -v ./internal/runner

# Chaos tests
go test -run=TestChaos -v ./internal/...

# Benchmarks
go test -bench=. -benchmem ./internal/...
```

## Dependencies

- `github.com/xinix00/hoplock` — lease-based leader election (CAS over blob store)
- `github.com/xinix00/hoplockserver` — client for the hoplockserver backend
- `github.com/xinix00/lean` — our own building blocks: `leanhttp` (THE HTTP
  layer, server and client, on host and node alike), `leanhttps`/`leantls`
  (the https dialer in `pkg/httputil`) and `leanrand` (node and task IDs,
  backoff jitter)

Everything else is Go stdlib (the CLI uses stdlib `flag`, the config is
`encoding/json`). That is on purpose and not a coincidence: HopOS' kernel
imports this module, so every dependency here also has to be defensible inside a
bare-metal image. `github.com/google/uuid` was dropped because it dragged
`database/sql/driver` in there for two ID calls, and `gopkg.in/yaml.v3` because
its package `init` linked the whole `regexp` engine into a kernel that never
parses a config file.


### Streamed artifact idle timeout

The HopOS download path writes the artifact directly into its app partition. Its 60-second idle timeout is refreshed by every successful read, independently of batched task-progress updates. A slow stream can take longer than 60 seconds overall; a stream that stops delivering bytes is closed and its unstarted placement is released. Header waiting has a separate 60-second limit.

`TestStreamIdleTracksBytesBelowProgressBatch` covers regular small reads that never reach a progress-report batch before the idle deadline, followed by genuine silence. This prevents progress-display throttling from becoming a transport deadline.

### Cage identity and physical cores

Every HopOS task owns one cage, including an SMP task. HOP reserves the first
free cage and stores that cage number in the task's `Pid`. Cage numbers are
independent of CPU numbers: six apps sharing one core can occupy cages 1–6.
HopOS selects physical cores using the requested core count, class and
sharegroup. Kernel FLIP adoption restores one cage reservation per task; stopping
a task releases its cage, while HopOS releases cores when no residents remain.

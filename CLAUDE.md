# Hop

Lightweight cluster orchestrator in Go. Simple alternative to Nomad.

## Documentation

See `/docs` for detailed documentation (start at `index.md`):

- `index.md` - Entry point: overview, quick start, map of all guides
- `architecture.md` - Architecture overview (state loop, registration, settle period)
- `data-structures.md` - Core data types and states
- `api.md` - HTTP API endpoints
- `cli.md` - CLI commands
- `configuration.md` - Config file options
- `development.md` - Build and development setup

**Important:** Keep the docs up-to-date when making changes to the architecture or API.

## Quick Start

```bash
# Build
go build -o bin/agent ./cmd/agent
go build -o bin/run ./cmd/cli

# Run standalone
./bin/agent --standalone --cluster=dev

# Deploy job
./bin/run apply --name test --command "echo hello"
```

## Design Principles

- Simplicity over features
- ExecRunner + DockerRunner (runner selected by `driver` field, auto-derived from `image`)
- State = `queued`, `downloading`, `running`, `stopping`, `failed`; a stopped task is removed
- Limits only when set (`CPUShares > 0`, `MemoryLimit > 0`)
- Single goroutine owns mutable state via ops channel (`do()` and `query()` helpers)
- Registration protocol separate from heartbeat (placed counts for accurate state)
- Settle period on new leader before reconciliation

## Key Architecture Details

- **Agent port**: 8080 (configurable), **Leader port**: agent port + 1000
- **Heartbeat**: 10s interval, pure liveness (id/endpoint/version only — no job exchange; desired state has a single author: the leader)
- **Registration**: POST /v1/agents with placed counts (jobName -> count)
- **Heartbeat**: POST /v1/heartbeat, returns 404 for unknown agents (triggers re-register)
- **Settle period**: 30s after becoming leader, no reconciliation during this time
- **Committed state**: leader snapshots desired state to S3 object `state/<cluster>` (next to the election lease, debounced ~1s); a new leader loads it at takeover — the snapshot is the ONLY truth (deletion is absence). See internal/leader/persist.go
- **Init jobs**: `cluster.init_jobs` in config seeds a baseline on a clean boot (no snapshot AND empty job store) — one-shot via the normal dispatch path, never continuous enforcement; store errors never trigger a seed. See internal/leader/init.go
- **State persistence**: one path — the leader's `StatePersister`. Backend follows the lock: S3 or hoplockserver (remote) in a cluster, a local crash-safe file (`paths.state_file`, tmp+fsync+rename) in standalone/mem. Agents keep no statefile; a rebooted agent is re-dispatched by the leader (nodes are stateless in cluster mode).
- **Node ID**: persisted in data/node-id, survives restarts
- **MaxRestarts**: *int — nil/omitted = unlimited with exponential backoff (1s…30s, counter resets after `restart_window` of uptime), 0 = no restarts, N = give up after N crashes within `restart_window`, -1 = unlimited (explicit). Task shows `next_restart_at` while the backoff runs.
- **Version**: injected at build time via `-ldflags "-X main.version=..."` (default: "dev")

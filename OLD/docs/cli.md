# CLI

The CLI binary is `run`. It talks to the leader API and signs every request
with HMAC (`X-Hop-Auth`) — see [api.md](api.md#authentication-x-hop-auth).

## Configuration

```bash
export HOP_LEADER=localhost:9080     # or --leader localhost:9080
export HOP_API_KEY=your-secret-key   # or --api-key your-secret-key
```

| Flag | Env var | Description |
|------|---------|-------------|
| `--leader` | `HOP_LEADER` | Leader address (default `localhost:9080`) |
| `--api-key` | `HOP_API_KEY` | Shared secret for HMAC request signing (empty = auth disabled) |

## Commands

```
run [flags] <command> [args]

  apply    Create or update a job (upsert by name)
  delete   Delete a job and all its tasks
  status   Show cluster status
  agents   List agents or show agent details
  logs     Stream task logs
```

## Cluster Status

```bash
./bin/run status
```

Output:
```
Leader:  localhost:9080
Agents:  3
Tasks:   5 running / 5 total

NAME    RUNNING   STATUS
web     3 / 3     OK
api     2 / 3     DEGRADED

AGENT     TASK      JOB    PORTS              STATE
agent-1   abc123    web    http:54321         running
agent-1   def456    web    http:54322         downloading
agent-2   ghi789    api    http:8080,grpc:9090 running
```

Shows jobs with expected vs running counts. Daemon jobs (count=-1) show `all(N)` where N = number of agents.

A task starts out `queued` and only reports `running` once it really runs; on
HopOS nodes `downloading` sits in between while the image streams into the slot
([task states](data-structures.md#task-states)). All of them occupy capacity.

## Jobs

### Create or update a job (upsert)

**Same command for create and update** — detected automatically based on job name:

```bash
# Apply a process job
./bin/run apply \
    --name api \
    --command "./api-binary" \
    --cpu 2000 \
    --memory 512M \
    --env "LOG_LEVEL=info"

# Output (INSERT):
# Job 'api' dispatched

# Update to a new version (rolling by default)
./bin/run apply \
    --name api \
    --command "./api-binary-v2"

# Output (UPDATE):
# Job 'api' updated (policy=rolling)

# Docker container (only on nodes with Docker)
./bin/run apply --name redis --image redis:7 --affinity node.docker=true
./bin/run apply --name my-app --image myapp:v2 --command "python serve.py"

# With affinity (only on arm64 nodes)
./bin/run apply --name api --command "./api" --affinity node.arch=arm64

# Pin to a specific node
./bin/run apply --name monitor --command "./monitor" --affinity node.id=node-1

# Platform-specific artifacts (agent picks first matching)
./bin/run apply --name tailscale --command "./tailscale" \
  --artifact "node.arch=amd64::https://pkgs.tailscale.com/stable/tailscale_amd64.tar.gz" \
  --artifact "node.arch=arm64::https://pkgs.tailscale.com/stable/tailscale_arm64.tar.gz"

# Simple artifact (no match = catch-all)
./bin/run apply --name app --command "./app" --artifact "https://example.com/app.tar.gz"

# Service discovery tags (picked up by hoplb)
./bin/run apply --name web --command "./web" --tag "hoplb-urlprefix=*.example.com"
```

### Apply Flags

| Flag | Description |
|------|-------------|
| `--name` | Job name (required, unique key for upsert) |
| `--command` | Command to execute (required for process jobs, optional for Docker) |
| `--image` | Docker image (uses Docker instead of process) |
| `--driver` | Runner: `exec` (default), `docker`, or `hop` (HopOS slot; needs `--artifact`, no command/image) |
| `--artifact` | Artifact URL (repeatable, with optional match: `key=val::URL`) |
| `--cpu` | CPU shares |
| `--memory` | Memory limit (e.g., 512M, 1G) |
| `--env` | Environment variables (KEY=VALUE, repeatable) |
| `--tag` | Service discovery tags (KEY=VALUE, repeatable) |
| `--affinity` | Node affinity constraints (key=value, repeatable, e.g. `node.arch=arm64`) |
| `--priority` | Scheduling priority (0 = highest; omit to append at the end) |
| `--update-policy` | Update policy: rolling (default), recreate, or blue-green |
| `--check-type` | Health check type: `http`, `tcp`, or `file` |
| `--check-path` | Health check path (HTTP endpoint or file path) |
| `--check-port` | Health check port name (for http/tcp, default: http) |
| `--check-failures` | Consecutive failures before unhealthy (default: 3) |

Either `--command` or `--image` (or both) is required — except for `--driver hop`,
which takes only artifacts. `count` is API-only (defaults to 1; POST to `/v1/jobs`
to set it).

Setting `--check-type` or `--check-path` enables health checks.

### Update Policies

Control how updates are rolled out:

```bash
# Rolling update (default) - zero downtime
./bin/run apply --name api --command "./v2" --update-policy rolling

# Recreate - downtime but fast
./bin/run apply --name api --command "./v2" --update-policy recreate

# Blue-green - zero downtime, 2x resources during switch
./bin/run apply --name api --command "./v2" --update-policy blue-green
```

| Policy | Downtime | Resources | Behavior |
|--------|----------|-----------|----------|
| `rolling` (default) | None | Normal | Start new → stop old, 1 at a time, 2s delay |
| `recreate` | Yes | Minimal | Stop all → start new version |
| `blue-green` | None | 2x during switch | Start all new → stop all old |

### Delete a job

```bash
./bin/run delete <job-name>
```

Deletes the job and stops all of its tasks.

## Agents

```bash
# List all agents
./bin/run agents

# Show agent details (capacity, resource usage)
./bin/run agents <agent-id>
```

List output:
```
ID        ENDPOINT              TEMP     LAST SEEN
agent-1   http://10.0.0.1:8080  45.0°C   15:04:05
agent-2   http://10.0.0.2:8080  52.5°C   15:04:03
agent-3   http://10.0.0.3:8080  -        15:04:07
```

`TEMP` is the node's CPU temperature from its heartbeat — a dash when the node
has no sensor, never a fake zero. A node with several sensors reports the
hottest one.

Detail output:
```
Agent:    agent-1
Endpoint: http://10.0.0.1:8080
CPU temp: 45.0°C
LastSeen: 15:04:05

Tasks:    3 running
CPU:      2.0 / 14 cores (2048 / 14336 shares)
Memory:   0.5 / 48.0 GB

Attributes:
  node.arch = arm64
  node.docker = true
  node.id = agent-1
  node.os = linux
```

## Logs

Stream task logs in real-time:

```bash
# Stream stdout (default)
./bin/run logs <task-id>

# Stream stderr
./bin/run logs <task-id> --stream stderr
```

Finds the agent running the task automatically via cluster status.

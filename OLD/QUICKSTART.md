# Quick Start

Get hop running in 30 seconds.

## Build

```bash
cd hop
go build -o ../bin/agent ./cmd/agent
go build -o ../bin/run ./cmd/cli
```

## Standalone Mode (single-node)

```bash
# Single-node dev/testing - agent becomes leader automatically
../bin/agent --cluster=dev --standalone

# Output:
# Starting hop agent dev
# Node a1b2c3d4 on 192.168.1.5:8080
# Cluster: dev
# Running in standalone mode (in-memory lock backend)
# Became leader!
```

## Deploy a Job

```bash
export HOP_LEADER=localhost:9080

# Deploy
../bin/run deploy --name nginx --command "nginx -g 'daemon off;'"

# Check status
../bin/run status

# View agents
../bin/run agents
```

## Multi-Node Cluster

Leader election runs over a shared lock backend (hoplockserver, or any
S3-compatible object store). Point every agent at the same backend.

```bash
# 1. Start hoplockserver (lease store for leader election)
#    See hoplockserver/README.md for setup — or use S3/R2 instead.
../bin/hoplockserver -listen :8090 -data ./data/lock

# 2. Start agents on each node, all pointing at the same lock backend
../bin/agent --cluster=my-cluster --lock http://lock-server:8090

# 3. Deploy a job
../bin/run deploy --name api --command "./server"
```

## With Config File

Only needed for custom requirements:

```json
{
  "cluster": {
    "name": "my-cluster",
    "lock": {"type": "hoplockserver", "url": "http://10.0.0.1:8090"}
  },
  "node": {"port": 8080},
  "paths": {"state_file": "/var/lib/hop/state.json"},
  "runner": {"isolate": true}
}
```

`lock.type` is `""` (default) | `"hoplockserver"` | `"s3"` | `"mem"`;
`runner.isolate` is chroot on Linux, sandbox on macOS. Unknown keys and
unquoted durations are startup errors — see
[docs/configuration.md](docs/configuration.md).

```bash
../bin/agent --config=config.json
```

## Production (systemd)

```bash
sudo tee /etc/systemd/system/hop.service <<EOF
[Unit]
Description=Hop Agent
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/agent --cluster=my-cluster --lock http://lock-server:8090
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable --now hop
```

## What Gets Auto-Detected

| Setting | Auto-Detected | Override |
|---------|---------------|----------|
| Node IP | Outbound interface | `node.ip` in config |
| Node Port | 8080 | `node.port` in config |
| Node ID | UUID (persisted in data/node-id) | `node.id` in config |
| Capacity | System CPU/RAM | `capacity.*` in config |
| Paths | ./data/* | `paths.*` in config |
| Leader Port | node.port + 1000 | N/A |
| Timeouts | Smart defaults | `timeouts.*` in config |
| State File | ./data/state-{cluster}.json | `paths.state_file` in config |

## Troubleshooting

```bash
# Check agent health
curl http://localhost:8080/health

# Check leader status
curl http://localhost:9080/v1/status

# View agent capacity
curl http://localhost:8080/capacity

# Who is leader?
curl http://localhost:8080/leader
```

## Next Steps

- Read [docs/architecture.md](docs/architecture.md) for system design
- See [docs/api.md](docs/api.md) for HTTP API reference
- See [BENCHMARKS.md](BENCHMARKS.md) for performance characteristics
- See [CHAOS.md](CHAOS.md) for failure handling

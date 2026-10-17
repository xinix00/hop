# hopdns

DNS service discovery for hop clusters with KISS federation.

## Build

```bash
go build -o bin/hopdns ./cmd/hopdns
```

## Run

```bash
# Single cluster
./bin/hopdns -listen :5353 -peer http://127.0.0.1:8080

# Federation: local + remote clusters
./bin/hopdns -listen :5353 \
  -peer http://127.0.0.1:8080 \
  -peer http://10.0.1.100:8080 \
  -peer http://10.0.2.100:8080
```

## Architecture

- `internal/dns/cache.go` - Thread-safe cache: `clusters map[cluster]map[jobName][]IP`
- `internal/dns/watcher.go` - Per-peer SSE watcher with cluster name auto-discovery
- `internal/dns/server.go` - DNS server using miekg/dns
- `cmd/hopdns/main.go` - CLI entry point

## Design

KISS approach — every cluster is a peer, including your own:
- `-peer` flag per cluster endpoint (repeatable)
- Each peer gets its own SSE watcher (connects to `/v1/events`)
- Cluster name auto-discovered from peer's `GET /v1/status` → `cluster_name`
- Peer down: last-known-good cache kept (stale), other peers unaffected — a failover doesn't black out DNS (same as hoplb)
- Peer reconnect: re-seeds the cache, replacing the stale entries

### DNS Resolution

| Query | Resolves to |
|-------|-------------|
| `myapp.hop.local` | IPs from **all** peers merged |
| `myapp.prod-eu.hop.local` | IPs from cluster "prod-eu" only |

### Requirements

- VPN between clusters (user's responsibility)
- hop with `cluster_name` in `/v1/status` (for cluster discovery)
- API keys must match if peers use authentication

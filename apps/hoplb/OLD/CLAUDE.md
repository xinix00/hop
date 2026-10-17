# hoplb

Simple hostname-based load balancer for hop.

## Structure

```
cmd/hoplb/main.go     - CLI entrypoint
internal/lb/
  route.go             - Route table and wildcard matching
  watcher.go           - Connects to hop via SSE, syncs routes on events
  proxy.go             - Reverse proxy with round-robin
```

## Design

- Routes based on `hoplb-urlprefix` tags from hop jobs
- Real-time route updates via SSE (`/v1/events`) from local hop agent
- Only routes to tasks with `state == "running"`
- Health checks are handled by hop, not hoplb

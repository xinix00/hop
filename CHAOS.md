# Chaos Testing

Catastrophic failure scenarios to ensure system resilience.

## Test Scenarios

### Leader Chaos Tests (`internal/leader/chaos_test.go`)

| Test | Scenario | Expected Behavior |
|------|----------|-------------------|
| `TestChaos_CascadingFailure` | 3/5 agents crash simultaneously | Leader detects, redispatches to survivors |
| `TestChaos_LeaderCrashDuringRollingUpdate` | Leader dies mid-update (partial rollout) | New leader inherits mixed state (v1+v2 coexist) |
| `TestChaos_NetworkPartition` | Agent timeout/unreachable | Leader skips slow agent, tries next |
| `TestChaos_AllAgentsDownExceptOne` | 4/5 agents crash | Single survivor holds state, no loss |
| `TestChaos_RapidAgentChurn` | Agents join/leave rapidly | System remains stable, dead agents cleaned up |
| `TestChaos_JobDispatchToDeadAgent` | Dispatch to crashed agent | Graceful error, retries next agent |
| `TestChaos_SplitBrainScenario` | Two leaders during partition | States merge when partition heals |
| `TestChaos_AgentReturnsAfterLongDowntime` | Agent rejoins with stale state | Leader updates agent with current state |
| `TestChaos_MultipleJobUpdatesDuringFailover` | Concurrent updates + failover | Mixed versions preserved, no data loss |
| `TestChaos_LeaderMemoryPressure` | 10k agents, 100k jobs | System survives (slow but functional) |
| `TestChaos_SimultaneousLeaderAndAgentCrash` | Double failure | Survivors recover gracefully |
| `TestChaos_HeartbeatStorm` | 100 agents x 100 HB each | System handles 10k+ heartbeats/sec |
| `TestChaos_ZeroAgentsAvailable` | No agents to dispatch to | Graceful error, no crash |
| `TestChaos_AgentFlapping` | Agent up/down repeatedly | Detected and handled over 5 cycles |

### Agent Chaos Tests (`internal/agent/chaos_test.go`)

| Test | Scenario | Expected Behavior |
|------|----------|-------------------|
| `TestChaos_AllTasksCrashSimultaneously` | 10 tasks fail at once | Agent attempts restart for all |
| `TestChaos_TaskExceedsMaxRestarts` | Task crash loops | Agent gives up after max_restarts |
| `TestChaos_CapacityExhaustion` | No CPU/memory left | Agent rejects new jobs gracefully |
| `TestChaos_TaskZombie` | Process dead but state says running | Monitor detects and restarts |
| `TestChaos_StateCorruption` | Task references deleted job | Handled without crash |
| `TestChaos_RapidJobDeletionAndCreation` | 100 create/delete cycles | System stable, no leaks |

## Running Chaos Tests

```bash
# All chaos tests
cd hop
go test -run=TestChaos -v ./internal/...

# Specific scenario
go test -run=TestChaos_CascadingFailure -v ./internal/leader

# Leader chaos only
go test -run=TestChaos -v ./internal/leader

# Agent chaos only
go test -run=TestChaos -v ./internal/agent

# With race detector (slower but catches concurrency issues)
go test -run=TestChaos -v -race ./internal/...
```

## Results Summary

### Leader Resilience

**Cascading failures:** PASS
- 3/5 agents down -> survivors take over
- Jobs redispatched automatically via reconciliation

**Leader crash during update:** PASS (with mixed state)
- Partial updates preserved
- Old and new versions coexist until new leader reconciles
- Settle period prevents premature reconciliation

**Network partitions:** PASS
- Slow/unreachable agents skipped
- Retries on healthy agents
- Agents stop tasks after 6 failed heartbeats (prevent duplicates)

**Split-brain recovery:** PASS
- Two leaders merge state when partition heals
- No data loss

**Heartbeat storm:** PASS
- 10k+ heartbeats/sec handled
- Channel-based state loop prevents lock contention

**Agent returns after downtime:** PASS
- Agent re-registers with placed counts
- Leader updates placement tracking accordingly

### Agent Resilience

**Mass task failure:** PASS
- 10 tasks crash -> all restarted
- Up to max_restarts limit (omitted = unlimited with backoff, 0 = no restarts, N = give up after N)

**Crash loops:** PASS
- Agent gives up after max attempts
- No infinite restart loops

**Capacity exhaustion:** PASS
- Jobs rejected with clear error (503)
- No over-commitment

**Rapid churn:** PASS
- 100 create/delete cycles: ~0.01 seconds
- No memory leaks

**State corruption:** PASS
- Orphan tasks handled gracefully
- No crashes

## Real-World Failure Scenarios

### Scenario 1: Data center power outage

```
3-node cluster, power fails in DC1 (2 nodes)

Before:
  Node 1 (DC1): 10 tasks
  Node 2 (DC1): 10 tasks
  Node 3 (DC2): 10 tasks

After:
  Node 3 (DC2): 30 tasks (took over all work)

Result: Zero downtime (if Node 3 has capacity)
```

**Test:** `TestChaos_AllAgentsDownExceptOne`

### Scenario 2: Leader election during deployment

```
Rolling update in progress: 5/10 instances updated
Leader crashes
New leader elected

New leader behavior:
1. Enters settle period (30s)
2. Surviving agents register with placed counts
3. After settle, reconciles with accurate placement data

Result: Partial update preserved, can continue or rollback
```

**Test:** `TestChaos_LeaderCrashDuringRollingUpdate`

### Scenario 3: Network partition

```
3-node cluster splits into [A, B] and [C]

Partition A+B (has leader):
  - Continues dispatching
  - Marks C as dead after 30s
  - Reconciles C's jobs to A/B

Partition C (isolated):
  - Heartbeats fail, failCount increases
  - After 6 failures: stops all tasks (prevent duplicates)
  - Keeps trying to reach leader or become one

When healed:
  - C registers with new leader (placed counts)
  - Leader updates placement tracking
  - C receives jobs during reconciliation

Result: No split-brain, safe isolation
```

**Test:** `TestChaos_SplitBrainScenario`, `TestChaos_NetworkPartition`

### Scenario 4: OOM killer strikes

```
Agent runs out of memory
Linux OOM killer terminates tasks

Agent behavior:
  - Monitor detects crashed tasks (5s check interval)
  - Restarts up to max_restarts (omitted = unlimited with backoff, 0 = no restarts, N = give up after N)
  - Health check initial_timeout gives grace period after restart

Result: Prevents infinite crash loops (when max_restarts > 0)
```

**Test:** `TestChaos_TaskExceedsMaxRestarts`

### Scenario 5: Disk full

```
Agent /var partition full
Cannot write state.json

Behavior:
  - Log error
  - Continue in-memory (debounced save retries)
  - Jobs survive (in leader's memory)
  - Recovers when disk space freed

Result: Degrades gracefully
```

*Not yet tested - TODO*

## Chaos Engineering in Production

### Gradual Rollout

```bash
# Test failover on staging
1. Run chaos tests: go test -run=TestChaos -v ./internal/...
2. Kill leader manually, observe recovery + settle period
3. Simulate network partition with iptables
4. Monitor metrics during chaos
```

### Metrics to Monitor

```prometheus
# Task recovery rate
rate(hop_task_failures_total[5m]) / rate(hop_task_starts_total[5m])

# Agent health
hop_agents_healthy / hop_agents_total

# Job degradation
hop_job_instances_running < hop_job_instances_expected
```

### What We Test vs Don't Test

**Tested:**
- Leader crashes (with settle period recovery)
- Agent crashes (single and multiple)
- Network timeouts
- Split-brain scenarios
- State corruption
- Resource exhaustion
- Rapid churn
- Agent flapping

**Not tested (yet):**
- Disk full scenarios
- Byzantine failures (malicious agents)
- Clock skew between nodes
- DNS failures
- TLS certificate expiry
- hoplock lease-election edge cases

## Known Limitations

**Single leader = SPOF:**
- If leader dies, no dispatching until failover
- Existing tasks keep running (agents are autonomous)
- Failover ~60s worst case: 3×10s failure detection + 30s settle (see the
  leader failover timeline in the monorepo CLAUDE.md)
- New leader needs settle period (30s) before reconciling

**No quorum:**
- Unlike k8s (etcd quorum), hop survives with 1 node
- Leader election is lease-based (hoplock CAS over a blob store) — no quorum,
  no log replication; the lock backend (hoplockserver/S3) is the source of truth

**State eventually consistent:**
- 10 second heartbeat interval = 10s propagation delay
- Acceptable for most workloads
- Not suitable for sub-second coordination

**No distributed consensus on job state:**
- Leader has authority
- Agents trust leader
- If leader state corrupted, manual intervention needed

## Mitigation Strategies

### High Availability

```
3 nodes minimum for HA:
- Agents race for the hoplock lease; the CAS winner is leader
- If leader dies, a survivor wins the expired lease (~60s worst case)
- Settle period (30s) for agents to register
- Reconciliation restores desired state
```

### Disaster Recovery

```bash
# Backup leader state
cp /var/lib/hop/state-{cluster}.json /backup/state-$(date +%s).json

# Restore after catastrophic failure
1. Stop all agents
2. Restore state.json on new leader
3. Start leader
4. Start agents (they register with placed counts)
```

# Lifecycles

Step-by-step behaviour of a Hop cluster, with the invariants that hold at
each step and the constants that set the pace. Component background is in
[architecture.md](architecture.md); request/response shapes are in
[api.md](api.md); settings are in [configuration.md](configuration.md). Back to
the [index](index.md).

Evidence: behaviour below is pinned by the unit tests in
`leader/src/tests/*.rs` (ported from the Go generation's `internal/leader`
tests, same names in snake_case) and `agent/src/tests.rs` (`internal/agent`
and `internal/agentloop`). Test names are cited in parentheses.

## Clocks and cadences

| What | Value | Source |
| --- | --- | --- |
| Owner tick (agent, runner, lease cadence, snapshot poll) | 1 s | `agentd/src/main.rs` `TICK`, `agentd-hopos/src/main.rs` `TICK` |
| Election tick (register / heartbeat / renew decision) | 10 s | `ELECTION_TICK` in `agentd/src/node.rs`, `agentd-hopos/src/node/cluster.rs` |
| Leader tick (settle end, dead agents, safety net) | 10 s | `LEADER_TICK` |
| Safety-net reconcile | every 3rd leader tick (~30 s) | `leader::RECONCILE_EVERY_TICKS` |
| Agent timeout (dead after silence) | 30 s | `leader::DEFAULT_AGENT_TIMEOUT`, `timeouts.node_dead_threshold` |
| Settle period of a clustered new leader | one agent timeout (30 s) | `Leader::enable_settle` |
| Lease TTL | 30 s default; HopOS minimum 15 s | `timeouts.leader_lease`, `HOPOS_LEASE_TTL` |
| Lease renew | every TTL/3 (10 s at default) | `agent::Elector::tick` |
| Lease-store call budget | max(TTL/3, 5 s), total per call | `discovery::backend_timeout_for` |
| Takeover attempt / isolation | after 4 / 7 failed election ticks | `agent/src/election.rs` |
| Snapshot debounce | 1 s | `leader::PERSIST_DEBOUNCE` |
| Monitor (poll running tasks, probes) | 5 s | `timeouts.health_check_interval` |
| Probe timeout | 5 s (per-check `timeout` overrides) | `timeouts.health_check_timeout` |
| Rolling-update step pause | 2 s (host only) | `leader::ROLLING_UPDATE_DELAY` |
| Leader -> agent calls (host) | 5 s `/run`, `/tasks`; 60 s stop/delete | `agentd/src/net.rs` `QUICK`, `SLOW` |
| Agent -> leader calls (register, heartbeat, notify) | 5 s | `LINK_TIMEOUT` |

All times are wall-clock Unix nanoseconds (`types::Time`) passed in as "now";
lease times are compared across nodes, so they use the wall clock, never a
monotonic one.

## 1. Node start and election

### Host (`agentd`)

1. Parse flags and the JSON config; decode `cluster.init_jobs` strictly (a
   typo stops the daemon here). Resolve the node id (config, then
   `data/node-id`, then a new 8-char id saved there) and the IP (`node.ip`,
   else an interface in `node.network`, else the outbound IP).
2. `HostRunner::init`: remove task directories and `hop-` containers left by
   a previous run. There is no task handover across a daemon restart.
3. Build `Agent` from `Settings` (measured cores/memory, `node.id`,
   `node.arch`, `node.os`, `node.docker` attributes, config attributes on top).
4. **Boot claim**: one synchronous `Discovery` claim against the lock
   (`elector::claim_now`). A free lock makes this node leader immediately,
   without waiting four ticks. Standalone (`--standalone` or no lock) uses an
   in-memory backend, so the claim always wins.
5. Spawn the lease thread, link thread, probe thread, persist thread, 4 prep
   workers, and bind both ports (8 connection threads each). Then
   `Node::boot` turns a won boot claim into `StartLeader` (see 2), and
   `HOP_UP` is logged: the ports are open.
6. Owner loop: process messages in order, tick every second.

### HopOS (`agentd-hopos`)

1. Network up, `BootConfig` from `HOPOS_*`. Missing API key (without
   `HOPOS_INSECURE=1`) or a bad lock config: the resident stays alive but opens
   no API (`HOPOS_API_NO_AUTH`, `HOP_LOCK_BAD`).
2. Spawn the clock task (SNTP at start, hourly; retry after 5 min), the store
   task and the download task.
3. Without a lock: `Node::new` is a standalone leader that registers its own
   agent at once (no settle, no snapshot). With a lock: `Node::new_clustered`
   with the leader half closed and an `Elector` that is not holding.
4. `sweep_strays`: stop kernel residents this fresh Hop does not know
   (`HOP_STRAY_STOPPED`). Hop never restores task state from hopfs.
5. Clustered: `cluster_boot` joins only once SNTP succeeded (until then
   `HOP_CLUSTER_NO_CLOCK`, no claim, no election). On join (`HOP_CLUSTER_JOIN`)
   it sends the boot claim; `Claimed(true)` makes it leader at once.
   Standalone: seed init jobs (`HOPOS_INIT_JOBS` or `/hop/init-jobs.json`).
6. Bind both ports (4 worker tasks each), log `HOP_UP`, run the owner loop.

### Election loop (both binaries, `agent::Election::tick` every 10 s)

- Leading: renew (answered from the `Elector`'s lease timer). Displaced ->
  step down without release. Not renewed and zero connected agents -> step
  down. Otherwise keep leading (a working LAN survives a lock-store outage)
  and heartbeat the own agent in-process (`SelfHeartbeat`; a 404 triggers
  `SelfRegister`).
- Following with a known leader: `Register` (`POST /v1/agents`) until
  accepted, then `Heartbeat` (`POST /v1/heartbeat`). The leader address is
  cached after a successful call; the store is only read when it is unknown.
- No leader known: see [leader failover](#10-leader-failover).

Invariants: at most one outstanding lease op of each kind (`Elector`); step
down always stops the leader half before releasing the lease
(`step_down_is_idempotent`).

## 2. Leader settle and state load

Triggered by `Request::StartLeader`.

1. Create `Leader::new(own_id, MemStore)`, agent timeout =
   `node_dead_threshold`. If clustered: `enable_settle(now)` (30 s).
2. Load the committed snapshot (`state/<cluster>` or the state file):
   - host: synchronously on the owner thread, waiting up to 2x the
     backend timeout;
   - HopOS: `HOP_LEADER_LOADING`, the state task loads it; the leader API
     answers 503 until `Mail::Loaded` arrives. If the node stepped down
     meanwhile, the result is dropped.
3. `load_committed_state`: the snapshot is the only truth; jobs absent from it
   are removed (`committed_state_boot_load`). `HOP_STATE_LOADED` with the job
   count. Unreadable or unreachable state logs `HOP_STATE_FAIL` and is **not**
   a clean boot.
4. Register the own agent with its current `placed` counts; log `HOP_LEADER`.
5. Clean boot (no snapshot object) and an empty store: seed init jobs through
   the normal dispatch path, unset priorities appended (`seed_init_jobs`,
   `HOP_INIT_SEEDED`). Existing names are never overwritten.
6. During settle: agents register with their `placed` counts, but
   `dispatch_job` only stores, `register_agent` does not reconcile, and
   `check_dead_agents` is skipped (`dispatch_during_settle_period`,
   `get_placed_counts_leader_with_settle_period`).
7. The first leader tick at or after the deadline sets `settled`, emits a
   `status` event and reconciles. With settle, a new leader does not duplicate
   tasks that are already running (`leader_restart_reject_unknown_heartbeat`:
   20 tasks on 4 agents stay 20; `failover_new_leader_does_not_duplicate_own_tasks`).

## 3. Agent registration and heartbeats

1. `POST /v1/agents` with `{"id", "endpoint", "version", "placed": {job: n}}`.
   `placed` counts tasks in **every** state (a failed task holds its
   reservation and must not be re-dispatched elsewhere).
2. `Leader::register_agent`:
   - same id still alive (within the agent timeout) on a different endpoint
     -> refused (HTTP 409) (`register_agent_duplicate_id_rejected`; allowed
     after the timeout, `..._allowed_after_timeout`);
   - any previous incarnation is dropped, the agent inserted in id order (the
     round-robin order), its `placed` map adopted (max `MAX_AGENTS` 256);
   - if settled: stop the surplus a returning agent brought (jobs whose other
     placements already meet `count`; daemons excluded), then reconcile
     (`trim_returning_agent_surplus`, `zombie_agent_no_over_scheduling`);
   - emits `agent:<id>`.
3. `POST /v1/heartbeat` every 10 s: pure liveness plus version and telemetry;
   it never exchanges jobs. Unknown id -> 404 -> the agent clears
   `registered` and registers again on the next tick
   (`tick_heartbeat_not_registered_reregisters`). Placement corrections come
   only from re-registration (`placed_count_fixed_by_re_registration`).
4. The leader's own agent heartbeats in-process (host: on the election tick;
   HopOS: on every 1 s tick while leading).
5. Death: on a leader tick, an agent silent for more than the agent timeout is
   removed with its `placed` entry, an `agent:<id>` event is emitted and jobs
   are reconciled onto the rest (`leader_redispatch_jobs_from_dead_agent`).
   Detection lands 30-40 s after the last heartbeat. A blip shorter than the
   timeout moves nothing (`network_blip_no_redistribution`,
   `network_blip_agent_unreachable`).

## 4. Job apply -> placement -> task run

### Leader side

1. `POST /v1/jobs`: parse (lenient), require `name`; one artifact and no
   `command`/`image` implies `driver: hop`; require something runnable (400
   otherwise). A fixed port with an explicit `rolling`/`blue-green` policy is
   refused with 400 (`HOP_JOB_REFUSED`).
2. Existing name -> update (section 7). New name -> priority defaults to
   `next_priority()` (= job count, i.e. last), then `dispatch_job`:
   - the job is always stored first (marks state dirty, so it reaches the
     snapshot); during settle that is all;
   - `count == -1` (daemon): run on every agent missing it, no round-robin
     (`count_minus_one_does_not_use_round_robin`);
   - otherwise `desired()` instances (`count <= 0` means 1), each through
     `place`.
   - Reply 201 `dispatched`, or 201 `pending` with the error if placement
     failed (the next reconcile retries).
3. `place(job, allow_preempt)`:
   - pass 1: try each agent once in round-robin order; `Accepted` books one
     instance in `placed`; 503 is "full"; 406/other/unreachable skip
     (`dispatch406_skips_to_next_agent`);
   - pass 2 (only with preemption, never during updates): on each full agent
     pick the job with the numerically highest effective priority that is
     strictly less important, `POST /stop/{victim}`, and retry there. A failed
     stop leaves the victim booked (`failover_preemption_stop_failure_no_ghosts`).
     Priority 0 is never evicted by an equal (`preemption_prio0_is_unevictable`).
4. `reconcile_jobs` (on agent register/death/unregister, job add/delete/
   priority change, settle end, and every 3rd tick): normalize priorities to
   0..N-1, reset round-robin, then for each job in priority order dispatch the
   missing `desired - placed` (`reschedule_underscheduled`). A job that does
   not fit now is not an error; only out-of-memory aborts a round.

### Agent side

1. `POST /run` (`?replace=1` for in-place update) -> `Agent::run`:
   affinity (406) before capacity; driver defaults from `image` (docker if set,
   else exec); `hop` jobs rounded to whole cores and 2 MiB blocks; limits
   `MAX_TASKS`/`MAX_JOBS` 1024; admission on CPU shares, memory and (HopOS)
   the largest free partition -> 503 `insufficient capacity` and one
   `HOP_NO_CAPACITY` line per job until it fits again.
2. Accepted: a task record is created in `Queued` and holds its reservation
   immediately (`task_in_state_immediately_after_accept`); `Action::Start`
   with the job reduced to the one artifact matching this node's attributes.
3. Runner:
   - host: dynamic ports (0) get a free kernel port; the prep worker does task
     dir, download/extract or `docker pull`; the owner then `launch`es
     (`HOP_JOB_PLACED pid=...` or `HOP_JOB_FAILED`). A task stopped during
     preparation is discarded, not started;
   - HopOS: `HopRunner::start` asks the kernel for a slot; an artifact is
     downloaded by the download task (one order at a time) and streamed in;
     state goes `Queued` -> `Downloading` (progress) -> `Running`
     (`HOP_JOB_PLACED slot=N`).
4. `on_started`:
   - ok -> `Running`, `started_at` reset to now, ports recorded; notify
     `started` (no health check) or `start` (has one);
   - `StartError::NoCapacity` (runner cannot place it) -> record removed,
     notify `unplaceable`; the leader `mark_unplaced`s and reconciles elsewhere
     (`unplaceable_job_is_not_restarted`, `mark_unplaced_boekt_af_en_reconcilet`);
   - `StartError::Failed` -> `Failed`, restart path (section 5).
   - A start that completes after a stop/delete removed the record is a ghost
     and is stopped at once (`stop_during_start_does_not_resurrect_task`).
5. Every task event goes to the leader as `POST /v1/notify`
   (`{"job","event","agent"}`) and appears on `/v1/events` as
   `job:<name>:<event>`.

On HopOS clusters, `/run` to an agent on another node is answered
optimistically from the relay and sent by the dispatch task; a refusal
arriving later books the instance off (`HOP_RELAY_REFUSED`) and is remembered
for 30 s so the next reconcile picks another agent.

## 5. Restarts and backoff

Detection: every monitor interval (5 s) the agent emits `Poll` for each
`Running` task; the runner answers `Running` or `Failed`.

1. Crash (`Failed` from a poll): state becomes `Stopping` (it keeps its
   reservation; no capacity window opens), notify `crash`, then
   `schedule_restart(ran = true)`. A failed start uses `ran = false` and
   notifies `crash` only on the task's first start.
2. `schedule_restart` first emits a `Stop` for the old attempt (also on the
   terminal path, so no cage/cgroup/container is left:
   `restart_terminal_path_cleans_runner_before_failed`).
3. Budget: if `ran` and the task ran longer than `restart_window` (default
   5 min), `restart_count` resets to 0 (`healthy_uptime_still_resets_restart_count`);
   a slow failing start never resets it (`slow_failing_start_does_not_reset_restart_count`).
4. `max_restarts`: default -1 (unlimited); `0` never restarts; `n` allows
   exactly `n` (`max_restarts_zero_means_no_restarts`,
   `max_restarts_one_allows_exactly_one_restart`). Exhausted -> `Failed`,
   terminal, still reserving and still counted in `placed`
   (`failover_failed_tasks_not_redispatched`).
5. Delay before restart number `count`: `count == 0` -> immediately;
   otherwise base 1, 2, 4, 8, 16 s for counts 1-5, then 30 s
   (`MAX_RESTART_DELAY`), jittered uniformly within the upper half
   [base/2, base]. Saturating, never zero (`restart_delay_saturates_without_overflow`).
   `next_restart_at` is visible on the task.
6. `restart_fire` (on the tick when due): atomically replace the record with a
   fresh `Queued` task (new id, `restart_count + 1`, same reservation) and
   start it. No restart fires after `shutdown`.

## 6. Health checks

Host only: on HopOS probes are not wired yet (`HOP_PROBE_SKIPPED`); tasks with
a `health_check` there are never probed.

1. On each poll that finds a task `Running` with a `health_check`, the agent
   emits `Probe` unless the task is younger than `initial_timeout` (only when
   that is set; 0 means no grace). Kinds: `http` (default; GET on the named
   port, default name `http`; 200-399 healthy), `tcp` (connect), `file` (exists
   on first check, then mtime must have advanced). A missing named port is a
   failed check (`check_health_missing_port`).
2. Probe cadence is the node's monitor interval (`timeouts.health_check_interval`,
   5 s); the job-level `health_check.interval` is not consulted by the agent.
   Timeout: the check's `timeout`, else `timeouts.health_check_timeout` (5 s).
3. `failure_threshold` consecutive failures (default 3) -> `Failed`, notify
   `crash`, restart path with `ran = true`. A success resets the counter
   (`check_health_failure_threshold_resets`).
4. The first passing check notifies `started` exactly once
   (`first_health_check_pass_fires_started`, `health_check_pass_only_notifies_once`).

## 7. Updates

`POST /v1/jobs` for an existing name calls `Leader::update_job`, which runs
synchronously (the API answers after the rollout):

- Policy: the job's `update_policy`, else `recreate` if any port is fixed
  (non-zero), else `rolling`. Fixed port + explicit rolling/blue-green is
  refused before anything changes (`update_with_a_fixed_port_only_recreates`).
- An unset priority keeps the old one (`update_keeps_old_priority_when_unset`).
- The new definition is stored with `deploying = true`; only a successful
  rollout clears it. A failed rollout or a leader dying mid-way leaves it set,
  and it rides along in the snapshot (`deploying_reflects_rollout_truth`). The
  agent never writes the flag back (`keep_rollout_flag_follows_the_store`).

| Policy | Steps | On failure |
| --- | --- | --- |
| rolling | Snapshot the old tasks (`/tasks` of agents where placed). Per old task `i`: if `i < count`, place one new instance without preemption; if all agents are full, run `?replace=1` on the old task's agent (admitted as if its predecessor were gone; the predecessor stops only after admission). Then stop the old task (unless replaced in place). Pause 2 s between steps. Finally reconcile (scale-up). Old tasks beyond `count` are just stopped (scale-down). | `Error::Rolling`; old tasks not yet reached keep running (`update_job_rolling_failure_keeps_old`) |
| recreate | Unbook all placements, `POST /stop/{job}` on each agent, then `dispatch_job` the new definition (preemption allowed). | `Error::Dispatch`; the new definition stays stored with `deploying` set and reconcile keeps trying |
| blue-green | Place `count` new instances without preemption next to the old, then stop all old tasks. | `Error::BlueGreen` with the old set intact (`update_job_blue_green_failure_keeps_old`) |

Updates never preempt a neighbour (`update_rolling_preempt_geen_buurman`).
Responses: 200 `{"status":"updated","policy":...}`, or 500 with the reason on
failure.

## 8. Task stop and delete

- Job delete (`DELETE /v1/jobs/{name}`): remove from the store (dirty ->
  snapshot), take its placements, `DELETE /delete/{job}` on every agent that
  had it, emit `job:<name>`, reconcile so freed capacity is reused. Idempotent
  (`delete_job_twice_does_not_error`). On the agent, `delete_job` removes the
  definition and every task record before the runner stops them (one `Stop`
  per task), then notifies `stop` (`delete_job_removes_tasks_before_stop`).
- Stop of a job's tasks without deleting it (`POST /stop/{job}`): preemption
  and recreate. Single task (`POST /stop-task/{id}`): rolling and blue-green.
- Invariant: the logical record is removed first and the runner is asked once;
  a failing physical cleanup is the runner's quarantine, never a task stuck in
  the planner (`stop_failure_still_removes_logical_task`).
- Host runner stop: SIGTERM to the process group (docker: `docker stop`, 10 s),
  SIGKILL after 10 s, quarantine if the group is still alive 1 s later; the
  task dir is only removed once the group is proven gone.
- HopOS runner stop: cooperative stop with a 3 s window, then the kernel
  revokes; an unconfirmed release keeps the slot quarantined.
- Unregister (`DELETE /v1/agents/{id}`): agent removed, then reconcile.

## 9. Log retention

Both runners keep logs in a `runner::LogStore` with `LogPolicy`:

- Per task two rings (stdout, stderr) of `tail_lines` lines (default 50,
  `runner.log_tail_lines`); the oldest line falls out. HopOS has only stdout
  and truncates lines at 512 bytes; the host caps a line at 64 KiB.
- When a task ends its rings are retired: closed and still readable for
  `keep_ms` (default 300 s, `runner.log_keep_seconds`), then swept. The host
  waits up to 1 s (`LOG_DRAIN_MS`) for reader threads to deliver final lines.
  A task id is never reused with old lines.
- A start that fails on HopOS writes its reason into the task's own log
  before retiring it.
- `GET /logs/{task}/{stdout|stderr}` returns the tail; with `follow` it
  streams new lines (SSE, polled every 500 ms, keepalive every 15 s) until the
  ring closes. Through the leader: `/v1/agents/{id}/logs/...`. Streams are
  capped per node (4 on a host, 3 on HopOS; 503 beyond).

## 10. Leader failover

1. The leader process dies (no SIGTERM handling: the lease is not released and
   must expire). Its lease was last renewed at most TTL/3 earlier, so it
   expires 20-30 s later at default settings.
2. Followers' heartbeats fail; each failure increments `fail_count`
   (`tick_register_fails_triggers_after4`).
3. From the 4th failure (~40 s) the follower clears its cached leader,
   invalidates the lease cache and tries to claim. The claim succeeds only once
   the lease has expired (CAS on the handle; generation + 1). Losers retry on
   later ticks; if the store says nobody leads, `fail_count` stays at 4 and the
   claim is retried every tick (`tick_no_leader_triggers_after4`).
4. Fail-safe at 7 failures: the follower stops all its tasks, **unless** the
   store answered and reports no live leader (then nobody would re-place
   them) (`tick_register_fails7_stops_all_tasks`,
   `tick_no_leader_never_stops_tasks`, `tick_leader_and_store_unreachable_stops_tasks`).
   The counter is then reset to 4.
5. The winner runs section 2: settle 30 s, load `state/<cluster>`, self-register.
6. Other agents find the new owner on the next lease read and register with
   their `placed` counts; the new leader learns placements instead of
   re-dispatching (`failover_new_leader_learns_from_local_agent`,
   `failover_new_leader_reschedules_orphaned_jobs` for jobs with no
   placements). After settle, under-placed jobs are reconciled.
7. A displaced old leader (renew gets 412 because the generation moved on)
   steps down without releasing (`HOP_LEASE_DISPLACED`, `HOP_LEADER_STOPPED`);
   its leader half is discarded, and its `/v1/events` subscribers are closed so
   they reconnect to the new leader.
8. Lock store unreachable while leading: the leader keeps leading as long as
   it sees connected agents (`tick_leader_raft_down_stays_leader`) and logs
   `HOP_LEASE_RENEW_FAIL` with the remaining validity; with zero agents it steps
   down (`tick_leader_raft_down_no_agents_loses_leadership`). Note that the
   count passed in is the leader's agent list, which includes its own
   self-registered agent.

Rough worst case with defaults: a new leader holds the lease 30-40 s after the
crash and starts placing missing work after its 30 s settle, so roughly
60-80 s end to end; tasks on surviving nodes keep running throughout.

See [cli.md](cli.md) for watching these transitions (`hop events`,
`hop agents`) and [hopos.md](hopos.md) for the HopOS markers.

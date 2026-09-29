# Lifecycles & Invariants

Step-by-step what actually happens on each major path, with the invariants
that must hold and file:line references. Use this to reason about edge
cases — a flow that can't be described in prose without "wait, what if …"
moments usually has a bug.

---

## 1. Single-goroutine state model

Every mutation of `Agent.tasks` / `Agent.jobs` (and the leader equivalents)
goes through an ops channel served by exactly one goroutine.

- `stateLoop` ([agent.go:223](../internal/agent/agent.go#L223)) drains
  `ops` and applies each `func(*agentState)` serially.
- `do(op)` ([agent.go:240](../internal/agent/agent.go#L240)) fires and
  forgets — write-only.
- `query[T](a, fn)` ([agent.go:245](../internal/agent/agent.go#L245))
  sends a read op and blocks on the reply channel.

**Invariants**
- No code outside `stateLoop` reads or writes `s.tasks` / `s.jobs` directly.
- Any function that uses `query` to *also* mutate (e.g. set
  `t.LastFailedAt = time.Now()` inside the closure) is legal — the closure
  runs on the state goroutine.
- `ops` has a 256-slot buffer. If 256 ops are in flight, `do` blocks the
  caller. **This means a long-running op blocks the whole agent.** Keep ops
  short; do I/O outside.

**Failure modes spotted today**
- A handler holding state ops would block all other state access. None
  found; current closures are pure-CPU.

---

## 2. Agent process lifecycle

### 2.1 Startup

1. `main()` ([cmd/agent/main.go:60](../cmd/agent/main.go)) parses flags,
   loads config, derives `nodeID`.
2. `ctx, cancel := context.WithCancel(...)` — root context for everything.
3. `signal.Notify(sigCh, SIGINT, SIGTERM)`; on signal, `cancel()` fires.
4. `run(ctx, cfg, nodeID)` constructs the agent and runs the loop:
   - `agent.New(...)` — sets attributes (auto-detected `node.os` /
     `node.arch` + config overrides), creates ExecRunner + DockerRunner,
     **getLeader defaults to a no-op returning ""** so handlers don't
     nil-deref before `SetLeaderFunc` is called.
   - `ag.SetLeaderFunc(disc.GetLeader)`
   - `ag.Init()` — requires `execRunner.Cleanup()` to succeed. When Docker is
     installed it also attempts `dockerRunner.Cleanup()`, but a temporarily
     unreachable daemon only produces a warning.
     ExecRunner.Cleanup currently **does NOT touch leftover
     taskdirs** ([process.go:381](../internal/runner/process.go#L381)) —
     just ensures `/tmp/hop` exists and is 0777. Stale dirs from a crashed
     predecessor stay; operator reboots if they care.
   - No local state load: the agent starts with an empty job store. If this
     node becomes leader it loads desired state from the `StatePersister`
     (`LoadCommittedState`); a pure agent is (re)dispatched by the leader.
   - Start the heartbeat loop in a goroutine (ticker every 10s).
   - `ag.Run(ctx)` — HTTP server + monitor goroutine. Blocks.

### 2.2 Shutdown

1. SIGTERM → `cancel()` → `ctx.Done()`.
2. Inside `Run`, a goroutine watches `ctx.Done()`:
   ```go
   go func() { <-ctx.Done(); a.shutdown(); close(shutdownDone) }()
   ```
   ([agent.go:293](../internal/agent/agent.go#L293))
3. `shutdown()` ([agent.go:362](../internal/agent/agent.go#L362)) runs HTTP
   drain and task stop in **parallel**:
   - HTTP: `server.Shutdown(ctx)` with 5s timeout.
   - Tasks: detach every task as `stopping` → `stopTasks` → one
     `runner.Stop()` attempt per task.
4. `Run()` does NOT return until `shutdownDone` closes
   ([agent.go:298-302](../internal/agent/agent.go#L298)) — main exits only
   after every task has had its single cleanup attempt.
5. Worst-case per task: 10s SIGTERM grace + 1s SIGKILL grace = ~11s.
   With parallel HTTP drain (5s), total agent shutdown ≤ ~11s.

**Invariants**
- `main()` does NOT return while any task is being stopped — guaranteed by
  the `shutdownDone` synchronization.
- Every present task gets one runner cleanup attempt. An owned exec process
  group gets SIGTERM; after 10s it gets SIGKILL, followed by 1s confirmation.
- A logical task is already gone before its runner cleanup starts. If an exec
  process group cannot be confirmed gone after SIGKILL, its runner ownership,
  taskdir and cgroup stay quarantined rather than being unsafely released.
- systemd's `TimeoutStopSec=30` gives us 19s headroom over the worst-case
  11s. Hard crash (kill -9 on hop) leaks taskdirs and mounts — by design;
  operator reboots.

**Failure modes spotted today** (all fixed)
- Stop's SIGKILL path was unreachable due to a polling channel that closed
  unconditionally → tasks survived hop shutdown.
- `Run()` returned before the shutdown goroutine finished → main exited
  mid-cleanup → tasks orphaned to PID 1.

---

## 3. Task lifecycle

A "task" is one running instance of a job; one job can have N tasks (Count).
Task IDs are random UUIDs, regenerated on every restart.

### 3.1 Birth: dispatch from leader

1. Leader's `sendJobToAgent` POSTs the job to `agent/run`
   ([dispatch.go:240](../internal/leader/dispatch.go#L240)).
2. Agent's `handleRun` ([handlers.go:189](../internal/agent/handlers.go#L189)):
   - Decodes job.
   - **Affinity check first** (before capacity) — returns 406 if mismatch.
   - Creates a `Task` with `newTask(&job)`.
   - **Atomically reserves capacity AND adds the task to state**
     ([handlers.go:217](../internal/agent/handlers.go#L217)). The task
     entry IS the reservation.
   - Returns 202 Accepted immediately.
   - Spawns a goroutine that runs `startJob`.

**Invariant**: a task in `s.tasks` always represents reserved capacity, no
matter what State it is in (Queued / Downloading / Running / Stopping /
Failed). Capacity is released only when the task is DELETED from `s.tasks`.

A task is born `queued` and becomes `running` only when it actually runs. On
HopOS the step in between is `downloading`, where the image streams into the
slot's partition (at most 4 at a time per node, progress in
`Task.Downloaded` / `Task.ImageSize`) — see
[architecture.md](architecture.md#hoprunner-hopos-nodes).

### 3.2 Start: setupTaskDir → mount → cmd.Start

`startJob` ([handlers.go:370](../internal/agent/handlers.go#L370)):
1. `resolveJobForRun(job)` — copies job, narrows `Artifacts` to the one
   that matches this node's attributes. Returns error if no match.
2. `allocatePortsForJob(runJob)` — for each named port: dynamic ports get
   a free TCP port; fixed ports are checked for availability and rejected
   if taken.
3. `runner.Run(runJob, task)` — the runner does everything else.

`ExecRunner.Run` ([process.go:60](../internal/runner/process.go#L60)):
1. `setupTaskDir` — mkdir `/tmp/hop/<taskID>`, mountVolume for each volume
   (tracked), `setupIsolationEnv` for chroot binds (tracked), and finally
   `fakeMeminfo` (bind a synthetic /proc/meminfo if MemoryLimit > 0;
   tracked).
2. Download artifact if present.
3. `prepareCgroup(taskID, MemoryLimit)` — mkdir
   `/sys/fs/cgroup/hop/<taskID>`, write memory.max, open dir fd, set
   `SysProcAttr.UseCgroupFD = true` so `clone3(CLONE_INTO_CGROUP)`
   parents the child in our cgroup atomically (Linux ≥ 5.7).
4. `setupCommand` — `/bin/sh -c "<job.Command>"` with chroot if Isolate;
   `Setpgid: true` so the whole task tree shares a process group.
5. `cmd.Start()` — fork+exec.

**Invariants**
- Every successful `mountVolume` / `setupIsolationEnv` / `fakeMeminfo`
  call appends its target to `r.mounts[taskID]`.
- `r.mounts[taskID]` is the **exact** list of mounts we need to undo at
  cleanup. Job-script mounts inside the chroot live in the child's
  `CLONE_NEWNS` namespace and die with the process; not our problem.
- `task.Pid` is the pgrp leader (set by `Setpgid`); all descendants
  inherit the pgrp unless they `setpgid` themselves.
- If `Run` fails at any step, every error path calls `cleanupTaskDir` +
  `removeCgroup` before returning. There is no path where Run errors
  without cleaning up.

### 3.3 Running: monitor loop

`monitorTasks` ([monitor.go:29](../internal/agent/monitor.go#L29)) ticks
every 5s. For each Running task:
1. `runner.Status(task)` — checks the process is still alive. If not:
   - Mark state Stopping (not Failed — Failed would free capacity, then
     the leader would re-dispatch *while* restartTask is creating a
     replacement → over-provisioning).
   - `delete(a.checkStates, task.ID)` so the next health-check pass starts
     fresh.
   - `notifyLeader(jobName, "crash")` (async).
   - `restartTask(task)` (async).
   - Continue (don't measure usage for a dead task).
2. `measureTaskUsage` — `/proc` walk (Linux) or `ps` (macOS), aggregated
   over the whole pgroup. Updates `t.CPUPercent` / `t.MemPercent`. On
   Linux + MemoryLimit > 0, also rewrites
   `taskDir/.hop-meminfo` so the bind-mounted `/proc/meminfo` inside the
   chroot reflects live RSS.
3. Health check if configured, with `failure_threshold` consecutive misses
   before triggering kill+restart.

### 3.4 Restart

`restartTask` ([handlers.go:451](../internal/agent/handlers.go#L451)):
1. Make one best-effort `runner.Stop(task)` call for the previous attempt.
2. Look up the job by name. If gone, remove the task and return.
3. **Read & possibly reset RestartCount** inside one state op:
   ```go
   if t.LastFailedAt was longer ago than RestartWindow { t.RestartCount = 0 }
   t.LastFailedAt = now
   return t.RestartCount
   ```
4. If `restartCount >= maxRestarts` (and maxRestarts >= 0): mark Failed,
   delete checkState, return.
5. **Backoff**: exponential with jitter, capped at 30s and cancellable by
   shutdown.
6. `resolveJobForRun(job)` — same filter as in startJob.
7. `allocatePortsForJob(runJob)` — if it fails, increment `RestartCount` and
   continue the same loop.
8. Atomic swap: `replacement.RestartCount = old.RestartCount + 1`;
   delete old from `s.tasks`, insert replacement.
9. `runner.Run(runJob, replacement)`. On failure, mark replacement Failed and
   continue the loop; that new attempt gets one cleanup call.

**Invariants**
- `RestartCount` only ever increments — never decrements within a window.
- `restartTask` always either (a) starts a new task, (b) marks Failed and
  returns, or (c) loops with a strictly higher count. Stack use is constant,
  including when restarts are unlimited.
- Every retry after the first goes through the backoff on the next loop entry.

### 3.5 Death: Stop / cleanup

`runner.Stop(task)` ([process.go:160](../internal/runner/process.go#L160)):
1. SIGTERM the pgroup.
2. Poll the owned process-generation record for `ESRCH` for up to 10s.
3. If still alive: SIGKILL.
4. Poll the same generation record for 1s more.
5. `cleanupTaskDir(taskID)`:
   - Iterate `r.mounts[taskID]` in **reverse order** (deepest first).
   - `MNT_DETACH` each.
   - `os.RemoveAll(taskDir)`. By this point the bind to host /dev is gone
     so RemoveAll can't unlink host /dev/null.
6. `removeCgroup(taskID)`.

**Invariants**
- Cleanup never walks into an active bind mount — protection comes from
  tracking, not /proc-scanning.
- If a bind mount mysteriously survives unmount (kernel refused MNT_DETACH
  somehow), RemoveAll still runs — but in practice MNT_DETACH always
  succeeds since it's lazy. We accept the (tiny) residual risk.

---

## 4. Leader lifecycle

### 4.1 Becoming leader

`becomeLeader` ([cmd/agent/main.go:177](../cmd/agent/main.go#L177)):
1. Create `leaderCtx` derived from agent ctx — cancelled either on
   shutdown OR on leadership loss.
2. `leader.New(...)` then `EnableSettle()` — settled flag stays false for
   `agentTimeout` (30s).
3. `go (*l).Run(leaderCtx)` — starts the leader's state loop AND a
   `settleTimer` that fires once.
4. Start the leader API server on port+1000.
5. Loop in `cmd/agent/main.go` keeps calling `loop.tick()` every 10s.

### 4.2 Settle

While `settled == false`:
- `DispatchJob` stores the job but does NOT dispatch ("dispatch deferred").
- Heartbeats and registrations still update placement counts.

At T = settleDelay:
- `settleTimer` fires inside `stateLoop` ([leader.go:132](../internal/leader/leader.go#L132)).
- Sets `settled = true`.
- `go l.reconcileJobs()`.

**Why settle**: a freshly-elected leader has no idea what's running where
until agents (re-)register and report `placed`. Without settle it would
dispatch every job to every agent → duplicates.

### 4.3 Reconciliation

`reconcileJobs` iterates every job, for each:
- Computes desired vs placed count.
- For daemon jobs (count=-1): dispatch to every agent missing the job.
- For regular jobs: dispatch the difference via round-robin.
- Skips jobs already in `dispatching` (prevents double dispatch).

`dispatchToAvailableAgent`:
1. First pass: try every agent in round-robin order.
2. Track agents that returned `errNoCapacity`.
3. Second pass on those: find a lower-priority "victim" job, stop it,
   then retry the dispatch.

**Invariants**
- A job that's in `s.dispatching[name]` is skipped by reconciliation —
  prevents races between two concurrent reconciles or reconcile + manual
  DispatchJob.
- `s.dispatching` is cleared via `defer l.unlockJob` on every dispatch
  path; `DeleteJobByName` clears it defensively too.

### 4.4 Dead-agent detection

Every 10s a goroutine scans `s.agents`. For each agent with
`LastSeen > agentTimeout (30s)`:
- Delete from `s.agents` and `s.placed`.
- `reconcileJobs()` to redistribute.

**Failure mode worth knowing**: an agent could be alive but unable to
heartbeat (firewall, leader down between failovers). 30s threshold +
the parallel hoplock lease race handle most of this.

---

## 5. Chroot isolation specifics (Linux only)

### What gets bind-mounted into the chroot

`setupIsolationEnv` ([process_linux.go:170](../internal/runner/process_linux.go#L170)):
- `/bin`, `/usr`, `/lib`, `/lib64` — recursive bind, remounted read-only.
  (Absolute symlinks would resolve back to taskDir/... after chroot →
  ELOOP. Binds work via CLONE_NEWNS propagation.)
- `/dev` — recursive bind, READ-WRITE. Char devices need to be writable
  (writes go to drivers, not the fs).
- `/proc`, `/sys` — recursive bind, remounted read-only.
- `/etc/resolv.conf` — single-file bind so the rest of /etc (passwd,
  shadow, …) stays hidden from the task.
- `/proc/meminfo` overmount (when MemoryLimit > 0) — see §6.

**Each successful bind is appended to `r.mounts[taskID]`.** The list is
the SOLE source of truth for cleanup.

### Cgroups v2

`prepareCgroup` ([process_linux.go:36](../internal/runner/process_linux.go#L36)):
- Per-task dir at `/sys/fs/cgroup/hop/<taskID>`.
- Write memory.max.
- Open dir as `O_DIRECTORY|O_CLOEXEC` → fd passed to `clone3` via
  `SysProcAttr.UseCgroupFD = true, CgroupFD = fd`.
- Kernel parents the child into the cgroup at fork time — no race with a
  later "move into cgroup".

**Caveat**: the agent needs write access to `/sys/fs/cgroup/hop/`. Under
systemd v2 cgroups this requires `Delegate=yes` on the service unit AND
ideally using the service's own cgroup as base. Currently the install
script uses no Delegate, so memory.max writes fail with EACCES. The agent
logs a warning and proceeds — cgroup exists but isn't size-limited.

---

## 6. Synthetic /proc/meminfo (Linux + MemoryLimit > 0)

`fakeMeminfo` ([process_linux.go:73](../internal/runner/process_linux.go#L73)):
- Writes `taskDir/.hop-meminfo` with MemTotal/MemFree/MemAvailable set to
  the task's `MemoryLimit`.
- Bind-mounts that file over `taskDir/proc/meminfo` (single-file bind).
- Adds the mount target to `r.mounts[taskID]`.

`refreshMeminfo` ([usage.go:15](../internal/agent/usage.go#L15)) runs on
every 5s monitor tick:
- Computes MemFree = MemoryLimit − measured RSS.
- Rewrites `.hop-meminfo` in place via `os.WriteFile`.
- Bind reads the same inode, so chrooted readers see the new content on
  next open/read.

**Why bind a file we wrote ourselves**: container-naive workloads (older
JVMs, native binaries, pre-cgroup-aware .NET) read MemTotal from
/proc/meminfo to size heaps. With the host's 64GB they OOM at the 1GB
limit. Our overmount lies just enough for them to size correctly.

**Why NOT atomically rename**: `rename` would point the bind at a stale
inode — readers in the chroot would see the original snapshot forever.
We write through the source path so the inode stays the same.

---

## 7. Things that intentionally leak

For each, the rationale is "cleaning it up would require state we don't
have, and the consequences of getting it wrong outweigh the cost of a
leak":

- **Stale taskdirs from a crashed agent.** No way to know what was
  mounted without persistence. Reboot or `rm -rf /tmp/hop` (agent
  stopped) cleans it up.
- **Taskdirs interrupted by a hard agent kill during setup.** Ordinary setup,
  artifact and `cmd.Start` errors roll back tracked mounts transactionally;
  a process kill can still interrupt that rollback. Same recovery as above.
- **DockerRunner residue** is handled separately via
  `docker rm -f hop-*` at startup.

---

## Open questions / things to double-check

(Use this as the scrap pad while reading.)

- [ ] On leader failover, the old leader's `dispatching` flag map is
      reset to empty in the new leader's stateLoop init. If the old
      leader was mid-dispatch and crashed, the new leader doesn't know.
      Settle should handle this (no dispatch during settle) — verify.

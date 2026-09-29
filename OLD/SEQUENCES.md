# Hop Sequence Diagrams

Overzicht van alle belangrijke call flows in hop, inclusief de volgorde waarin componenten elkaar aanroepen.

## Inhoudsopgave

- [1. Agent Startup](#1-agent-startup)
- [2. Leader Election (becomeLeader)](#2-leader-election-becomeleader)
- [3. Heartbeat Loop](#3-heartbeat-loop)
- [4. Agent Registration](#4-agent-registration)
- [5. Job Dispatch (nieuw)](#5-job-dispatch-nieuw)
- [6. Job Update (Rolling)](#6-job-update-rolling)
- [7. Job Update (Recreate)](#7-job-update-recreate)
- [8. Job Update (Blue-Green)](#8-job-update-blue-green)
- [9. Job Delete](#9-job-delete)
- [10. Task Monitoring & Restart](#10-task-monitoring--restart)
- [11. Dead Agent Detection & Reconciliation](#11-dead-agent-detection--reconciliation)
- [12. Leader Failover](#12-leader-failover)
- [13. Proxy to Leader (hopdns/hoplb/hopprom)](#13-proxy-to-leader)
- [14. Agent Isolation (network split)](#14-agent-isolation-network-split)
- [Bevindingen: Volgordes & Mogelijke Issues](#bevindingen)

---

## 1. Agent Startup

Wat er gebeurt wanneer een hop agent proces start.

```mermaid
sequenceDiagram
    participant M as main()
    participant C as config.Load
    participant D as Discovery
    participant A as Agent
    participant R as ExecRunner
    participant DR as DockerRunner
    participant FS as Filesystem

    M->>C: Load(configPath)
    C-->>M: cfg

    M->>M: getOrCreateNodeID(cfg)
    M->>M: getOutboundIP()

    M->>M: buildBackend(cfg) → hoplock.Backend (hoplockserver / S3 / mem)
    M->>D: discovery.New(backend, ip, leaderPort, leaseTTL)
    D-->>M: disc

    M->>A: agent.New(cfg, nodeID, nil)
    A->>R: runner.NewExecRunner(runnerCfg)
    A->>DR: runner.NewDockerRunner()
    A-->>M: ag

    M->>A: SetLeaderFunc(disc.GetLeader)

    M->>A: Init()
    A->>R: Cleanup()
    R->>FS: RemoveAll(rootfsBase) + MkdirAll
    A->>DR: Cleanup()
    DR->>DR: docker ps -a --filter name=hop-
    DR->>DR: docker rm -f (elk)

    M->>A: LoadState()
    A->>FS: ReadFile(state.json)
    A->>A: store jobs in agentState

    Note over M: Start heartbeat/leader goroutine (zie #3)
    M->>A: Run(ctx)
    A->>A: go stateLoop(ctx)
    A->>A: go monitorTasks(ctx)
    A->>A: ListenAndServe(:8080)
```

---

## 2. Leader Election (becomeLeader)

Wat er gebeurt wanneer een agent succesvol leader wordt.

```mermaid
sequenceDiagram
    participant M as main/tick()
    participant D as Discovery
    participant A as Agent
    participant L as Leader
    participant API as API Server
    participant SL as stateLoop

    M->>D: TryBecomeLeader()
    D->>D: conditional CAS write van de lease-blob (PUT If-None-Match) → lock backend
    D-->>M: true (success)

    M->>M: becomeLeader()

    M->>L: leader.New(ag.ID(), ag, nil)
    Note over L: agent implementeert JobStore interface
    M->>L: EnableSettle()
    Note over L: settleDelay = agentTimeout (30s)

    M->>L: go Run(leaderCtx)
    L->>SL: go stateLoop(ctx)
    Note over SL: settled=false, settleTimer=30s

    L->>L: Start deadAgentCheck ticker (10s)

    M->>API: api.NewServer(leader, :9080)
    M->>API: go srv.Run(leaderCtx)
    API->>API: ListenAndServe(:9080)

    Note over M: API is nu bereikbaar voor andere agents

    M->>L: RegisterAgent(ag.ID(), ag.Endpoint(), version, placedCounts)
    L->>SL: query: store agent + placed counts
    Note over SL: settled=false → shouldReconcile=false
    Note over L: Geen reconcile (settle period)

    Note over SL: Na 30s: settleTimer fires
    SL->>SL: settled = true
    SL->>L: go reconcileJobs()
```

**Kritieke volgorde:** `go l.Run()` MOET voor `RegisterAgent()` starten. Anders deadlock: RegisterAgent doet een `query()` op de ops channel, maar stateLoop leest er nog niet van.

---

## 3. Heartbeat Loop

De main loop die elke 10 seconden draait op elke agent.

```mermaid
sequenceDiagram
    participant T as tick() [10s]
    participant D as Discovery
    participant A as Agent
    participant API as Leader API
    participant L as Leader

    alt Wij zijn leader
        T->>D: RenewLease()
        D->>D: CAS renew van de lease-blob (PUT If-Match op laatste ETag) → lock backend
        alt Lock backend bereikbaar
            D-->>T: true
            T->>T: failCount = 0
            T->>API: sendHeartbeat(self)
            Note over T: Leader heartbeat naar zichzelf
        else Lock backend onbereikbaar
            D-->>T: false
            alt Agents nog connected
                T->>L: GetAgents()
                L-->>T: agents (len > 0)
                Note over T: Blijf leader (agents alive)
            else Geen agents
                T->>T: Stop leader, reset state
            end
        end
    else Leader bekend + geregistreerd
        T->>API: POST /v1/heartbeat {id, endpoint, jobs, stateTime}
        API->>L: Heartbeat(id, endpoint, jobs, stateTime)
        L->>L: Update agent.LastSeen
        alt Agent heeft nieuwere state
            L->>A: SyncJobs(agentJobs, stateTime)
        end
        L-->>API: {jobs, state_time}
        API-->>T: 200 OK + jobs
        T->>A: SyncJobs(leaderJobs, stateTime)
    else Leader bekend + NIET geregistreerd
        T->>API: POST /v1/agents {id, endpoint, placed}
        Note over T: Zie #4 Agent Registration
    else Heartbeat faalt
        T->>T: failCount++
        alt failCount >= 3
            T->>D: TryBecomeLeader()
            Note over T: Zie #2 Leader Election
        end
    else Geen leader
        T->>T: failCount++
        T->>D: TryBecomeLeader()
    end
```

---

## 4. Agent Registration

Wanneer een agent zich (opnieuw) registreert bij de leader.

```mermaid
sequenceDiagram
    participant AG as Agent (tick)
    participant API as Leader API (:9080)
    participant L as Leader
    participant SL as leaderState

    AG->>AG: GetPlacedTaskCounts()
    Note over AG: jobID → count van lokale tasks

    AG->>API: POST /v1/agents {id, endpoint, version, placed}
    API->>L: RegisterAgent(id, endpoint, version, placed)

    L->>SL: query: clear old state + store agent + placed
    Note over SL: delete old agents[id] + placed[id]
    Note over SL: store new agent + placed counts

    SL-->>L: settled?

    alt settled = true
        L->>L: reconcileJobs()
        Note over L: Zie #11 Reconciliation
    else settled = false (settle period)
        Note over L: Geen reconcile nu, wacht op settle timer
    end

    API-->>AG: 200 {status: "registered", jobs, state_time}
    AG->>AG: registered = true
```

---

## 5. Job Dispatch (nieuw)

Wanneer een nieuwe job wordt aangemaakt via CLI of API.

```mermaid
sequenceDiagram
    participant CLI as CLI / User
    participant API as Leader API
    participant L as Leader
    participant JS as JobStore (Agent)
    participant AG as Target Agent
    participant R as Runner

    CLI->>API: POST /v1/jobs {name, command, count, ...}
    API->>API: job.ID = uuid.New()
    API->>L: FindJobByName(name)
    L-->>API: nil (new job)

    API->>L: DispatchJob(&job)
    L->>JS: StoreJob(job)
    Note over JS: Job altijd opgeslagen, ook als dispatch faalt

    alt count == -1 (daemon)
        L->>L: reconcileJob(job, agents)
        loop Voor elke agent zonder deze job
            L->>AG: sendJobToAgent(agent, job)
            AG-->>L: 200 OK
            L->>L: trackPlacement(agent.ID, job.ID)
        end
    else count > 0 (regulier)
        L->>L: dispatchInstances(job, count)
        L->>L: dispatching[job.ID] = true
        loop count keer
            L->>L: dispatchToAvailableAgent(job)
            L->>L: nextAgent() (round-robin)
            L->>AG: POST /run (sendJobToAgent)
            AG->>AG: handleRun: capacity check + reserve
            AG-->>L: 202 Accepted
            L->>L: trackPlacement(agent.ID, job.ID)

            Note over AG: Async op agent:
            AG->>AG: startJob(job)
            AG->>AG: allocatePortsForJob()
            AG->>R: runner.Run(job, ports)
            R-->>AG: task
            AG->>AG: store task in agentState
        end
        L->>L: dispatching[job.ID] = false
    end

    API-->>CLI: 201 {id, name, status: "dispatched"}
```

---

## 6. Job Update (Rolling)

Zero-downtime rolling update: 1 instance tegelijk vervangen.

```mermaid
sequenceDiagram
    participant API as Leader API
    participant L as Leader
    participant JS as JobStore
    participant AG1 as Agent (old)
    participant AG2 as Agent (new)

    API->>L: UpdateJob(newJob)
    L->>L: FindJobByName → oldJob
    Note over L: policy = "rolling"

    Note over L: Verwijder old uit store (voorkomt reconcile race)
    L->>JS: DeleteJob(oldJob.ID)

    loop Voor elke instance (1..count)
        Note over L: Stap 1: Start nieuwe instance
        L->>L: dispatchToAvailableAgent(newJob)
        L->>AG2: POST /run (newJob)
        AG2-->>L: 202 Accepted
        L->>L: trackPlacement(AG2, newJob.ID)

        Note over L: Stap 2: Stop oude instance
        L->>L: stopOneInstance(oldJob)
        L->>L: Find agent met placed[oldJob.ID] > 0
        L->>AG1: DELETE /delete/{oldJob.ID}
        AG1->>AG1: Stop task + remove from state

        Note over L: Stap 3: Wacht 2s (behalve laatste)
        L->>L: time.Sleep(2s)
    end

    Note over L: Store nieuwe job definitie
    L->>JS: StoreJob(newJob)
```

**Volgorde per instance:** dispatch new → stop old → delay. Dit garandeert dat er altijd minstens N-1 instances draaien.

**Waarom DeleteJob eerst?** De oude job wordt direct uit de store verwijderd zodat `reconcileJobs` (als het triggert tijdens de update) niet probeert extra oude instances te dispatchen. De in-memory `oldJob` referentie is genoeg voor `stopOneInstance`.

---

## 7. Job Update (Recreate)

Simpele update met downtime: alles stoppen, dan alles starten.

```mermaid
sequenceDiagram
    participant L as Leader
    participant JS as JobStore
    participant AG as Agents

    L->>L: updateRecreate(old, new)

    Note over L: Stap 1: Delete alles van old
    L->>L: DeleteJobByID(old)
    par Parallel delete op alle agents
        L->>AG: DELETE /delete/{old.ID}
    end
    L->>JS: DeleteJob(old.ID)
    L->>L: reconcileJobs()

    Note over L: Stap 2: Dispatch nieuwe versie
    L->>L: DispatchJob(new)
    L->>JS: StoreJob(new)
    L->>L: dispatchInstances(new, count)
    loop count keer
        L->>AG: POST /run (new)
    end
```

---

## 8. Job Update (Blue-Green)

Zero-downtime met 2x resources: start alles nieuw, dan stop alles oud.

```mermaid
sequenceDiagram
    participant L as Leader
    participant JS as JobStore
    participant AG as Agents

    L->>L: updateBlueGreen(old, new)

    Note over L: Stap 1: Dispatch alle nieuwe instances
    L->>L: DispatchJob(new)
    L->>JS: StoreJob(new)
    L->>L: dispatchInstances(new, count)
    loop count keer
        L->>AG: POST /run (new)
    end

    Note over L: Nu draaien OLD + NEW tegelijk (2x resources)

    Note over L: Stap 2: Delete oude versie
    L->>L: DeleteJobByID(old)
    par Parallel delete op alle agents
        L->>AG: DELETE /delete/{old.ID}
    end
    L->>JS: DeleteJob(old.ID)
```

---

## 9. Job Delete

Verwijderen van een job en alle bijbehorende tasks.

```mermaid
sequenceDiagram
    participant CLI as CLI / User
    participant API as Leader API
    participant L as Leader
    participant JS as JobStore
    participant AG as Agents
    participant R as Runner

    CLI->>API: DELETE /v1/jobs/{name}
    API->>L: DeleteJob(name)
    L->>L: FindJobByName(name) → job

    L->>L: DeleteJobByID(job)

    L->>L: query: find agents met placed[job.ID] > 0
    Note over L: Clear placed counts in zelfde query

    par Parallel op alle agents met deze job
        L->>AG: DELETE /delete/{job.ID}
        AG->>AG: deleteJobByID(jobID)
        AG->>AG: Remove job from state
        AG->>AG: Mark tasks as "stopping"
        par Stop tasks parallel
            AG->>R: runner.Stop(task)
            R->>R: SIGTERM → 10s → SIGKILL
            R->>R: cleanupTaskDir
        end
        AG-->>L: {deleted: N}
    end

    Note over L: Wacht tot ALLE agents klaar zijn (wg.Wait)
    L->>JS: DeleteJob(job.ID)

    alt Er waren agents met deze job
        L->>L: reconcileJobs()
        Note over L: Freed capacity direct beschikbaar
    end

    API-->>CLI: 204 No Content
```

---

## 10. Task Monitoring & Restart

De agent monitort elke 5 seconden alle running tasks.

```mermaid
sequenceDiagram
    participant Mon as monitorTasks [5s]
    participant S as agentState
    participant R as Runner
    participant A as Agent

    Mon->>S: query: get running tasks + jobs
    S-->>Mon: [{task, job}, ...]

    loop Voor elke running task
        Mon->>R: Status(task)

        alt Process crashed (state != running)
            Mon->>S: do: task.State = failed
            Mon->>A: go restartTask(task)

            A->>A: GetJobByName(task.JobName)
            A->>A: Check restartCount vs maxRestarts

            alt Onder restart limiet
                A->>R: Stop(oldTask) [cleanup]
                A->>A: allocatePortsForJob(job)
                A->>R: Run(job, newPorts)
                R-->>A: newTask
                A->>S: do: delete old, store new, restartCount++
            else Boven limiet
                A->>S: do: task.State = failed
                Note over A: Geeft op, geen restart meer
            end

        else Process running + health check geconfigureerd
            alt Binnen initialTimeout grace period
                Note over Mon: Skip health check
            else Health check uitvoeren (http/tcp/file)
                Mon->>Mon: checkHealth(task, hc)
                Note over Mon: http: HTTP GET 127.0.0.1:{port}{path}<br/>tcp: net.DialTimeout 127.0.0.1:{port}<br/>file: os.Stat(path), mtime > lastCheckTime

                alt Check succeeds
                    Mon->>Mon: failCount = 0
                else Check fails, under threshold
                    Mon->>Mon: failCount++ (< failure_threshold)
                    Note over Mon: Log warning, task blijft running
                else Check fails, at threshold (default 3)
                    Mon->>S: do: task.State = failed
                    Mon->>R: go Stop(task)
                    Mon->>A: go restartTask(task)
                end
            end
        end
    end

    Note over Mon: Piggyback: state persistence
    alt needsSave == true
        Mon->>A: SaveState()
        A->>A: WriteFile(state.json)
    end
```

---

## 11. Dead Agent Detection & Reconciliation

De leader controleert elke 10 seconden of agents nog leven.

```mermaid
sequenceDiagram
    participant H as health.go [10s]
    participant SL as leaderState
    participant L as Leader
    participant AG as Agents
    participant JS as JobStore

    H->>SL: query: settled?
    alt Niet settled
        Note over H: Skip (wacht op settle period)
    else Settled
        H->>SL: query: check LastSeen vs agentTimeout(30s)
        Note over SL: Remove dead agents + placed counts
        SL-->>H: hadDead?

        alt Minstens 1 agent dood
            H->>L: reconcileJobs()
        end
    end

    Note over L: reconcileJobs()
    L->>JS: GetJobs()
    L->>L: GetAgents()

    Note over L: Stap 1: Rebuild placed from reality
    L->>L: GetClusterStatus()
    par Parallel fetch van alle agents
        L->>AG: GET /tasks
        AG-->>L: [tasks...]
    end
    L->>SL: do: rebuild placed from actual tasks

    Note over L: Stap 2: Reconcile elke job
    loop Voor elke job
        alt Daemon (count == -1)
            L->>SL: query: agents zonder placed[job.ID]
            loop Agents die job missen
                L->>AG: POST /run (sendJobToAgent)
                L->>L: trackPlacement
            end
        else Regular (count > 0)
            L->>SL: query: sum placed across live agents
            alt totalPlaced < desired
                L->>L: dispatchInstances(job, missing)
            end
        end
    end
```

---

## 12. Leader Failover

Complete flow wanneer de leader crasht en een andere agent het overneemt.

```mermaid
sequenceDiagram
    participant A1 as Agent A (was follower)
    participant A2 as Agent B (was follower)
    participant OLD as Old Leader (crashed)
    participant LK as Lock backend (hoplockserver/S3)
    participant NL as New Leader (A1)

    Note over OLD: T=0s: Leader crasht

    loop Heartbeat elke 10s
        A1->>OLD: POST /v1/heartbeat
        OLD--xA1: Connection refused
        A1->>A1: failCount++ (1, 2, 3...)
    end

    Note over A1: T≈30s: failCount >= 3

    A1->>LK: TryBecomeLeader() — CAS write op de verlopen lease
    LK-->>A1: success!
    A1->>A1: becomeLeader()

    A1->>NL: leader.New() + EnableSettle()
    A1->>NL: go Run(ctx)
    Note over NL: settled=false, timer=30s
    A1->>NL: Start API server :9080
    A1->>NL: RegisterAgent(self, placedCounts)
    Note over NL: Geen reconcile (settling)

    Note over A2: T≈40s: A2 heartbeat naar oude leader faalt

    A2->>OLD: POST /v1/heartbeat
    OLD--xA2: Connection refused
    A2->>A2: failCount >= 3
    A2->>LK: TryBecomeLeader() — CAS write op de lease
    LK-->>A2: false (lease al gehouden door A1)
    A2->>LK: GetLeader()
    LK-->>A2: A1 address

    Note over A2: T≈50s: A2 heartbeat naar A1
    A2->>NL: POST /v1/heartbeat
    NL-->>A2: 404 "not registered"
    A2->>A2: registered = false

    A2->>NL: POST /v1/agents {id, endpoint, placed}
    NL->>NL: RegisterAgent(A2, placedCounts)
    Note over NL: settled=false → geen reconcile

    Note over NL: T≈60s: Settle timer fires
    NL->>NL: settled = true
    NL->>NL: reconcileJobs()
    Note over NL: Alle agents geregistreerd met placed counts
    Note over NL: Dispatch missing instances indien nodig
```

---

## 13. Proxy to Leader

Hoe hopdns/hoplb/hopprom cluster data opvragen via hun lokale agent.

```mermaid
sequenceDiagram
    participant S as hopdns/hoplb/hopprom
    participant AG as Lokale Agent (:8080)
    participant D as Discovery
    participant API as Leader API (:9080)

    S->>AG: GET /v1/agents (of /v1/jobs, /v1/status)
    AG->>AG: proxyToLeader()
    AG->>D: getLeader()
    D-->>AG: leaderAddr

    AG->>API: GET /v1/agents (forward)
    API->>API: handleGetAgents()
    API-->>AG: [agents...]

    AG-->>S: [agents...] (proxied response)
```

---

## 14. Agent Isolation (network split)

Wat er gebeurt wanneer een agent volledig geisoleerd raakt.

```mermaid
sequenceDiagram
    participant A as Isolated Agent
    participant D as Discovery
    participant L as Leader (onbereikbaar)
    participant R as Runner

    loop Elke 10s tick
        A->>L: POST /v1/heartbeat
        L--xA: timeout
        A->>A: failCount++ (4, 5, 6...)
    end

    Note over A: failCount >= 3: Try become leader
    A->>D: TryBecomeLeader()
    D--xA: Lock backend onbereikbaar
    D-->>A: false

    Note over A: failCount >= 6: ISOLATION MODE
    A->>A: StopAllTasks()
    A->>A: Mark all running → stopping
    par Stop alle tasks parallel
        A->>R: Stop(task)
        R->>R: SIGTERM → SIGKILL
    end
    A->>A: failCount = 3 (blijf proberen)

    Note over A: Voorkomt dat geisoleerde agent<br/>duplicaten draait van tasks die de leader<br/>ook op andere agents heeft geplaatst
```

---

## Bevindingen

### Volgordes die correct zijn

1. **becomeLeader**: `go l.Run()` start VOOR `RegisterAgent()` — voorkomt deadlock op ops channel.

2. **Init() volgorde**: `Cleanup()` (exec + docker) → `LoadState()` → `Run()`. Clean slate van processen, dan pas state laden.

3. **Capacity reservation**: `handleRun` doet capacity check + reserve in 1 atomaire `query()` — voorkomt TOCTOU race bij concurrent dispatch.

4. **Task stop volgorde**: Mark als "stopping" in state (voorkomt restart door monitor) → dan pas `runner.Stop()`.

5. **Rolling update volgorde**: dispatch new → stop old → delay. Garandeert minimaal N-1 instances.

6. **Delete volgorde**: Stop tasks op alle agents (parallel, `wg.Wait`) → delete job uit store → reconcile. Garandeert dat capacity vrij is voor reconcile.

7. **Settle period**: Leader wacht 30s na election voordat reconcileJobs draait. Geeft agents tijd om te registreren met placed counts. Voorkomt dubbele dispatches.

8. **Heartbeat 404 → re-register**: Agent detecteert nieuwe leader correct en re-registreert met placed counts.


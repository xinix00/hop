//! De eigenaar van alle staat van de node: agent, leader-helft, verkiezing, runner en de twee API's.
//!
//! Eén struct achter `&mut self`, op één thread (handboek §1), zoals `Node`
//! in `agentd-hopos`. Berichten ([`Node::on_msg`]) en de tik ([`Node::tick`])
//! zijn de enige ingangen; wat de agent daarna wil (starten, stoppen,
//! pollen, controleren, melden) voert de node meteen uit, in de volgorde
//! waarin de agent het vroeg. Wat traag is of de leader aanroept, gaat naar
//! een eigen thread (zie [`crate::msg`]): de eigenaar wacht nooit op het
//! net, behalve de leader-helft op zijn agents (Go deed dat ook, met
//! termijnen) en het laden van de clusterstaat bij het leider worden.

use std::collections::BTreeMap;
use std::sync::mpsc::Sender;
use std::time::Duration;

use agent::{
    Action, Agent, Election, Event, LinkError, Request as LinkRequest, StartError, StartOk, Status,
};
use api::{Effect, EventLog, LeaderApi, LeaderCluster, LeaderEffect, NodeApi, Request, Response};
use hostnet::Http;
use leader::{Leader, MemStore};
use runner::host::{HostRunner, TaskSpec};
use runner::{RunState, Stream};
use types::json::Value;
use types::time::{MILLISECOND, SECOND};
use types::{Driver, Job, Map, Nanos, Time};

use crate::elector::{Elector, now_ms};
use crate::link::{LinkJob, ProbeJob};
use crate::msg::{Chunk, Msg, Poll, Port, Reply};
use crate::net::Net;
use crate::persist::{self, PersistOp};
use crate::prep::{Prep, PrepJob};

/// De versie die de agent aan de leader meldt.
pub(crate) const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Hoe vaak de verkiezing tikt: register of heartbeat bij de leader, of de
/// lease vernieuwen (Go: `loop.Run(10 s)`).
const ELECTION_TICK: Nanos = 10 * SECOND;

/// Hoe vaak de leader tikt (dode agents, settle, de vangnet-reconcile).
const LEADER_TICK: Nanos = 10 * SECOND;

/// Hoeveel rondes acties de node na één invoer hoogstens uitvoert; de
/// volgende tik pakt de rest op.
const ACTION_ROUNDS: usize = 16;

/// Hoeveel rondes vervolg-aanroepen de verkiezing na één antwoord krijgt.
const LINK_ROUNDS: usize = 4;

/// Hoeveel stromen (SSE, een log-tail, een doorgegeven stroom) tegelijk
/// open mogen staan. Een stroom houdt een verbindingsthread vast zolang hij
/// loopt, en de pools zijn vast ([`crate::http::WORKERS`] per poort): met
/// vier stromen houdt elke poort minstens de helft van zijn threads vrij
/// voor heartbeats, registraties en de CLI.
pub(crate) const MAX_STREAMS: usize = crate::http::WORKERS / 2;

/// Wat de node bij de start meekrijgt.
pub(crate) struct Parts {
    pub(crate) agent: Agent,
    pub(crate) runner: HostRunner,
    pub(crate) elector: Elector,
    pub(crate) election: Election,
    pub(crate) key: Vec<u8>,
    pub(crate) cluster: String,
    pub(crate) clustered: bool,
    pub(crate) init_jobs: Vec<Value>,
    pub(crate) node_dead: Nanos,
    pub(crate) load_wait: Duration,
    pub(crate) link: Sender<LinkJob>,
    pub(crate) probes: Sender<ProbeJob>,
    pub(crate) persist: Sender<PersistOp>,
    pub(crate) prep: Prep,
}

/// De node.
pub(crate) struct Node {
    agent: Agent,
    leader: Option<Leader<MemStore>>,
    runner: HostRunner,
    elector: Elector,
    election: Election,
    node_api: NodeApi,
    leader_api: LeaderApi,
    key: Vec<u8>,
    clustered: bool,
    init_jobs: Vec<Value>,
    node_dead: Nanos,
    load_wait: Duration,
    http: Http,
    link: Sender<LinkJob>,
    probes: Sender<ProbeJob>,
    persist: Sender<PersistOp>,
    prep: Prep,
    /// De poorten van een taak in voorbereiding, tot zijn start.
    ports: BTreeMap<String, BTreeMap<String, u16>>,
    /// De meldingen voor `/v1/events`: die van de leader
    /// (`drain_events`) en die van `POST /v1/notify`, met hun event.
    events: EventLog,
    /// Open stromen; aangemeld bij het antwoord, afgemeld met
    /// [`Msg::StreamDone`].
    streams: usize,
    next_election: Nanos,
    next_leader: Nanos,
    snapshot_failures: u32,
}

/// Een map van `types` als `BTreeMap`, zoals de runner hem wil.
fn to_btree<V: Clone>(m: &Map<V>) -> BTreeMap<String, V> {
    m.iter()
        .map(|(k, v)| (String::from(k), v.clone()))
        .collect()
}

/// Een vrije TCP-poort van de kernel: binden op 0 en weer loslaten.
///
/// Er zit een venster tussen loslaten en de bind van de taak; Go had
/// hetzelfde venster, en een botsing is een mislukte start die herstart.
fn free_port() -> Option<u16> {
    std::net::TcpListener::bind("0.0.0.0:0")
        .ok()?
        .local_addr()
        .ok()
        .map(|a| a.port())
}

impl Node {
    /// Een node uit zijn onderdelen; tikt meteen bij de eerste [`Node::tick`].
    pub(crate) fn new(p: Parts, now: Nanos) -> Self {
        let key = p.key;
        Self {
            node_api: NodeApi::new(&key, false),
            leader_api: LeaderApi::new(&key, &p.cluster),
            agent: p.agent,
            leader: None,
            runner: p.runner,
            elector: p.elector,
            election: p.election,
            key,
            clustered: p.clustered,
            init_jobs: p.init_jobs,
            node_dead: p.node_dead,
            load_wait: p.load_wait,
            http: Http::new(),
            link: p.link,
            probes: p.probes,
            persist: p.persist,
            prep: p.prep,
            ports: BTreeMap::new(),
            events: EventLog::new(),
            streams: 0,
            next_election: now,
            next_leader: now.saturating_add(LEADER_TICK),
            snapshot_failures: 0,
        }
    }

    /// De eerste stap na de start: was de boot-claim raak, dan leidt deze node nu.
    pub(crate) fn boot(&mut self, now: Nanos) {
        let reqs = self
            .election
            .become_leader_now(&mut self.elector, &mut self.agent);
        self.requests(now, reqs);
    }

    /// Verwerkt één bericht.
    pub(crate) fn on_msg(&mut self, msg: Msg, now: Nanos) {
        match msg {
            Msg::Http { port, req, reply } => {
                let r = self.handle(port, &req, now);
                // Een verbinding die niet meer wacht, is weg. Was het een
                // stroom, dan meldt niemand hem af: dat doen we hier.
                if let Err(e) = reply.send(r)
                    && e.0.is_stream()
                {
                    self.streams = self.streams.saturating_sub(1);
                }
            }
            Msg::Poll { poll, reply } => {
                let _ = reply.send(self.poll(now, poll));
            }
            Msg::StreamDone => self.streams = self.streams.saturating_sub(1),
            Msg::Prepared {
                worker,
                task_id,
                driver,
                spec,
                result,
            } => {
                self.prep.done(worker);
                self.launch(now, &task_id, driver, &spec, result);
            }
            Msg::Progress {
                task_id,
                got,
                total,
            } => self.agent.on_progress(&task_id, got, total),
            Msg::Lease(r) => {
                self.elector.on_reply(r);
                self.flush_notes();
            }
            Msg::Link { req, result } => {
                let reqs = self
                    .election
                    .on_reply(&mut self.elector, &mut self.agent, &req, result);
                self.requests(now, reqs);
            }
            Msg::Probe { task_id, outcome } => self.agent.on_probe(now, &task_id, outcome),
            Msg::SnapshotFailed(why) => {
                if let Some(l) = self.leader.as_mut() {
                    l.snapshot_failed();
                }
                self.snapshot_failures = self.snapshot_failures.saturating_add(1);
                // Luid, één keer, dan tellen (handboek §6).
                if self.snapshot_failures <= 3 || self.snapshot_failures.is_multiple_of(100) {
                    eprintln!(
                        "hop: cluster state not saved ({}x): {why} HOP_STATE_SAVE_FAIL",
                        self.snapshot_failures
                    );
                }
            }
        }
        self.drain(now);
        self.collect_events();
    }

    /// Laat de tijd verstrijken: agent, runner, verkiezing, leader, de snapshot.
    pub(crate) fn tick(&mut self, now: Nanos) {
        let actions = self.agent.tick(now);
        self.run_actions(now, actions);
        self.runner.tick(now / MILLISECOND);
        self.elector.tick(now_ms());
        if now >= self.next_election {
            self.next_election = now.saturating_add(ELECTION_TICK);
            let connected = self.leader.as_ref().map_or(0, |l| l.agents().len());
            let reqs = self
                .election
                .tick(&mut self.elector, &mut self.agent, connected);
            self.requests(now, reqs);
            self.flush_notes();
        }
        if now >= self.next_leader {
            self.next_leader = now.saturating_add(LEADER_TICK);
            self.leader_tick(now);
        }
        self.persist_snapshot(now);
        self.drain(now);
        self.collect_events();
    }

    /// Sluit af: elke taak één stoppoging, de runner ruimt op, de lease los.
    pub(crate) fn shutdown(&mut self, now: Nanos) {
        self.agent.shutdown();
        self.drain(now);
        self.runner.shutdown();
        let reqs = self
            .election
            .step_down(&mut self.elector, &mut self.agent, true);
        self.requests(now, reqs);
    }

    fn flush_notes(&mut self) {
        for n in self.elector.take_notes() {
            eprintln!("{n}");
        }
    }

    // ---- De API's --------------------------------------------------------------

    fn handle(&mut self, port: Port, req: &Request, now: Nanos) -> Reply {
        match port {
            Port::Leader => self.leader_reply(req, now),
            Port::Agent => {
                let (resp, effect) = self.node_api.handle(&mut self.agent, now, None, req);
                self.effect(resp, effect, req, now)
            }
        }
    }

    /// Een verzoek aan de leader-API van deze node, met zijn [`LeaderEffect`] als antwoord.
    fn leader_reply(&mut self, req: &Request, now: Nanos) -> Reply {
        let (resp, effect) = self.leader_handle(req, now);
        match effect {
            LeaderEffect::None => Reply::Plain(resp),
            LeaderEffect::Tasks { agents, scope } => Reply::Tasks { agents, scope },
            LeaderEffect::Agent {
                endpoint,
                path,
                stream,
            } => self.admit(Reply::Agent {
                endpoint,
                path,
                stream,
            }),
            LeaderEffect::Events => self.admit(Reply::Subscribe {
                head: resp,
                seq: self.events.seq(),
            }),
        }
    }

    /// Laat een stroom toe als er plaats is ([`MAX_STREAMS`]); anders 503.
    /// Een antwoord dat geen stroom is, gaat ongeteld door.
    fn admit(&mut self, r: Reply) -> Reply {
        if !r.is_stream() {
            return r;
        }
        if self.streams >= MAX_STREAMS {
            return Reply::Plain(Response::error(
                503,
                &format!("too many open streams ({MAX_STREAMS}); try again later"),
            ));
        }
        self.streams += 1;
        r
    }

    /// Een verzoek aan de leader-API van deze node.
    fn leader_handle(&mut self, req: &Request, now: Nanos) -> (Response, LeaderEffect) {
        let Some(leader) = self.leader.as_mut() else {
            if req.path == "/health" {
                return (Response::error(503, "not the leader"), LeaderEffect::None);
            }
            let who = self.agent.leader_addr();
            let msg = if who.is_empty() {
                String::from("this node is not the leader, and no leader is known")
            } else {
                format!("this node is not the leader; the leader is {who}")
            };
            return (Response::error(503, &msg), LeaderEffect::None);
        };
        let mut net = Net {
            agent: &mut self.agent,
            now,
            http: &self.http,
            key: &self.key,
        };
        let mut cluster = LeaderCluster::new(leader, &mut net).with_events(&mut self.events);
        self.leader_api.handle(&mut cluster, now, req)
    }

    /// Haalt de meldingen van de leader in de rij van `/v1/events`.
    fn collect_events(&mut self) {
        let Some(l) = self.leader.as_mut() else {
            return;
        };
        for e in l.drain_events() {
            self.events.push(&e);
        }
    }

    /// Wat een open stroom sinds zijn volgnummer mist.
    fn poll(&mut self, now: Nanos, poll: Poll) -> Chunk {
        let mut c = Chunk::default();
        match poll {
            Poll::Logs {
                task_id,
                stream,
                seq,
            } => match self.runner.logs(now / MILLISECOND, &task_id, stream) {
                Some(ring) => {
                    for line in ring.since(seq) {
                        api::data_frame(line, &mut c.text);
                    }
                    c.seq = ring.seq();
                    // Dicht is dicht: wat er nog stond, zit in deze hap.
                    c.done = ring.is_closed();
                }
                // Verlopen of nooit gekend: de stroom is af.
                None => c.done = true,
            },
            Poll::Events { seq } => {
                self.collect_events();
                if self.leader.is_none() {
                    // Een ex-leider houdt geen abonnees vast (Go: de
                    // lifecycle-context); de lezer verbindt met de nieuwe.
                    c.done = true;
                    return c;
                }
                c.seq = self.events.since(seq, &mut c.text);
            }
        }
        c
    }

    /// Voert een [`Effect`] van de agent-API uit.
    fn effect(&mut self, resp: Response, effect: Effect, req: &Request, now: Nanos) -> Reply {
        match effect {
            Effect::None => Reply::Plain(resp),
            // De leader is deze node: dezelfde handler in-proces.
            Effect::Proxy { ref leader, .. }
                if self.leader.is_some() && *leader == self.election.own_leader() =>
            {
                self.leader_reply(req, now)
            }
            Effect::Proxy { leader, stream } => self.admit(Reply::Proxy { leader, stream }),
            Effect::Logs { task_id, stream } => {
                let stream = match stream {
                    api::LogStream::Stdout => Stream::Stdout,
                    api::LogStream::Stderr => Stream::Stderr,
                };
                if api::is_follow(req) {
                    if self
                        .runner
                        .logs(now / MILLISECOND, &task_id, stream)
                        .is_none()
                    {
                        return Reply::Plain(Response::error(
                            404,
                            &format!("no logs for task {task_id}"),
                        ));
                    }
                    return self.admit(Reply::Follow {
                        head: resp,
                        task_id,
                        stream,
                    });
                }
                match self.runner.logs(now / MILLISECOND, &task_id, stream) {
                    Some(ring) => Reply::Events {
                        head: resp,
                        lines: ring.tail().map(String::from).collect(),
                    },
                    None => {
                        Reply::Plain(Response::error(404, &format!("no logs for task {task_id}")))
                    }
                }
            }
            // De host heeft geen kern om te flippen; NodeApi weigert al met 501.
            Effect::Flip { .. } => Reply::Plain(Response::error(
                501,
                "this node cannot flip its kernel (not a HopOS node)",
            )),
        }
    }

    // ---- De acties van de agent ------------------------------------------------

    /// Voert acties uit tot de agent niets meer vraagt (begrensd).
    fn drain(&mut self, now: Nanos) {
        for _ in 0..ACTION_ROUNDS {
            let actions = self.agent.take_actions();
            if actions.is_empty() {
                return;
            }
            self.run_actions(now, actions);
        }
    }

    fn run_actions(&mut self, now: Nanos, actions: Vec<Action>) {
        let ms = now / MILLISECOND;
        for a in actions {
            match a {
                Action::Start { task_id, job } => self.start(now, task_id, &job),
                Action::Stop { task_id, .. } => {
                    self.prep.cancel(&task_id);
                    self.ports.remove(&task_id);
                    if let Err(e) = self.runner.stop(ms, &task_id) {
                        eprintln!("hop: stop {task_id}: {e} HOP_STOP_FAILED");
                    }
                }
                Action::Poll { task_id, .. } => {
                    // Een taak in voorbereiding heeft nog geen proces.
                    if self.agent.is_starting(&task_id) {
                        continue;
                    }
                    let st = match self.runner.status(&task_id) {
                        RunState::Running => Status::Running,
                        RunState::Failed => {
                            if let Some(code) = self.runner.exit_code(&task_id) {
                                eprintln!("hop: task {task_id} exited with code {code}");
                            }
                            Status::Failed
                        }
                    };
                    self.agent.on_status(now, &task_id, st);
                }
                Action::Probe { task_id, probe } => {
                    let _ = self.probes.send(ProbeJob { task_id, probe });
                }
                Action::Notify { job, event } => self.notify(now, &job, event),
                // Op de host is er geen overdracht: een herstart van de daemon
                // begint schoon (Go: `Init`), want de processen van de vorige
                // zijn niet over te nemen. De gewenste staat staat bij de leader.
            }
        }
    }

    /// Een [`Action::Start`]: poorten toewijzen, en het trage deel naar een werker.
    fn start(&mut self, now: Nanos, task_id: String, job: &Job) {
        let driver = job.driver();
        if driver == Driver::Hop {
            eprintln!(
                "hop: job {} task {task_id}: the hop driver needs a HopOS node HOP_JOB_FAILED",
                job.name
            );
            self.agent
                .on_started(now, &task_id, driver, Err(StartError::Failed));
            return;
        }
        let mut ports = BTreeMap::new();
        for (name, &p) in job.ports.iter() {
            let port = if p == 0 { free_port().unwrap_or(0) } else { p };
            ports.insert(String::from(name), port);
        }
        let spec = TaskSpec {
            task_id: task_id.clone(),
            job_name: job.name.clone(),
            command: job.command.clone(),
            image: job.image.clone(),
            user: job.user.clone(),
            env: to_btree(&job.env),
            ports: ports.clone(),
            volumes: to_btree(&job.volumes),
            cpu_shares: job.cpu_shares,
            memory_limit: job.memory_limit,
            artifact: job.artifacts.first().cloned(),
            node_attrs: self.agent.attributes().clone(),
        };
        self.ports.insert(task_id.clone(), ports);
        self.prep.submit(PrepJob {
            task_id,
            driver,
            spec: Box::new(spec),
        });
    }

    /// Het snelle deel van een start, na de voorbereiding.
    fn launch(
        &mut self,
        now: Nanos,
        task_id: &str,
        driver: Driver,
        spec: &TaskSpec,
        result: Result<runner::host::Prepared, String>,
    ) {
        let ports = self.ports.remove(task_id).unwrap_or_default();
        if !self.agent.is_starting(task_id) {
            // Gestopt tijdens de voorbereiding: niet meer starten.
            if let Ok(p) = result {
                self.runner.discard(p);
            }
            return;
        }
        let outcome = result.and_then(|p| {
            self.runner
                .launch(now / MILLISECOND, driver, spec, p)
                .map_err(|e| e.to_string())
        });
        match outcome {
            Ok(pid) => {
                eprintln!(
                    "hop: job {} task {task_id} started HOP_JOB_PLACED pid={pid}",
                    spec.job_name
                );
                let mut placed = Map::new();
                for (k, v) in ports {
                    let _ = placed.insert(k, v);
                }
                let ok = StartOk {
                    pid: i64::from(pid),
                    ports: placed,
                };
                self.agent.on_started(now, task_id, driver, Ok(ok));
            }
            Err(why) => {
                eprintln!(
                    "hop: job {} task {task_id} did not start: {why} HOP_JOB_FAILED",
                    spec.job_name
                );
                self.agent
                    .on_started(now, task_id, driver, Err(StartError::Failed));
            }
        }
    }

    /// Een taakgebeurtenis naar de leader (`POST /v1/notify`), in-proces als die hier woont.
    fn notify(&mut self, now: Nanos, job: &str, event: Event) {
        if let Some(l) = self.leader.as_mut() {
            if event == Event::Unplaceable {
                let id = String::from(self.agent.id());
                let mut net = Net {
                    agent: &mut self.agent,
                    now,
                    http: &self.http,
                    key: &self.key,
                };
                let _ = l.mark_unplaced(&id, job, &mut net);
            }
            // In de rij van `/v1/events` mét het event, zoals een notify
            // van een andere agent via de API (LeaderCluster::with_events).
            self.events
                .push_topic(&format!("job:{job}:{}", event.as_str()));
            return;
        }
        let leader = self.agent.leader_addr();
        if leader.is_empty() {
            return;
        }
        let body = format!(
            r#"{{"job":{},"event":"{}","agent":{}}}"#,
            quote(job),
            event.as_str(),
            quote(self.agent.id())
        );
        let _ = self.link.send(LinkJob::Notify {
            url: format!("http://{leader}/v1/notify"),
            body,
        });
    }

    // ---- De verkiezing ---------------------------------------------------------

    /// Voert de aanroepen van de verkiezing uit; in-proces waar het kan.
    fn requests(&mut self, now: Nanos, mut reqs: Vec<LinkRequest>) {
        for _ in 0..LINK_ROUNDS {
            if reqs.is_empty() {
                return;
            }
            let mut next = Vec::new();
            for r in reqs {
                next.extend(self.request(now, r));
            }
            reqs = next;
        }
    }

    fn request(&mut self, now: Nanos, r: LinkRequest) -> Vec<LinkRequest> {
        match &r {
            LinkRequest::Register { leader } => {
                let url = format!("http://{leader}/v1/agents");
                let body = self.register_body();
                let _ = self.link.send(LinkJob::Election { req: r, url, body });
                Vec::new()
            }
            LinkRequest::Heartbeat { leader } => {
                let url = format!("http://{leader}/v1/heartbeat");
                let body = format!(
                    r#"{{"id":{},"endpoint":{},"version":"{VERSION}","temp_milli_c":{}}}"#,
                    quote(self.agent.id()),
                    quote(self.agent.endpoint()),
                    crate::boot::cpu_temp_milli_c()
                );
                let _ = self.link.send(LinkJob::Election { req: r, url, body });
                Vec::new()
            }
            LinkRequest::SelfHeartbeat { .. } => {
                let known = self.leader.as_mut().map(|l| {
                    let telemetry = types::Telemetry {
                        temp_milli_c: crate::boot::cpu_temp_milli_c(),
                        ..types::Telemetry::default()
                    };
                    l.heartbeat(self.agent.id(), VERSION, telemetry, Time(now))
                });
                let result = match known {
                    Some(true) => Ok(()),
                    Some(false) => Err(LinkError::NotRegistered),
                    None => Err(LinkError::Failed),
                };
                self.election
                    .on_reply(&mut self.elector, &mut self.agent, &r, result)
            }
            LinkRequest::SelfRegister { .. } => {
                let result = if self.register_self(now) {
                    Ok(())
                } else {
                    Err(LinkError::Failed)
                };
                self.election
                    .on_reply(&mut self.elector, &mut self.agent, &r, result)
            }
            LinkRequest::StartLeader => {
                self.become_leader(now);
                Vec::new()
            }
            LinkRequest::StopLeader => {
                if self.leader.take().is_some() {
                    eprintln!("hop: stepped down as leader HOP_LEADER_STOPPED");
                }
                Vec::new()
            }
        }
    }

    /// De body van `POST /v1/agents`: wie we zijn en wat we al draaien.
    fn register_body(&self) -> String {
        let mut placed = String::new();
        for (name, n) in self.agent.placed_task_counts() {
            if !placed.is_empty() {
                placed.push(',');
            }
            placed.push_str(&quote(&name));
            placed.push(':');
            placed.push_str(&n.to_string());
        }
        format!(
            r#"{{"id":{},"endpoint":{},"version":"{VERSION}","placed":{{{placed}}}}}"#,
            quote(self.agent.id()),
            quote(self.agent.endpoint())
        )
    }

    /// Registreert de eigen agent bij de eigen leader, met wat hij draait.
    fn register_self(&mut self, now: Nanos) -> bool {
        let Some(l) = self.leader.as_mut() else {
            return false;
        };
        let me = types::Agent {
            id: String::from(self.agent.id()),
            endpoint: String::from(self.agent.endpoint()),
            version: String::from(VERSION),
            ..types::Agent::default()
        };
        let mut placed = Map::new();
        for (name, n) in self.agent.placed_task_counts() {
            let _ = placed.insert(name, u32::try_from(n).unwrap_or(u32::MAX));
        }
        let mut net = Net {
            agent: &mut self.agent,
            now,
            http: &self.http,
            key: &self.key,
        };
        match l.register_agent(me, placed, Time(now), &mut net) {
            Ok(ok) => ok,
            Err(e) => {
                eprintln!("hop: leader refused own agent: {e} HOP_LEADER_FAIL");
                l.agent(self.agent.id()).is_some()
            }
        }
    }

    /// Start de leader-helft: staat laden, zichzelf registreren, en op een schone boot de init-jobs.
    ///
    /// Go: `agentloop.BecomeLeader`. Een opslag die niet antwoordt is geen
    /// lege opslag: dan geen init-jobs (nooit zaaien op een storing).
    fn become_leader(&mut self, now: Nanos) {
        if self.leader.is_some() {
            return;
        }
        let mut l = Leader::new(String::from(self.agent.id()), MemStore::new());
        l.set_agent_timeout(self.node_dead);
        if self.clustered {
            l.enable_settle(Time(now));
        }
        let clean = match persist::load(&self.persist, self.load_wait) {
            Ok(snapshot) => match l.load_committed_state(snapshot.as_deref()) {
                Ok(loaded) => {
                    if loaded {
                        eprintln!(
                            "hop: committed cluster state loaded: {} job(s) HOP_STATE_LOADED",
                            l.jobs().len()
                        );
                    }
                    !loaded
                }
                Err(e) => {
                    eprintln!("hop: committed state unreadable: {e} HOP_STATE_FAIL");
                    false
                }
            },
            Err(e) => {
                eprintln!("hop: committed state not loaded: {e} HOP_STATE_FAIL");
                false
            }
        };
        self.leader = Some(l);
        self.register_self(now);
        eprintln!(
            "hop: became leader, agent {} registered at {} HOP_LEADER",
            self.agent.id(),
            self.election.own_leader()
        );
        if clean && !self.init_jobs.is_empty() {
            self.seed_init_jobs(now);
        }
    }

    fn seed_init_jobs(&mut self, now: Nanos) {
        let Some(l) = self.leader.as_mut() else {
            return;
        };
        if !l.jobs().is_empty() {
            return;
        }
        let jobs = match leader::decode_init_jobs(&self.init_jobs) {
            Ok(j) => j,
            Err(e) => {
                eprintln!("hop: cluster.init_jobs: {e} HOP_INIT_FAIL");
                return;
            }
        };
        let n = jobs.len();
        let mut net = Net {
            agent: &mut self.agent,
            now,
            http: &self.http,
            key: &self.key,
        };
        match l.seed_init_jobs(jobs, &mut net) {
            Ok(()) => eprintln!("hop: clean boot, seeded {n} init job(s) HOP_INIT_SEEDED"),
            Err(e) => eprintln!("hop: init jobs: {e} HOP_INIT_FAIL"),
        }
    }

    fn leader_tick(&mut self, now: Nanos) {
        let Some(l) = self.leader.as_mut() else {
            return;
        };
        let mut net = Net {
            agent: &mut self.agent,
            now,
            http: &self.http,
            key: &self.key,
        };
        if let Err(e) = l.tick(Time(now), &mut net) {
            eprintln!("hop: leader tick: {e}");
        }
    }

    fn persist_snapshot(&mut self, now: Nanos) {
        let Some(l) = self.leader.as_mut() else {
            return;
        };
        match l.poll_snapshot(Time(now)) {
            Ok(Some(s)) => {
                if self.persist.send(PersistOp::Save(s.into_bytes())).is_err() {
                    l.snapshot_failed();
                }
            }
            Ok(None) => {}
            Err(e) => eprintln!("hop: snapshot: {e}"),
        }
    }
}

/// Een string als JSON-string.
fn quote(s: &str) -> String {
    let mut out = String::new();
    if types::json::write_string(s, &mut out).is_err() {
        return String::from("\"\"");
    }
    out
}

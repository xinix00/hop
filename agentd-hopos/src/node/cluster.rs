//! De node in een cluster: de verkiezing, het leider worden en aftreden, en de weg naar de andere nodes.
//!
//! Zonder lock-config (`HOPOS_LOCK_URL`, `HOPOS_S3_*`) is de node de
//! standalone leader die hij altijd was, en staat hier niets aan. Met een
//! lock doet hij wat de daemon doet (`agentd/src/node.rs`), met dezelfde
//! bouwstenen:
//!
//! - [`agent::Election`] en de gedeelde [`agent::Elector`]: leider worden via
//!   de lease, of de leader kennen en daar registreren en heartbeaten. De
//!   verkiezing tikt elke [`ELECTION_TICK`];
//! - de aanroepen die het net op gaan, gaan als opdracht naar een taak (zie
//!   [`crate::mail`]): de lease naar de lease-taak, register en heartbeat naar
//!   de link-taak, de staat naar de staat-taak, en wat de leader bij andere
//!   agents vraagt naar de dispatch-taak ([`crate::relay`]). De antwoorden
//!   komen als [`Mail`] terug in [`Node::on_mail`];
//! - leider worden is twee stappen: de verkiezing zegt het, de staat-taak
//!   leest de gecommitte staat, en pas met die staat gaat de leader-API open
//!   (tot dan 503). Een opslag die niet antwoordt is geen lege opslag: dan
//!   geen init-jobs (Go: nooit zaaien op een storing);
//! - aftreden: eerst de leader-helft weg, dan pas de lease los (de volgorde
//!   van [`agent::Election::step_down`]);
//! - de klok: een lease is een tijd op de wandklok van de schrijver, en
//!   elke node vergelijkt hem met de zijne. De kern van HopOS zet de klok
//!   vast op een datum tot SNTP lukt (`HOPOS_CLOCK_FIXED`); een node met
//!   zo'n klok ziet elke lease als levend (hij neemt nooit over) of schrijft
//!   er een die al verlopen is (een ander neemt hem meteen af). Daarom doet
//!   een geclusterde node pas mee als zijn klok gezet is: tot dan geen
//!   claim en geen verkiezing, en één regel die zegt waarom
//!   (`HOP_CLUSTER_NO_CLOCK`). De eerste claim na de klok is de boot-claim:
//!   is de lock vrij, dan leidt de node meteen.
//!
//! Een submodule van `node`, zodat hij bij de staat van de [`Node`] kan
//! zonder dat die staat buiten de node zichtbaar wordt.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use agent::{Discoverer, Election, Elector, LeaseReply, LinkError, Request as LinkRequest};
use api::{Response, TasksScope};
use leader::{Leader, MemStore, RunReply};
use types::time::SECOND;
use types::{Map, Nanos, Time};

use super::{Images, Node};
use crate::VERSION;
use crate::env::BootConfig;
use crate::forward::{Forward, Routed, Source};
use crate::local::Local;
use crate::lock::ClusterConfig;
use crate::mail::{
    DispatchQueue, LeaseOps, LeaseQueue, LinkJob, LinkQueue, Mail, StateOp, StateQueue,
};
use crate::relay::Relay;
use hop_http::Reply;
use runner::SystemApi;

/// Hoe vaak de verkiezing tikt: register of heartbeat bij de leader, of de
/// lease vernieuwen (Go: `loop.Run(10 s)`, de daemon ook).
pub(crate) const ELECTION_TICK: Nanos = 10 * SECOND;

/// Hoeveel rondes vervolg-aanroepen de verkiezing na één antwoord krijgt.
const LINK_ROUNDS: usize = 4;

/// Wat de binary een geclusterde node meegeeft.
pub struct ClusterParts {
    /// De lock, de sleutels en de lease.
    pub cfg: ClusterConfig,
    /// Of de wandklok gezet is (SNTP): pas dan doet de node mee.
    pub clock_ok: fn() -> bool,
    /// De rij naar de lease-taak.
    pub lease: &'static LeaseQueue,
    /// De rij naar de link-taak.
    pub link: &'static LinkQueue,
    /// De rij naar de staat-taak.
    pub state: &'static StateQueue,
    /// De rij naar de dispatch-taak.
    pub dispatch: &'static DispatchQueue,
    /// Nu in milliseconden op de wandklok (de lease vergelijkt tijden van nodes).
    pub wall_ms: fn() -> u64,
    /// De init-jobs (een JSON-array), gezaaid als deze node leider wordt op
    /// een schone staat.
    pub init_jobs: Option<String>,
    /// Na hoe lang zonder heartbeat een agent dood is.
    pub node_dead: Nanos,
}

/// De clusterstaat van de node.
pub struct Cluster {
    election: Election,
    elector: Elector<LeaseOps>,
    link: &'static LinkQueue,
    state: &'static StateQueue,
    dispatch: &'static DispatchQueue,
    relay: Relay,
    /// De leader-API is open: de verkiezing zei "leid", en de staat is geladen.
    live: bool,
    /// De verkiezing zei "leid"; de staat-taak leest de gecommitte staat.
    loading: bool,
    init_jobs: Option<String>,
    node_dead: Nanos,
    next_election: Nanos,
    snapshot_failures: u32,
    dropped_total: u64,
    wall_ms: fn() -> u64,
    clock_ok: fn() -> bool,
    /// De klok is gezet en de boot-claim is verstuurd: de node doet mee.
    joined: bool,
    /// De boot-claim staat uit; een raak antwoord maakt de node meteen leider.
    boot_claim: bool,
    label: String,
}

impl Cluster {
    /// De boeken van de agents op andere nodes (voor de leader-transport).
    pub(crate) fn relay_mut(&mut self) -> &mut Relay {
        &mut self.relay
    }

    /// Of de leader-API open is.
    pub fn is_live(&self) -> bool {
        self.live
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

/// De temperatuur van de kern (CTRL_TEMP, HopOS alpha.9); 0 is geen meting.
fn temp_milli_c() -> i64 {
    applib::app().map_or(0, |a| i64::from(a.ctrl().temp_milli_c()))
}

/// De weigering van een doorgifte waar alleen een antwoord kan (de tests
/// en [`Node::handle`]); de binary voert hem uit in `forward::serve`.
pub(crate) fn refuse_forward(f: &Forward) -> Response {
    let to = match f {
        Forward::Leader { addr, .. } => format!("the leader {addr}"),
        Forward::Agent { endpoint, .. } => format!("agent {endpoint}"),
        Forward::Tasks { .. } => String::from("the agents on other nodes"),
    };
    Response::error(
        502,
        &format!("hop on HopOS: this request goes to {to}, which only a connection task can do"),
    )
}

impl<S: SystemApi + agent::Store, I: Images> Node<S, I> {
    /// Een geclusterde node: de verkiezing beslist wie leidt, niet de boot.
    ///
    /// De leader-helft staat dicht tot de verkiezing hem opent
    /// ([`Node::cluster_boot`] met een raak boot-claim, of later een
    /// gewonnen claim), en dan pas met de gecommitte staat van de cluster.
    pub fn new_clustered(
        cfg: &BootConfig,
        sys: S,
        images: I,
        now: Nanos,
        parts: ClusterParts,
    ) -> Self {
        let mut node = Self::build(cfg, sys, images, now);
        node.agent.set_leader_addr("");
        // Nooit "houdend" bij de start: de boot-claim komt pas met de klok.
        let elector = Elector::new(
            LeaseOps(parts.lease),
            parts.wall_ms,
            parts.cfg.ttl_ms,
            false,
        );
        node.cluster = Some(Cluster {
            election: Election::new(&cfg.node_ip, cfg.port),
            elector,
            link: parts.link,
            state: parts.state,
            dispatch: parts.dispatch,
            relay: Relay::new(),
            live: false,
            loading: false,
            init_jobs: parts.init_jobs,
            node_dead: parts.node_dead,
            next_election: now,
            snapshot_failures: 0,
            dropped_total: 0,
            wall_ms: parts.wall_ms,
            clock_ok: parts.clock_ok,
            joined: false,
            boot_claim: false,
            label: parts.cfg.label(),
        });
        node.lines.push(format!(
            "hop: cluster {} with lock {}, lease {} s HOP_CLUSTER",
            cfg.cluster,
            parts.cfg.label(),
            parts.cfg.ttl_ms / 1000
        ));
        node
    }

    /// De eerste stap na de start: doe mee zodra de klok gezet is.
    pub fn cluster_boot(&mut self, now: Nanos) {
        self.join(now);
        self.after_cluster(now);
    }

    /// Doet mee als de klok gezet is: de boot-claim. Geeft of de node meedoet.
    fn join(&mut self, now: Nanos) -> bool {
        let Some(c) = self.cluster.as_mut() else {
            return false;
        };
        if c.joined {
            return true;
        }
        if !(c.clock_ok)() {
            self.once(
                "no-clock",
                String::from(
                    "hop: the wall clock is not synced (SNTP), so lease times mean nothing; this node joins the cluster once it is HOP_CLUSTER_NO_CLOCK",
                ),
            );
            return false;
        }
        c.joined = true;
        // Eén claim vóór de eerste tik van de verkiezing: is de lock vrij,
        // dan leidt deze node meteen, zonder de takeover-drempel van vier
        // tikken (zoals de boot-claim van de daemon).
        c.boot_claim = true;
        let _ = c.elector.try_become_leader();
        c.next_election = now.saturating_add(ELECTION_TICK);
        self.lines.push(format!(
            "hop: wall clock synced; joining cluster lock {} HOP_CLUSTER_JOIN",
            c.label
        ));
        true
    }

    /// Of deze node de leader-API bedient: standalone altijd, in een cluster alleen live.
    pub(super) fn leads(&self) -> bool {
        self.cluster.as_ref().is_none_or(Cluster::is_live)
    }

    /// De 503 van een leader-API die (nog) niet leidt, of `None` als hij leidt.
    pub(super) fn not_leading(&self, req: &api::Request) -> Option<Response> {
        let c = self.cluster.as_ref()?;
        if c.live {
            return None;
        }
        if req.path == "/health" {
            return Some(Response::error(503, "not the leader"));
        }
        if c.loading {
            return Some(Response::error(
                503,
                "this node is becoming the leader; the cluster state is loading",
            ));
        }
        let who = self.agent.leader_addr();
        let msg = if who.is_empty() {
            String::from("this node is not the leader, and no leader is known")
        } else {
            format!("this node is not the leader; the leader is {who}")
        };
        Some(Response::error(503, &msg))
    }

    /// De `X-Hop-Auth` van een `GET` op `path`, of `None` zonder sleutel.
    fn get_auth(&self, path: &str) -> Option<String> {
        auth::sign_call(&self.key, "GET", path, b"")
            .map(|s| String::from_utf8_lossy(&s).into_owned())
    }

    /// De rondgang van `/v1/tasks` als doorgifte, als er agents op andere nodes zijn.
    pub(super) fn tasks_forward(
        &self,
        agents: &[(String, String)],
        scope: &TasksScope,
    ) -> Option<Forward> {
        self.cluster.as_ref()?;
        if agents.iter().all(|(id, _)| id == self.agent.id()) {
            return None;
        }
        let mut list = Vec::new();
        list.try_reserve_exact(agents.len()).ok()?;
        for (id, endpoint) in agents {
            let src = if id == self.agent.id() {
                Source::Known(super::own_tasks(&self.agent))
            } else {
                Source::Ask(endpoint.clone())
            };
            list.push((id.clone(), src));
        }
        Some(Forward::Tasks {
            agents: list,
            auth: self.get_auth("/tasks"),
            scope: scope.clone(),
        })
    }

    /// Een doorgifte naar een agent op een andere node, als deze node geclusterd is.
    pub(super) fn agent_forward(
        &self,
        endpoint: &str,
        path: &str,
        stream: bool,
    ) -> Option<Forward> {
        self.cluster.as_ref()?;
        Some(Forward::Agent {
            endpoint: String::from(endpoint),
            path: String::from(path),
            auth: self.get_auth(path),
            stream,
        })
    }

    /// Laat een doorgegeven stroom toe als er plaats is (`MAX_STREAMS`); anders 503.
    pub(super) fn admit_forward(&mut self, f: Forward) -> Routed {
        if !f.is_stream() {
            return Routed::Forward(f);
        }
        if self.streams >= super::MAX_STREAMS {
            return Routed::Reply(Reply::Plain(Response::error(
                503,
                &format!(
                    "too many open streams ({}); try again later",
                    super::MAX_STREAMS
                ),
            )));
        }
        self.streams += 1;
        Routed::Forward(f)
    }

    /// Een taakgebeurtenis naar de leader op een andere node (`POST /v1/notify`).
    pub(super) fn notify_leader(&mut self, job: &str, event: agent::Event) {
        let Some(c) = self.cluster.as_ref() else {
            return;
        };
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
        let job = LinkJob::Notify {
            url: format!("http://{leader}/v1/notify"),
            body,
        };
        if c.link.try_send(job).is_err() {
            self.once(
                "link-full",
                String::from(
                    "hop: the link queue is full; a notify to the leader was dropped HOP_LINK_FULL",
                ),
            );
        }
    }

    /// De tik van de cluster: de lease-cadans, de verkiezing, de snapshot en de takenlijsten.
    pub(super) fn cluster_tick(&mut self, now: Nanos) {
        if !self.join(now) {
            return;
        }
        let Some(c) = self.cluster.as_mut() else {
            return;
        };
        c.elector.tick((c.wall_ms)());
        if now >= c.next_election {
            c.next_election = now.saturating_add(ELECTION_TICK);
            let connected = if c.live {
                self.leader.agents().len()
            } else {
                0
            };
            let reqs = c.election.tick(&mut c.elector, &mut self.agent, connected);
            self.requests(now, reqs);
        }
        self.persist_snapshot(now);
        self.after_cluster(now);
    }

    /// Vraagt de takenlijsten van de agents op andere nodes opnieuw (elke leader-tik).
    pub(super) fn refresh_remote_tasks(&mut self) {
        let Some(c) = self.cluster.as_mut() else {
            return;
        };
        if !c.live {
            return;
        }
        let own = self.agent.id();
        c.relay
            .refresh(self.leader.agents().iter().filter(|a| a.id != own));
    }

    /// Na elke invoer: de regels van de elector, en de aanroepen van de leader naar de dispatch-taak.
    pub(super) fn after_cluster(&mut self, now: Nanos) {
        let Some(c) = self.cluster.as_mut() else {
            return;
        };
        for n in c.elector.take_notes() {
            self.lines.push(n);
        }
        let unsent = c.relay.flush(c.dispatch);
        let dropped = c.relay.take_dropped();
        if dropped > 0 {
            c.dropped_total = c.dropped_total.saturating_add(dropped);
            let total = c.dropped_total;
            if total <= 3 || total.is_multiple_of(100) {
                self.lines.push(format!(
                    "hop: {dropped} call(s) to agents on other nodes did not fit the dispatch queue ({total} so far); the next reconcile retries HOP_RELAY_FULL"
                ));
            }
        }
        // Een `/run` die de rij niet haalde, is een agent die niet antwoordde.
        for (agent, job) in unsent {
            self.on_ran(now, &agent, &job, RunReply::Unreachable);
        }
    }

    /// Verwerkt een antwoord van een van de taken van de cluster.
    pub fn on_mail(&mut self, mail: Mail, now: Nanos) {
        if self.cluster.is_none() {
            return;
        }
        match mail {
            Mail::Lease(r) => {
                let Some(c) = self.cluster.as_mut() else {
                    return;
                };
                let won = matches!(r, LeaseReply::Claimed(true));
                let boot = matches!(r, LeaseReply::Claimed(_)) && c.boot_claim;
                c.elector.on_reply(r);
                if boot {
                    c.boot_claim = false;
                    if won {
                        let reqs = c
                            .election
                            .become_leader_now(&mut c.elector, &mut self.agent);
                        self.requests(now, reqs);
                    }
                }
            }
            Mail::Note(line) => self.lines.push(line),
            Mail::Link { req, result } => {
                let Some(c) = self.cluster.as_mut() else {
                    return;
                };
                let reqs = c
                    .election
                    .on_reply(&mut c.elector, &mut self.agent, &req, result);
                self.requests(now, reqs);
            }
            Mail::Loaded(got) => self.finish_leading(now, got),
            Mail::SaveFailed(why) => self.snapshot_failed(&why),
            Mail::Ran { agent, job, reply } => self.on_ran(now, &agent, &job, reply),
            Mail::Tasks { agent, tasks } => {
                if let Some(c) = self.cluster.as_mut() {
                    c.relay.on_tasks(&agent, tasks);
                }
            }
        }
        self.after_cluster(now);
    }

    /// Een agent op een andere node antwoordde op `POST /run`.
    ///
    /// Aangenomen is wat de leader al dacht. Een weigering boekt de
    /// plaatsing af (zoals een hand-back van een agent) en blijft even
    /// staan, zodat de reconcile die volgt een andere agent kiest.
    fn on_ran(&mut self, now: Nanos, agent: &str, job: &str, reply: RunReply) {
        if reply == RunReply::Accepted {
            return;
        }
        let Some(c) = self.cluster.as_mut() else {
            return;
        };
        c.relay.refuse(now, agent, job, reply);
        if !c.live {
            return;
        }
        self.lines.push(format!(
            "hop: agent {agent} refused job {job} ({reply:?}); placing it elsewhere HOP_RELAY_REFUSED"
        ));
        let pool = self.runner.pool_largest();
        let mut net = Local {
            agent: &mut self.agent,
            now,
            pool_largest: pool,
            relay: Some(&mut c.relay),
        };
        if let Err(e) = self.leader.mark_unplaced(agent, job, &mut net) {
            self.lines.push(format!("hop: leader after a refusal: {e}"));
        }
    }

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
                self.send_link(LinkJob::Election { req: r, url, body })
            }
            LinkRequest::Heartbeat { leader } => {
                let url = format!("http://{leader}/v1/heartbeat");
                let body = format!(
                    r#"{{"id":{},"endpoint":{},"version":"{VERSION}","temp_milli_c":{}}}"#,
                    quote(self.agent.id()),
                    quote(self.agent.endpoint()),
                    temp_milli_c()
                );
                self.send_link(LinkJob::Election { req: r, url, body })
            }
            LinkRequest::SelfHeartbeat { .. } => {
                let live = self.leads();
                let known = live.then(|| {
                    self.leader
                        .heartbeat(self.agent.id(), VERSION, temp_milli_c(), Time(now))
                });
                let result = match known {
                    Some(true) => Ok(()),
                    Some(false) => Err(LinkError::NotRegistered),
                    None => Err(LinkError::Failed),
                };
                self.election_reply(&r, result)
            }
            LinkRequest::SelfRegister { .. } => {
                let result = if self.leads() && self.register_own(now) {
                    Ok(())
                } else {
                    Err(LinkError::Failed)
                };
                self.election_reply(&r, result)
            }
            LinkRequest::StartLeader => {
                self.start_leading();
                Vec::new()
            }
            LinkRequest::StopLeader => {
                self.stop_leading();
                Vec::new()
            }
        }
    }

    fn election_reply(
        &mut self,
        r: &LinkRequest,
        result: Result<(), LinkError>,
    ) -> Vec<LinkRequest> {
        let Some(c) = self.cluster.as_mut() else {
            return Vec::new();
        };
        c.election
            .on_reply(&mut c.elector, &mut self.agent, r, result)
    }

    /// Zet een register of heartbeat in de rij van de link-taak; het vervolg
    /// van de verkiezing als de rij vol is (dat telt als een mislukte
    /// aanroep, zoals een leader die niet antwoordt).
    fn send_link(&mut self, job: LinkJob) -> Vec<LinkRequest> {
        let Some(c) = self.cluster.as_ref() else {
            return Vec::new();
        };
        let Err(sync::Full(back)) = c.link.try_send(job) else {
            return Vec::new();
        };
        self.once(
            "link-full",
            String::from(
                "hop: the link queue is full; a call to the leader counts as failed HOP_LINK_FULL",
            ),
        );
        match back {
            LinkJob::Election { req, .. } => self.election_reply(&req, Err(LinkError::Failed)),
            LinkJob::Notify { .. } => Vec::new(),
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
            placed.push_str(&format!("{n}"));
        }
        format!(
            r#"{{"id":{},"endpoint":{},"version":"{VERSION}","placed":{{{placed}}}}}"#,
            quote(self.agent.id()),
            quote(self.agent.endpoint())
        )
    }

    /// Registreert de eigen agent bij de eigen leader, met wat hij draait.
    fn register_own(&mut self, now: Nanos) -> bool {
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
        let pool = self.runner.pool_largest();
        let mut net = Local {
            agent: &mut self.agent,
            now,
            pool_largest: pool,
            relay: self.cluster.as_mut().map(Cluster::relay_mut),
        };
        match self.leader.register_agent(me, placed, Time(now), &mut net) {
            Ok(ok) => ok,
            Err(e) => {
                self.lines.push(format!(
                    "hop: leader refused own agent: {e} HOP_LEADER_FAIL"
                ));
                self.leader.agent(self.agent.id()).is_some()
            }
        }
    }

    /// De verkiezing zei "leid": de staat-taak leest de gecommitte staat.
    fn start_leading(&mut self) {
        let Some(c) = self.cluster.as_mut() else {
            return;
        };
        if c.live || c.loading {
            return;
        }
        if c.state.try_send(StateOp::Load).is_ok() {
            c.loading = true;
            self.lines.push(format!(
                "hop: won the lease; loading the cluster state from {} HOP_LEADER_LOADING",
                c.label
            ));
        } else {
            // De rij is vol (snapshots van een vorige termijn): meteen
            // leiden zou zonder staat zijn, dus de volgende tik opnieuw.
            self.lines.push(String::from(
                "hop: the state queue is full; becoming leader waits for the next tick HOP_STATE_FAIL",
            ));
        }
    }

    /// De gecommitte staat is er (of niet): de leader-helft gaat open.
    fn finish_leading(&mut self, now: Nanos, got: Result<Option<Vec<u8>>, String>) {
        let Some(c) = self.cluster.as_mut() else {
            return;
        };
        if !c.loading {
            return;
        }
        c.loading = false;
        if !c.election.is_leading() {
            // Afgetreden terwijl de staat onderweg was.
            return;
        }
        let mut l = Leader::new(String::from(self.agent.id()), MemStore::new());
        l.set_agent_timeout(c.node_dead);
        l.enable_settle(Time(now));
        let clean = match got {
            Ok(snapshot) => match l.load_committed_state(snapshot.as_deref()) {
                Ok(true) => {
                    self.lines.push(format!(
                        "hop: committed cluster state loaded: {} job(s) HOP_STATE_LOADED",
                        l.jobs().len()
                    ));
                    false
                }
                Ok(false) => true,
                Err(e) => {
                    self.lines.push(format!(
                        "hop: committed state unreadable: {e} HOP_STATE_FAIL"
                    ));
                    false
                }
            },
            Err(e) => {
                self.lines.push(format!(
                    "hop: committed state not loaded: {e} HOP_STATE_FAIL"
                ));
                false
            }
        };
        self.leader = l;
        c.relay.clear();
        c.live = true;
        let own = String::from(c.election.own_leader());
        let seed = if clean { c.init_jobs.clone() } else { None };
        self.register_own(now);
        self.lines.push(format!(
            "hop: became leader, agent {} registered HOP_LEADER leader={own}",
            self.agent.id()
        ));
        if let Some(specs) = seed {
            self.seed_cluster_jobs(now, &specs);
        }
    }

    /// Zaait de init-jobs in een leader zonder jobs (een schone clusterstaat).
    fn seed_cluster_jobs(&mut self, now: Nanos, specs: &str) {
        if !self.leader.jobs().is_empty() {
            return;
        }
        let jobs = types::json::parse_str(specs)
            .map_err(|e| format!("{e}"))
            .and_then(|v| {
                let list = v
                    .as_array()
                    .ok_or_else(|| String::from("not a JSON array"))?;
                leader::decode_init_jobs(list).map_err(|e| format!("{e}"))
            });
        let jobs = match jobs {
            Ok(j) => j,
            Err(e) => {
                self.lines
                    .push(format!("hop: init jobs: {e} HOP_INIT_FAIL"));
                return;
            }
        };
        let n = jobs.len();
        let pool = self.runner.pool_largest();
        let mut net = Local {
            agent: &mut self.agent,
            now,
            pool_largest: pool,
            relay: self.cluster.as_mut().map(Cluster::relay_mut),
        };
        match self.leader.seed_init_jobs(jobs, &mut net) {
            Ok(()) => self.lines.push(format!(
                "hop: clean cluster state, seeded {n} init job(s) HOP_INIT_SEEDED"
            )),
            Err(e) => self
                .lines
                .push(format!("hop: init jobs: {e} HOP_INIT_FAIL")),
        }
    }

    /// Treedt af: de leader-helft dicht en leeg; de lease laat de verkiezing los.
    fn stop_leading(&mut self) {
        let Some(c) = self.cluster.as_mut() else {
            return;
        };
        let was = c.live;
        c.live = false;
        c.loading = false;
        c.relay.clear();
        // Wat de ex-leader nog bij andere agents wilde (een `/run` van een
        // reconcile), gaat niet meer: de nieuwe leader beslist. De rij is
        // MPMC (Vyukov); de eigenaar leest hem hier leeg zonder waker, de
        // dispatch-taak blijft de enige die erop wacht.
        while c.dispatch.try_recv().is_some() {}
        self.leader = Leader::new(String::from(self.agent.id()), MemStore::new());
        if was {
            self.lines.push(String::from(
                "hop: stepped down as leader HOP_LEADER_STOPPED",
            ));
        }
    }

    /// Schrijft de snapshot van de leader weg als hij dat vraagt.
    fn persist_snapshot(&mut self, now: Nanos) {
        let Some(c) = self.cluster.as_mut() else {
            return;
        };
        if !c.live {
            return;
        }
        match self.leader.poll_snapshot(Time(now)) {
            Ok(Some(s)) => {
                if c.state.try_send(StateOp::Save(s.into_bytes())).is_err() {
                    self.leader.snapshot_failed();
                }
            }
            Ok(None) => {}
            Err(e) => self.lines.push(format!("hop: snapshot: {e}")),
        }
    }

    fn snapshot_failed(&mut self, why: &str) {
        let Some(c) = self.cluster.as_mut() else {
            return;
        };
        if c.live {
            self.leader.snapshot_failed();
        }
        c.snapshot_failures = c.snapshot_failures.saturating_add(1);
        let n = c.snapshot_failures;
        // Luid, drie keer, dan tellen (handboek §6).
        if n <= 3 || n.is_multiple_of(100) {
            self.lines.push(format!(
                "hop: cluster state not saved ({n}x): {why} HOP_STATE_SAVE_FAIL"
            ));
        }
    }
}

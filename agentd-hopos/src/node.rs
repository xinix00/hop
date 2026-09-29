//! De eigenaar van alle staat van de node: agent, leader, runner en de twee API's.
//!
//! Eén struct achter `&mut self` (handboek §1). Een verzoek ([`Node::handle`])
//! en de tik ([`Node::tick`]) zijn de enige ingangen; wat de agent daarna
//! wil (starten, stoppen, pollen, wegschrijven) voert de node meteen uit,
//! in de volgorde waarin de agent het vroeg.
//!
//! Die ingangen zijn `async`: een actie die de kern raakt (een slot, een
//! brok image, een status, de staat op hopfs) wacht met `.await` op de
//! verbinding, en de eigenaar-taak geeft dan de core terug. De netstack,
//! de verbindingstaken en de rest draaien intussen door; alleen de staat van
//! de node wacht, want die heeft één eigenaar (handboek §1 en §4).
//!
//! De traits [`Images`] en [`Sink`] zijn daardoor niet object-safe (een
//! methode die een future geeft, kan niet in een vtable); de downloader
//! krijgt de sink daarom als generieke parameter in plaats van als
//! `&mut dyn Sink`. Er is per node één downloader en één soort sink, dus dat
//! kost niets.
//!
//! De markers voor de console verzamelt hij als regels ([`Node::take_lines`]);
//! de binary zet ze met `applib::log!` op het log, een test leest ze.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;

use agent::{Action, Agent, Event, Settings, StartError, StartOk, Status};
use api::{Effect, LeaderApi, LeaderCluster, NodeApi, Request, Response};
use hop_http::{Reply, refuse};
use leader::{Leader, MemStore};
use runner::{
    HopRunner, LogPolicy, RunState, Runner, StartRequest, Started, Stream, SystemApi, TaskRef,
};
use types::time::{MILLISECOND, SECOND};
use types::{Driver, Job, Map, Nanos, Time};

use crate::VERSION;
use crate::env::BootConfig;
use crate::local::Local;

/// Hoe vaak de leader tikt (dode agents, settle, de vangnet-reconcile om de
/// derde tik): 10 s, de cadans van de Go-leader.
pub(crate) const LEADER_TICK: Nanos = 10 * SECOND;

/// De eerste dynamische poort; elke taak heeft een eigen IP op het slot-LAN,
/// dus een nummer botst alleen binnen één taak.
const DYNAMIC_PORT_BASE: u16 = 20_000;

/// Hoeveel rondes acties de node na één invoer hoogstens uitvoert. Elke
/// uitkomst die de agent meldt, kan nieuwe acties geven (een mislukte start
/// plant een herstart); de grens houdt een fout in die keten uit een
/// oneindige lus, de volgende tik pakt de rest op.
const ACTION_ROUNDS: usize = 16;

/// Welke API een verzoek krijgt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Port {
    /// De agent-API (poort P).
    Agent,
    /// De leader-API (poort P + 1000).
    Leader,
}

/// Waar de bytes van een image heen gaan tijdens een download.
///
/// Elke methode wacht op de kern (de runner stroomt de bytes de kooi in),
/// dus een future; `-> impl Future` en niet `async fn`, omdat de executor
/// geen `Send` eist en de lint `async_fn_in_trait` dat niet kan weten.
pub trait Sink {
    /// De lengte van het image; zonder lengte geen start.
    fn begin(&mut self, size: u64) -> impl Future<Output = Result<(), String>>;
    /// De volgende bytes.
    fn chunk(&mut self, bytes: &[u8]) -> impl Future<Output = Result<(), String>>;
}

/// De downloader van artifacts: haalt `url` op en voert de bytes aan `sink`.
///
/// Generiek over de sink (zie de moduledoc): een future-methode is niet
/// object-safe.
pub trait Images {
    /// Haalt het artifact op; een fout is een tekst voor de log van de taak.
    fn fetch<K: Sink>(
        &mut self,
        url: &str,
        sink: &mut K,
    ) -> impl Future<Output = Result<(), String>>;
}

/// De runner als [`Sink`] voor één taak.
struct Feed<'a, S> {
    runner: &'a mut HopRunner<S>,
    ms: u64,
    task: &'a str,
    placed: Option<u32>,
    failure: Option<runner::Error>,
}

impl<S: SystemApi> Sink for Feed<'_, S> {
    async fn begin(&mut self, size: u64) -> Result<(), String> {
        self.runner
            .image_begin(self.ms, self.task, size)
            .await
            .map_err(|e| {
                let text = format!("{e}");
                self.failure = Some(e);
                text
            })
    }

    async fn chunk(&mut self, bytes: &[u8]) -> Result<(), String> {
        match self.runner.image_chunk(self.ms, self.task, bytes).await {
            Ok(Started::Running { pid }) => {
                self.placed = Some(pid);
                Ok(())
            }
            Ok(Started::AwaitImage) => Ok(()),
            Ok(Started::Aborted) => Err(String::from("task stopped during its start")),
            Err(e) => {
                let text = format!("{e}");
                self.failure = Some(e);
                Err(text)
            }
        }
    }
}

/// Een map van `types` als `BTreeMap`, zoals de runner hem wil.
fn to_btree<V: Clone>(m: &Map<V>) -> BTreeMap<String, V> {
    m.iter()
        .map(|(k, v)| (String::from(k), v.clone()))
        .collect()
}

/// De node.
pub struct Node<S, I> {
    agent: Agent,
    leader: Leader<MemStore>,
    runner: HopRunner<S>,
    images: I,
    node_api: NodeApi,
    leader_api: LeaderApi,
    /// `ip:poort` van de leader op deze node.
    own_leader: String,
    next_port: u16,
    next_leader_tick: Nanos,
    lines: Vec<String>,
    said: BTreeSet<&'static str>,
}

impl<S: SystemApi + agent::Store, I: Images> Node<S, I> {
    /// Een node volgens `cfg`, met de kern achter `sys`, artifacts via `images`, op tijd `now`.
    ///
    /// De leader draait hier (standalone) en kent de eigen agent vanaf het
    /// begin; zijn settle-periode staat uit, want er is niemand anders om op
    /// te wachten.
    ///
    /// Synchroon: hier wordt niets bij de kern gevraagd. Wat de agent na de
    /// registratie nog wil, blijft in zijn rij staan tot de eerste
    /// [`Node::restore`], [`Node::handle`] of [`Node::tick`] het uitvoert.
    pub fn new(cfg: &BootConfig, sys: S, images: I, now: Nanos) -> Self {
        let endpoint = format!("http://{}:{}", cfg.node_ip, cfg.port);
        let own_leader = format!("{}:{}", cfg.node_ip, cfg.leader_port());
        let mut attributes = BTreeMap::new();
        attributes.insert(String::from("node.id"), cfg.node_id.clone());
        attributes.insert(String::from("node.os"), String::from("hopos"));
        attributes.insert(String::from("node.arch"), String::from("arm64"));
        let settings = Settings {
            id: cfg.node_id.clone(),
            endpoint: endpoint.clone(),
            attributes: attributes.clone(),
            cpu_cores: cfg.cores,
            memory_bytes: cfg.memory,
            // Het zaad van de taak-id's: de klok en het node-id. Geen
            // entropiebron in een app; uniek genoeg op één node.
            seed: now ^ fnv(cfg.node_id.as_bytes()),
            ..Settings::default()
        };
        let mut agent = Agent::new(settings);
        agent.set_leader_addr(&own_leader);
        let mut node = Self {
            agent,
            leader: Leader::new(cfg.node_id.clone(), MemStore::new()),
            runner: HopRunner::new(sys, attributes, LogPolicy::default()),
            images,
            node_api: NodeApi::new(&cfg.api_key, false),
            leader_api: LeaderApi::new(&cfg.api_key, &cfg.cluster),
            own_leader,
            next_port: DYNAMIC_PORT_BASE,
            next_leader_tick: now.saturating_add(LEADER_TICK),
            lines: Vec::new(),
            said: BTreeSet::new(),
        };
        node.register_self(now);
        node
    }

    /// Meldt de eigen agent bij de eigen leader.
    fn register_self(&mut self, now: Nanos) {
        let me = types::Agent {
            id: String::from(self.agent.id()),
            endpoint: String::from(self.agent.endpoint()),
            version: String::from(VERSION),
            ..types::Agent::default()
        };
        let pool = self.runner.pool_largest();
        let mut net = Local {
            agent: &mut self.agent,
            now,
            pool_largest: pool,
        };
        match self
            .leader
            .register_agent(me, Map::new(), Time(now), &mut net)
        {
            Ok(_) => {
                let line = format!(
                    "hop: leader for cluster, agent {} registered HOP_LEADER leader={}",
                    self.agent.id(),
                    self.own_leader
                );
                self.lines.push(line);
            }
            Err(e) => self.lines.push(format!(
                "hop: leader refused own agent: {e} HOP_LEADER_FAIL"
            )),
        }
    }

    /// Herstelt de agent-staat uit hopfs en neemt de kooien van zijn lopende taken over.
    ///
    /// Na een herstart van Hop (of een kern-flip) draaien de apps door, maar
    /// een lege agent kent ze niet meer en zou op hun kooien stuiten. Geeft
    /// het aantal overgenomen kooien; niets opgeslagen is 0.
    pub async fn restore(&mut self, now: Nanos) -> Result<usize, agent::Error> {
        let running = self.agent.restore_from(self.runner.system_mut()).await?;
        let slots: Vec<(String, runner::Slot)> = running
            .into_iter()
            .filter_map(|(id, pid)| {
                let pid = u32::try_from(pid).ok().filter(|&p| p > 0)?;
                Some((id, runner::Slot(pid)))
            })
            .collect();
        self.runner.adopt_running(&slots);
        self.drain(now).await;
        Ok(slots.len())
    }

    /// De agent (tests en diagnose).
    pub fn agent(&self) -> &Agent {
        &self.agent
    }

    /// De runner (tests en diagnose).
    pub fn runner(&self) -> &HopRunner<S> {
        &self.runner
    }

    /// De runner, muteerbaar (de binary zet er de klok mee).
    pub fn runner_mut(&mut self) -> &mut HopRunner<S> {
        &mut self.runner
    }

    /// De regels voor de console sinds de vorige keer.
    pub fn take_lines(&mut self) -> Vec<String> {
        core::mem::take(&mut self.lines)
    }

    /// Eén keer een regel over iets dat nog niet aangesloten is, niet per tik.
    fn once(&mut self, key: &'static str, line: String) {
        if self.said.insert(key) {
            self.lines.push(line);
        }
    }

    /// Behandelt één verzoek op `port` en voert daarna de acties van de agent uit.
    pub async fn handle(&mut self, port: Port, req: &Request, now: Nanos) -> Reply {
        let reply = match port {
            Port::Leader => Reply::Plain(self.leader_handle(req, now)),
            Port::Agent => {
                let pool = self.runner.pool_largest();
                let (resp, effect) = self.node_api.handle(&mut self.agent, now, pool, req);
                self.effect(resp, effect, req, now)
            }
        };
        self.drain(now).await;
        reply
    }

    /// Een verzoek aan de leader, met de eigen agent als transport.
    fn leader_handle(&mut self, req: &Request, now: Nanos) -> Response {
        let pool = self.runner.pool_largest();
        let mut net = Local {
            agent: &mut self.agent,
            now,
            pool_largest: pool,
        };
        let mut cluster = LeaderCluster::new(&mut self.leader, &mut net);
        self.leader_api.handle(&mut cluster, now, req)
    }

    /// Voert een [`Effect`] uit, of weigert hem luid.
    fn effect(&mut self, resp: Response, effect: Effect, req: &Request, now: Nanos) -> Reply {
        match effect {
            Effect::None => Reply::Plain(resp),
            // De leader is deze node: geen proxy over het net, maar dezelfde
            // handler in-proces (dezelfde HMAC, dezelfde body).
            Effect::Proxy { ref leader, .. } if *leader == self.own_leader => {
                Reply::Plain(self.leader_handle(req, now))
            }
            Effect::Logs {
                ref task_id,
                stream,
            } => {
                let stream = match stream {
                    api::LogStream::Stdout => Stream::Stdout,
                    api::LogStream::Stderr => Stream::Stderr,
                };
                match self.runner.logs(now / MILLISECOND, task_id, stream) {
                    Some(ring) => Reply::Events {
                        head: resp,
                        lines: ring.tail().map(String::from).collect(),
                    },
                    None => Reply::Plain(refuse(&effect).unwrap_or(resp)),
                }
            }
            other => Reply::Plain(refuse(&other).unwrap_or(resp)),
        }
    }

    /// Laat de tijd verstrijken: heartbeat en tik van de leader, tik van de agent, de logpomp.
    pub async fn tick(&mut self, now: Nanos) {
        // De eigen agent is altijd levend zolang deze taak draait; zonder
        // heartbeat zou de eigen leader hem na 30 s dood verklaren.
        self.leader
            .heartbeat(self.agent.id(), VERSION, 0, Time(now));
        if now >= self.next_leader_tick {
            self.next_leader_tick = now.saturating_add(LEADER_TICK);
            let pool = self.runner.pool_largest();
            let mut net = Local {
                agent: &mut self.agent,
                now,
                pool_largest: pool,
            };
            if let Err(e) = self.leader.tick(Time(now), &mut net) {
                self.lines.push(format!("hop: leader tick: {e}"));
            }
        }
        let actions = self.agent.tick(now);
        self.run_actions(now, actions).await;
        self.drain(now).await;
        self.runner.pump_logs(now / MILLISECOND).await;
    }

    /// Voert acties uit tot de agent niets meer vraagt (begrensd).
    async fn drain(&mut self, now: Nanos) {
        for _ in 0..ACTION_ROUNDS {
            let actions = self.agent.take_actions();
            if actions.is_empty() {
                return;
            }
            self.run_actions(now, actions).await;
        }
    }

    /// Voert de acties uit, in volgorde; elke kern-call is een `.await`.
    async fn run_actions(&mut self, now: Nanos, actions: Vec<Action>) {
        let ms = now / MILLISECOND;
        for a in actions {
            match a {
                Action::Start { task_id, job } => self.start(now, &task_id, &job).await,
                Action::Stop { task_id, pid, .. } => {
                    let pid = u32::try_from(pid).unwrap_or(0);
                    if let Err(e) = self.runner.stop(ms, &TaskRef { id: &task_id, pid }).await {
                        self.lines.push(format!("hop: stop {task_id}: {e} HOP_STOP_FAILED"));
                    }
                }
                Action::Poll { task_id, pid, .. } => {
                    let pid = u32::try_from(pid).unwrap_or(0);
                    let st = match self.runner.status(ms, &TaskRef { id: &task_id, pid }).await {
                        Ok(RunState::Running) => Status::Running,
                        Ok(RunState::Failed) | Err(_) => Status::Failed,
                    };
                    self.agent.on_status(now, &task_id, st);
                }
                Action::Probe { .. } => self.once(
                    "probe",
                    String::from(
                        "hop: health probes are not wired on HopOS yet; tasks with a health_check stay unprobed HOP_PROBE_SKIPPED",
                    ),
                ),
                Action::Notify { job, event } => self.notify(now, &job, event),
                Action::SaveState => {
                    if let Err(e) = self.agent.save_to(self.runner.system_mut()).await {
                        self.once(
                            "save",
                            format!("hop: agent state not saved to hopfs: {e} HOP_STATE_SKIPPED"),
                        );
                    }
                }
            }
        }
    }

    /// Een taakgebeurtenis naar de eigen leader, zoals `POST /v1/notify`.
    fn notify(&mut self, now: Nanos, job: &str, event: Event) {
        if event == Event::Unplaceable {
            let pool = self.runner.pool_largest();
            let mut net = Local {
                agent: &mut self.agent,
                now,
                pool_largest: pool,
            };
            let id = String::from(net.agent.id());
            let _ = self.leader.mark_unplaced(&id, job, &mut net);
        }
        self.leader.notify(&format!("job:{job}:{}", event.as_str()));
    }

    /// Wijst de poorten van een job toe; 0 is dynamisch.
    fn ports(&mut self, job: &Job) -> BTreeMap<String, u16> {
        let mut out = BTreeMap::new();
        for (name, &p) in job.ports.iter() {
            let port = if p == 0 {
                let n = self.next_port;
                self.next_port = self.next_port.checked_add(1).unwrap_or(DYNAMIC_PORT_BASE);
                n
            } else {
                p
            };
            out.insert(String::from(name), port);
        }
        out
    }

    /// Een [`Action::Start`]: de runner, het image, en de uitkomst terug naar de agent.
    async fn start(&mut self, now: Nanos, task_id: &str, job: &Job) {
        let ms = now / MILLISECOND;
        let env = to_btree(&job.env);
        let tags = to_btree(&job.tags);
        let volumes = to_btree(&job.volumes);
        let ports = self.ports(job);
        let artifact = job.artifacts.first();
        let req = StartRequest {
            task_id,
            job_name: &job.name,
            image: &job.image,
            artifacts: job.artifacts.len(),
            extract: artifact.map_or("", |a| a.extract.as_str()),
            cpu_shares: u32::try_from(job.cpu_shares.max(0)).unwrap_or(u32::MAX),
            memory_limit: job.memory_limit,
            env: &env,
            tags: &tags,
            volumes: &volumes,
            ports: &ports,
        };
        let outcome = match self.runner.start(ms, &req).await {
            Ok(Started::Running { pid }) => Ok(pid),
            Ok(Started::Aborted) => Err((StartError::Failed, String::from("aborted"))),
            Ok(Started::AwaitImage) => {
                let url = artifact.map_or("", |a| a.url.as_str());
                self.stream(ms, task_id, url).await
            }
            Err(e) => Err((start_error(&e), format!("{e}"))),
        };
        match outcome {
            Ok(pid) => {
                self.lines.push(format!(
                    "hop: job {} task {task_id} placed HOP_JOB_PLACED slot={pid}",
                    job.name
                ));
                let mut placed = Map::new();
                for (k, v) in &ports {
                    let _ = placed.insert(k.clone(), *v);
                }
                let ok = StartOk {
                    pid: i64::from(pid),
                    ports: placed,
                };
                self.agent.on_started(now, task_id, Driver::Hop, Ok(ok));
            }
            Err((kind, why)) => {
                self.lines.push(format!(
                    "hop: job {} task {task_id} did not start: {why} HOP_JOB_FAILED",
                    job.name
                ));
                self.agent.on_started(now, task_id, Driver::Hop, Err(kind));
            }
        }
    }

    /// Haalt het image op en stroomt het de kooi in; de kooi als het lukte.
    ///
    /// De download en elke brok naar de kern zijn `.await`s: een image van
    /// megabytes houdt de staat van de node zo lang vast, maar niet de core.
    async fn stream(
        &mut self,
        ms: u64,
        task_id: &str,
        url: &str,
    ) -> Result<u32, (StartError, String)> {
        let mut feed = Feed {
            runner: &mut self.runner,
            ms,
            task: task_id,
            placed: None,
            failure: None,
        };
        let fetched = self.images.fetch(url, &mut feed).await;
        let (placed, failure) = (feed.placed, feed.failure.take());
        match (fetched, placed) {
            (Ok(()), Some(pid)) => Ok(pid),
            (Ok(()), None) => {
                // Het image was korter dan zijn lengte: de kooi terug.
                let _ = self
                    .runner
                    .stop(
                        ms,
                        &TaskRef {
                            id: task_id,
                            pid: 0,
                        },
                    )
                    .await;
                Err((
                    StartError::Failed,
                    String::from("image ended before its length"),
                ))
            }
            (Err(why), _) => {
                // Een download die faalde, laat de gereserveerde kooi niet staan.
                let _ = self
                    .runner
                    .stop(
                        ms,
                        &TaskRef {
                            id: task_id,
                            pid: 0,
                        },
                    )
                    .await;
                let kind = failure.as_ref().map_or(StartError::Failed, start_error);
                Err((kind, why))
            }
        }
    }
}

/// Een runner-fout als uitkomst voor de agent: vol is teruggeven, de rest herstarten.
fn start_error(e: &runner::Error) -> StartError {
    match e {
        runner::Error::NoCapacity(_) => StartError::NoCapacity,
        _ => StartError::Failed,
    }
}

/// FNV-1a over `b`: een zaad uit het node-id.
fn fnv(b: &[u8]) -> u64 {
    b.iter().fold(0xcbf2_9ce4_8422_2325, |h, &c| {
        (h ^ u64::from(c)).wrapping_mul(0x0100_0000_01b3)
    })
}

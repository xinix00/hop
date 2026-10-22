//! De eigenaar van alle staat van de node: agent, leader, runner en de twee API's.
//!
//! Eén struct achter `&mut self` (handboek §1). Een verzoek ([`Node::handle`]),
//! de tik ([`Node::tick`]) en een brok van de downloadtaak
//! ([`Node::on_piece`]) zijn de enige ingangen; wat de agent daarna
//! wil (starten, stoppen, pollen, wegschrijven) voert de node meteen uit,
//! in de volgorde waarin de agent het vroeg.
//!
//! Die ingangen zijn `async`: een actie die de kern raakt (een slot, een
//! brok image, een status, de staat bij de kern) wacht met `.await` op de
//! verbinding, en de eigenaar-taak geeft dan de core terug. De netstack,
//! de verbindingstaken en de rest draaien intussen door; alleen de staat van
//! de node wacht, want die heeft één eigenaar (handboek §1 en §4).
//!
//! De download van een artifact is géén ingang die wacht: een start die op
//! zijn image wacht, wordt een opdracht voor de downloadtaak
//! ([`Node::take_order`], zie [`crate::download`]), en de bytes komen terug
//! als [`Piece`]s ([`Node::on_piece`]), één brok per keer. Tussen twee
//! brokken door handelt de eigenaar de bus af, dus de API staat nooit stil
//! op een download (03-10).
//!
//! De traits [`Images`] en [`Sink`] zijn niet object-safe (een
//! methode die een future geeft, kan niet in een vtable); de downloader
//! krijgt de sink daarom als generieke parameter in plaats van als
//! `&mut dyn Sink`. Elke downloader (de downloadtaak, en de flip van de
//! node) heeft één soort sink, dus dat kost niets.
//!
//! De staat van de agent (en standalone die van de leader) legt hij bij de
//! kern neer, zodat een herstarte Hop zijn taken terugvindt
//! ([`Node::restore`], `crate::restart`): na elke tik waarin hij veranderde
//! (de tik is de debounce, 1 s, zoals `leader::PERSIST_DEBOUNCE`), elke
//! [`STATE_REFRESH`] opnieuw, vlak vóór een herstart, en een laatste keer
//! bij de stop-bel ([`Node::on_stop`]).
//!
//! De markers voor de console verzamelt hij als regels ([`Node::take_lines`]);
//! de binary zet ze met `applib::log!` op het log, een test leest ze.

use alloc::collections::{BTreeMap, BTreeSet, VecDeque};
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;

use agent::{
    Action, Agent, Event, Handoff, Settings, StartError, StartOk, Status, Store, StoreError,
};
use api::{
    Effect, EventLog, LeaderApi, LeaderCluster, LeaderEffect, LogStream, Method, NodeApi, Request,
    Response,
};
use hop_http::{Ask, Chunk, Reply, refuse};
use leader::{Leader, MemStore};
use runner::{
    HopRunner, LogPolicy, RunState, Runner, Slot, SlotState, StartRequest, Started, Stream,
    SystemApi, TaskRef,
};
use types::time::{MILLISECOND, SECOND};
use types::{Artifact, Driver, Job, Map, Nanos, SysUsage, Telemetry, Time};

use crate::VERSION;
use crate::download::{Order, Piece};
use crate::env::{BootConfig, ColdFlip};
use crate::forward::{Forward, Routed};
use crate::local::Local;

mod cluster;

pub(crate) use cluster::refuse_forward;
pub use cluster::{Cluster, ClusterParts};

/// Hoe vaak de leader tikt (dode agents, settle, de vangnet-reconcile om de
/// derde tik): 10 s, de cadans van de Go-leader.
pub(crate) const LEADER_TICK: Nanos = 10 * SECOND;

/// Hoeveel stromen (`/v1/events`, een log-tail) tegelijk open mogen staan.
/// Een stroom houdt een verbindingstaak vast zolang hij loopt; de binary
/// heeft er per poort een vast aantal (`WORKERS`, 4), dus met drie stromen
/// houdt elke poort er minstens één vrij voor de CLI en de GUI. Drie, omdat
/// het dashboard er twee nodig heeft (`/v1/events` en een log-tail) en een
/// herlaad er kort een derde bij opent terwijl de oude nog sluit.
pub(crate) const MAX_STREAMS: usize = 3;

/// De eerste dynamische poort; elke taak heeft een eigen IP op het slot-LAN,
/// dus een nummer botst alleen binnen één taak.
const DYNAMIC_PORT_BASE: u16 = 20_000;

/// Hoeveel rondes acties de node na één invoer hoogstens uitvoert. Elke
/// uitkomst die de agent meldt, kan nieuwe acties geven (een mislukte start
/// plant een herstart); de grens houdt een fout in die keten uit een
/// oneindige lus, de volgende tik pakt de rest op.
const ACTION_ROUNDS: usize = 16;

/// Hoe lang een koude flip die de kern aannam, mag duren voordat Hop hem
/// geweigerd noemt. Een koude sprong start Hop opnieuw, dus een Hop die
/// dan nog leeft, sprong niet mee. De kern doet er na zijn antwoord een
/// halve seconde, de bevriezing van hopfs (hoogstens 2 s), elke bewoner
/// die Hop liet staan (hoogstens 3 s per stuk) en de app-cores (1 s) over.
const COLD_FLIP_WAIT: Nanos = 30 * SECOND;

/// Hoe vaak Hop zijn staat bij de kern opnieuw neerlegt, ook als er niets
/// veranderde.
///
/// Na een warme kern-flip draait Hop door, maar het blob ligt in het
/// geheugen van de óude kern: de nieuwe heeft niets. Hop ziet de wissel
/// niet (de system-client verbindt stil opnieuw), dus legt hij het blob
/// gewoon op dit ritme opnieuw neer. Dat is één call met een paar KB per
/// tien seconden; een herstart binnen tien seconden na een flip is de enige
/// die dan nog een lege kern treft. Een oudere kern die de ops weigerde,
/// krijgt op dit ritme ook zijn tweede kans (een flip naar een nieuwe kern).
pub(crate) const STATE_REFRESH: Nanos = 10 * SECOND;

/// Wat [`Node::restore`] bij de start aantrof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kept {
    /// De kern had niets voor Hop: een schone start.
    Clean,
    /// De kern houdt geen staat (een oudere kern): schoon, zoals vóór de
    /// kern-staat.
    Unsupported,
    /// De staat van de vorige Hop, met zoveel overgenomen kooien.
    Adopted(usize),
    /// Er lag iets, maar het was niet te lezen. Een staat die niet te lezen
    /// is, is geen lege staat: dan geen init-jobs (Go: nooit zaaien op een
    /// opslagfout).
    Unreadable,
}

impl Kept {
    /// Of dit een schone start is: alleen dan zaait een standalone node zijn init-jobs.
    pub fn is_clean(self) -> bool {
        matches!(self, Self::Clean | Self::Unsupported)
    }
}

/// De staat bij de kern: wanneer hij weer moet.
#[derive(Debug, Default)]
struct Keep {
    /// Veranderd sinds de laatste geslaagde schrijf.
    dirty: bool,
    /// De kern weigerde de staat-ops (een oudere kern): alleen het ritme
    /// van [`STATE_REFRESH`] probeert het nog.
    refused: bool,
    /// Wanneer het blob hoe dan ook weer moet.
    next: Nanos,
    /// Mislukte schrijfpogingen op rij (luid, drie keer, dan tellen).
    failures: u32,
}

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
/// Elke methode kan wachten (een artifact gaat als [`Piece`] door de rij
/// naar de eigenaar, een kernbundel de kern in), dus een future;
/// `-> impl Future` en niet `async fn`, omdat de executor geen `Send` eist
/// en de lint `async_fn_in_trait` dat niet kan weten.
pub trait Sink {
    /// De lengte van het image; zonder lengte geen start.
    fn begin(&mut self, size: u64) -> impl Future<Output = Result<(), String>>;
    /// De volgende bytes.
    fn chunk(&mut self, bytes: &[u8]) -> impl Future<Output = Result<(), String>>;
}

/// De downloader van artifacts: haalt `artifact` op en voert de bytes aan `sink`.
///
/// Het hele artifact, niet alleen zijn URL: de koppen en de sleutels in
/// `auth` (Basic, `s3://`) horen bij de download, zoals op de host
/// ([`runner::artifact`]). Generiek over de sink (zie de moduledoc): een
/// future-methode is niet object-safe.
pub trait Images {
    /// Haalt het artifact op; een fout is een tekst voor de log van de taak.
    fn fetch<K: Sink>(
        &mut self,
        artifact: &Artifact,
        sink: &mut K,
    ) -> impl Future<Output = Result<(), String>>;
}

/// Een start die op zijn image wacht; de download loopt in de downloadtaak.
#[derive(Debug)]
struct Loading {
    /// Het nummer van de opdracht ([`Order::seq`]).
    seq: u64,
    task_id: String,
    job: String,
    artifact: Artifact,
    ports: BTreeMap<String, u16>,
    /// De opdracht is de downloadtaak in gegaan.
    sent: bool,
    /// De kern plaatste het image (de laatste brok); het einde komt nog.
    placed: Option<u32>,
}

/// Hoe een start afliep: de kooi, of het soort fout en de reden.
type Outcome = Result<u32, (StartError, String)>;

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
    /// De clustersleutel: een doorgifte van de leader naar de eigen agent
    /// gaat in-proces, maar door dezelfde HMAC-toets.
    key: Vec<u8>,
    /// De meldingen voor `/v1/events` (leader en `POST /v1/notify`).
    events: EventLog,
    /// Open stromen, aangemeld bij het antwoord en afgemeld met
    /// [`Node::stream_done`].
    streams: usize,
    /// `ip:poort` van de leader op deze node.
    own_leader: String,
    next_port: u16,
    next_leader_tick: Nanos,
    lines: Vec<String>,
    said: BTreeSet<&'static str>,
    /// De cluster: verkiezing, lease en de agents op andere nodes; `None`
    /// standalone (zie [`cluster`]).
    cluster: Option<Cluster>,
    /// Het slot van Hop zelf.
    own_slot: Slot,
    /// Het gebruik van de kern en van Hop, voor de heartbeat; gemeten op
    /// het ritme van de monitor ([`Node::measure_system`]).
    kern: SysUsage,
    hop: SysUsage,
    next_measure: Nanos,
    /// Een koude flip die de kern aannam: leeft deze Hop op dit moment nog,
    /// dan sprong de kern niet ([`COLD_FLIP_WAIT`]).
    cold_flip_until: Option<Nanos>,
    /// Kan het board koud flippen (`HOPOS_COLD_FLIP`, [`Node::cold_refusal`]).
    cold_flip: ColdFlip,
    /// Draaide er sinds de start van deze Hop ooit iets op een app-core:
    /// Hop zelf buiten `system`, of een plaatsing buiten `system`. Zonder
    /// zekerheid ja (een job zonder groep kan op de OS-core vallen, maar
    /// telt): met `fresh` weigert Hop dan liever zelf dan te stoppen.
    app_core_used: bool,
    /// De starts die op hun image wachten, in volgorde; de eerste is de
    /// download die loopt (als hij `sent` is).
    loads: VecDeque<Loading>,
    /// Het nummer van de volgende opdracht.
    next_seq: u64,
    /// De staat bij de kern ([`Node::keep_state`]).
    keep: Keep,
    /// De kern nam een herstart aan: deze Hop vertrekt zodra zijn antwoord
    /// de deur uit is ([`Node::take_leaving`], `crate::restart`).
    leaving: bool,
}

impl<S: SystemApi + Store, I: Images> Node<S, I> {
    /// Een node volgens `cfg`, met de kern achter `sys`, artifacts via `images`, op tijd `now`.
    ///
    /// De leader draait hier (standalone) en kent de eigen agent vanaf het
    /// begin; zijn settle-periode staat uit, want er is niemand anders om op
    /// te wachten.
    ///
    /// Synchroon: hier wordt niets bij de kern gevraagd. Wat de agent na de
    /// registratie nog wil, blijft in zijn rij staan tot de eerste
    /// [`Node::restore`], [`Node::handle`] of [`Node::tick`] het uitvoert.
    /// Na een overname ([`Node::restore`]) registreert de agent zich
    /// opnieuw, met wat hij dan draait.
    pub fn new(cfg: &BootConfig, sys: S, images: I, now: Nanos) -> Self {
        let mut node = Self::build(cfg, sys, images, now);
        node.register_self(now);
        node
    }

    /// De node zonder registratie bij een leader: de standalone-leader
    /// registreert de eigen agent meteen ([`Node::new`]), een geclusterde
    /// node pas als de verkiezing hem de leiding geeft.
    fn build(cfg: &BootConfig, sys: S, images: I, now: Nanos) -> Self {
        let endpoint = format!("http://{}:{}", cfg.node_ip, cfg.port);
        let own_leader = format!("{}:{}", cfg.node_ip, cfg.leader_port());
        let mut attributes = BTreeMap::new();
        attributes.insert(String::from("node.id"), cfg.node_id.clone());
        attributes.insert(String::from("node.os"), String::from("hopos"));
        // De architectuur van deze bewoner, voor de `match` van de artifacts
        // (02-10: hardcoded arm64 liet een riscv64-node arm64-ELF's halen).
        let arch = if cfg!(target_arch = "riscv64") {
            "riscv64"
        } else {
            "arm64"
        };
        attributes.insert(String::from("node.arch"), String::from(arch));
        let settings = Settings {
            id: cfg.node_id.clone(),
            endpoint: endpoint.clone(),
            attributes: attributes.clone(),
            cpu_cores: cfg.cores,
            memory_bytes: cfg.memory,
            free_groups: free_groups(cfg.system_core, &cfg.hop_group),
            // Het zaad van de taak-id's: de klok en het node-id. Geen
            // entropiebron in een app; uniek genoeg op één node.
            seed: now ^ fnv(cfg.node_id.as_bytes()),
            ..Settings::default()
        };
        let mut agent = Agent::new(settings);
        agent.set_leader_addr(&own_leader);
        Self {
            agent,
            leader: Leader::new(cfg.node_id.clone(), MemStore::new()),
            runner: HopRunner::new(sys, attributes, LogPolicy::default()),
            images,
            // FLIP: Hop op HopOS kan de kern vervangen (crate::flip).
            node_api: NodeApi::new(&cfg.api_key, true),
            leader_api: LeaderApi::new(&cfg.api_key, &cfg.cluster),
            key: cfg.api_key.clone(),
            events: EventLog::new(),
            streams: 0,
            own_leader,
            next_port: DYNAMIC_PORT_BASE,
            next_leader_tick: now.saturating_add(LEADER_TICK),
            lines: Vec::new(),
            said: BTreeSet::new(),
            cluster: None,
            own_slot: Slot(cfg.slot),
            kern: SysUsage::default(),
            hop: SysUsage::default(),
            next_measure: now,
            cold_flip_until: None,
            cold_flip: cfg.cold_flip,
            app_core_used: cfg.hop_group != agent::SYSTEM_GROUP,
            loads: VecDeque::new(),
            next_seq: 1,
            keep: Keep {
                next: now.saturating_add(STATE_REFRESH),
                ..Keep::default()
            },
            leaving: false,
        }
    }

    /// Meldt de eigen agent bij de eigen leader.
    fn register_self(&mut self, now: Nanos) {
        match self.register_with_placed(now) {
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

    /// De registratie bij de eigen (standalone) leader, met wat de agent al
    /// draait: na een overname telt de leader die taken als geplaatst en
    /// plaatst hij ze niet nog eens.
    fn register_with_placed(&mut self, now: Nanos) -> leader::Result<bool> {
        let me = types::Agent {
            id: String::from(self.agent.id()),
            endpoint: String::from(self.agent.endpoint()),
            version: String::from(VERSION),
            ..types::Agent::default()
        };
        let mut placed = Map::new();
        for (name, n) in self.agent.placed_task_counts() {
            placed.insert(name, u32::try_from(n).unwrap_or(u32::MAX))?;
        }
        let pool = self.runner.pool_largest();
        let mut net = Local {
            agent: &mut self.agent,
            now,
            pool_largest: pool,
            relay: self.cluster.as_mut().map(Cluster::relay_mut),
        };
        self.leader.register_agent(me, placed, Time(now), &mut net)
    }

    /// Legt het gemeten gebruik van een taak vast: cpu als procent van zijn
    /// eigen cores (de meetlat van de app via de kern, docs/apps.md) en
    /// geheugen als procent van zijn limiet (wat de app zelf in gebruik
    /// meldt), met de cores van zijn slot als noemer van de cpu en de core
    /// waarop het slot draait. Zonder meting blijft het vorige getal staan.
    async fn record_usage(&mut self, task_id: &str, pid: u32) {
        let (cpu, mem, cores, core) = self.runner.usage(&TaskRef { id: task_id, pid }).await;
        let limit = self
            .agent
            .task(task_id)
            .and_then(|t| self.agent.get_job(&t.job_name))
            .map_or(0, |j| j.memory_limit);
        // Met één decimaal: een kleine app in een ruime limiet (welcome,
        // 150 KB in 32 MB) is anders altijd 0 (03-10, LicheeRV).
        let mem_pct = match mem {
            Some(m) if limit > 0 => {
                (u128::from(m.min(limit)) * 1000 / u128::from(limit)) as f64 / 10.0
            }
            _ => return,
        };
        if let Some(c) = cpu {
            self.agent
                .record_usage(task_id, f64::from(c), mem_pct, cores, core);
        }
    }

    /// Meet de kern (slot 0) en Hop zelf, één keer per monitor-interval.
    ///
    /// Dezelfde meetlat als een app ([`HopRunner::usage`]); de standen per
    /// slot houdt de kern-verbinding bij, en een taak zit nooit in slot 0 of
    /// in het slot van Hop. Een slot dat niet draait, of een kern die slot 0
    /// nog niet kent (die geeft een fout), is niets gemeten.
    async fn measure_system(&mut self, now: Nanos) {
        if now < self.next_measure {
            return;
        }
        self.next_measure = now.saturating_add(self.agent.settings().monitor_interval());
        self.kern = self.slot_usage(Slot(0)).await;
        self.hop = self.slot_usage(self.own_slot).await;
    }

    async fn slot_usage(&mut self, slot: Slot) -> SysUsage {
        let s = self.runner.system_mut().slot_status(slot).await;
        if s.state != SlotState::Running {
            return SysUsage::default();
        }
        SysUsage {
            cpu_percent: s.cpu_pct.map(f64::from),
            mem_bytes: s.mem_sys,
            ram_bytes: s.mem_limit,
            core: s.core,
        }
    }

    /// Wat de heartbeat meldt: de temperatuur die de kern elke seconde op
    /// de control-page zet (CTRL_TEMP, HopOS alpha.9; 0 is geen meting) en
    /// het laatst gemeten gebruik van de kern en van Hop.
    fn telemetry(&self) -> Telemetry {
        Telemetry {
            temp_milli_c: applib::app().map_or(0, |a| i64::from(a.ctrl().temp_milli_c())),
            kern: self.kern,
            hop: self.hop,
        }
    }

    /// Stopt de bewoners die de kern nog heeft maar deze Hop niet kent.
    ///
    /// Roep hem NA [`Node::restore`]: wat de vorige Hop bij de kern
    /// neerlegde en nog draait, is dan van deze. Wat overblijft, is van
    /// niemand: een taak die startte na de laatste staat, een job die rond
    /// de herstart verwijderd werd, alles na een koude boot (het blob sterft
    /// met de kern) of op een kern die geen staat houdt. Zonder deze stap
    /// hield zo'n bewoner zijn slot tot de volgende koude boot (GEMETEN
    /// 01-10 op de Pi 4). Een staat uit een bestand op hopfs overnemen was
    /// erger: na een koude boot werd die een spook op het slot van de
    /// volgende bewoner (02-10, de M4: twee minuten 503 "port 80 is taken"
    /// tot Hop het slot met de echte spin erin stopte); het blob in de kern
    /// kent geen vorige boot, en de overname eist een slot dat draait.
    pub async fn sweep_strays(&mut self) {
        for slot in self.runner.sweep_strays().await {
            self.lines.push(format!(
                "hop: stray resident in slot {} stopped: not in the saved state HOP_STRAY_STOPPED slot={}",
                slot.0, slot.0
            ));
        }
    }

    /// Neemt de staat van de vorige Hop over uit de kern: zijn jobs, en de
    /// taken waarvan de kern het slot als draaiend meldt.
    ///
    /// Vóór [`Node::sweep_strays`] en vóór de eerste registratie bij een
    /// leader: de leader ziet de taken dan meteen als geplaatst. Standalone
    /// komt ook de gecommitte staat van de leader terug, en registreert de
    /// agent zich opnieuw met wat hij nu draait. Een taak waarvan het slot
    /// niet meer draait, of al van een ander is, neemt hij niet over: die
    /// plaatst de leader opnieuw.
    pub async fn restore(&mut self, now: Nanos) -> Kept {
        let blob = match self.runner.system_mut().load().await {
            Ok(Some(b)) => b,
            Ok(None) => return Kept::Clean,
            Err(e @ StoreError::Refused { .. }) => {
                self.keep.refused = true;
                self.lines.push(format!(
                    "hop: this kernel keeps no state for Hop ({e}); a restart of Hop stops every app HOP_KERNEL_STATE_NONE"
                ));
                return Kept::Unsupported;
            }
            Err(e) => {
                self.lines.push(format!(
                    "hop: the kernel's state for Hop not read: {e}; starting empty HOP_KERNEL_STATE_FAIL"
                ));
                return Kept::Unreadable;
            }
        };
        let h = match Handoff::parse(&blob) {
            Ok(Some(h)) => h,
            Ok(None) => return Kept::Clean,
            Err(e) => {
                self.lines.push(format!(
                    "hop: the kernel's state for Hop is unreadable ({} bytes): {e}; starting empty HOP_KERNEL_STATE_FAIL",
                    blob.len()
                ));
                return Kept::Unreadable;
            }
        };
        drop(blob);
        let n = self.adopt(now, h).await;
        Kept::Adopted(n)
    }

    /// De overname zelf: de kooien bij de runner, de records bij de agent,
    /// standalone de leader. Geeft het aantal overgenomen kooien.
    async fn adopt(&mut self, now: Nanos, h: Handoff) -> usize {
        let own = self.own_slot;
        let mut want = Vec::new();
        for t in &h.tasks {
            let Some(pid) = u32::try_from(t.pid)
                .ok()
                .filter(|&p| p > 0 && Slot(p) != own)
            else {
                continue;
            };
            if want.try_reserve(1).is_err() {
                break;
            }
            want.push((t.id.clone(), Slot(pid)));
        }
        let adopted = self.runner.adopt_running(&want).await;
        let total = h.tasks.len();
        let tasks: Vec<types::Task> = h
            .tasks
            .into_iter()
            .filter(|t| adopted.contains(&t.id))
            .collect();
        let n = tasks.len();
        self.agent.adopt(h.jobs, tasks);
        if n > 0 {
            // Zonder zekerheid ja (zie het veld): een overgenomen kooi kan
            // op een app-core staan.
            self.app_core_used = true;
        }
        if self.cluster.is_none() {
            if let Some(l) = h.leader
                && let Err(e) = self.leader.load_committed_state(Some(l.as_bytes()))
            {
                self.lines.push(format!(
                    "hop: the leader's state in the kernel's state is unreadable: {e} HOP_KERNEL_STATE_FAIL"
                ));
            }
            if let Err(e) = self.register_with_placed(now) {
                self.lines.push(format!(
                    "hop: leader refused own agent after the restore: {e} HOP_LEADER_FAIL"
                ));
            }
        }
        self.lines.push(format!(
            "hop: adopted {n} running cage(s) from the kernel's state HOP_ADOPTED n={n}"
        ));
        let gone = total.saturating_sub(n);
        if gone > 0 {
            self.lines.push(format!(
                "hop: {gone} task(s) in the kernel's state no longer run; the leader places them again HOP_ADOPT_SKIPPED n={gone}"
            ));
        }
        self.drain(now).await;
        n
    }

    /// Legt de staat bij de kern neer als hij veranderde of als het ritme
    /// het zegt ([`STATE_REFRESH`]); na elke tik.
    async fn keep_state(&mut self, now: Nanos) {
        let mut changed = self.agent.take_dirty();
        // Standalone is de leader van deze node ook staat die Hop moet
        // meenemen; zijn eigen debounce zegt wanneer hij veranderde.
        if self.cluster.is_none() && matches!(self.leader.poll_snapshot(Time(now)), Ok(Some(_))) {
            changed = true;
        }
        if changed && !self.keep.refused {
            self.keep.dirty = true;
        }
        if self.keep.dirty || now >= self.keep.next {
            let _ = self.save_state(now).await;
        }
    }

    /// Legt de staat nu bij de kern neer; geeft de maat van het blob.
    async fn save_state(&mut self, now: Nanos) -> Result<usize, String> {
        self.keep.next = now.saturating_add(STATE_REFRESH);
        let leader = if self.cluster.is_none() {
            match self.leader.snapshot(Time(now)) {
                Ok(s) => Some(s),
                Err(e) => return Err(self.state_failed(&format!("the leader's snapshot: {e}"))),
            }
        } else {
            None
        };
        let r = self
            .agent
            .save_to(self.runner.system_mut(), leader.as_deref())
            .await;
        match r {
            Ok(n) => {
                self.keep.dirty = false;
                self.keep.failures = 0;
                if self.keep.refused {
                    self.keep.refused = false;
                    self.lines.push(String::from(
                        "hop: the kernel keeps Hop's state now HOP_KERNEL_STATE_KEPT",
                    ));
                }
                Ok(n)
            }
            Err(agent::Error::Store(e @ StoreError::Refused { .. })) => {
                // Een oudere kern: niet elke tik opnieuw, alleen op het ritme.
                self.keep.dirty = false;
                if !self.keep.refused {
                    self.keep.refused = true;
                    self.lines.push(format!(
                        "hop: this kernel keeps no state for Hop ({e}); a restart of Hop stops every app HOP_KERNEL_STATE_NONE"
                    ));
                }
                Err(format!("this kernel keeps no state for Hop: {e}"))
            }
            Err(e @ agent::Error::Store(StoreError::TooBig { .. })) => {
                // Groeit niet vanzelf weer in: niet elke tik een blob van
                // een megabyte maken, alleen op het ritme.
                self.keep.dirty = false;
                Err(self.state_failed(&format!("{e}")))
            }
            Err(e) => Err(self.state_failed(&format!("{e}"))),
        }
    }

    /// Een schrijf die mislukte: luid, drie keer, dan eens per honderd
    /// (handboek §6). De vlag blijft staan: de volgende tik probeert opnieuw.
    fn state_failed(&mut self, why: &str) -> String {
        self.keep.failures = self.keep.failures.saturating_add(1);
        let n = self.keep.failures;
        if n <= 3 || n.is_multiple_of(100) {
            self.lines.push(format!(
                "hop: state for the kernel not saved ({n}x): {why} HOP_KERNEL_STATE_FAIL"
            ));
        }
        String::from(why)
    }

    /// De kern luidde de stop-bel (een stop van het slot, of een herstart
    /// die niet op Hop wachtte): de staat een laatste keer naar de kern,
    /// binnen de gratie.
    ///
    /// Daarna doet de eigenaar niets meer: wat na dit blob nog veranderde,
    /// zou de volgende Hop niet kennen.
    pub async fn on_stop(&mut self, now: Nanos) {
        self.hand_over(now, "stop requested").await;
    }

    /// Of de kern net een herstart aannam (één keer `true`): de eigenaar
    /// laat dan eerst het antwoord wegschrijven en roept [`Node::leave`].
    pub fn take_leaving(&mut self) -> bool {
        core::mem::take(&mut self.leaving)
    }

    /// Vertrekt na een aangenomen herstart: één regel waarom nu, de staat
    /// een laatste keer naar de kern, en daarna niets meer (zoals
    /// [`Node::on_stop`]).
    pub async fn leave(&mut self, now: Nanos, why: crate::restart::Departure) {
        use crate::restart::{Departure, REPLY_GRACE};
        let line = match why {
            Departure::Replied => {
                String::from("hop: the restart reply is out; leaving for the new Hop")
            }
            Departure::GraceOver => format!(
                "hop: the restart reply was not out within {} ms; leaving for the new Hop anyway",
                REPLY_GRACE.as_millis()
            ),
            Departure::Bell => {
                String::from("hop: the kernel rang the stop bell before the restart reply was out")
            }
        };
        self.lines
            .push(format!("{line} HOP_RESTART_LEAVING why={}", why.as_str()));
        self.hand_over(now, "leaving for the restart").await;
    }

    /// De staat een laatste keer naar de kern, met de regel `HOP_STOPPING`.
    async fn hand_over(&mut self, now: Nanos, what: &str) {
        let r = self.save_state(now).await;
        let tasks = self.agent.tasks().count();
        match r {
            Ok(n) => self.lines.push(format!(
                "hop: {what}; state of {tasks} task(s) handed to the kernel ({n} bytes) HOP_STOPPING"
            )),
            Err(e) => self.lines.push(format!(
                "hop: {what}; the state did not reach the kernel: {e} HOP_STOPPING"
            )),
        }
    }

    /// Zaait de init-jobs uit `specs` (een JSON-array van jobspecs, zoals
    /// `hopos.init[]`) bij een schone boot; geeft hoeveel jobs de leader
    /// kreeg.
    ///
    /// "Schoon" is aan de aanroeper én hier: de binary roept dit alleen aan
    /// als [`Node::restore`] geen staat vond ([`Kept::is_clean`]; een staat
    /// die niet te lezen is, is geen lege staat, en een staat zonder jobs
    /// is een operator die alles verwijderde), en hier zaait hij alleen in
    /// een leader zonder jobs. Zo overschrijft een zaadje nooit wat een
    /// operator of een vorige bewoner neerzette (Go: `agentloop`,
    /// `cleanBoot && len(l.GetJobs()) == 0`). Een spec die niet klopt, is
    /// een fout voor het hele setje: luid bij de start, niet half gezaaid.
    pub async fn seed_init_jobs(&mut self, specs: &str, now: Nanos) -> Result<usize, String> {
        if !self.leader.jobs().is_empty() {
            return Ok(0);
        }
        let v = types::json::parse_str(specs).map_err(|e| format!("init jobs: {e}"))?;
        let list = v
            .as_array()
            .ok_or_else(|| String::from("init jobs: not a JSON array"))?;
        let jobs = leader::decode_init_jobs(list).map_err(|e| format!("{e}"))?;
        let names: Vec<String> = jobs.iter().map(|j| j.name.clone()).collect();
        let pool = self.runner.pool_largest();
        let mut net = Local {
            agent: &mut self.agent,
            now,
            pool_largest: pool,
            relay: self.cluster.as_mut().map(Cluster::relay_mut),
        };
        self.leader
            .seed_init_jobs(jobs, &mut net)
            .map_err(|e| format!("init jobs: {e}"))?;
        self.lines.push(format!(
            "hop: clean boot, seeded {} init job(s): {} HOP_INIT_SEEDED",
            names.len(),
            names.join(",")
        ));
        self.drain(now).await;
        Ok(names.len())
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
    ///
    /// Een verzoek voor een andere node (een doorgifte, zie
    /// [`Node::handle_routed`]) kan hier niet: dat wordt een luide 502.
    pub async fn handle(&mut self, port: Port, req: &Request, now: Nanos) -> Reply
    where
        S: crate::flip::KernFlip + crate::restart::KernRestart,
    {
        match self.handle_routed(port, req, now).await {
            Routed::Reply(r) => r,
            Routed::Forward(f) => Reply::Plain(refuse_forward(&f)),
        }
    }

    /// Behandelt één verzoek op `port`: een antwoord, of een doorgifte naar
    /// een andere node die de verbindingstaak uitvoert (`forward::serve`).
    pub async fn handle_routed(&mut self, port: Port, req: &Request, now: Nanos) -> Routed
    where
        S: crate::flip::KernFlip + crate::restart::KernRestart,
    {
        let mut routed = match port {
            Port::Leader => self.leader_reply(req, now),
            Port::Agent => {
                let pool = self.runner.pool_largest();
                let (resp, effect) = self.node_api.handle(&mut self.agent, now, pool, req);
                // FLIP: de kern-flip wacht op download en kern, dus hier en
                // niet in het synchrone `effect` (crate::flip).
                if let Effect::Flip { url, sha256, cold } = &effect {
                    let r = self.flip(url, sha256, *cold, now).await;
                    match r {
                        Ok(()) => {
                            self.lines.push(String::from(
                                "hop: the kernel took the bundle and flips now HOP_FLIP_ACCEPTED",
                            ));
                            Routed::Reply(Reply::Plain(resp))
                        }
                        Err(e) => {
                            self.lines
                                .push(format!("hop: kernel flip failed: {e} HOP_FLIP_FAIL"));
                            Routed::Reply(Reply::Plain(Response::error(502, &e)))
                        }
                    }
                } else if let Effect::Restart { url, sha256 } = &effect {
                    match self.restart(url, sha256, now).await {
                        Ok(()) => {
                            self.leaving = true;
                            self.lines.push(String::from(
                                "hop: the kernel took the restart; this Hop stops now and the next one adopts its cages HOP_RESTART_ACCEPTED",
                            ));
                            Routed::Reply(Reply::Plain(resp))
                        }
                        Err(e) => {
                            self.lines
                                .push(format!("hop: restart of Hop failed: {e} HOP_RESTART_FAIL"));
                            Routed::Reply(Reply::Plain(Response::error(502, &e)))
                        }
                    }
                } else {
                    self.effect(resp, effect, req, now)
                }
            }
        };
        if port == Port::Agent
            && let Routed::Reply(reply) = &mut routed
        {
            // De browser van het dashboard praat met deze poort: élk antwoord
            // draagt de CORS-koppen, ook een dat van de leader in-proces kwam
            // (`/v1/...`) en de kop van een stroom. Een doorgifte krijgt ze
            // van de verbindingstaak (`forward::execute`).
            api::cors(req, reply.head_mut());
        }
        self.drain(now).await;
        self.collect_events();
        self.after_cluster(now);
        routed
    }

    /// FLIP: haal de bundel, stop bij een koude flip eerst de eigen taken
    /// op deze node, en vraag de kern de flip (crate::flip).
    ///
    /// Een koude flip die het board niet kan ([`Node::cold_refusal`]),
    /// weigert Hop meteen: geen download, geen stop, één regel
    /// (`HOP_FLIP_FAIL`) en een 502 met "ask warm".
    ///
    /// De taken stoppen pas ná de download: een URL die niet werkt, laat
    /// alles draaien. Ze stoppen via de agent (`hold_for_flip`, dezelfde
    /// Stop-acties als een preemptie, maar de records blijven), zodat de
    /// telling van de leader klopt als de kern toch niet springt; springt
    /// hij, dan plaatst de leader van de cluster ze opnieuw. Weigert de kern (de FLIP geeft een fout, of deze Hop leeft
    /// na [`COLD_FLIP_WAIT`] nog), dan herstarten ze hier
    /// ([`Node::cold_flip_back`]).
    async fn flip(&mut self, url: &str, sha256: &str, cold: bool, now: Nanos) -> Result<(), String>
    where
        S: crate::flip::KernFlip,
    {
        if cold && let Some(why) = self.cold_refusal() {
            return Err(String::from(why));
        }
        let how = if cold { " (cold)" } else { "" };
        self.lines.push(format!(
            "hop: kernel flip{how} requested from {url} HOP_FLIP"
        ));
        let slot = crate::flip::fetch(self.runner.system_mut(), &mut self.images, url).await?;
        if !cold {
            return crate::flip::ask(self.runner.system_mut(), slot, sha256, false).await;
        }
        let n = self.agent.hold_for_flip();
        self.drain(now).await;
        self.lines.push(format!(
            "hop: cold flip: {n} task(s) on this node stopped, their jobs come back after the new kernel HOP_FLIP_COLD_STOP stopped={n}"
        ));
        let r = crate::flip::ask(self.runner.system_mut(), slot, sha256, true).await;
        match r {
            Ok(()) => self.cold_flip_until = Some(now.saturating_add(COLD_FLIP_WAIT)),
            Err(_) => self.cold_flip_back(now).await,
        }
        r
    }

    /// RESTART: haal een nieuw image als er een `url` is, leg de staat nu
    /// bij de kern neer, en vraag de herstart (crate::restart).
    ///
    /// Zonder staat geen herstart: de nieuwe Hop zou elke app als bewoner
    /// van niemand stoppen. Een weigering laat alles draaien en ruimt het
    /// slot van het image op.
    async fn restart(&mut self, url: &str, sha256: &str, now: Nanos) -> Result<(), String>
    where
        S: crate::restart::KernRestart,
    {
        let what = if url.is_empty() {
            String::from("same image")
        } else {
            format!("image {url}")
        };
        self.lines.push(format!(
            "hop: restart of Hop requested ({what}) HOP_RESTART"
        ));
        let slot = if url.is_empty() {
            None
        } else {
            Some(crate::restart::fetch(self.runner.system_mut(), &mut self.images, url).await?)
        };
        if let Err(e) = self.save_state(now).await {
            if let Some(slot) = slot {
                let _ = self.runner.system_mut().stop_slot(slot, 0).await;
            }
            return Err(format!(
                "the state did not reach the kernel, so no restart: {e}"
            ));
        }
        crate::restart::ask(self.runner.system_mut(), slot, sha256).await
    }

    /// Waarom dit board nu niet koud kan flippen, als Hop dat al weet: het
    /// board-contract van de kern (`HOPOS_COLD_FLIP`) en de eigen
    /// plaatsingen. `None` is vragen; de kern weigert dan nog zelf.
    fn cold_refusal(&self) -> Option<&'static str> {
        match self.cold_flip {
            ColdFlip::No => Some("cold flip: no PSCI on this board, ask warm"),
            ColdFlip::Fresh if self.app_core_used => Some(
                "cold flip: CPU_OFF has no way back on this board and an app core ran, ask warm",
            ),
            ColdFlip::Fresh | ColdFlip::Yes => None,
        }
    }

    /// De koude flip ging niet door: de taken die hij stopte, herstarten.
    async fn cold_flip_back(&mut self, now: Nanos) {
        self.cold_flip_until = None;
        let n = self.agent.resume_after_flip(now);
        self.lines.push(format!(
            "hop: cold flip refused by the kernel, {n} stopped task(s) restart HOP_FLIP_COLD_BACK"
        ));
        self.drain(now).await;
    }

    /// Een verzoek aan de leader, met de eigen agent als transport.
    fn leader_handle(&mut self, req: &Request, now: Nanos) -> (Response, LeaderEffect) {
        let pool = self.runner.pool_largest();
        let mut net = Local {
            agent: &mut self.agent,
            now,
            pool_largest: pool,
            relay: self.cluster.as_mut().map(Cluster::relay_mut),
        };
        let mut cluster =
            LeaderCluster::new(&mut self.leader, &mut net).with_events(&mut self.events);
        self.leader_api.handle(&mut cluster, now, req)
    }

    /// Een verzoek aan de leader, met zijn [`LeaderEffect`] uitgevoerd.
    ///
    /// De standalone-cluster heeft één agent, deze: de rondgang van
    /// `/v1/tasks` en de doorgifte naar een agent gaan in-proces. Een andere
    /// agent kent deze leader niet, en wie hem toch noemt, krijgt dat luid.
    fn leader_reply(&mut self, req: &Request, now: Nanos) -> Routed {
        if let Some(r) = self.not_leading(req) {
            return Routed::Reply(Reply::Plain(r));
        }
        let (resp, effect) = self.leader_handle(req, now);
        match effect {
            LeaderEffect::None => Routed::Reply(Reply::Plain(resp)),
            LeaderEffect::Log(line) => {
                self.lines.push(line);
                Routed::Reply(Reply::Plain(resp))
            }
            LeaderEffect::Tasks { agents, scope } => {
                // Agents op andere nodes: de rondgang doet de verbindingstaak.
                if let Some(f) = self.tasks_forward(&agents, &scope) {
                    return Routed::Forward(f);
                }
                let mut results = Vec::new();
                if results.try_reserve_exact(agents.len()).is_err() {
                    return Routed::Reply(Reply::Plain(Response::empty(500)));
                }
                for (id, _) in agents {
                    // Een andere agent kent deze leader niet: hij ontbreekt,
                    // zoals een agent die niet antwoordt.
                    let tasks = if id == self.agent.id() {
                        own_tasks(&self.agent)
                    } else {
                        None
                    };
                    results.push((id, tasks));
                }
                Routed::Reply(Reply::Plain(scope.reply(&results)))
            }
            LeaderEffect::Agent {
                endpoint,
                path,
                stream,
            } => {
                if endpoint != self.agent.endpoint() {
                    // Een agent op een andere node: de verbindingstaak geeft
                    // het verzoek ondertekend door.
                    if let Some(f) = self.agent_forward(&endpoint, &path, stream) {
                        return self.admit_forward(f);
                    }
                    return Routed::Reply(Reply::Plain(Response::error(
                        502,
                        &format!(
                            "hop on HopOS: agent {endpoint} is not on this node, and a standalone leader has no other nodes"
                        ),
                    )));
                }
                // Dezelfde route op de eigen agent-API, ondertekend zoals een
                // doorgifte over de draad dat zou zijn.
                let mut inner = Request::new(Method::Get, &path, b"");
                if let Some(sig) = auth::sign_call(&self.key, "GET", &path, b"") {
                    inner.headers.push((
                        String::from(auth::AUTH_HEADER),
                        String::from_utf8_lossy(&sig).into_owned(),
                    ));
                }
                let pool = self.runner.pool_largest();
                let (resp, effect) = self.node_api.handle(&mut self.agent, now, pool, &inner);
                self.effect(resp, effect, &inner, now)
            }
            LeaderEffect::Events => Routed::Reply(self.admit(Reply::Stream {
                head: resp,
                first: String::from(api::PING),
                ask: Ask::Events {
                    seq: self.events.seq(),
                },
            })),
        }
    }

    /// Laat een stroom toe als er plaats is ([`MAX_STREAMS`]); anders 503.
    fn admit(&mut self, r: Reply) -> Reply {
        if !matches!(r, Reply::Stream { .. }) {
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

    /// Een open stroom is af (de verbindingstaak meldt het).
    pub fn stream_done(&mut self) {
        self.streams = self.streams.saturating_sub(1);
    }

    /// Wat een open stroom sinds zijn volgnummer mist.
    pub fn poll(&mut self, ask: &Ask, now: Nanos) -> Chunk {
        let mut c = Chunk::default();
        match ask {
            Ask::Logs {
                task_id,
                stream,
                seq,
            } => match self
                .runner
                .logs(now / MILLISECOND, task_id, runner_stream(*stream))
            {
                Some(ring) => {
                    for line in ring.since(*seq) {
                        api::data_frame(line, &mut c.text);
                    }
                    c.seq = ring.seq();
                    c.done = ring.is_closed();
                }
                None => c.done = true,
            },
            Ask::Events { seq } => {
                if !self.leads() {
                    // Een ex-leider houdt geen abonnees vast; de lezer
                    // verbindt met de nieuwe (zoals de daemon).
                    c.done = true;
                    return c;
                }
                self.collect_events();
                c.seq = self.events.since(*seq, &mut c.text);
            }
        }
        c
    }

    /// Haalt de meldingen van de leader in de rij van `/v1/events`.
    fn collect_events(&mut self) {
        for e in self.leader.drain_events() {
            self.events.push(&e);
        }
    }

    /// Voert een [`Effect`] uit, of weigert hem luid.
    fn effect(&mut self, resp: Response, effect: Effect, req: &Request, now: Nanos) -> Routed {
        match effect {
            Effect::None => Routed::Reply(Reply::Plain(resp)),
            // De leader is deze node: geen proxy over het net, maar dezelfde
            // handler in-proces (dezelfde HMAC, dezelfde body).
            Effect::Proxy { ref leader, .. } if *leader == self.own_leader => {
                self.leader_reply(req, now)
            }
            // De leader staat op een andere node: de verbindingstaak geeft
            // het verzoek ongewijzigd door (`forward`).
            Effect::Proxy { leader, stream } if self.cluster.is_some() => {
                self.admit_forward(Forward::Leader {
                    addr: leader,
                    req: req.clone(),
                    stream,
                })
            }
            Effect::Logs {
                ref task_id,
                stream: which,
            } => {
                let stream = runner_stream(which);
                if api::is_follow(req)
                    && self
                        .runner
                        .logs(now / MILLISECOND, task_id, stream)
                        .is_some()
                {
                    return Routed::Reply(self.admit(Reply::Stream {
                        head: resp,
                        first: String::new(),
                        ask: Ask::Logs {
                            task_id: task_id.clone(),
                            stream: which,
                            seq: 0,
                        },
                    }));
                }
                Routed::Reply(match self.runner.logs(now / MILLISECOND, task_id, stream) {
                    Some(ring) => Reply::Events {
                        head: resp,
                        lines: ring.tail().map(String::from).collect(),
                    },
                    None => Reply::Plain(refuse(&effect).unwrap_or(resp)),
                })
            }
            other => Routed::Reply(Reply::Plain(refuse(&other).unwrap_or(resp))),
        }
    }

    /// Laat de tijd verstrijken: heartbeat en tik van de leader, tik van de agent, de logpomp.
    pub async fn tick(&mut self, now: Nanos) {
        // De eigen agent is altijd levend zolang deze taak draait; zonder
        // heartbeat zou de eigen leader hem na 30 s dood verklaren.
        self.measure_system(now).await;
        let leads = self.leads();
        if leads {
            let telemetry = self.telemetry();
            self.leader
                .heartbeat(self.agent.id(), VERSION, telemetry, Time(now));
        }
        if leads && now >= self.next_leader_tick {
            self.next_leader_tick = now.saturating_add(LEADER_TICK);
            self.refresh_remote_tasks();
            let pool = self.runner.pool_largest();
            let mut net = Local {
                agent: &mut self.agent,
                now,
                pool_largest: pool,
                relay: self.cluster.as_mut().map(Cluster::relay_mut),
            };
            if let Err(e) = self.leader.tick(Time(now), &mut net) {
                self.lines.push(format!("hop: leader tick: {e}"));
            }
        }
        if self.cold_flip_until.is_some_and(|t| now >= t) {
            self.cold_flip_back(now).await;
        }
        let actions = self.agent.tick(now);
        self.run_actions(now, actions).await;
        self.drain(now).await;
        self.runner.pump_logs(now / MILLISECOND).await;
        self.cluster_tick(now);
        self.collect_events();
        self.keep_state(now).await;
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
                    if st == Status::Running {
                        self.record_usage(&task_id, pid).await;
                    }
                }
                Action::Probe { .. } => self.once(
                    "probe",
                    String::from(
                        "hop: health probes are not wired on HopOS yet; tasks with a health_check stay unprobed HOP_PROBE_SKIPPED",
                    ),
                ),
                Action::Refused { job, why } => {
                    let c = self.agent.capacity();
                    self.lines.push(format!(
                        "hop: job {job} refused here: no capacity ({why}; cpu {}/{} shares, memory {}/{} bytes) HOP_NO_CAPACITY",
                        c.cpu_used_shares,
                        u64::from(c.cpu_cores) * 1024,
                        c.memory_used_bytes,
                        c.memory_bytes
                    ));
                }
                Action::Notify { job, event } => self.notify(now, &job, event),
            }
        }
    }

    /// Een taakgebeurtenis naar de eigen leader, zoals `POST /v1/notify`.
    fn notify(&mut self, now: Nanos, job: &str, event: Event) {
        if !self.leads() {
            // De leader staat op een andere node: `POST /v1/notify` daar.
            self.notify_leader(job, event);
            return;
        }
        if event == Event::Unplaceable {
            let pool = self.runner.pool_largest();
            let mut net = Local {
                agent: &mut self.agent,
                now,
                pool_largest: pool,
                relay: self.cluster.as_mut().map(Cluster::relay_mut),
            };
            let id = String::from(net.agent.id());
            let _ = self.leader.mark_unplaced(&id, job, &mut net);
        }
        // In de rij van `/v1/events` mét het event, zoals een notify over
        // de API (LeaderCluster::with_events); de leader zelf onthoudt alleen
        // de naam.
        self.events
            .push_topic(&format!("job:{job}:{}", event.as_str()));
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
        let started = self.runner.start(ms, &req).await;
        // De kern zette hem op een core: buiten `system` telt dat als een
        // app-core ([`Node::cold_refusal`]).
        if matches!(started, Ok(Started::Running { .. } | Started::AwaitImage))
            && tags.get("sharegroup").map(String::as_str) != Some(agent::SYSTEM_GROUP)
        {
            self.app_core_used = true;
        }
        let outcome = match started {
            Ok(Started::Running { pid }) => Ok(pid),
            Ok(Started::Aborted) => Err((StartError::Failed, String::from("aborted"))),
            Ok(Started::AwaitImage) => {
                let artifact = artifact.cloned().unwrap_or_default();
                if self.loads.try_reserve(1).is_ok() {
                    self.loads.push_back(Loading {
                        seq: self.next_seq,
                        task_id: String::from(task_id),
                        job: job.name.clone(),
                        artifact,
                        ports,
                        sent: false,
                        placed: None,
                    });
                    self.next_seq = self.next_seq.saturating_add(1);
                    return;
                }
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
                Err((StartError::Failed, String::from("out of memory")))
            }
            Err(e) => Err((start_error(&e), format!("{e}"))),
        };
        self.report_start(now, task_id, &job.name, &ports, outcome);
    }

    /// Meldt de afloop van een start aan de agent, met de regel voor het log.
    fn report_start(
        &mut self,
        now: Nanos,
        task_id: &str,
        job: &str,
        ports: &BTreeMap<String, u16>,
        outcome: Outcome,
    ) {
        match outcome {
            Ok(pid) => {
                self.lines.push(format!(
                    "hop: job {job} task {task_id} placed HOP_JOB_PLACED slot={pid}"
                ));
                let mut placed = Map::new();
                for (k, v) in ports {
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
                    "hop: job {job} task {task_id} did not start: {why} HOP_JOB_FAILED"
                ));
                self.agent.on_started(now, task_id, Driver::Hop, Err(kind));
            }
        }
    }

    /// De volgende download voor de downloadtaak, als er een klaarligt.
    ///
    /// Eén tegelijk: de volgende komt pas als de vorige klaar is, zodat de
    /// starts hun image in volgorde krijgen.
    pub fn take_order(&mut self) -> Option<Order> {
        let front = self.loads.front_mut().filter(|l| !l.sent)?;
        front.sent = true;
        Some(Order {
            seq: front.seq,
            artifact: front.artifact.clone(),
        })
    }

    /// Een opdracht die de rij van de downloadtaak niet in kon: de volgende
    /// [`Node::take_order`] geeft hem opnieuw.
    pub fn order_back(&mut self, order: &Order) {
        if let Some(front) = self.loads.front_mut().filter(|l| l.seq == order.seq) {
            front.sent = false;
        }
    }

    /// Het nummer van de download die de node nog wil; 0 als er geen loopt.
    pub fn wanted(&self) -> u64 {
        self.loads.front().filter(|l| l.sent).map_or(0, |l| l.seq)
    }

    /// Een stap van de lopende download: de lengte, een brok de kern in, of het einde.
    ///
    /// Een stap van een download die de node niet meer wil (een ander
    /// nummer), telt niet. Faalt een stap, dan is de start klaar; de
    /// downloadtaak ziet dat aan [`Node::wanted`] en stopt.
    pub async fn on_piece(&mut self, seq: u64, piece: Piece, now: Nanos) {
        let Some(load) = self.loads.front().filter(|l| l.sent && l.seq == seq) else {
            return;
        };
        let ms = now / MILLISECOND;
        let task = load.task_id.clone();
        let placed = load.placed;
        let outcome = match piece {
            Piece::Begin(size) => match self.runner.image_begin(ms, &task, size).await {
                Ok(()) => return,
                Err(e) => Err((start_error(&e), format!("{e}"))),
            },
            Piece::Bytes(bytes) => match self.runner.image_chunk(ms, &task, &bytes).await {
                Ok(Started::AwaitImage) => return,
                Ok(Started::Running { pid }) => {
                    if let Some(l) = self.loads.front_mut() {
                        l.placed = Some(pid);
                    }
                    return;
                }
                Ok(Started::Aborted) => Err((
                    StartError::Failed,
                    String::from("task stopped during its start"),
                )),
                Err(e) => Err((start_error(&e), format!("{e}"))),
            },
            Piece::End(Ok(())) => placed.ok_or((
                StartError::Failed,
                String::from("image ended before its length"),
            )),
            Piece::End(Err(why)) => Err((StartError::Failed, why)),
        };
        self.finish_load(now, outcome).await;
    }

    /// Sluit de lopende download af: de kooi terug als hij faalde, de afloop
    /// naar de agent, en wat de agent daarna wil.
    async fn finish_load(&mut self, now: Nanos, outcome: Outcome) {
        let Some(load) = self.loads.pop_front() else {
            return;
        };
        if outcome.is_err() {
            // Een start die faalde, laat geen gereserveerde kooi staan.
            let id = TaskRef {
                id: &load.task_id,
                pid: 0,
            };
            let _ = self.runner.stop(now / MILLISECOND, &id).await;
        }
        self.report_start(now, &load.task_id, &load.job, &load.ports, outcome);
        self.drain(now).await;
        self.collect_events();
        self.after_cluster(now);
    }

    /// Haalt elke download die klaarligt meteen op, zonder downloadtaak.
    ///
    /// Alleen voor de tests: die willen na een verzoek of een tik de
    /// plaatsing zien zonder zelf een taak te draaien.
    #[cfg(test)]
    pub(crate) async fn settle(&mut self, now: Nanos) {
        /// Verzamelt de stappen van één download.
        struct Collect(Vec<Piece>);
        impl Sink for Collect {
            async fn begin(&mut self, size: u64) -> Result<(), String> {
                self.0.push(Piece::Begin(size));
                Ok(())
            }
            async fn chunk(&mut self, bytes: &[u8]) -> Result<(), String> {
                self.0.push(Piece::Bytes(bytes.to_vec()));
                Ok(())
            }
        }
        for _ in 0..64 {
            let Some(order) = self.take_order() else {
                return;
            };
            let mut pieces = Collect(Vec::new());
            let r = self.images.fetch(&order.artifact, &mut pieces).await;
            pieces.0.push(Piece::End(r));
            for p in pieces.0 {
                self.on_piece(order.seq, p, now).await;
            }
        }
    }
}

/// De taken van de eigen agent; `None` zonder geheugen.
fn own_tasks(agent: &Agent) -> Option<Vec<types::Task>> {
    let mut out = Vec::new();
    for t in agent.tasks() {
        out.try_reserve(1).ok()?;
        out.push(t.clone());
    }
    Some(out)
}

/// De logstroom van de API als die van de runner.
fn runner_stream(s: LogStream) -> Stream {
    match s {
        LogStream::Stdout => Stream::Stdout,
        LogStream::Stderr => Stream::Stderr,
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

/// De groepen op een core die Hop niet uitdeelt: Hop's eigen groep
/// (`HOPOS_HOP_GROUP`, de core van Hop zelf, eigen of gedeeld met de kern),
/// en `system` als de kern zijn core deelt (`HOPOS_SYSTEM_CORE=1`).
fn free_groups(system_core: bool, hop_group: &str) -> Vec<String> {
    let mut g = Vec::from([String::from(hop_group)]);
    if system_core && hop_group != agent::SYSTEM_GROUP {
        g.push(String::from(agent::SYSTEM_GROUP));
    }
    g
}

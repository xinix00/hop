//! De watcher van één peer, als toestandsmachine zonder I/O.
//!
//! Elke cluster is een peer, ook de eigen; er is geen aparte "lokale"
//! watcher. Per peer:
//!
//! 1. **Ontdekken.** `GET /v1/status` geeft `cluster_name`. Faalt dat (of is
//!    de naam leeg, een Hop van vóór de federatie), dan over [`RECONNECT`]
//!    opnieuw. De naam is stabiel: hij wordt één keer ontdekt.
//! 2. **Luisteren.** De driver opent `/v1/events` (hoplib's stroom, die zelf
//!    herverbindt met [`RECONNECT`] als vaste wachttijd, zoals Go's
//!    interval) en geeft elke gebeurtenis aan [`Watcher::event`].
//! 3. **Herladen.** Een `ping` (de eerste gebeurtenis van elke verbinding)
//!    zaait meteen een volledige verversing: wat er tussen twee
//!    verbindingen gebeurde, zag niemand. Een `job`- of `task`-melding zet
//!    die job op de lijst; een `status` (de lezer miste meldingen) of een
//!    `agent` (een endpoint kwam of ging) vraagt een volledige verversing.
//!    Beide wachten [`DEBOUNCE`] vanaf de eerste melding, zodat een storm
//!    van meldingen één verversing wordt.
//! 4. **Herbouwen.** De driver haalt de staat op en geeft hem terug; de
//!    watcher bouwt het clusterbeeld ([`build_jobs`], [`build_job`]) en
//!    geeft een [`Update`] die de eigenaar van de cache toepast.
//!
//! Herverbinden is beleid, en het beleid is Go's: een stroom die wegvalt,
//! laat de cache staan zoals hij was (verouderd maar bruikbaar, zoals
//! hoplb), want een failover van de leader mag DNS niet zwart maken terwijl
//! de taken doordraaien. De volgende `ping` vervangt hem. Een mislukte
//! verversing laat de cache ook staan: een job-verversing valt terug op
//! een volledige (Go's `refreshJob`), een volledige probeert het over
//! [`RECONNECT`] opnieuw.
//!
//! Het adres van een taak: dat van zijn agent (het endpoint, `http://ip:poort`),
//! zoals in Go. Op HopOS heeft elke taak een eigen slot-adres op het
//! interne net van de node; een bewoner die zijn eigen node kent
//! ([`Local`]), geeft voor de taken daar dat slot-adres, want dat is wat een
//! buurslot rechtstreeks bereikt.
//!
//! Tijd is een monotone teller in nanoseconden van de driver.

use alloc::string::String;
use alloc::vec::Vec;
use core::net::Ipv4Addr;
use core::time::Duration;

use hoplib::{AgentInfo, AgentTasks, Event, JobStatus, Task, TaskState, Topic};

use crate::cache::Jobs;
use crate::config::owned;
use crate::{Error, Result};

/// Hoe lang een peer wacht na een mislukte ontdekking, een weggevallen
/// stroom of een mislukte verversing: Go's `interval`.
pub const RECONNECT: Duration = Duration::from_secs(5);

/// Hoe lang meldingen samengevoegd worden tot één verversing: Go's
/// `debounce`.
pub const DEBOUNCE: Duration = Duration::from_millis(500);

/// Hoeveel verschillende jobs op de lijst mogen voordat het een volledige
/// verversing wordt: meer meldingen in één halve seconde is een cluster dat
/// alles tegelijk verandert.
pub const MAX_PENDING: usize = 32;

/// De grootste pid die een slot kan zijn (`abi::layout::Slot` is een `u8`).
const SLOT_MAX: i64 = 255;

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// De eigen node van een bewoner op HopOS: zijn taken krijgen hun
/// slot-adres in plaats van dat van de agent.
#[derive(Clone, Copy, Debug)]
pub struct Local<'a> {
    /// Het id van de node (de agent), uit `ER_ATTR_NODE_ID`.
    pub node: &'a str,
    /// Het adres van slot `n` op het interne net (applib's `slot_ip`).
    pub slot_ip: fn(u64) -> [u8; 4],
}

/// Wat de driver nu moet doen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Vraag `/v1/status`; geef de naam aan [`Watcher::discovered`] of de
    /// fout aan [`Watcher::discover_failed`].
    Discover,
    /// Haal de staat op en geef hem aan [`Watcher::refreshed_full`] of
    /// [`Watcher::refreshed_job`]; een fout aan [`Watcher::refresh_failed`].
    Refresh(Refresh),
    /// Wacht op een gebeurtenis, tot dit moment (`None`: zonder termijn).
    Wait(Option<u64>),
}

/// Wat er ververst moet worden.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refresh {
    /// Alles: `GET /v1/agents` en `GET /v1/tasks`.
    Full,
    /// Deze jobs: `GET /v1/jobs/{naam}/status` per job.
    Jobs(Vec<String>),
}

/// Een bericht aan de eigenaar van de cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Update {
    /// Vervang het hele beeld van een cluster ([`crate::Cache::update`]).
    Replace {
        /// Het cluster.
        cluster: String,
        /// Het nieuwe beeld.
        jobs: Jobs,
    },
    /// Zet één job ([`crate::Cache::set`]).
    Set {
        /// Het cluster.
        cluster: String,
        /// De job.
        job: String,
        /// Zijn adressen (leeg: geen lopende taak).
        ips: Vec<Ipv4Addr>,
    },
}

impl Update {
    /// Past het bericht toe op `cache`.
    pub fn apply(self, cache: &mut crate::Cache) -> Result {
        match self {
            Update::Replace { cluster, jobs } => cache.update(&cluster, jobs),
            Update::Set { cluster, job, ips } => cache.set(&cluster, &job, ips),
        }
    }

    /// Het cluster van het bericht.
    #[must_use]
    pub fn cluster(&self) -> &str {
        match self {
            Update::Replace { cluster, .. } | Update::Set { cluster, .. } => cluster,
        }
    }
}

/// Wat een gebeurtenis voor de cache betekent.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Kind {
    /// Een nieuwe verbinding: meteen alles.
    Seed,
    /// Alles, na het samenvoegen.
    Full,
    /// Deze job, na het samenvoegen.
    Job(String),
    /// Niets.
    Ignore,
}

fn classify(ev: &Event) -> Kind {
    match ev.topic() {
        Ok(Topic::Ping) => Kind::Seed,
        Ok(Topic::Status | Topic::Agent { .. }) => Kind::Full,
        Ok(Topic::Job { name }) if !name.is_empty() => Kind::Job(name),
        Ok(Topic::Task { job, .. }) if !job.is_empty() => Kind::Job(job),
        Ok(_) => Kind::Ignore,
        // Een melding die niet leest, zegt in elk geval dat er iets
        // veranderde: dan alles.
        Err(_) => Kind::Full,
    }
}

/// De watcher van één peer.
#[derive(Clone, Debug)]
pub struct Watcher {
    endpoint: String,
    cluster: Option<String>,
    /// Wanneer de volgende ontdekking mag.
    retry_at: u64,
    /// Wanneer de verversing loopt, als er een klaarstaat.
    due: Option<u64>,
    /// Er staat een volledige verversing klaar.
    full: bool,
    /// De jobs die klaarstaan (zonder dubbelen, hoogstens [`MAX_PENDING`]).
    jobs: Vec<String>,
    interval: u64,
    debounce: u64,
}

impl Watcher {
    /// Een watcher voor de peer op `endpoint` (alleen voor de logregels).
    pub fn new(endpoint: &str) -> Result<Self> {
        Ok(Self {
            endpoint: owned(endpoint)?,
            cluster: None,
            retry_at: 0,
            due: None,
            full: false,
            jobs: Vec::new(),
            interval: nanos(RECONNECT),
            debounce: nanos(DEBOUNCE),
        })
    }

    /// Dezelfde watcher met een andere wachttijd na een fout (tests).
    #[must_use]
    pub fn with_interval(mut self, d: Duration) -> Self {
        self.interval = nanos(d);
        self
    }

    /// Het endpoint van de peer.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// De clusternaam, als hij ontdekt is.
    #[must_use]
    pub fn cluster(&self) -> Option<&str> {
        self.cluster.as_deref()
    }

    /// Wat de driver nu moet doen.
    pub fn next(&mut self, now: u64) -> Action {
        if self.cluster.is_none() {
            return if now >= self.retry_at {
                Action::Discover
            } else {
                Action::Wait(Some(self.retry_at))
            };
        }
        match self.due {
            Some(t) if t <= now => {
                self.due = None;
                let jobs = core::mem::take(&mut self.jobs);
                if core::mem::take(&mut self.full) {
                    Action::Refresh(Refresh::Full)
                } else {
                    Action::Refresh(Refresh::Jobs(jobs))
                }
            }
            other => Action::Wait(other),
        }
    }

    /// `/v1/status` antwoordde met `name`. Een lege naam is
    /// [`Error::NoClusterName`], en dan over [`RECONNECT`] opnieuw.
    pub fn discovered(&mut self, now: u64, name: &str) -> Result {
        let name = name.trim();
        if name.is_empty() {
            self.discover_failed(now);
            return Err(Error::NoClusterName);
        }
        self.cluster = Some(lower(name)?);
        Ok(())
    }

    /// `/v1/status` faalde: over [`RECONNECT`] opnieuw.
    pub fn discover_failed(&mut self, now: u64) {
        self.retry_at = now.saturating_add(self.interval);
    }

    /// Een gebeurtenis van de stroom.
    pub fn event(&mut self, now: u64, ev: &Event) -> Result {
        match classify(ev) {
            Kind::Seed => {
                self.full = true;
                self.jobs.clear();
                self.due = Some(now);
            }
            Kind::Full => self.pend_full(now),
            Kind::Job(name) => {
                if self.full {
                    return Ok(());
                }
                let name = lower(&name)?;
                if !self.jobs.contains(&name) {
                    if self.jobs.len() >= MAX_PENDING {
                        self.pend_full(now);
                        return Ok(());
                    }
                    self.jobs.try_reserve(1)?;
                    self.jobs.push(name);
                }
                self.due.get_or_insert(now.saturating_add(self.debounce));
            }
            Kind::Ignore => {}
        }
        Ok(())
    }

    fn pend_full(&mut self, now: u64) {
        self.full = true;
        self.jobs.clear();
        self.due.get_or_insert(now.saturating_add(self.debounce));
    }

    /// De stroom viel weg. De cache blijft staan (zie de moduledoc); wat
    /// klaarstond, vervalt, want de `ping` van de volgende verbinding zaait
    /// opnieuw.
    pub fn lost(&mut self) {
        self.due = None;
        self.full = false;
        self.jobs.clear();
    }

    /// De staat voor een volledige verversing kwam binnen: het nieuwe
    /// beeld van het cluster.
    pub fn refreshed_full(
        &mut self,
        agents: &[AgentInfo],
        tasks: &[AgentTasks],
        local: Option<&Local<'_>>,
    ) -> Result<Update> {
        Ok(Update::Replace {
            cluster: self.cluster_owned()?,
            jobs: build_jobs(agents, tasks, local)?,
        })
    }

    /// De staat van één job kwam binnen: zijn nieuwe adressen.
    pub fn refreshed_job(
        &mut self,
        job: &str,
        status: &JobStatus,
        local: Option<&Local<'_>>,
    ) -> Result<Update> {
        Ok(Update::Set {
            cluster: self.cluster_owned()?,
            job: lower(job)?,
            ips: build_job(job, &status.agents, &status.tasks, local)?,
        })
    }

    /// Een verversing faalde. De cache blijft staan; een job-verversing
    /// valt meteen terug op een volledige (Go), een volledige probeert het
    /// over [`RECONNECT`] opnieuw.
    pub fn refresh_failed(&mut self, now: u64, what: &Refresh) {
        self.full = true;
        self.jobs.clear();
        let at = match what {
            Refresh::Full => now.saturating_add(self.interval),
            Refresh::Jobs(_) => now,
        };
        self.due = Some(self.due.map_or(at, |d| d.min(at)));
    }

    fn cluster_owned(&self) -> Result<String> {
        owned(self.cluster.as_deref().ok_or(Error::NoClusterName)?)
    }
}

/// `s` in kleine ASCII-letters: DNS vergelijkt zonder hoofdletters, dus de
/// cache ook.
fn lower(s: &str) -> Result<String> {
    let mut o = String::new();
    o.try_reserve_exact(s.len())?;
    o.extend(s.chars().map(|c| c.to_ascii_lowercase()));
    Ok(o)
}

/// Het IPv4-adres uit een endpoint (`http://ip:poort` naar ip); `None` voor
/// een hostnaam, een IPv6-adres of iets dat geen URL is (Go's `extractIP`,
/// maar hopdns geeft alleen A-records).
#[must_use]
pub fn extract_ip(endpoint: &str) -> Option<Ipv4Addr> {
    let (_, rest) = endpoint.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next()?;
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let host = host.split(':').next()?;
    host.parse().ok()
}

/// Het adres van `task` op agent `agent`, als hij loopt.
fn task_ip(
    agent: &str,
    agent_ip: Option<Ipv4Addr>,
    task: &Task,
    local: Option<&Local<'_>>,
) -> Option<Ipv4Addr> {
    if task.state != TaskState::Running {
        return None;
    }
    if let Some(l) = local
        && l.node == agent
        && task.driver == "hop"
        && (1..=SLOT_MAX).contains(&task.pid)
    {
        return Some(Ipv4Addr::from((l.slot_ip)(task.pid.unsigned_abs())));
    }
    agent_ip
}

fn push_unique(list: &mut Vec<Ipv4Addr>, ip: Ipv4Addr) -> Result {
    if !list.contains(&ip) {
        list.try_reserve(1)?;
        list.push(ip);
    }
    Ok(())
}

/// Het adres per agent-id uit de agentlijst.
fn agent_ip(agents: &[AgentInfo], id: &str) -> Option<Ipv4Addr> {
    agents
        .iter()
        .find(|a| a.id == id)
        .and_then(|a| extract_ip(&a.endpoint))
}

/// Het beeld van een cluster uit zijn agents en al hun taken (Go's
/// `refresh`): per job de adressen van de lopende taken, elk één keer. Een
/// job zonder lopende taak staat er niet in; een agent die de leader niet
/// antwoordde (`tasks: None`), draagt niets bij.
pub fn build_jobs(
    agents: &[AgentInfo],
    tasks: &[AgentTasks],
    local: Option<&Local<'_>>,
) -> Result<Jobs> {
    let mut jobs = Jobs::new();
    for at in tasks {
        let ip = agent_ip(agents, &at.agent);
        for task in at.tasks.as_deref().unwrap_or_default() {
            let Some(addr) = task_ip(&at.agent, ip, task, local) else {
                continue;
            };
            let name = lower(&task.job_name)?;
            match jobs.get_mut(&name) {
                Some(list) => push_unique(list, addr)?,
                None => {
                    let mut list = Vec::new();
                    push_unique(&mut list, addr)?;
                    jobs.insert(&name, list)?;
                }
            }
        }
    }
    Ok(jobs)
}

/// De adressen van één job uit zijn status (Go's `refreshJob`).
pub fn build_job(
    job: &str,
    agents: &[AgentInfo],
    tasks: &[AgentTasks],
    local: Option<&Local<'_>>,
) -> Result<Vec<Ipv4Addr>> {
    let mut ips = Vec::new();
    for at in tasks {
        let ip = agent_ip(agents, &at.agent);
        for task in at.tasks.as_deref().unwrap_or_default() {
            if !task.job_name.eq_ignore_ascii_case(job) {
                continue;
            }
            if let Some(addr) = task_ip(&at.agent, ip, task, local) {
                push_unique(&mut ips, addr)?;
            }
        }
    }
    Ok(ips)
}

#[cfg(test)]
mod tests;

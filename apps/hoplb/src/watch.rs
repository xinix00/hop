//! De staat van de watcher: wat hoplb van het cluster weet, en welke
//! routetabel daaruit volgt (Go: `internal/lb/watcher.go`).
//!
//! Bezit de cache die Go ook had: agent naar hostnaam, de jobs (met hun
//! `hoplb-urlprefix`, `hoplb-port` en of ze door de tag-filter komen) en per
//! relevante job de taken per agent. Bezit geen verbinding: de schil vraagt
//! de agent (via hoplib) en geeft de antwoorden hier binnen; deze module zegt
//! wat een gebeurtenis betekent ([`Watcher::classify`]), wanneer de schil
//! weer moet vragen ([`Pending`]) en welke tabel er nu geldt
//! ([`Watcher::build_routes`]).
//!
//! De regels van Go, ongewijzigd:
//!
//! - een job is relevant als hij door de tag-filter komt (`-tag lb:haas`:
//!   tag `lb` is `haas`; zonder filter elke job) en een `hoplb-urlprefix`
//!   heeft;
//! - alleen taken in `running` krijgen verkeer, op de poort uit
//!   `hoplb-port` of anders de eerste poort van de taak;
//! - de host van een backend is de host uit het endpoint van zijn agent;
//! - een job-gebeurtenis (`{"name":..}`) betekent dat de definitie (en dus
//!   de tags) veranderde: altijd de hele lijst opnieuw, ook voor een job die
//!   eerder irrelevant was (gemeten 08-09-2026 op traqqr: een hernoemde tag
//!   werd tot een herstart genegeerd); een taak-gebeurtenis (`{"job":..}`)
//!   alleen die job, en niets voor een bekende irrelevante job.

use alloc::borrow::Cow;
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use hoplib::{AgentInfo, AgentTasks, Job, JobStatus, Task, TaskState};

use crate::error::{Error, Result, try_push, try_string};
use crate::json;
use crate::route::{Backend, Route, RouteTable};

/// De tag met het patroon van een job.
pub const TAG_URLPREFIX: &str = "hoplb-urlprefix";

/// De tag met de naam van de poort die verkeer krijgt.
pub const TAG_PORT: &str = "hoplb-port";

/// Hoe lang gebeurtenissen samen mogen komen voor de schil vraagt (Go:
/// `debounce.Reset(500 * time.Millisecond)`).
pub const DEBOUNCE: Duration = Duration::from_millis(500);

/// Hoe lang de schil wacht na een verbroken stroom (Go: `interval`, 5 s).
/// hoplib's backoff begint korter (1 s) en verdubbelt tot 30 s; dit is de
/// wacht van een mislukte synchronisatie.
pub const RETRY: Duration = Duration::from_secs(5);

/// Wat een gebeurtenis betekent (Go: `classifyEvent`).
///
/// `line` is een `data:`-regel of alleen de data. Het antwoord is de job en
/// of de hele lijst opnieuw moet; `None` is negeren (een ping, onzin, een
/// taak van een bekende irrelevante job).
pub fn classify_event<'a>(
    line: &'a str,
    is_relevant: impl Fn(&str) -> bool,
    is_known: impl Fn(&str) -> bool,
) -> Option<(Cow<'a, str>, bool)> {
    let data = line.strip_prefix("data:").unwrap_or(line).trim();
    if let Some(name) = json::string_field(data, "name").filter(|n| !n.is_empty()) {
        return Some((name, true));
    }
    let job = json::string_field(data, "job").filter(|j| !j.is_empty())?;
    let known = is_known(&job);
    if known && !is_relevant(&job) {
        return None;
    }
    Some((job, !known))
}

/// Leest `key:value` (Go: `parseTagFilter`); zonder `:` is de waarde leeg.
pub fn parse_tag_filter(filter: &str) -> (&str, &str) {
    filter.split_once(':').unwrap_or((filter, ""))
}

/// Wat hoplb van één job onthoudt.
#[derive(Debug)]
struct JobEntry {
    name: String,
    prefix: String,
    port: String,
    relevant: bool,
}

/// De cache van de watcher (Go: `Watcher`, zonder de verbinding).
///
/// # Invariants
///
/// `agents` en `jobs` zijn gesorteerd op id en naam, zonder dubbelen;
/// `tasks` heeft alleen relevante jobs, gesorteerd op naam.
#[derive(Debug, Default)]
pub struct Watcher {
    /// De tag-filter als (sleutel, waarde); `None` laat alles door.
    filter: Option<(String, String)>,
    /// Agent-id naar hostnaam.
    agents: Vec<(String, String)>,
    jobs: Vec<JobEntry>,
    /// Job naar zijn taken per agent.
    tasks: Vec<(String, Vec<AgentTasks>)>,
}

impl Watcher {
    /// Een lege cache met tag-filter `tag` (`""` is geen filter).
    pub fn new(tag: &str) -> Result<Self> {
        let filter = if tag.is_empty() {
            None
        } else {
            let (k, v) = parse_tag_filter(tag);
            Some((try_string(k)?, try_string(v)?))
        };
        Ok(Self {
            filter,
            ..Self::default()
        })
    }

    /// Komt `job` door de tag-filter (Go: `jobMatchesFilter`)?
    pub fn matches_filter(&self, job: &Job) -> bool {
        match &self.filter {
            None => true,
            Some((k, v)) => job.tags.get(k).map_or("", String::as_str) == v,
        }
    }

    /// Wat een gebeurtenis betekent, gezien wat deze cache weet.
    pub fn classify<'a>(&self, line: &'a str) -> Option<(Cow<'a, str>, bool)> {
        classify_event(line, |j| self.is_relevant(j), |j| self.is_known(j))
    }

    /// Geeft deze job routes?
    pub fn is_relevant(&self, job: &str) -> bool {
        self.job(job).is_some_and(|j| j.relevant)
    }

    /// Kent de cache deze job?
    pub fn is_known(&self, job: &str) -> bool {
        self.job(job).is_some()
    }

    /// De relevante jobs, gesorteerd.
    pub fn relevant(&self) -> impl Iterator<Item = &str> {
        self.jobs
            .iter()
            .filter(|j| j.relevant)
            .map(|j| j.name.as_str())
    }

    /// Een volledige synchronisatie (Go: `sync`): de agents, de jobs, en alle
    /// taken in één lijst; de cache wordt helemaal vervangen.
    pub fn apply_full(
        &mut self,
        agents: &[AgentInfo],
        jobs: &[Job],
        tasks: Vec<AgentTasks>,
    ) -> Result {
        let mut hosts = Vec::new();
        for a in agents {
            set_host(&mut hosts, &a.id, &a.endpoint)?;
        }
        let mut entries: Vec<JobEntry> = Vec::new();
        for j in jobs {
            let prefix = j.tags.get(TAG_URLPREFIX).map_or("", String::as_str);
            let port = j.tags.get(TAG_PORT).map_or("", String::as_str);
            let e = JobEntry {
                name: try_string(&j.name)?,
                prefix: try_string(prefix)?,
                port: try_string(port)?,
                relevant: self.matches_filter(j) && !prefix.is_empty(),
            };
            match entries.binary_search_by(|x| x.name.as_str().cmp(&j.name)) {
                Ok(i) => {
                    if let Some(slot) = entries.get_mut(i) {
                        *slot = e;
                    }
                }
                Err(i) => crate::error::try_insert(&mut entries, i, e)?,
            }
        }
        // INVARIANT: beide gesorteerd, via set_host en de binaire invoeging.
        self.agents = hosts;
        self.jobs = entries;
        // De taken per relevante job, in één gang over de lijst: geen kopie
        // van de hele lijst per job.
        let mut per_job: Vec<(String, Vec<AgentTasks>)> = Vec::new();
        for name in self.relevant() {
            try_push(&mut per_job, (try_string(name)?, Vec::new()))?;
        }
        for at in tasks {
            for t in at.tasks.into_iter().flatten() {
                let Ok(i) = per_job.binary_search_by(|(j, _)| j.as_str().cmp(&t.job_name)) else {
                    continue;
                };
                let Some((_, list)) = per_job.get_mut(i) else {
                    continue;
                };
                match list.last_mut() {
                    Some(last) if last.agent == at.agent => {
                        let mine = last.tasks.get_or_insert_with(Vec::new);
                        try_push(mine, t)?;
                    }
                    _ => {
                        let mut mine = Vec::new();
                        try_push(&mut mine, t)?;
                        let agent = try_string(&at.agent)?;
                        try_push(
                            list,
                            AgentTasks {
                                agent,
                                tasks: Some(mine),
                            },
                        )?;
                    }
                }
            }
        }
        // INVARIANT: `per_job` volgt de gesorteerde relevante jobs.
        self.tasks = per_job;
        Ok(())
    }

    /// De taken van één job (Go: `syncJob`): de agents uit het antwoord
    /// komen erbij, de taken van de job worden vervangen.
    pub fn apply_job(&mut self, name: &str, status: JobStatus) -> Result {
        for a in &status.agents {
            set_host(&mut self.agents, &a.id, &a.endpoint)?;
        }
        if self.is_relevant(name) {
            self.set_tasks(name, status.tasks)?;
        }
        Ok(())
    }

    fn set_tasks(&mut self, name: &str, tasks: Vec<AgentTasks>) -> Result {
        match self.tasks.binary_search_by(|(j, _)| j.as_str().cmp(name)) {
            Ok(i) => {
                if let Some(slot) = self.tasks.get_mut(i) {
                    slot.1 = tasks;
                }
                Ok(())
            }
            Err(i) => crate::error::try_insert(&mut self.tasks, i, (try_string(name)?, tasks)),
        }
    }

    fn job(&self, name: &str) -> Option<&JobEntry> {
        self.jobs
            .binary_search_by(|j| j.name.as_str().cmp(name))
            .ok()
            .and_then(|i| self.jobs.get(i))
    }

    fn host(&self, agent: &str) -> Option<&str> {
        self.agents
            .binary_search_by(|(id, _)| id.as_str().cmp(agent))
            .ok()
            .and_then(|i| self.agents.get(i))
            .map(|(_, h)| h.as_str())
            .filter(|h| !h.is_empty())
    }

    /// De routetabel uit de cache (Go: `buildRoutes`).
    ///
    /// Per relevante job, per agent met een bekende host, per draaiende taak
    /// met een poort: één backend onder het patroon van de job. Twee jobs
    /// met hetzelfde patroon delen één route.
    pub fn build_routes(&self) -> Result<RouteTable> {
        let mut routes: Vec<Route> = Vec::new();
        for (name, per_agent) in &self.tasks {
            let Some(job) = self.job(name).filter(|j| j.relevant) else {
                continue;
            };
            let mut backends = Vec::new();
            for at in per_agent {
                let Some(host) = self.host(&at.agent) else {
                    continue;
                };
                for t in at.tasks.iter().flatten() {
                    if t.state != TaskState::Running {
                        continue;
                    }
                    let Some(port) = task_port(t, &job.port) else {
                        continue;
                    };
                    try_push(&mut backends, backend(host, port)?)?;
                }
            }
            if !backends.is_empty() {
                try_push(&mut routes, Route::new(try_string(&job.prefix)?, backends))?;
            }
        }
        RouteTable::from_routes(routes)
    }
}

/// De poort van een taak: de benoemde, anders de eerste (Go: `taskPort`).
pub fn task_port(task: &Task, name: &str) -> Option<u16> {
    if !name.is_empty()
        && let Some(p) = task.ports.get(name)
    {
        return Some(*p).filter(|p| *p != 0);
    }
    task.ports.iter().map(|(_, p)| *p).find(|p| *p != 0)
}

/// De host uit een endpoint (Go: `extractHost`): `http://10.0.0.1:8080` is
/// `10.0.0.1`, `http://[::1]:80` is `::1`. Leeg als het geen URL is.
pub fn extract_host(endpoint: &str) -> &str {
    let Some((_, rest)) = endpoint.split_once("://") else {
        return "";
    };
    let auth = rest.split(['/', '?', '#']).next().unwrap_or("");
    let auth = auth.rsplit_once('@').map_or(auth, |(_, h)| h);
    if let Some(v6) = auth.strip_prefix('[') {
        return v6.split(']').next().unwrap_or("");
    }
    auth.split(':').next().unwrap_or("")
}

/// Een backend op `host:port`, met haken om een IPv6-adres.
fn backend(host: &str, port: u16) -> Result<Backend> {
    let mut a = String::new();
    a.try_reserve(host.len() + 8)
        .map_err(|_| Error::OutOfMemory {
            bytes: host.len() + 8,
        })?;
    if host.contains(':') {
        a.push('[');
        a.push_str(host);
        a.push(']');
    } else {
        a.push_str(host);
    }
    a.push(':');
    let mut digits = [0u8; 5];
    let mut n = port;
    let mut i = digits.len();
    loop {
        i -= 1;
        if let Some(d) = digits.get_mut(i) {
            *d = b'0' + (n % 10) as u8;
        }
        n /= 10;
        if n == 0 {
            break;
        }
    }
    a.push_str(core::str::from_utf8(digits.get(i..).unwrap_or_default()).unwrap_or(""));
    Ok(Backend {
        address: a,
        healthy: true,
    })
}

/// Zet agent `id` op de host van `endpoint`; een lege host laat hem weg
/// (Go: alleen `if h := extractHost(...); h != ""`).
fn set_host(hosts: &mut Vec<(String, String)>, id: &str, endpoint: &str) -> Result {
    let h = extract_host(endpoint);
    if h.is_empty() {
        return Ok(());
    }
    match hosts.binary_search_by(|(x, _)| x.as_str().cmp(id)) {
        Ok(i) => {
            if let Some(slot) = hosts.get_mut(i) {
                slot.1 = try_string(h)?;
            }
            Ok(())
        }
        Err(i) => crate::error::try_insert(hosts, i, (try_string(id)?, try_string(h)?)),
    }
}

/// Wat de schil moet vragen als de wacht om is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Sync {
    /// Alles: agents, jobs, taken (Go: `sync`).
    Full,
    /// Alleen de taken van deze jobs (Go: `syncJob` per job).
    Jobs(Vec<String>),
}

/// De gebeurtenissen die op hun beurt wachten (Go: `pending`, `pendingFull`
/// en de `debounce`-timer).
///
/// De eerste gebeurtenis zet de wekker op [`DEBOUNCE`]; wat daarna binnen
/// komt, gaat mee in dezelfde ronde.
#[derive(Debug, Default)]
pub struct Pending {
    jobs: Vec<String>,
    full: bool,
    due: Option<u64>,
}

impl Pending {
    /// Een lege rij.
    pub fn new() -> Self {
        Self::default()
    }

    /// Neemt een gebeurtenis over job `job` op; `full` vraagt de hele lijst.
    pub fn push(&mut self, job: &str, full: bool, now_ns: u64) -> Result {
        if self.due.is_none() {
            let d = u64::try_from(DEBOUNCE.as_nanos()).unwrap_or(u64::MAX);
            self.due = Some(now_ns.saturating_add(d));
        }
        self.full |= full;
        if !self.jobs.iter().any(|j| j == job) {
            try_push(&mut self.jobs, try_string(job)?)?;
        }
        Ok(())
    }

    /// Wanneer de wacht om is; `None` als er niets wacht.
    pub fn due(&self) -> Option<u64> {
        self.due
    }

    /// De ronde die nu moet, als de wacht om is; de rij is daarna leeg.
    pub fn take(&mut self, now_ns: u64) -> Option<Sync> {
        if self.due? > now_ns {
            return None;
        }
        let full = core::mem::take(&mut self.full);
        let jobs = core::mem::take(&mut self.jobs);
        self.due = None;
        Some(if full { Sync::Full } else { Sync::Jobs(jobs) })
    }

    /// Zet een volledige ronde klaar over `delay`: een synchronisatie die
    /// faalde (de agent even weg), zonder te wachten op de volgende
    /// gebeurtenis. Go wachtte wel, en hield dan een lege tabel tot er iets
    /// gebeurde.
    pub fn retry(&mut self, now_ns: u64, delay: Duration) {
        let d = u64::try_from(delay.as_nanos()).unwrap_or(u64::MAX);
        let at = now_ns.saturating_add(d);
        self.full = true;
        self.due = Some(self.due.map_or(at, |x| x.min(at)));
    }

    /// Gooit alles weg (een volledige synchronisatie dekt het al).
    pub fn clear(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests;

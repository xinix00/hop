//! Staat die een herstart van Hop overleeft: de agent-staat als JSON naar buiten en terug.
//!
//! Op HopOS draaien de apps door als Hop zelf herstart (`POST /restart`,
//! met hetzelfde of een nieuw image). Een crash van Hop is dat niet: de kern
//! herstart een falende Hop niet, de watchdog reset de node en de koude
//! boot begint zonder blob. Een lege agent
//! kent zijn taken dan niet meer: de veger stopt ze als bewoners van
//! niemand en de leader plaatst alles opnieuw. Met deze overdracht legt
//! Hop zijn jobs en taken bij de kern neer ([`Store`], op HopOS een blob in
//! het geheugen van de kern), en neemt de volgende Hop ze terug.
//!
//! Waarom in de kern en niet op hopfs: het blob sterft met de boot. Tot
//! 7726782 (02-10) stond deze staat in `/hop/agent-state.json`; dat
//! bestand overleefde een koude boot, en de taak van een vorige boot werd
//! een spook op het slot van een nieuwe bewoner (de M4: twee minuten 503
//! "port 80 is taken"). Een blob in kerngeheugen kan geen vorige boot
//! kennen, en de node neemt alleen een kooi over die de kern als draaiend
//! meldt en die nog van niemand is.
//!
//! JSON, omdat `types` die vorm al draagt (API en clusterstaat): een tweede
//! codering zou een tweede plek zijn om een veld te vergeten. `state_time`
//! gaat NIET mee: dat is de leeftijd van de laatste synchronisatie van déze
//! agent, en een geërfde tijdstempel liet de volgende denken dat hij al bij is.
//!
//! Wat de agent niet bezit, reist als tekst mee die hij niet leest
//! ([`Handoff::leader`]): op een standalone node de gecommitte staat van de
//! leader in hetzelfde proces, die anders met Hop zou sterven.

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::future::Future;

use types::de::ObjectBuilder;
use types::json::{self, Value};
use types::{Job, Task};

use crate::node::Agent;
use crate::{Error, Result};

/// De versie in het blob, zodat een agent van een andere versie een onbekende vorm weigert.
///
/// 1 was het bestand op hopfs (tot 7726782); 2 draagt de staat van de leader mee.
pub const HANDOFF_VERSION: u64 = 2;

/// Een fout van de staat-opslag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreError {
    /// De opslag weigerde de vraag zelf: op HopOS een kern die de staat-ops
    /// niet kent (een oudere kern), en die dus niets voor Hop bewaart.
    Refused {
        /// De status van het antwoord.
        status: u16,
    },
    /// De weg naar de opslag faalde (de verbinding, een time-out, een
    /// antwoord dat niet klopt); later kan het weer lukken.
    Io,
    /// Het blob is groter dan de opslag neemt.
    TooBig {
        /// De maat van het blob.
        len: usize,
        /// Het maximum van de opslag.
        max: usize,
    },
    /// Geen geheugen voor het blob.
    OutOfMemory,
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { status } => write!(
                f,
                "the store refused the call (status {status}); an older kernel does not know the state ops"
            ),
            Self::Io => f.write_str("the call to the store failed"),
            Self::TooBig { len, max } => {
                write!(f, "the state is {len} bytes, the store takes {max}")
            }
            Self::OutOfMemory => f.write_str("out of memory for the state"),
        }
    }
}

/// Waar de agent zijn staat laat: op HopOS een blob in het geheugen van de kern.
///
/// Asynchroon: op HopOS is elke vraag een call over de verbinding met de
/// kern. Wachten is daar de core teruggeven aan de andere taken, niet de
/// executor zelf rondes laten draaien (handboek §4). De vorm is `-> impl
/// Future` (geen `Send` nodig: taken verhuizen niet); een implementatie mag
/// `async fn` schrijven.
pub trait Store {
    /// Vervangt het blob, in zijn geheel.
    fn save(&mut self, blob: &[u8]) -> impl Future<Output = Result<(), StoreError>>;
    /// Leest het blob; `None` als er niets ligt.
    fn load(&mut self) -> impl Future<Output = Result<Option<Vec<u8>>, StoreError>>;
}

/// Een gelezen overdracht: wat de vorige Hop wist.
///
/// Nog niet overgenomen: de node kiest welke taken hij terugneemt (alleen
/// die waarvan de kern de kooi als draaiend meldt) en geeft die aan
/// [`Agent::adopt`].
#[derive(Debug, Default)]
pub struct Handoff {
    /// De jobs die de agent kende.
    pub jobs: Vec<Job>,
    /// De taken, met hun kooi in `pid` (0 = nog geen).
    pub tasks: Vec<Task>,
    /// Wat de node naast de agent meegaf (op HopOS standalone: de snapshot
    /// van de leader); de agent leest het niet.
    pub leader: Option<String>,
}

impl Handoff {
    /// Leest een blob van [`Agent::snapshot`]; `None` voor een leeg blob.
    ///
    /// Een onbruikbaar blob is een FOUT en geen stilte: de aanroeper kiest
    /// dan zelf hoe hij verder gaat, in plaats van te denken dat de
    /// overdracht slaagde.
    pub fn parse(blob: &[u8]) -> Result<Option<Self>> {
        if blob.is_empty() {
            return Ok(None);
        }
        let v = json::parse(blob)?;
        let obj = types::de::object(&v, "handoff")?;
        let version = obj.get("version").and_then(Value::as_u64).unwrap_or(0);
        if version != HANDOFF_VERSION {
            return Err(Error::Version(version));
        }
        let mut h = Self::default();
        if let Some(Value::Object(o)) = obj.get("jobs") {
            for (_, j) in o.iter().filter(|(_, j)| !j.is_null()) {
                types::try_push(&mut h.jobs, Job::from_value(j, false)?)?;
            }
        }
        if let Some(Value::Object(o)) = obj.get("tasks") {
            for (_, t) in o.iter().filter(|(_, t)| !t.is_null()) {
                types::try_push(&mut h.tasks, Task::from_value(t)?)?;
            }
        }
        h.leader = match obj.get("leader") {
            Some(l) if !l.is_null() => Some(types::de::string(l, "leader")?),
            _ => None,
        };
        Ok(Some(h))
    }
}

impl Agent {
    /// De staat als JSON-blob, met `leader` als tekst erbij.
    ///
    /// Roep hem zo LAAT mogelijk aan vóór de herstart: wat erna nog bijkomt,
    /// kent de volgende agent niet.
    pub fn snapshot(&self, leader: Option<&str>) -> Result<String> {
        let mut jobs = ObjectBuilder::new();
        for (name, job) in self.jobs_map() {
            jobs.field(name, job.to_value()?)?;
        }
        let mut tasks = ObjectBuilder::new();
        for t in self.tasks() {
            tasks.field(&t.id, t.to_value()?)?;
        }
        let mut o = ObjectBuilder::new();
        o.field("version", Value::uint(HANDOFF_VERSION))?;
        o.field("jobs", jobs.build())?;
        o.field("tasks", tasks.build())?;
        if let Some(l) = leader {
            o.field("leader", Value::String(types::try_string(l)?))?;
        }
        Ok(json::to_string(&o.build())?)
    }

    /// Neemt de jobs en de taken van een vorige agent over.
    ///
    /// De taken zijn de taken die de node koos (zie [`Handoff`]); elk record
    /// houdt zijn reservering vast, zoals een eigen taak. Wat niet meer past
    /// ([`crate::MAX_JOBS`], [`crate::MAX_TASKS`]), valt weg: de leader
    /// plaatst het opnieuw.
    pub fn adopt(&mut self, jobs: Vec<Job>, tasks: Vec<Task>) {
        self.restore_entries(jobs, tasks);
    }

    /// Schrijft de staat weg in `store`, met `leader` erbij.
    pub async fn save_to<S: Store>(&self, store: &mut S, leader: Option<&str>) -> Result<usize> {
        let blob = self.snapshot(leader)?;
        store.save(blob.as_bytes()).await.map_err(Error::Store)?;
        Ok(blob.len())
    }
}

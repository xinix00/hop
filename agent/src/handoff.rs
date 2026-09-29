//! Staat die een herstart overleeft: de agent-staat als JSON naar buiten en terug.
//!
//! Dit bestaat voor de kern-flip van HopOS, waar het OS zichzelf onder zijn
//! eigen apps vandaan vervangt. De apps blijven draaien, maar de agent start
//! opnieuw met een lege staat; zonder overdracht kent hij zijn eigen taken
//! niet meer en stuit hij op kooien die door precies die taken bezet zijn.
//!
//! JSON, omdat `types` die vorm al draagt (API en clusterstaat): een tweede
//! codering zou een tweede plek zijn om een veld te vergeten. `state_time`
//! gaat NIET mee: dat is de leeftijd van de laatste synchronisatie van déze
//! agent, en een geërfde tijdstempel liet de volgende denken dat hij al bij is.

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use types::de::ObjectBuilder;
use types::json::{self, Value};
use types::{Job, Task};

use crate::node::Agent;
use crate::{Error, Result};

/// De versie in het blob, zodat een agent van een andere versie een onbekende vorm weigert.
pub const HANDOFF_VERSION: u64 = 1;

/// Een fout van de staat-opslag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreError {
    /// Het medium weigerde (vol, kapot, onbereikbaar).
    Io,
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("store i/o failed")
    }
}

/// Waar de agent zijn staat laat: een bestand op een host, een geheugenblok dat een kern-flip overleeft.
pub trait Store {
    /// Overschrijft het blob.
    fn save(&mut self, blob: &[u8]) -> Result<(), StoreError>;
    /// Leest het blob; `None` als er niets staat.
    fn load(&mut self) -> Result<Option<Vec<u8>>, StoreError>;
}

impl Agent {
    /// De staat als JSON-blob.
    ///
    /// Roep hem zo LAAT mogelijk aan vóór de sprong: wat erna nog bijkomt,
    /// kent de volgende agent niet.
    pub fn snapshot(&self) -> Result<String> {
        let mut jobs = ObjectBuilder::new();
        for (name, job) in self.jobs_map() {
            jobs.field(name, job.to_value()?)?;
        }
        let mut tasks = ObjectBuilder::new();
        for (id, e) in self.tasks_map() {
            tasks.field(id, e.task.to_value()?)?;
        }
        let mut o = ObjectBuilder::new();
        o.field("version", Value::uint(HANDOFF_VERSION))?;
        o.field("jobs", jobs.build())?;
        o.field("tasks", tasks.build())?;
        Ok(json::to_string(&o.build())?)
    }

    /// Zet een eerder gemaakte snapshot terug; geeft de kooien voor de runner.
    ///
    /// Een onbruikbaar blob is een FOUT en geen stilte: de aanroeper kiest dan
    /// om met een lege staat door te gaan (de taken draaien nog, ze zijn alleen
    /// niet meer bekend) in plaats van te denken dat de overdracht slaagde.
    ///
    /// Het resultaat zijn de taken met een kooi (`pid >= 1`), GEEN staatfilter:
    /// een taak die queued of failed staat houdt zijn kooi net zo goed bezet.
    pub fn restore(&mut self, blob: &[u8]) -> Result<Vec<(String, i64)>> {
        if blob.is_empty() {
            return Ok(Vec::new());
        }
        let v = json::parse(blob)?;
        let obj = types::de::object(&v, "handoff")?;
        let version = obj.get("version").and_then(Value::as_u64).unwrap_or(0);
        if version != HANDOFF_VERSION {
            return Err(Error::Version(version));
        }
        let mut jobs = Vec::new();
        if let Some(Value::Object(o)) = obj.get("jobs") {
            for (_, j) in o.iter().filter(|(_, j)| !j.is_null()) {
                types::try_push(&mut jobs, Job::from_value(j, false)?)?;
            }
        }
        let mut tasks = Vec::new();
        let mut slots = Vec::new();
        if let Some(Value::Object(o)) = obj.get("tasks") {
            for (_, t) in o.iter().filter(|(_, t)| !t.is_null()) {
                let task = Task::from_value(t)?;
                if task.pid >= 1 {
                    types::try_push(&mut slots, (task.id.clone(), task.pid))?;
                }
                types::try_push(&mut tasks, task)?;
            }
        }
        self.restore_entries(jobs, tasks);
        Ok(slots)
    }

    /// Schrijft de staat weg (na een [`crate::Action::SaveState`]).
    pub fn save_to<S: Store>(&self, store: &mut S) -> Result {
        let blob = self.snapshot()?;
        store.save(blob.as_bytes()).map_err(Error::Store)
    }

    /// Leest de staat terug bij het opstarten; geeft de kooien voor de runner.
    pub fn restore_from<S: Store>(&mut self, store: &mut S) -> Result<Vec<(String, i64)>> {
        match store.load().map_err(Error::Store)? {
            Some(blob) => self.restore(&blob),
            None => Ok(Vec::new()),
        }
    }
}

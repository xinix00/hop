//! Gecommitte clusterstaat: de leider is de enige auteur en schrijft één snapshot.
//!
//! De snapshot zegt wat er moet draaien, de leider zorgt dat het draait
//! (15-07). Verwijderen is afwezigheid: een job die niet in de snapshot
//! staat is weg. De snapshot loopt tot [`PERSIST_DEBOUNCE`] achter; een
//! crash in dat venster verliest hoogstens de nieuwste mutaties, zichtbaar
//! (de job is afwezig) en opnieuw in te dienen. Het object in de bucket
//! hernoemen of wissen is de "schone boot"-schakelaar van de operator.
//!
//! Deze module schrijft niets zelf: [`Leader::poll_snapshot`] geeft de bytes
//! en de adapter zet ze in S3 of een bestand.

use alloc::string::String;
use alloc::vec::Vec;

use types::de::{self, ObjectBuilder};
use types::json::{self, Value};
use types::time::SECOND;
use types::{Job, Nanos, Time, try_push};

use crate::{Error, JobStore, Leader, MAX_JOBS, Result};

/// Hoe lang mutaties samenvallen tot één snapshot: 1 s. Een storm van 127
/// jobs wordt zo een handvol PUTs in plaats van 127.
pub const PERSIST_DEBOUNCE: Nanos = SECOND;

impl<S: JobStore> Leader<S> {
    /// De snapshot om weg te schrijven, als de staat vies is en al
    /// [`PERSIST_DEBOUNCE`] zo is; anders `None`.
    ///
    /// De eerste aanroep na een mutatie start het debounce-venster; een
    /// latere aanroep na het venster geeft de bytes en maakt de staat schoon.
    /// Mislukt het schrijven, dan roept de adapter [`Leader::snapshot_failed`].
    pub fn poll_snapshot(&mut self, now: Time) -> Result<Option<String>> {
        if !self.dirty {
            return Ok(None);
        }
        let seen = *self.dirty_seen.get_or_insert(now);
        if now.since(seen) < PERSIST_DEBOUNCE {
            return Ok(None);
        }
        let snapshot = self.snapshot(now)?;
        self.dirty = false;
        self.dirty_seen = None;
        Ok(Some(snapshot))
    }

    /// Markeert de staat weer als vies na een mislukte schrijf: niet fataal,
    /// de cluster draait door op de waarheid in geheugen en de volgende ronde
    /// probeert het opnieuw.
    pub fn snapshot_failed(&mut self) {
        self.dirty = true;
    }

    /// De snapshot als JSON: `{"updated": ..., "jobs": [...]}`.
    pub fn snapshot(&self, now: Time) -> Result<String> {
        let mut jobs = Vec::new();
        jobs.try_reserve_exact(self.store.jobs().len())
            .map_err(|_| Error::OutOfMemory)?;
        for j in self.store.jobs() {
            jobs.push(j.to_value()?);
        }
        let mut updated = String::new();
        now.write_rfc3339(&mut updated)?;
        let mut o = ObjectBuilder::new();
        o.field("updated", Value::String(updated))?;
        o.field("jobs", Value::Array(jobs))?;
        Ok(json::to_string(&o.build())?)
    }

    /// Vult de store uit de gecommitte snapshot; `None` is een schone boot.
    ///
    /// De snapshot is de enige waarheid, niet meer en niet minder (18-07):
    /// een job die lokaal nog bestaat maar niet in de snapshot staat, is
    /// elders verwijderd terwijl deze node weg was, en gaat weg. Een kapotte
    /// snapshot is een fout (liever luid dan half geladen). Geeft terug of er
    /// een snapshot was; `Ok(false)` betekent schone boot (voor init-jobs).
    pub fn load_committed_state(&mut self, snapshot: Option<&[u8]>) -> Result<bool> {
        let Some(data) = snapshot else {
            return Ok(false);
        };
        let v = json::parse(data)?;
        let obj = de::object(&v, "state")?;
        let mut updated = Time::ZERO;
        let mut jobs: Vec<Job> = Vec::new();
        for (k, item) in obj.iter() {
            match k {
                "updated" => {
                    let s = de::string(item, k)?;
                    updated = Time::parse_rfc3339(&s)?;
                }
                "jobs" if !item.is_null() => {
                    jobs = de::list(item, k, MAX_JOBS, |j| Job::from_value(j, false))?;
                }
                _ => {}
            }
        }
        let mut stale: Vec<String> = Vec::new();
        for j in self.store.jobs() {
            if !jobs.iter().any(|s| s.name == j.name) {
                try_push(&mut stale, types::try_string(&j.name)?)?;
            }
        }
        for name in &stale {
            self.store_remove(name);
        }
        for j in jobs {
            self.store.put(j)?;
        }
        self.store.set_state_time(updated);
        Ok(true)
    }
}

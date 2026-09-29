//! De opslag van de gewenste staat: de jobs, op naam.
//!
//! De leider is de enige auteur van gewenste staat; de store is waar die
//! staat woont. In Go was dat de jobs-map van de lokale agent (zodat een
//! nieuwe leider zonder sync verder kon); hier is het een trait, zodat de
//! agent-crate zijn eigen tabel kan aanbieden en de tests [`MemStore`] nemen.

use alloc::vec::Vec;

use types::{Job, Time, TryClone};

use crate::{Error, MAX_JOBS, Result};

/// De jobs van de cluster, op naam.
pub trait JobStore {
    /// Alle jobs.
    fn jobs(&self) -> &[Job];

    /// De job met deze naam.
    fn get(&self, name: &str) -> Option<&Job> {
        self.jobs().iter().find(|j| j.name == name)
    }

    /// De job met deze naam, veranderbaar.
    fn get_mut(&mut self, name: &str) -> Option<&mut Job>;

    /// Zet de job neer (upsert op naam).
    fn put(&mut self, job: Job) -> Result;

    /// Haalt de job weg en geeft hem terug.
    fn remove(&mut self, name: &str) -> Option<Job>;

    /// Het tijdstip van de laatst geladen snapshot.
    fn state_time(&self) -> Time;

    /// Zet het tijdstip van de laatst geladen snapshot.
    fn set_state_time(&mut self, t: Time);

    /// Herschrijft alleen de prioriteit; `false` als de job niet (meer) bestaat.
    fn set_priority(&mut self, name: &str, priority: i64) -> bool {
        match self.get_mut(name) {
            Some(j) => {
                j.priority = Some(priority);
                true
            }
            None => false,
        }
    }

    /// Herschrijft alleen de `deploying`-vlag; `false` als de job niet bestaat.
    fn set_deploying(&mut self, name: &str, deploying: bool) -> bool {
        match self.get_mut(name) {
            Some(j) => {
                j.deploying = deploying;
                true
            }
            None => false,
        }
    }
}

/// Een store in geheugen, begrensd op [`MAX_JOBS`], in invoegvolgorde.
#[derive(Debug, Default)]
pub struct MemStore {
    jobs: Vec<Job>,
    state_time: Time,
}

impl MemStore {
    /// Een lege store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl JobStore for MemStore {
    fn jobs(&self) -> &[Job] {
        &self.jobs
    }

    fn get_mut(&mut self, name: &str) -> Option<&mut Job> {
        self.jobs.iter_mut().find(|j| j.name == name)
    }

    fn put(&mut self, job: Job) -> Result {
        if let Some(slot) = self.get_mut(&job.name) {
            *slot = job;
            return Ok(());
        }
        if self.jobs.len() >= MAX_JOBS {
            return Err(Error::TooMany {
                what: "jobs",
                max: MAX_JOBS,
            });
        }
        types::try_push(&mut self.jobs, job)?;
        Ok(())
    }

    fn remove(&mut self, name: &str) -> Option<Job> {
        let i = self.jobs.iter().position(|j| j.name == name)?;
        Some(self.jobs.remove(i))
    }

    fn state_time(&self) -> Time {
        self.state_time
    }

    fn set_state_time(&mut self, t: Time) {
        self.state_time = t;
    }
}

impl TryClone for MemStore {
    fn try_clone(&self) -> types::Result<Self> {
        Ok(Self {
            jobs: self.jobs.try_clone()?,
            state_time: self.state_time,
        })
    }
}

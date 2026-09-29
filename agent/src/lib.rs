//! De node: taken, heartbeats, herstarts, gezondheidscontroles, staat die een herstart overleeft.
//!
//! Deze crate bezit de staat van één node: de jobs die hij kent, de taken die
//! hij draait (elk record IS een capaciteitsreservering), de herstartbudgetten
//! en de gezondheidstellers. Hij bezit geen I/O. De staat heeft één eigenaar,
//! [`Agent`], en die wordt gedreven door een executor-taak die hem voedt met
//! wat de wereld deed (een runner startte, een probe antwoordde, de tijd
//! verstreek) en de [`Action`]s uitvoert die hij teruggeeft, IN VOLGORDE.
//!
//! Waar de Go-versie een goroutine per herstart en een ops-kanaal had, is hier
//! één toestandsmachine: `tick(now)` geeft de acties die nu moeten, en elke
//! invoer (`on_started`, `on_status`, `on_probe`) duwt er nieuwe bij. De tests
//! voeden hem met de hand; daarom kunnen ze elke race die de Go-tests met
//! kanalen naspeelden, deterministisch opschrijven.
//!
//! De runner kent deze crate niet (en omgekeerd): de executor lijmt een
//! [`Action::Start`] aan `runner::Runner::start`.
//!
//! Tijd is overal `now` in nanoseconden sinds 1970 ([`types::Time`]).

#![cfg_attr(not(test), no_std)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]
#![forbid(unsafe_code)]

extern crate alloc;

mod action;
mod election;
mod handoff;
mod health;
mod ids;
mod node;
mod settings;

use core::fmt;

pub use action::{Action, Event, Outcome, Probe, StartError, StartOk, Status};
pub use election::{Discoverer, Election, LinkError, Request};
pub use handoff::{HANDOFF_VERSION, Store, StoreError};
pub use node::{Agent, Capacity};
pub use settings::Settings;

/// Het maximum aantal taken op één node.
///
/// Op HopOS begrenst de kern de kooien toch (256, zie `runner::MAX_CAGES`);
/// op een host is 1024 ruim boven het drukste gemeten geval (127 taken in de
/// delete-storm van 15-07). Een verzameling die van buiten gevoed wordt, is
/// begrensd; een leader die meer stuurt, krijgt een weigering.
pub const MAX_TASKS: usize = 1024;

/// Het maximum aantal jobs dat de node kent (op de leader-node is dit de hele store).
pub const MAX_JOBS: usize = 1024;

/// Standaard onbeperkt herstarten, met exponentiële backoff tot 30 s.
///
/// Een eindige standaard (het was 5 in 5 minuten) maakte van een afhankelijkheid
/// die laat opstartte een blijvende storing: de traqqr-webapps brandden hun
/// budget op voordat RavenDB er was (08-09-2026) en bleven "failed" tot iemand
/// ze opnieuw postte. Jobs die moeten opgeven zetten `max_restarts` zelf.
pub const DEFAULT_MAX_RESTARTS: i64 = -1;

/// Na zoveel echte uptime begint het herstartbudget opnieuw.
pub const DEFAULT_RESTART_WINDOW: u64 = 5 * types::time::MINUTE;

/// De bovengrens van de backoff tussen twee herstarts.
pub const MAX_RESTART_DELAY: u64 = 30 * types::time::SECOND;

/// Zoveel opeenvolgende mislukte controles voordat een taak ongezond is (15 s bij 5 s interval).
pub const DEFAULT_FAILURE_THRESHOLD: u32 = 3;

/// Een agent-fout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// De node-attributen passen niet bij de affinity van de job (406).
    AffinityMismatch,
    /// De node heeft de ruimte niet (503).
    NoCapacity,
    /// [`MAX_TASKS`] bereikt.
    TooManyTasks,
    /// [`MAX_JOBS`] bereikt.
    TooManyJobs,
    /// Geen artifact past bij de attributen van deze node.
    NoArtifact,
    /// Onbekende taak of job.
    NotFound,
    /// Een JSON-fout uit `types`.
    Json(types::Error),
    /// Een overdracht van een andere versie.
    Version(u64),
    /// De staat-opslag faalde.
    Store(StoreError),
    /// Geheugen op.
    Alloc,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::AffinityMismatch => f.write_str("affinity mismatch"),
            Error::NoCapacity => f.write_str("insufficient capacity"),
            Error::TooManyTasks => write!(f, "node holds the maximum of {MAX_TASKS} tasks"),
            Error::TooManyJobs => write!(f, "node knows the maximum of {MAX_JOBS} jobs"),
            Error::NoArtifact => f.write_str("no matching artifact for this node's attributes"),
            Error::NotFound => f.write_str("not found"),
            Error::Json(e) => write!(f, "json: {e}"),
            Error::Version(v) => write!(
                f,
                "agent handoff: version {v}, this agent speaks {HANDOFF_VERSION}"
            ),
            Error::Store(e) => write!(f, "state store: {e}"),
            Error::Alloc => f.write_str("out of memory"),
        }
    }
}

impl From<types::Error> for Error {
    fn from(e: types::Error) -> Self {
        match e {
            types::Error::OutOfMemory => Error::Alloc,
            other => Error::Json(other),
        }
    }
}

/// Het resultaat-type van deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

#[cfg(test)]
mod tests;

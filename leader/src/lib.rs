//! De cluster-hersenen: plaatsing, spreiding, updates (rolling, recreate, blue-green), failover met settle-periode.
//!
//! Deze crate bezit de beslissingen van de leider en niets anders: welke
//! agent welke instantie krijgt, wanneer een agent dood is, hoe een update
//! uitrolt, wat er na een leiderswissel gebeurt. Hij spreekt geen HTTP en
//! leest geen klok:
//!
//! - Verzoeken (registreren, heartbeat, job indienen, verwijderen) komen
//!   binnen als methode-aanroepen met waarden; antwoorden gaan terug als
//!   waarden. De HTTP-adapter vertaalt.
//! - Wat de leider bij een agent moet doen (een job starten, stoppen, de
//!   taken opvragen) gaat door een [`Transport`]; de tests spelen daar de
//!   nep-agents, zoals de Go-tests `httptest`-servers speelden.
//! - Jobs liggen in een [`JobStore`]; [`MemStore`] is de gewone.
//! - "Nu" is een parameter ([`types::Time`], Unix-nanoseconden).
//! - De leader-lease staat in [`lease`], achter de [`lease::LeaseStore`]-trait.
//!
//! Eén eigenaar (handboek §1): de [`Leader`] is een gewone struct achter
//! `&mut self`. De Go-versie had één goroutine met een ops-kanaal, plus
//! tombstones, dispatch-vlaggen en een naveeg-lus tegen de races daartussen
//! (de delete-storm-zombies van 15-07). Met één eigenaar bestaan die races
//! niet: een reconcile ziet de store zoals hij is, niet een kopie van even
//! geleden, en die hele machinerie is dus weggelaten.
//!
//! Een update draait synchroon (de API gaf in Go ook pas antwoord na de
//! uitrol); de pauze tussen twee rolling-stappen vraagt de leider aan de
//! [`Transport`], zodat een test hem telt en de host hem slaapt.

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

mod dispatch;
mod events;
mod health;
mod init;
pub mod lease;
mod persist;
mod state;
mod store;
mod transport;
mod update;

#[cfg(test)]
mod testkit;
#[cfg(test)]
mod tests;

use core::fmt;

pub use events::{Event, MAX_EVENTS};
pub use init::decode_init_jobs;
pub use persist::PERSIST_DEBOUNCE;
pub use state::{Leader, TaskRef};
pub use store::{JobStore, MemStore};
pub use transport::{RunReply, Transport};

use types::Name;
use types::time::{Nanos, SECOND};

/// Hoeveel agents een leider hoogstens bijhoudt. De grootste Hop-cluster
/// draait enkele tientallen nodes; de plaatsing loopt kwadratisch over
/// agents en jobs, en 256 houdt dat onder een milliseconde.
pub const MAX_AGENTS: usize = 256;

/// Hoeveel jobs een cluster hoogstens heeft. De grootste gemeten vloot had
/// er 127 (15-07, de delete-storm op de Altra); 1024 laat ruimte en houdt
/// een snapshot onder de 1 MiB van de JSON-parser.
pub const MAX_JOBS: usize = 1024;

/// Na hoe lang zonder heartbeat een agent dood is: 30 s, drie gemiste
/// heartbeats van 10 s.
pub const DEFAULT_AGENT_TIMEOUT: Nanos = 30 * SECOND;

/// De pauze tussen twee stappen van een rolling update: 2 s, zodat de nieuwe
/// instantie kan starten voordat de volgende oude stopt.
pub const ROLLING_UPDATE_DELAY: Nanos = 2 * SECOND;

/// Om de hoeveel ticks (van 10 s) een vangnet-reconcile loopt: elke derde,
/// dus ~30 s. Reconcile is verder event-gedreven; dit vangt een job die
/// onderplaatst raakte terwijl er niets gebeurde (gemeten 01-08:
/// cloudflared bleef na een gefragmenteerde pool voorgoed op 0/1 staan
/// terwijl de ruimte allang terug was).
pub const RECONCILE_EVERY_TICKS: u64 = 3;

/// Waarom geen enkele agent een instantie aannam.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// Er zijn geen agents.
    NoAgents,
    /// Alle agents die de job konden draaien zaten vol (503).
    Full {
        /// Hoeveel agents vol zaten.
        agents: usize,
    },
    /// Geen agent nam hem aan (affinity, fouten, onbereikbaar).
    NoneAccepted {
        /// Hoeveel agents geprobeerd zijn.
        tried: usize,
    },
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoAgents => f.write_str("no agents available"),
            Self::Full { agents } => write!(f, "no capacity on all {agents} agent(s)"),
            Self::NoneAccepted { tried } => write!(f, "no agent accepted after trying {tried}"),
        }
    }
}

/// Wat er mis kan gaan in de leider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// De heap was op.
    OutOfMemory,
    /// Een fout in JSON of een veld.
    Json(types::Error),
    /// Een job zonder naam.
    NameRequired,
    /// De job bestaat niet.
    NotFound {
        /// De job.
        job: Name,
    },
    /// Een instantie kon niet geplaatst worden. De job is wel opgeslagen:
    /// de volgende reconcile probeert het opnieuw.
    Dispatch {
        /// De job.
        job: Name,
        /// Welke instantie (vanaf 1).
        instance: usize,
        /// Van hoeveel.
        count: usize,
        /// Waarom.
        why: Refusal,
    },
    /// Alle agents weigerden een daemon.
    DaemonRejected {
        /// De job.
        job: Name,
    },
    /// Een rolling update stopte halverwege; de oude instanties die nog niet
    /// aan de beurt waren draaien door.
    Rolling {
        /// De job.
        job: Name,
        /// Bij welke instantie (vanaf 1).
        instance: usize,
        /// Waarom.
        why: Refusal,
    },
    /// Een blue-green update kon niet alles naast het oude zetten; het oude
    /// draait ongewijzigd door.
    BlueGreen {
        /// De job.
        job: Name,
        /// Bij welke instantie (vanaf 1).
        instance: usize,
        /// Van hoeveel.
        count: usize,
        /// Waarom.
        why: Refusal,
    },
    /// Een verzameling is vol.
    TooMany {
        /// Welke.
        what: &'static str,
        /// De grens.
        max: usize,
    },
    /// Een init-job in de config is ongeldig.
    InitJob {
        /// De plek in `cluster.init_jobs` (vanaf 0).
        index: usize,
        /// De fout in de spec.
        cause: types::Error,
    },
}

impl From<types::Error> for Error {
    fn from(e: types::Error) -> Self {
        match e {
            types::Error::OutOfMemory => Self::OutOfMemory,
            e => Self::Json(e),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfMemory => f.write_str("out of memory"),
            Self::Json(e) => write!(f, "{e}"),
            Self::NameRequired => f.write_str("job name required"),
            Self::NotFound { job } => write!(f, "job {job} not found"),
            Self::Dispatch {
                job,
                instance,
                count,
                why,
            } => write!(
                f,
                "{job}: failed to dispatch instance {instance}/{count}: {why}"
            ),
            Self::DaemonRejected { job } => write!(f, "all agents rejected daemon job {job}"),
            Self::Rolling { job, instance, why } => {
                write!(
                    f,
                    "{job}: rolling update failed at instance {instance}: {why}"
                )
            }
            Self::BlueGreen {
                job,
                instance,
                count,
                why,
            } => write!(
                f,
                "{job}: blue-green: failed to dispatch instance {instance}/{count}: {why}"
            ),
            Self::TooMany { what, max } => write!(f, "more than {max} {what}"),
            Self::InitJob { index, cause } => write!(f, "init job {index}: {cause}"),
        }
    }
}

impl core::error::Error for Error {}

/// Het resultaat van elke faalbare handeling in deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

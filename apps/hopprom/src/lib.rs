//! hopprom: the Prometheus exporter for Hop clusters, as a sans-I/O core.
//!
//! De kern bezit drie dingen, en geen socket:
//!
//! - [`poll`]: één pollronde over een [`Source`] (`/v1/agents`,
//!   `/v1/jobs`, `/v1/tasks`, `/v1/agents/{id}/capacity`, `/v1/status`,
//!   `/leader`), met de regel welke aanroep verplicht is (agents, jobs,
//!   taken) en welke best-effort (capaciteit, status, leader). Het lezen
//!   van de antwoorden en het ondertekenen doet `hoplib`, de gedeelde client
//!   van de Hop-plugins.
//! - [`Collector`]: van een [`Poll`] naar [`Metrics`], met de tellers
//!   (starts, failures, restarts) als delta tussen twee polls. De enige
//!   staat is die van de vorige poll en de tellers zelf.
//! - [`render`]: [`Metrics`] als Prometheus-tekst (HELP, TYPE, ge-escapete
//!   labels), in de volgorde en met de namen van de Go-generatie
//!   (`OLD/internal/exporter`).
//!
//! De feature `std` voegt [`host`] toe (de bron over `hoplib::host`), de
//! feature `hopos` voegt `hopos` toe (de bron over `hoplib::hopos`), en
//! beide `http` (de twee routes over leanhttp). De binaries zijn dun: een
//! eigenaar van de [`Collector`] en een server.
//!
//! De kern is `no_std` met `alloc` (zonder features bouwt hij voor het
//! bare-metal target), en elke allocatie is faalbaar: een cluster met
//! duizend taken is invoer van buiten.

#![cfg_attr(not(any(test, feature = "std")), no_std)]
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

mod collector;
pub mod config;
mod error;
mod exporter;
mod metrics;
pub mod poll;

#[cfg(feature = "hopos")]
pub mod hopos;
#[cfg(feature = "std")]
pub mod host;
#[cfg(any(feature = "std", feature = "hopos"))]
pub mod http;

pub use collector::{Collector, HEALTHY_WITHIN};
pub use error::{Error, Result};
pub use exporter::{CONTENT_TYPE, render};
pub use metrics::{AgentMetric, CapacityMetric, JobMetric, Metrics};
pub use poll::{AgentTasks, Capacity, Leader, Miss, Poll, Source, Status, Step, poll};

/// Voegt `n` toe aan de teller onder `key`, faalbaar.
pub(crate) fn bump(map: &mut types::Map<u64>, key: &str, n: u64) -> Result {
    if let Some(v) = map.get_mut(key) {
        *v = v.saturating_add(n);
        return Ok(());
    }
    map.insert(types::try_string(key)?, n)?;
    Ok(())
}

/// Voegt `v` achteraan toe, faalbaar: eerst ruimte, dan de push.
pub(crate) fn push<T>(vec: &mut alloc::vec::Vec<T>, v: T) -> Result {
    vec.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
    vec.push(v);
    Ok(())
}

#[cfg(test)]
mod tests;

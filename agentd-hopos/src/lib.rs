//! Hop als eerste bewoner van HopOS: agent en leader in één slot, met de bevoegde system-API.
//!
//! De bibliotheek-kant van de bewoner, host-getest; de binary (`main.rs`)
//! zet hem alleen op de app-core neer. Hij bezit:
//!
//! - [`BootConfig`]: wat de kern via de env van het slot meegeeft
//!   (`HOPOS_*`, zie [`env`](mod@env)), in plaats van `hopos.cfg` en de bootparams
//!   van de Go-kern.
//! - [`Node`]: de ene eigenaar van alle staat van de node: de
//!   [`agent::Agent`], de [`leader::Leader`] (standalone, of in een cluster
//!   als de verkiezing hem de leiding geeft), de [`runner::HopRunner`] over
//!   de system-API, en de twee API's. Verzoeken en de tik komen binnen als `async`
//!   methode-aanroepen; de acties van de agent voert hij meteen uit, in
//!   volgorde, en wat de kern raakt wacht met `.await` (geen geneste
//!   executor-rondes, handboek §4).
//! - [`Hub`]: de brievenbus tussen de verbindingstaken en de eigenaar-taak
//!   (handboek §1: wie iets wil met de staat, stuurt een bericht).
//! - [`download`]: de downloadtaak, die de artifacts naast de eigenaar
//!   ophaalt en de bytes als berichten teruggeeft, zodat de API nooit op
//!   een download wacht.
//! - [`Handoff`]: de overdracht van een verbinding van de acceptor van een
//!   poort aan een vrije werker uit een vaste pool.
//!
//! - De cluster (`HOPOS_LOCK_*`): de lock ([`lock`], [`hoplock`], [`s3`]),
//!   de taken die het net op gaan en hun rijen ([`mail`], [`lease`],
//!   [`link`], [`relay`]), en de doorgiftes van de verbindingstaken naar
//!   de leader of een agent op een andere node ([`forward`], over
//!   [`client`]).
//!
//! Wat hij niet bezit: sockets en de executor (de binary), de frames naar de
//! kern (`hopos-runner`), HTTP (`hop-http`).

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

pub mod client;
pub mod download;
pub mod env;
pub mod fetch;
pub mod flip;
pub mod forward;
mod handoff;
pub mod hoplock;
mod hub;
pub mod lease;
pub mod link;
mod local;
pub mod lock;
pub mod mail;
mod node;
pub mod objstore;
pub mod relay;
pub mod restart;
pub mod s3;
pub mod sntp;

pub use env::{BootConfig, BootError};
pub use fetch::{Connect, HttpImages, Resolve};
pub use handoff::Handoff;
pub use hub::{Answer, Hub, Question};
pub use node::{Cluster, ClusterParts, Images, Kept, Node, Port, Sink};

/// Hoe lang Hop wacht tussen pogingen als de kern bij de start niet
/// antwoordt (een geweigerde of verbroken system-verbinding, geen weigering
/// van de op): de klok ([`sntp::set_clock`]), de staat (`Node::restore`) en
/// de init-jobs. Samen 6,25 s; daarna geeft de stap het op zoals voorheen.
/// Gezien op ijzer (ORION, 24-10): een herstarte Hop kreeg vlak na zijn
/// start "system call transport: connection closed".
pub const KERNEL_BACKOFF: [core::time::Duration; 5] = [
    core::time::Duration::from_millis(250),
    core::time::Duration::from_millis(500),
    core::time::Duration::from_secs(1),
    core::time::Duration::from_secs(2),
    core::time::Duration::from_millis(2500),
];

/// De versie die de agent aan de leader meldt.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_cluster;
#[cfg(test)]
mod tests_net;

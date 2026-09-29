//! Hop als eerste bewoner van HopOS: agent en leader in één slot, met de bevoegde system-API.
//!
//! De bibliotheek-kant van de bewoner, host-getest; de binary (`main.rs`)
//! zet hem alleen op de app-core neer. Hij bezit:
//!
//! - [`BootConfig`]: wat de kern via de env van het slot meegeeft
//!   (`HOPOS_*`, zie [`env`]), in plaats van `hopos.cfg` en de bootparams
//!   van de Go-kern.
//! - [`Node`]: de ene eigenaar van alle staat van de node: de
//!   [`agent::Agent`], de [`leader::Leader`] van de standalone-cluster (zoals
//!   de Go-kern in fase 1), de [`runner::HopRunner`] over de system-API, en
//!   de twee API's. Verzoeken en de tik komen binnen als `async`
//!   methode-aanroepen; de acties van de agent voert hij meteen uit, in
//!   volgorde, en wat de kern raakt wacht met `.await` (geen geneste
//!   executor-rondes, handboek §4).
//! - [`Hub`]: de brievenbus tussen de verbindingstaken en de eigenaar-taak
//!   (handboek §1: wie iets wil met de staat, stuurt een bericht).
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

pub mod env;
mod hub;
mod local;
mod node;

pub use env::{BootConfig, BootError};
pub use hub::Hub;
pub use node::{Images, Node, Port, Sink};

/// De versie die de agent aan de leader meldt.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests;

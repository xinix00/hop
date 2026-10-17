//! hopdns: DNS-service-discovery voor Hop, met federatie.
//!
//! Een jobnaam wordt een A-record: `myapp.hop.local` geeft de adressen van
//! elke taak van `myapp` in de staat `running`, over alle clusters samen;
//! `myapp.prod-eu.hop.local` alleen die van cluster `prod-eu`. Elke cluster
//! is een peer, ook de eigen; de naam van een cluster komt uit zijn
//! `/v1/status` (`cluster_name`).
//!
//! Deze bibliotheek is `no_std` met `alloc` en doet geen I/O (sans-I/O). Ze
//! bezit:
//!
//! - [`wire`]: het DNS-draadformaat (RFC 1035): een vraag lezen, een
//!   antwoord schrijven met meerdere A-records, CNAME, SOA en EDNS, en een
//!   antwoord lezen voor de zelftoets.
//! - [`cache`]: de cache per cluster, jobnaam naar adressen.
//! - [`server`]: de beslissing per vraag: CNAME, dienst, NXDOMAIN, REFUSED.
//! - [`cnames`]: de statische CNAME-tabel met wildcards.
//! - [`config`]: de vlaggen, de peers (`key@host`) en het YAML-bestand.
//! - [`watcher`]: de toestandsmachine per peer (ontdekken, verbinden,
//!   event, herladen, herbouwen, herverbinden) en het bouwen van een
//!   clusterbeeld uit agents en taken.
//! - [`table`]: een kleine gesorteerde tabel met faalbare invoeging.
//!
//! Wat hier niet staat: sockets, threads, HTTP en de klok. Die zijn van de
//! binaries: `hopdns` (feature `std`, module `host`) en `hopdns-hopos` (feature
//! `hopos`, de bewoner op applib).

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

pub mod cache;
pub mod cnames;
pub mod config;
mod error;
pub mod server;
pub mod table;
pub mod watcher;
pub mod wire;

#[cfg(feature = "std")]
pub mod host;

pub use cache::{Cache, Jobs};
pub use cnames::CNAMEs;
pub use config::{Config, Flags, Peer};
pub use error::{Error, Result};
pub use server::Server;
pub use watcher::{Action, Update, Watcher};

/// Het domein zonder `-domain`: dat van de README en de Go-jobspecs.
pub const DEFAULT_DOMAIN: &str = "hop.local";

/// De TTL van elk record, ook de negatieve (SOA-minimum): vijf seconden,
/// het ritme waarop een taak kan verhuizen (Go: `Ttl: 5`, "match our 5s
/// polling interval").
pub const TTL: u32 = 5;

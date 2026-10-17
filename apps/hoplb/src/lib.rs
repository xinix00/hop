//! hoplb: de load balancer van Hop op hostnaam, met Prometheus-metrics.
//!
//! Deze crate is de kern, `no_std` met `alloc` en zonder I/O. Hij bezit:
//!
//! - [`route`]: de routetabel. Een `hoplb-urlprefix`-tag van een job is een
//!   patroon (`app.example.com` of `*.example.com`, één niveau diep); een
//!   route kiest round-robin uit de draaiende taken.
//! - [`watch`]: de staat van de watcher: de agents, de jobs met de tag-filter
//!   (`-tag lb:haas`), de taken, welke gebeurtenis van de agent wat betekent,
//!   en de routetabel die daaruit volgt. Wie de agent vraagt, is de schil.
//! - [`proxy`]: de reverse proxy als toestandsmachine over de
//!   `AsyncRead`/`AsyncWrite` van leanhttp: kop lezen, route kiezen, bellen,
//!   koppen doorgeven met `X-Forwarded-For`, de body in begrensde happen
//!   stromen, het antwoord terug.
//! - [`metrics`]: verzoeken, statuscodes en latentie-percentielen per domein
//!   en backend, en de Prometheus-tekst daarvan.
//! - [`admin`]: de admin-poort, `/health` en `/metrics`.
//!
//! De twee schillen staan in de bins: `hoplb` (feature `std`: threads met
//! elk één eigenaar, std-sockets) en `hoplb-hopos` (feature `hopos`: de
//! bewoner op HopOS, taken op de executor van applib). Beide draaien precies
//! deze kern; wat verschilt is wie de sockets en de klok bezit.
//!
//! De Go-generatie (`OLD/`) is de specificatie: de tests staan hier naam
//! voor naam, de benchmarks als tests met een meetregel.

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

pub mod admin;
mod error;
pub mod http;
mod json;
pub mod metrics;
pub mod proxy;
pub mod route;
pub mod watch;

pub use error::{Error, Result};
pub use metrics::{Metrics, Record};
pub use route::{Backend, Pick, Route, RouteTable};
pub use watch::Watcher;

/// De naam en versie van deze build, voor de logregel bij de start.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod testutil;

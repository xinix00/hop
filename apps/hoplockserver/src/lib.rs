//! hoplockserver: de compare-and-swap-API onder hoplock, zonder I/O.
//!
//! De server die de leases van Hop's leader-election draagt: GET, PUT en
//! DELETE op sleutels, met `If-Match` en `If-None-Match` als voorwaarden en
//! één gedeelde `X-API-Key`. Een nep-S3 voor lease-bestanden, zonder buckets
//! en zonder SigV4. De Go-code (`OLD/`) is de specificatie; elk antwoord is
//! dat van Go, byte voor byte waar de client het leest (status, `ETag`,
//! body), zodat Hop's Rust-client (`store::HoplockLease` in hop) en de
//! Go-client (`OLD/client`) er zonder verschil tegen praten.
//!
//! Deze crate bezit de logica en niets dat blokkeert:
//!
//! - [`key`]: de sleutel als pad, met Go's `filepath.Clean` en de weigering
//!   van alles dat buiten de datamap reikt.
//! - [`etag`]: de ETag, `"<hex sha256 van de body>"`, deterministisch.
//! - [`store`]: de store als trait ([`Store`]: `get`, `put_if`, `delete`),
//!   de voorwaarden en de geheugen-store voor de tests.
//! - [`server`]: de HTTP-afhandeling als toestandsmachine: een kop wordt een
//!   antwoord, een body-vraag of een store-operatie; een uitkomst wordt een
//!   antwoord.
//! - [`http`]: de naad naar leanhttp, over elke verbinding.
//! - [`volume`]: de store op een volume met alleen bestandscalls (hopfs op
//!   HopOS), met een controlegetal per record.
//! - `host` (feature `std`): de bestanden-store en de server op std-sockets.
//!
//! Wie de store bezit, is de aanroeper: op de host één store-thread, op
//! HopOS één store-taak. De verbindingen sturen berichten; er is geen slot
//! (handboek §1).

#![no_std]
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
#[cfg(any(test, feature = "std"))]
extern crate std;

pub mod etag;
pub mod http;
pub mod key;
pub mod server;
pub mod store;
pub mod volume;

#[cfg(feature = "std")]
pub mod host;

#[cfg(test)]
mod tests;

pub use etag::Etag;
pub use key::Key;
pub use server::{Done, Head, Op, PendingPut, Response, Server, Step};
pub use store::{Condition, Error, MemStore, Object, Result, Store};

/// De grootste body die een PUT meekrijgt: 1 MiB, zoals Go's
/// `http.MaxBytesReader(w, r.Body, 1<<20)`. leanhttp begrenst op hetzelfde
/// getal (`leanhttp::MAX_BODY_BYTES`) en weigert daarboven met 413.
pub const MAX_BODY: usize = 1 << 20;

/// De standaardpoort van de server (Go: `-listen :8090`).
pub const DEFAULT_PORT: u16 = 8090;

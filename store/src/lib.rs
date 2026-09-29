//! De opslag van de host: de leader-lease en de gecommitte clusterstaat op S3, hoplockserver of een bestand.
//!
//! Deze crate bezit de I/O die `discovery` bewust niet heeft: de
//! lease-backends die het CAS-protocol van hoplock over de draad spreken
//! ([`S3Lease`], [`HoplockLease`]) en de drie plekken waar de leaseholder de
//! gecommitte clusterstaat bewaart ([`S3StateStore`], [`HoplockStateStore`],
//! [`FileStateStore`]). De keuze zelf bezit hij niet: welke opslag de staat
//! krijgt, zegt [`discovery::state_store_for`], zodat daemon en HopOS nooit
//! uiteenlopen.
//!
//! Elke backend is van één eigenaar. De daemon draait de lease op een eigen
//! thread en de staat op de leader-thread; alles is daarom `Send`, niets is
//! `Sync` nodig en er is geen slot. Elke aanroep blokkeert zijn thread met
//! een termijn uit [`discovery::backend_timeout_for`], en die termijn is
//! het budget van de hele aanroep: verbinden, kop, body en een eventuele
//! tweede poging samen (niet per fase).
//!
//! De lease-JSON is die van Go's `hoplock.State`, zodat Go- en Rust-nodes
//! één bucket of één hoplockserver kunnen delen (zie `wire`).
//!
//! Geheimen (de S3-sleutel, het sessietoken, de API-sleutel van de
//! hoplockserver) staan in geen enkele `Display` of `Debug` van deze crate.

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

mod file;
mod hoplock;
mod s3;
mod wire;

use std::fmt;
use std::time::Duration;

use config::{Config, LockConfig};
use discovery::{Backend, LeaseState, MemBackend, StateStoreKind};

pub use file::FileStateStore;
pub use hoplock::{HoplockLease, HoplockStateStore};
pub use s3::{S3Lease, S3StateStore};

/// Hoeveel bytes een snapshot of lease bij het lezen hoogstens is: 4 MiB,
/// de grens van een gebufferde GET in leans3 ([`leans3::MAX_BUFFERED_GET`]).
/// Een hoplockserver neemt zelf hoogstens 1 MiB per PUT aan.
pub const MAX_OBJECT: usize = 4 << 20;

/// Hoeveel van een foutbody in een melding komt.
const ERROR_BODY: usize = 512;

/// Wat er mis kan gaan bij het openen of gebruiken van de opslag.
///
/// De `Display` noemt operatie, sleutel of pad en status; nooit een geheim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// `cluster.lock.type` is geen `hoplockserver`, `s3` of `mem`.
    UnknownLockType {
        /// Het gelezen type.
        kind: String,
    },
    /// De lock-sectie mist een verplicht veld voor zijn type.
    Incomplete {
        /// Het type (`s3`, `hoplockserver`).
        kind: &'static str,
        /// Welk veld ontbreekt.
        missing: &'static str,
    },
    /// Een S3-operatie faalde.
    S3 {
        /// De operatie (`GET`, `PUT`, ...).
        op: &'static str,
        /// De sleutel.
        key: String,
        /// De fout van leans3.
        source: leans3::Error,
    },
    /// Een verzoek aan de hoplockserver kwam niet aan of niet terug.
    Http {
        /// De operatie.
        op: &'static str,
        /// De URL, zonder gebruikersgegevens.
        url: String,
        /// De fout van de client.
        source: hostnet::Error,
    },
    /// De hoplockserver antwoordde met een onverwachte status.
    Status {
        /// De operatie.
        op: &'static str,
        /// De URL, zonder gebruikersgegevens.
        url: String,
        /// De status.
        code: u16,
        /// Het begin van de body.
        body: String,
    },
    /// Een antwoord zonder ETag, waar de ETag de handle is.
    MissingEtag {
        /// De operatie.
        op: &'static str,
        /// De sleutel.
        key: String,
    },
    /// Het lease-object is geen `hoplock.State`, of liet zich niet schrijven.
    BadLease {
        /// De sleutel.
        key: String,
        /// Wat er mis is.
        why: &'static str,
    },
    /// Een lokaal bestand faalde.
    File {
        /// De stap (`mkdir`, `write`, `fsync`, `rename`, `read`).
        op: &'static str,
        /// Het pad.
        path: String,
        /// De zin van het OS.
        why: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownLockType { kind } => write!(
                f,
                "store: unknown lock type {kind:?} (want one of: hoplockserver, s3, mem)"
            ),
            Self::Incomplete { kind, missing } => {
                write!(f, "store: lock type {kind} needs {missing}")
            }
            Self::S3 { op, key, source } => write!(f, "store: s3 {op} {key}: {source}"),
            Self::Http { op, url, source } => write!(f, "store: {op} {url}: {source}"),
            Self::Status {
                op,
                url,
                code,
                body,
            } => write!(f, "store: {op} {url}: status {code}: {body}"),
            Self::MissingEtag { op, key } => write!(f, "store: {op} {key}: response has no ETag"),
            Self::BadLease { key, why } => write!(f, "store: lease {key}: {why}"),
            Self::File { op, path, why } => write!(f, "store: state file: {op} {path}: {why}"),
        }
    }
}

impl std::error::Error for Error {}

/// Het resultaat-type van deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

/// De gecommitte clusterstaat: één blob, onvoorwaardelijk overschreven door de leaseholder.
///
/// Geen CAS: de schrijver is per constructie de huidige leaseholder, dus er
/// is één schrijver en de laatste schrijf wint terecht. Het object hernoemen
/// of verwijderen is de "schoon opstarten"-schakelaar van de operator.
pub trait StateStore {
    /// Overschrijft de snapshot.
    fn save(&mut self, snapshot: &[u8]) -> Result;
    /// Leest de snapshot; `None` is "er is nog niets", een schone start en geen fout.
    fn load(&mut self) -> Result<Option<Vec<u8>>>;
    /// Waar de staat woont, voor de opstartregel; zonder geheimen.
    fn describe(&self) -> String;
}

/// De sleutel van de lease: `lock.key` als die gezet is, anders `leases/<cluster>`.
fn lease_key(cfg: &Config) -> String {
    if cfg.cluster.lock.key.is_empty() {
        format!("leases/{}", cfg.cluster.name)
    } else {
        cfg.cluster.lock.key.clone()
    }
}

/// De sleutel van de staat: `state/<cluster>`, naast de lease.
fn state_key(cluster: &str) -> String {
    format!("state/{cluster}")
}

/// De lease-TTL uit de config, in milliseconden.
fn ttl_ms(cfg: &Config) -> u64 {
    cfg.timeouts.leader_lease / types::time::MILLISECOND
}

/// De termijn van één backend-aanroep bij deze config.
fn call_timeout(cfg: &Config) -> Duration {
    Duration::from_millis(discovery::backend_timeout_for(ttl_ms(cfg)))
}

/// Opent de opslag voor de gecommitte clusterstaat; de ENIGE poort.
///
/// De keuze is die van [`discovery::state_store_for`]: een bruikbare
/// S3-sectie (lease en staat in dezelfde bucket), anders een
/// hoplockserver-URL, anders het lokale bestand `paths.state_file`. Er is
/// altijd een opslag: ook standalone heeft de leader een duurzaam thuis voor
/// de gewenste staat.
pub fn open_state_store(cfg: &Config, standalone: bool) -> Box<dyn StateStore + Send> {
    let lock = &cfg.cluster.lock;
    let kind = state_store_for(standalone, lock);
    match kind {
        StateStoreKind::S3 => Box::new(S3StateStore::new(
            &lock.s3,
            &cfg.cluster.name,
            call_timeout(cfg),
        )),
        StateStoreKind::HoplockServer => Box::new(HoplockStateStore::new(
            &lock.url,
            &lock.api_key,
            &cfg.cluster.name,
            call_timeout(cfg),
        )),
        StateStoreKind::File => Box::new(FileStateStore::new(&cfg.paths.state_file)),
    }
}

/// [`discovery::state_store_for`] op een lock-sectie.
fn state_store_for(standalone: bool, lock: &LockConfig) -> StateStoreKind {
    discovery::state_store_for(
        standalone,
        &lock.kind,
        &lock.url,
        &lock.s3.endpoint,
        &lock.s3.bucket,
    )
}

/// De lease-backend van een node: in het geheugen, op S3 of op een hoplockserver.
#[derive(Debug)]
pub enum Lease {
    /// Standalone of `mem`: in dit proces.
    Mem(MemBackend),
    /// Een S3-compatibele bucket.
    S3(S3Lease),
    /// Een hoplockserver.
    Hoplock(HoplockLease),
}

impl Lease {
    /// De laatste fout achter een [`discovery::Error::Unreachable`], voor de logregel.
    ///
    /// `discovery` kent alleen drie antwoorden; waaróm de opslag onbereikbaar
    /// was (403, DNS, een termijn) bewaart de backend hier tot de volgende
    /// aanroep die slaagt.
    pub fn last_error(&self) -> Option<&Error> {
        match self {
            Self::Mem(_) => None,
            Self::S3(l) => l.last_error(),
            Self::Hoplock(l) => l.last_error(),
        }
    }
}

impl Backend for Lease {
    fn read(&mut self) -> discovery::Result<(LeaseState, String)> {
        match self {
            Self::Mem(b) => b.read(),
            Self::S3(b) => b.read(),
            Self::Hoplock(b) => b.read(),
        }
    }

    fn write(&mut self, prev: &str, state: &LeaseState) -> discovery::Result<String> {
        match self {
            Self::Mem(b) => b.write(prev, state),
            Self::S3(b) => b.write(prev, state),
            Self::Hoplock(b) => b.write(prev, state),
        }
    }

    fn delete(&mut self, handle: &str) -> discovery::Result {
        match self {
            Self::Mem(b) => b.delete(handle),
            Self::S3(b) => b.delete(handle),
            Self::Hoplock(b) => b.delete(handle),
        }
    }
}

/// Opent de lease-backend uit de config (Go: `buildBackend`).
///
/// Standalone en `mem` geven de in-memory opslag; `s3` een [`S3Lease`] met
/// ghost-overname na één lease-TTL; `""` en `hoplockserver` een
/// [`HoplockLease`]. Een onbekend type, of een type zonder zijn verplichte
/// velden, is een fout bij het openen in plaats van een lease die elke tick
/// stil faalt.
pub fn open_lease(cfg: &Config, standalone: bool) -> Result<Lease> {
    if standalone {
        return Ok(Lease::Mem(MemBackend::new()));
    }
    let lock = &cfg.cluster.lock;
    match lock.kind.as_str() {
        "mem" => Ok(Lease::Mem(MemBackend::new())),
        "s3" => {
            if lock.s3.endpoint.is_empty() {
                return Err(Error::Incomplete {
                    kind: "s3",
                    missing: "s3.endpoint",
                });
            }
            if lock.s3.bucket.is_empty() {
                return Err(Error::Incomplete {
                    kind: "s3",
                    missing: "s3.bucket",
                });
            }
            Ok(Lease::S3(S3Lease::new(
                &lock.s3,
                &lease_key(cfg),
                call_timeout(cfg),
                ttl_ms(cfg),
            )))
        }
        "" | "hoplockserver" => {
            if lock.url.is_empty() {
                return Err(Error::Incomplete {
                    kind: "hoplockserver",
                    missing: "url",
                });
            }
            Ok(Lease::Hoplock(HoplockLease::new(
                &lock.url,
                &lock.api_key,
                &lease_key(cfg),
                call_timeout(cfg),
            )))
        }
        other => Err(Error::UnknownLockType {
            kind: other.to_string(),
        }),
    }
}

/// Of de lock-sectie genoeg zegt voor een werkende backend (Go: `lockConfigured`).
pub fn lock_configured(lock: &LockConfig) -> bool {
    match lock.kind.as_str() {
        "s3" => !lock.s3.endpoint.is_empty() && !lock.s3.bucket.is_empty(),
        "mem" => true,
        _ => !lock.url.is_empty(),
    }
}

/// Een korte omschrijving van de backend voor de opstartregel (Go: `lockLabel`), zonder geheimen.
pub fn lock_label(lock: &LockConfig) -> String {
    match lock.kind.as_str() {
        "s3" => format!("s3 ({}/{})", redact_url(&lock.s3.endpoint), lock.s3.bucket),
        "mem" => String::from("mem (in-process)"),
        _ => format!("hoplockserver ({})", redact_url(&lock.url)),
    }
}

/// Een URL zonder `gebruiker:wachtwoord@`, zodat een geheim in de URL niet in een logregel belandt.
fn redact_url(url: &str) -> String {
    let (scheme, rest) = match url.split_once("://") {
        Some((s, r)) => (Some(s), r),
        None => (None, url),
    };
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    let host = match authority.rsplit_once('@') {
        Some((_, host)) => host,
        None => authority,
    };
    match scheme {
        Some(s) => format!("{s}://{host}{tail}"),
        None => format!("{host}{tail}"),
    }
}

/// Het begin van een foutbody, als tekst en getrimd.
fn body_excerpt(body: &[u8]) -> String {
    let cut = body.get(..ERROR_BODY).unwrap_or(body);
    String::from_utf8_lossy(cut).trim().to_string()
}

#[cfg(test)]
mod tests;

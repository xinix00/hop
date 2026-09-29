//! De taak-backends achter één trait: HopOS-slots (via de bevoegde system-API), processen, docker.
//!
//! Deze crate bezit de runner-kant van een taak: welke kooi een taak houdt,
//! de startfase (het image dat de partitie in stroomt), de stop met
//! quarantaine als de vrijgave niet bevestigd is, en de laatste logregels.
//! Hij bezit NIET de beslissing óf een taak draait: dat is de agent, die de
//! runner via zijn acties aanstuurt (de agent-crate kent deze crate niet; de
//! executor-taak lijmt ze).
//!
//! Sans-I/O: de tijd komt binnen als `now` (milliseconden), het netwerk
//! (de artifact-download) doet de aanroeper, en de kern zit achter
//! [`SystemApi`]. Wat de kern raakt, is een future: de aanroeper wacht met
//! `.await` en geeft zo de core terug aan de andere taken; er wordt nooit
//! geblokkeerd en nooit een geneste executor-ronde gedraaid (handboek §4).
//!
//! De proces- en docker-backends van de Go-versie hebben een OS nodig en
//! staan (nog) niet in deze crate; zie het eindrapport van de port.

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
#[cfg(feature = "std")]
extern crate std;

mod env;
mod hopos;
/// De host-backends (feature `std`): processen, Docker, downloads, uitpakken en isolatie.
#[cfg(feature = "std")]
pub mod host;
mod logs;
mod system;

use alloc::collections::BTreeMap;
use alloc::string::String;
use core::fmt;
use core::future::Future;

pub use env::{attr_env_vars, env_key, port_env_vars};
pub use hopos::{HOP_STOP_TIMEOUT_MS, HopRunner, MAX_CONCURRENT_DOWNLOADS};
pub use logs::{LogPolicy, LogRing, LogStore};
pub use system::{Slot, SlotApp, SlotState, SlotStatus, StartSpec, Streamed, SysError, SystemApi};

/// Een runner-fout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// De node kan de taak niet PLAATSEN (geen vrije core, pool past niet).
    ///
    /// Geen crash: de taak hoort op een andere node, of hier zodra er iets
    /// vrijkomt. De agent geeft hem daarom terug aan de leader in plaats van
    /// hem te herstarten; herstarten is een storm (elke poging haalt het image
    /// opnieuw en faalt weer).
    NoCapacity(String),
    /// De job past niet bij deze runner (container op HopOS, geen artifact, ...).
    Rejected(&'static str),
    /// De taak is deze runner onbekend.
    UnknownTask,
    /// Er lopen al [`MAX_CONCURRENT_DOWNLOADS`] images; de taak blijft "queued".
    Busy,
    /// De image-stroom klopt niet: geen lengte, te veel of te weinig bytes.
    Stream(&'static str),
    /// De kern kon de stop niet bevestigen; de kooi blijft in quarantaine.
    Quarantined(Slot),
    /// Een fout van de kern die geen plaatsingsfout is.
    System(SysError),
    /// Deze runner heeft geen startfase met een image.
    Unsupported,
    /// Geheugen op.
    Alloc,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NoCapacity(why) => write!(f, "runner: no capacity to place the task: {why}"),
            Error::Rejected(why) => write!(f, "hop driver: {why}"),
            Error::UnknownTask => f.write_str("runner: unknown task"),
            Error::Busy => write!(
                f,
                "runner: {MAX_CONCURRENT_DOWNLOADS} downloads already running"
            ),
            Error::Stream(why) => write!(f, "hop driver: image stream: {why}"),
            Error::Quarantined(slot) => {
                write!(
                    f,
                    "hop driver: stop of slot {} not confirmed; slot quarantined",
                    slot.0
                )
            }
            Error::System(e) => write!(f, "hop driver: {e}"),
            Error::Unsupported => f.write_str("runner: no image phase"),
            Error::Alloc => f.write_str("runner: out of memory"),
        }
    }
}

/// Het resultaat-type van deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

/// Wat een runner over een lopende taak zegt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunState {
    /// De app draait (of zit nog in zijn startfase).
    Running,
    /// De app is weg: gecrasht, beëindigd, of nooit begonnen.
    Failed,
}

/// De stroom waar een logregel vandaan kwam.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stream {
    /// Standaard uitvoer (op HopOS de enige: de hop-ABI log-ring).
    Stdout,
    /// Standaard fout.
    Stderr,
}

/// Alles wat een runner nodig heeft om een taak te starten.
///
/// De agent heeft het artifact al gekozen (precies één, het eerste dat bij de
/// node past) en de poorten toegewezen.
#[derive(Clone, Copy, Debug)]
pub struct StartRequest<'a> {
    /// Het taak-id.
    pub task_id: &'a str,
    /// De jobnaam: op HopOS de namespace van de app in de object-store.
    pub job_name: &'a str,
    /// Het container-image (docker); leeg voor exec en hop.
    pub image: &'a str,
    /// Het aantal artifacts na de resolutie door de agent (0 of 1).
    pub artifacts: usize,
    /// Het extract-veld van het artifact (leeg = een rauw image).
    pub extract: &'a str,
    /// CPU in shares; 1024 is één hele core.
    pub cpu_shares: u32,
    /// Geheugenlimiet in bytes; 0 is geen limiet.
    pub memory_limit: u64,
    /// De env van de job.
    pub env: &'a BTreeMap<String, String>,
    /// De tags van de job (`sharegroup`, `core-class`).
    pub tags: &'a BTreeMap<String, String>,
    /// Gedeeld pad naar lokaal pad.
    pub volumes: &'a BTreeMap<String, String>,
    /// De toegewezen poorten, naam naar poort.
    pub ports: &'a BTreeMap<String, u16>,
}

/// Een verwijzing naar een lopende taak.
#[derive(Clone, Copy, Debug)]
pub struct TaskRef<'a> {
    /// Het taak-id.
    pub id: &'a str,
    /// Wat de runner bij de start teruggaf (HopOS: de kooi); 0 = nog in de startfase.
    pub pid: u32,
}

/// Hoe een start verliep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Started {
    /// De app draait; `pid` is het procesnummer of de kooi.
    Running {
        /// Procesnummer (exec), kooi (hop), 0 (docker).
        pid: u32,
    },
    /// De kooi is gereserveerd; de aanroeper haalt nu het image op en stroomt
    /// het binnen via [`Runner::image_begin`] en [`Runner::image_chunk`].
    AwaitImage,
    /// De taak werd tijdens de start gestopt; er is niets gepubliceerd.
    Aborted,
}

/// Een taak-backend.
///
/// Een backend met een startfase (HopOS: het image dat de partitie in
/// stroomt) antwoordt op `start` met [`Started::AwaitImage`]; de aanroeper
/// doet de download en voert de bytes.
///
/// Wat een backend bij zijn kern of OS moet vragen (`start`, de image-fase,
/// `stop`, `status`), is een future, om dezelfde reden als bij
/// [`SystemApi`]: op HopOS is dat een call over een verbinding op dezelfde
/// executor, en wachten is de core teruggeven. De vorm is `-> impl Future`
/// (zie [`SystemApi`] voor waarom niet `async fn`); de trait is daarmee niet
/// object-safe, en de agent-lijm is generiek over de runner. [`Runner::logs`]
/// leest alleen de eigen ringen en blijft synchroon.
pub trait Runner {
    /// Begint een taak.
    fn start(&mut self, now: u64, req: &StartRequest<'_>) -> impl Future<Output = Result<Started>>;

    /// Meldt de lengte van het image; zonder lengte geen start.
    fn image_begin(
        &mut self,
        _now: u64,
        _task_id: &str,
        _size: u64,
    ) -> impl Future<Output = Result> {
        async { Err(Error::Unsupported) }
    }

    /// Voert de volgende bytes van het image; geeft `Running` na de laatste byte.
    fn image_chunk(
        &mut self,
        _now: u64,
        _task_id: &str,
        _chunk: &[u8],
    ) -> impl Future<Output = Result<Started>> {
        async { Err(Error::Unsupported) }
    }

    /// Stopt een taak. Een onbekende taak is niet van ons: `Ok` zonder iets te doen.
    ///
    /// Een fout betekent dat de vrijgave niet bevestigd is; de runner houdt de
    /// hulpbron dan zelf in quarantaine, zodat niemand hem hergebruikt.
    fn stop(&mut self, now: u64, task: &TaskRef<'_>) -> impl Future<Output = Result>;

    /// De toestand van een taak.
    fn status(&mut self, now: u64, task: &TaskRef<'_>) -> impl Future<Output = Result<RunState>>;

    /// De laatste regels van een taak, ook nog even na zijn einde.
    fn logs(&self, now: u64, task_id: &str, stream: Stream) -> Option<&LogRing>;
}

#[cfg(test)]
mod tests;

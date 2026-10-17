//! De client voor de API van een Hop-agent: ondertekende verzoeken, getypeerde antwoorden, de SSE-stroom.
//!
//! Deze crate bezit wat de plugins (hoplb, hopdns, hopprom) gemeen hebben:
//! hoe een verzoek aan de agent eruitziet ([`Client`], [`Request`]), hoe een
//! antwoord gelezen wordt ([`Call::parse`], [`Status`], [`Capacity`], ...),
//! de lezer van `/v1/events` ([`Events`], [`Event`]) en het wachtbeleid bij
//! het herverbinden ([`Backoff`]). De kern is `no_std` met `alloc` en doet
//! geen I/O: hij maakt bytes en leest bytes, zodat hij op de host en in een
//! HopOS-slot dezelfde is en tegen vaste bytes te toetsen valt.
//!
//! Het transport kiest de aanroeper met een feature:
//!
//! - `std`: [`host::Agent`], blokkerend over `hostnet::Http`, één executor
//!   per thread zoals hostnet.
//! - `hopos`: [`hopos::Agent`], async op de executor van de app-core, over
//!   `applib::tcp::TcpConn` en de client van leanhttp.
//!
//! De ondertekening (`X-Hop-Auth`) is die van hop's `auth`-crate, niet een
//! tweede kopie: client en server rekenen dan per definitie hetzelfde.
//!
//! Het adres is dat van de LOKALE agent (`http://127.0.0.1:7878` of het
//! slot-adres): die geeft `/v1/...` door aan de leader, zoals de Go-plugins
//! het ook deden. Een plugin hoeft dus niet te weten wie er leidt.

// De kern is no_std; alleen de host-module (feature `std`) en de tests
// linken std. Dat de kern het zonder kan, bewijst de no_std-bouw van de poort.
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

mod backoff;
mod client;
mod model;
mod sse;

#[cfg(feature = "hopos")]
pub mod hopos;
#[cfg(feature = "std")]
pub mod host;

use core::fmt;

pub use backoff::{BACKOFF_MAX, BACKOFF_MIN, Backoff};
pub use client::{Call, Client, Request, Response};
pub use model::{AgentTasks, Applied, Capacity, JobStatus, Leader, Status};
pub use sse::{Event, Events, MAX_EVENT_BYTES, Topic};

pub use auth::AUTH_HEADER;
/// Een agent zoals de leader hem kent (`GET /v1/agents`): hop's `types::Agent`.
pub use types::Agent as AgentInfo;
pub use types::{Job, Map, Task, TaskState, Time};

/// De grootste body die een antwoord mag hebben: de grens van de
/// JSON-parser van hop (`types::json::MAX_INPUT`, 1 MiB). Een grotere body
/// zou toch geweigerd worden; zo weigert het transport hem al vóór hij in
/// het geheugen staat.
pub const MAX_BODY: usize = types::json::MAX_INPUT;

/// Hoeveel bytes een [`Reason`] draagt.
pub const REASON_BYTES: usize = 96;

/// Een korte tekst bij een fout: de melding van de server of van het net.
///
/// Vaste maat, zodat [`Error`] `Copy` blijft en een fout nooit zelf een
/// allocatie vraagt; een langere tekst wordt op een tekengrens afgekapt.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Reason {
    buf: [u8; REASON_BYTES],
    len: usize,
}

impl Reason {
    /// De tekst van `s`, afgekapt op [`REASON_BYTES`].
    pub fn new(s: &str) -> Self {
        let mut r = Self {
            buf: [0; REASON_BYTES],
            len: 0,
        };
        r.push(s);
        r
    }

    /// De tekst van iets dat `Display` heeft, afgekapt op [`REASON_BYTES`].
    pub fn of(d: &dyn fmt::Display) -> Self {
        let mut r = Self::new("");
        // Een volle buffer is afkappen, geen fout: `push` weigert nooit.
        let _ = fmt::write(&mut r, format_args!("{d}"));
        r
    }

    /// De tekst.
    pub fn as_str(&self) -> &str {
        core::str::from_utf8(self.buf.get(..self.len).unwrap_or_default()).unwrap_or_default()
    }

    /// Voegt zoveel van `s` toe als past, op een tekengrens.
    fn push(&mut self, s: &str) {
        let room = REASON_BYTES.saturating_sub(self.len);
        let mut take = s.len().min(room);
        while !s.is_char_boundary(take) {
            take -= 1;
        }
        if let (Some(dst), Some(src)) = (
            self.buf.get_mut(self.len..self.len + take),
            s.as_bytes().get(..take),
        ) {
            dst.copy_from_slice(src);
            self.len += take;
        }
    }
}

impl fmt::Write for Reason {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.push(s);
        Ok(())
    }
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_str())
    }
}

/// Wat er mis kan gaan bij een aanroep van de agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// De heap kon een allocatie niet leveren.
    OutOfMemory,
    /// Het antwoord was geen JSON van de verwachte vorm.
    Json(types::Error),
    /// De agent (of de leader achter hem) antwoordde met een foutstatus.
    Status {
        /// De HTTP-status.
        code: u16,
        /// De melding uit `{"error": ...}`, of leeg.
        reason: Reason,
    },
    /// Het adres is geen `host:poort`, `http://` of `https://`.
    BadUrl,
    /// Het transport faalde: opzoeken, verbinden, een termijn, een reset.
    Net(Reason),
    /// De stroom eindigde (de agent sloot hem, of hij leidt niet meer).
    Closed,
    /// Eén SSE-gebeurtenis is groter dan de grens.
    TooLarge {
        /// Hoeveel bytes er zonder einde lagen.
        len: usize,
        /// De grens ([`MAX_EVENT_BYTES`]).
        max: usize,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfMemory => f.write_str("out of memory"),
            Self::Json(e) => write!(f, "reply: {e}"),
            Self::Status { code, reason } if reason.as_str().is_empty() => {
                write!(f, "status {code}")
            }
            Self::Status { code, reason } => write!(f, "status {code}: {reason}"),
            Self::BadUrl => f.write_str("agent address must be host:port or an http(s) URL"),
            Self::Net(why) => write!(f, "net: {why}"),
            Self::Closed => f.write_str("stream closed"),
            Self::TooLarge { len, max } => {
                write!(f, "event of {len} bytes without an end exceeds {max}")
            }
        }
    }
}

impl core::error::Error for Error {}

impl From<types::Error> for Error {
    fn from(e: types::Error) -> Self {
        match e {
            types::Error::OutOfMemory => Self::OutOfMemory,
            e => Self::Json(e),
        }
    }
}

/// Het resultaat van elke faalbare handeling in deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

#[cfg(test)]
mod tests;

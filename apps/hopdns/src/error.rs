//! De fouten van de bibliotheek.

use core::fmt;

/// Wat er mis kan gaan in de kern van hopdns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// Een allocatie faalde.
    OutOfMemory,
    /// `-h` of `-help`: de aanroeper drukt het gebruik af.
    Help,
    /// Een onbekende vlag, op deze plek in de argumenten (0-geteld).
    UnknownFlag {
        /// De plek.
        at: usize,
    },
    /// Een vlag zonder waarde, op deze plek.
    MissingValue {
        /// De plek.
        at: usize,
    },
    /// Een argument dat geen vlag is, op deze plek.
    UnexpectedArgument {
        /// De plek.
        at: usize,
    },
    /// Geen enkele `-peer`.
    NoPeer,
    /// Een luisteradres dat geen `host:poort` is.
    BadListen,
    /// Een peer-URL die niet met `http://` of `https://` begint.
    BadPeer,
    /// Het YAML-bestand las niet, op deze regel (1-geteld).
    Yaml {
        /// De regel.
        line: usize,
    },
    /// Een peer meldde geen `cluster_name` (een Hop van vóór de federatie).
    NoClusterName,
    /// Een DNS-naam die niet op de draad past (te lang, een lege label).
    BadName,
    /// Het antwoord past niet in de buffer.
    NoRoom,
    /// Een DNS-boodschap die niet leest.
    Malformed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::OutOfMemory => f.write_str("out of memory"),
            Error::Help => f.write_str("help requested"),
            Error::UnknownFlag { at } => write!(f, "argument {at}: unknown flag"),
            Error::MissingValue { at } => write!(f, "argument {at}: flag needs a value"),
            Error::UnexpectedArgument { at } => write!(f, "argument {at}: not a flag"),
            Error::NoPeer => {
                f.write_str("at least one -peer required (e.g., -peer http://127.0.0.1:8080)")
            }
            Error::BadListen => f.write_str("listen address is not host:port"),
            Error::BadPeer => f.write_str("peer is not an http:// or https:// URL"),
            Error::Yaml { line } => write!(f, "config: cannot read line {line}"),
            Error::NoClusterName => {
                f.write_str("remote cluster did not report cluster_name (upgrade hop?)")
            }
            Error::BadName => f.write_str("name does not fit DNS"),
            Error::NoRoom => f.write_str("message does not fit the buffer"),
            Error::Malformed => f.write_str("malformed DNS message"),
        }
    }
}

impl core::error::Error for Error {}

/// Het resultaat van deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

impl From<alloc::collections::TryReserveError> for Error {
    fn from(_: alloc::collections::TryReserveError) -> Self {
        Error::OutOfMemory
    }
}

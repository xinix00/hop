//! De fouten van de kern: één kleine enum met de getallen erin (handboek §6).

use core::fmt;

use leanhttp::IoError;

/// Een fout van de kern.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// De heap is op; `bytes` is wat er gevraagd werd (0 als onbekend).
    OutOfMemory {
        /// De gevraagde grootte.
        bytes: usize,
    },
    /// De verbinding faalde.
    Io(IoError),
    /// De andere kant sloot voor er iets begon (geen fout voor een
    /// keep-alive-verbinding tussen twee verzoeken).
    Eof,
    /// De andere kant sloot midden in een bericht.
    UnexpectedEof,
    /// Een kop past niet in de leesbuffer.
    HeadTooLarge {
        /// De grens in bytes.
        limit: usize,
    },
    /// Een verzoek dat de proxy niet kan lezen; de tekst zegt wat.
    BadRequest(&'static str),
    /// Een antwoord van een backend dat de proxy niet kan lezen.
    BadResponse(&'static str),
    /// Een verzoek met iets dat de proxy bewust niet doet.
    Unsupported(&'static str),
    /// Een adres of URL die niet te lezen is.
    BadAddress,
    /// Een backend was niet te bereiken.
    Dial(IoError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::OutOfMemory { bytes } => write!(f, "out of memory ({bytes} bytes)"),
            Error::Io(e) => write!(f, "connection: {e}"),
            Error::Eof => f.write_str("end of stream"),
            Error::UnexpectedEof => f.write_str("unexpected end of stream"),
            Error::HeadTooLarge { limit } => write!(f, "header block over {limit} bytes"),
            Error::BadRequest(why) => write!(f, "bad request: {why}"),
            Error::BadResponse(why) => write!(f, "bad response: {why}"),
            Error::Unsupported(why) => write!(f, "unsupported: {why}"),
            Error::BadAddress => f.write_str("bad address"),
            Error::Dial(e) => write!(f, "dial: {e}"),
        }
    }
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Self {
        Error::Io(e)
    }
}

impl From<leanhttp::Error> for Error {
    fn from(e: leanhttp::Error) -> Self {
        match e {
            leanhttp::Error::Io(io) => Error::Io(io),
            leanhttp::Error::Alloc { bytes } => Error::OutOfMemory { bytes },
            leanhttp::Error::Eof => Error::Eof,
            leanhttp::Error::UnexpectedEof => Error::UnexpectedEof,
            _ => Error::Io(IoError::Other),
        }
    }
}

/// Het resultaat-type van deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

/// Een `String` uit `s`, faalbaar (handboek §6: geen afbrekende allocatie).
pub(crate) fn try_string(s: &str) -> Result<alloc::string::String> {
    let mut out = alloc::string::String::new();
    out.try_reserve_exact(s.len())
        .map_err(|_| Error::OutOfMemory { bytes: s.len() })?;
    out.push_str(s);
    Ok(out)
}

/// Legt `v` achteraan in `vec`, faalbaar.
pub(crate) fn try_push<T>(vec: &mut alloc::vec::Vec<T>, v: T) -> Result {
    vec.try_reserve(1).map_err(|_| Error::OutOfMemory {
        bytes: core::mem::size_of::<T>(),
    })?;
    vec.push(v);
    Ok(())
}

/// Voegt `v` in op plek `i` van `vec`, faalbaar.
pub(crate) fn try_insert<T>(vec: &mut alloc::vec::Vec<T>, i: usize, v: T) -> Result {
    vec.try_reserve(1).map_err(|_| Error::OutOfMemory {
        bytes: core::mem::size_of::<T>(),
    })?;
    vec.insert(i, v);
    Ok(())
}

/// Legt `bytes` achter `out`, faalbaar.
pub(crate) fn try_extend(out: &mut alloc::vec::Vec<u8>, bytes: &[u8]) -> Result {
    out.try_reserve(bytes.len())
        .map_err(|_| Error::OutOfMemory { bytes: bytes.len() })?;
    out.extend_from_slice(bytes);
    Ok(())
}

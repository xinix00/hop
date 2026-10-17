//! De fouten van de kern. Het net en het lezen van antwoorden zijn van de
//! bron (hoplib); wat hier kan falen, is geheugen.

use core::fmt;

/// Wat er in de kern mis kan gaan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// De heap is op.
    OutOfMemory,
    /// Een andere fout uit hop's `types` (een map, een string).
    Types(types::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfMemory => f.write_str("out of memory"),
            Self::Types(e) => write!(f, "{e}"),
        }
    }
}

impl core::error::Error for Error {}

impl From<types::Error> for Error {
    fn from(e: types::Error) -> Self {
        match e {
            types::Error::OutOfMemory => Self::OutOfMemory,
            e => Self::Types(e),
        }
    }
}

/// Het resultaat van elke faalbare handeling in deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

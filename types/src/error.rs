//! De fouten van deze crate, met de getallen en de veldnaam erbij.
//!
//! Een fout alloceert niet: de naam van een onbekend of fout veld staat in
//! een [`Name`] van vaste maat, zodat een fout ook te melden is wanneer de
//! heap net op is.

use core::fmt;

/// Hoeveel bytes van een veldnaam een fout vasthoudt. Een JSON-sleutel is
/// in de praktijk kort (`health_check_interval` is 21); wat langer is wordt
/// afgekapt, en dat is genoeg om de typefout terug te vinden.
pub const NAME_BYTES: usize = 32;

/// Een veldnaam of sleutel in een fout, afgekapt tot [`NAME_BYTES`].
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Name {
    buf: [u8; NAME_BYTES],
    len: usize,
}

impl Name {
    /// Neemt de eerste [`NAME_BYTES`] bytes van `s` over, op een tekengrens.
    pub fn new(s: &str) -> Self {
        let mut end = s.len().min(NAME_BYTES);
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        let mut buf = [0u8; NAME_BYTES];
        let src = s.as_bytes().get(..end).unwrap_or_default();
        if let Some(dst) = buf.get_mut(..end) {
            dst.copy_from_slice(src);
        }
        Self { buf, len: end }
    }

    /// De naam als tekst.
    pub fn as_str(&self) -> &str {
        let bytes = self.buf.get(..self.len).unwrap_or_default();
        core::str::from_utf8(bytes).unwrap_or_default()
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_str())
    }
}

/// Wat er mis kan gaan bij het lezen, schrijven of kopiëren van Hop-typen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// De heap kon de allocatie niet leveren.
    OutOfMemory,
    /// De invoer is groter dan de grens.
    TooLarge {
        /// De maat van de invoer in bytes.
        len: usize,
        /// De grens in bytes.
        max: usize,
    },
    /// De invoer nest dieper dan de grens.
    TooDeep {
        /// Waar de grens werd geraakt.
        offset: usize,
        /// De maximale diepte.
        max: usize,
    },
    /// De invoer is geen geldige JSON.
    Syntax {
        /// Het byte waar het misging.
        offset: usize,
        /// Wat daar had moeten staan.
        expected: &'static str,
    },
    /// Na het document staat nog iets anders dan witruimte.
    Trailing {
        /// Waar het tweede document begint.
        offset: usize,
    },
    /// Een object noemt dezelfde sleutel twee keer.
    DuplicateKey {
        /// De dubbele sleutel.
        key: Name,
    },
    /// Een getal dat JSON niet kan dragen (NaN, oneindig).
    NotFinite,
    /// Een veld heeft het verkeerde JSON-type.
    WrongType {
        /// Het veld.
        field: Name,
        /// Het type dat verwacht werd.
        want: &'static str,
    },
    /// Een sleutel die het schema niet kent (alleen in strikte modus).
    UnknownField {
        /// De onbekende sleutel.
        field: Name,
    },
    /// Een getal buiten het bereik van het veld.
    OutOfRange {
        /// Het veld.
        field: Name,
    },
    /// Een waarde die het veld niet accepteert.
    Invalid {
        /// Het veld.
        field: Name,
        /// Waarom niet.
        why: &'static str,
    },
    /// Een verzameling is voller dan de grens.
    TooMany {
        /// Het veld.
        field: Name,
        /// De grens.
        max: usize,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfMemory => f.write_str("out of memory"),
            Self::TooLarge { len, max } => write!(f, "input of {len} bytes exceeds {max}"),
            Self::TooDeep { offset, max } => {
                write!(f, "nesting deeper than {max} at byte {offset}")
            }
            Self::Syntax { offset, expected } => {
                write!(f, "invalid JSON at byte {offset}: expected {expected}")
            }
            Self::Trailing { offset } => {
                write!(
                    f,
                    "more than one JSON document (second starts at byte {offset})"
                )
            }
            Self::DuplicateKey { key } => write!(f, "duplicate key \"{key}\""),
            Self::NotFinite => f.write_str("number is not finite"),
            Self::WrongType { field, want } => write!(f, "{field}: expected {want}"),
            Self::UnknownField { field } => write!(f, "unknown field \"{field}\""),
            Self::OutOfRange { field } => write!(f, "{field}: number out of range"),
            Self::Invalid { field, why } => write!(f, "{field}: {why}"),
            Self::TooMany { field, max } => write!(f, "{field}: more than {max} entries"),
        }
    }
}

impl core::error::Error for Error {}

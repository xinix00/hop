//! Base64 in de drie smaken die de tunnel tegenkomt.
//!
//! Bezit het decoderen van het token (standaard met opvulling, of de
//! URL-veilige vorm zonder, zoals een token uit een dashboard-URL), het
//! decoderen van de CA's uit `cfroots.pem`, en het coderen van de kopbundel
//! voor de edge (standaard-alfabet zonder opvulling, zie [`crate::edgeproto`]).
//! Geen alloc: de aanroeper geeft de uitvoer, en een te kleine uitvoer is
//! een fout, nooit een afgekapte waarde.

#![forbid(unsafe_code)]

use core::fmt;

/// Het standaard-alfabet (RFC 4648 §4).
const STD: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Waarom een decodering faalde.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    /// Een teken buiten het alfabet, op deze plek.
    Char {
        /// De byte-offset.
        at: usize,
    },
    /// Een lengte die geen base64 kan zijn (één teken over).
    Length,
    /// De uitvoerbuffer is te klein.
    Full,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Char { at } => write!(f, "not base64 at byte {at}"),
            Self::Length => f.write_str("base64 of an impossible length"),
            Self::Full => f.write_str("decoded value does not fit"),
        }
    }
}

/// De waarde van één teken, in het standaard- of het URL-veilige alfabet.
fn value(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' | b'-' => Some(62),
        b'/' | b'_' => Some(63),
        _ => None,
    }
}

/// Decodeert `src` naar `out` en geeft het aantal bytes.
///
/// Neemt beide alfabetten en opvulling aan of niet; witruimte slaat hij
/// over (een PEM-blok heeft regels). Dat is milder dan Go's twee aparte
/// decoders, en bewust: het token komt in beide vormen voorbij en het
/// resultaat wordt daarna toch als JSON gelezen, dus een verkeerde
/// interpretatie valt daar alsnog op.
pub(crate) fn decode(src: &[u8], out: &mut [u8]) -> Result<usize, Error> {
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    let mut n = 0usize;
    let mut chars = 0usize;
    let mut pad = false;
    for (at, &c) in src.iter().enumerate() {
        if c.is_ascii_whitespace() {
            continue;
        }
        if c == b'=' {
            pad = true;
            continue;
        }
        if pad {
            // Een teken na de opvulling: dat is geen base64 meer.
            return Err(Error::Char { at });
        }
        let v = value(c).ok_or(Error::Char { at })?;
        chars += 1;
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            let b = u8::try_from((acc >> bits) & 0xff).unwrap_or(0);
            *out.get_mut(n).ok_or(Error::Full)? = b;
            n += 1;
        }
    }
    if chars % 4 == 1 {
        return Err(Error::Length);
    }
    Ok(n)
}

/// Hoeveel tekens [`encode_raw`] voor `n` bytes schrijft.
pub(crate) const fn encoded_len(n: usize) -> usize {
    (n * 4).div_ceil(3)
}

/// Codeert `src` in het standaard-alfabet zonder opvulling
/// (Go's `RawStdEncoding`) en roept `put` per teken aan.
pub(crate) fn encode_raw(src: &[u8], mut put: impl FnMut(u8)) {
    let sym = |i: u32| {
        STD.get(usize::try_from(i & 63).unwrap_or(0))
            .copied()
            .unwrap_or(b'A')
    };
    let mut chunks = src.chunks_exact(3);
    for c in &mut chunks {
        let &[a, b, c] = c else {
            continue;
        };
        let w = (u32::from(a) << 16) | (u32::from(b) << 8) | u32::from(c);
        put(sym(w >> 18));
        put(sym(w >> 12));
        put(sym(w >> 6));
        put(sym(w));
    }
    match *chunks.remainder() {
        [a] => {
            let w = u32::from(a) << 16;
            put(sym(w >> 18));
            put(sym(w >> 12));
        }
        [a, b] => {
            let w = (u32::from(a) << 16) | (u32::from(b) << 8);
            put(sym(w >> 18));
            put(sym(w >> 12));
            put(sym(w >> 6));
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::String;
    use alloc::vec::Vec;

    fn enc(s: &[u8]) -> String {
        let mut out = String::new();
        encode_raw(s, |c| out.push(char::from(c)));
        assert_eq!(out.len(), encoded_len(s.len()));
        out
    }

    fn dec(s: &str) -> Result<Vec<u8>, Error> {
        let mut out = [0u8; 256];
        let n = decode(s.as_bytes(), &mut out)?;
        Ok(out[..n].to_vec())
    }

    // RFC 4648 §10, zonder opvulling (RawStdEncoding).
    #[test]
    fn rfc4648_vectors_raw() {
        for (plain, want) in [
            ("", ""),
            ("f", "Zg"),
            ("fo", "Zm8"),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg"),
            ("fooba", "Zm9vYmE"),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(enc(plain.as_bytes()), want);
            assert_eq!(dec(want).unwrap(), plain.as_bytes());
        }
    }

    #[test]
    fn padding_and_both_alphabets() {
        assert_eq!(dec("Zm9vYg==").unwrap(), b"foob");
        assert_eq!(dec("-_8").unwrap(), [0xfb, 0xff]);
        assert_eq!(dec("+/8=").unwrap(), [0xfb, 0xff]);
        assert_eq!(dec("Zm9v\nYmFy\n").unwrap(), b"foobar");
    }

    #[test]
    fn refusals() {
        assert_eq!(dec("Zm9v!"), Err(Error::Char { at: 4 }));
        assert_eq!(dec("Zm9vY"), Err(Error::Length));
        assert_eq!(dec("Zg==Zg"), Err(Error::Char { at: 4 }));
        let mut small = [0u8; 2];
        assert_eq!(decode(b"Zm9v", &mut small), Err(Error::Full));
    }
}

//! De ETag: de SHA-256 van de body in hex, tussen aanhalingstekens.
//!
//! Bezit alleen de vorm. Go schrijft een aanhalingsteken, dan
//! `hex.EncodeToString(sha256.Sum256(body))`, dan weer een aanhalingsteken.
//! Deterministisch op de inhoud: een client kan na een schrijf zelf
//! narekenen wat de server zal zeggen, en de server hoeft niets te onthouden.
//! Een sterke ETag (zonder `W/`), en de aanhalingstekens horen erbij: Hop's
//! client stuurt hem ongewijzigd terug in `If-Match`.

use core::fmt;

/// De lengte van een ETag: 64 hexcijfers plus twee aanhalingstekens.
pub const LEN: usize = 66;

/// Een ETag, zonder allocatie.
///
/// # Invariants
///
/// Byte 0 en byte 65 zijn `"`, de 64 ertussen kleine hexcijfers.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Etag([u8; LEN]);

impl Etag {
    /// De ETag van `body`.
    #[must_use]
    pub fn of(body: &[u8]) -> Etag {
        Etag::from_digest(&auth::sha256(body))
    }

    /// De ETag van een SHA-256 die al berekend is.
    #[must_use]
    pub fn from_digest(digest: &[u8; 32]) -> Etag {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = [b'"'; LEN];
        for (pair, b) in out[1..LEN - 1].chunks_exact_mut(2).zip(digest) {
            pair[0] = HEX[usize::from(b >> 4)];
            pair[1] = HEX[usize::from(b & 0x0f)];
        }
        // INVARIANT: de randen bleven `"`, het midden is hex.
        Etag(out)
    }

    /// De ETag als tekst, met aanhalingstekens.
    #[must_use]
    pub fn as_str(&self) -> &str {
        // De invariant maakt dit ASCII; een lege tekst kan dus niet.
        core::str::from_utf8(&self.0).unwrap_or_default()
    }
}

impl fmt::Display for Etag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for Etag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<str> for Etag {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn etag_is_quoted_hex_sha256() {
        // sha256("hello"), zoals `printf hello | shasum -a 256`.
        assert_eq!(
            Etag::of(b"hello").as_str(),
            "\"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824\""
        );
        assert_eq!(
            Etag::of(b"").as_str(),
            "\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\""
        );
    }
}

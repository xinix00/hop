//! De sleutel: een relatief pad onder de datamap, schoongemaakt zoals Go.
//!
//! Bezit alleen de vorm. Go's `store.path` weigert een lege sleutel, een
//! NUL-byte en een leidende `/`, maakt de rest schoon met `filepath.Clean`
//! en weigert dan `.`, alles wat met `..` begint en alles met `../` erin.
//! Wat overblijft is het pad onder de datamap; `a/./b`, `a//b` en `a/b/`
//! zijn dus dezelfde sleutel als `a/b`, precies als in Go.

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use crate::store::Error;

/// Een geldige, schoongemaakte sleutel.
///
/// # Invariants
///
/// Niet leeg, geen NUL, geen leidende of afsluitende `/`, geen leeg, `.`
/// of `..`-segment, en hij begint niet met `..`: de vorm die Go's
/// `filepath.Clean` teruggeeft en die `store.path` doorlaat.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(String);

impl Key {
    /// Toetst en maakt `raw` schoon, of [`Error::BadKey`] zoals Go's `ErrBadKey`.
    pub fn parse(raw: &str) -> Result<Key, Error> {
        if raw.is_empty() || raw.contains('\0') || raw.starts_with('/') {
            return Err(Error::BadKey);
        }
        let clean = clean(raw)?;
        if clean == "." || clean.starts_with("..") || clean.contains("../") {
            return Err(Error::BadKey);
        }
        // INVARIANT: `clean` is Go's `Clean` van een relatief pad dat de
        // toetsen hierboven doorstond.
        Ok(Key(clean))
    }

    /// De sleutel als tekst.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Go's `path.Clean` voor een relatief pad (`filepath.Clean` op Unix).
///
/// Lege en `.`-segmenten vallen weg, `..` neemt het vorige gewone segment
/// mee en blijft staan als er niets meer mee te nemen is; leeg wordt `.`.
fn clean(p: &str) -> Result<String, Error> {
    let mut segs: Vec<&str> = Vec::new();
    segs.try_reserve(p.len() / 2 + 1)
        .map_err(|_| Error::OutOfMemory { bytes: p.len() })?;
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => match segs.last() {
                Some(&last) if last != ".." => {
                    segs.pop();
                }
                _ => segs.push(".."),
            },
            s => segs.push(s),
        }
    }
    let mut out = String::new();
    out.try_reserve(p.len().max(1))
        .map_err(|_| Error::OutOfMemory { bytes: p.len() })?;
    for (i, s) in segs.iter().enumerate() {
        if i > 0 {
            out.push('/');
        }
        out.push_str(s);
    }
    if out.is_empty() {
        out.push('.');
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// De tabel van Go's `path.Clean`-tests voor relatieve paden.
    #[test]
    fn clean_is_gos_clean() {
        for (input, want) in [
            ("abc", "abc"),
            ("abc/def", "abc/def"),
            ("a/b/c", "a/b/c"),
            (".", "."),
            ("..", ".."),
            ("../..", "../.."),
            ("../../abc", "../../abc"),
            ("", "."),
            ("abc/", "abc"),
            ("abc/def/", "abc/def"),
            ("abc//def//ghi", "abc/def/ghi"),
            ("abc/./def", "abc/def"),
            ("./abc/def", "abc/def"),
            ("abc/.", "abc"),
            ("abc/def/ghi/../jkl", "abc/def/jkl"),
            ("abc/def/../ghi/../jkl", "abc/jkl"),
            ("abc/def/..", "abc"),
            ("abc/def/../..", "."),
            ("abc/def/../../..", ".."),
            ("abc/def/../../../ghi/jkl/../../../mno", "../../mno"),
            ("abc/./../def", "def"),
            ("abc//./../def", "def"),
            ("abc/../../././../def", "../../def"),
        ] {
            assert_eq!(clean(input).unwrap(), want, "{input:?}");
        }
    }

    #[test]
    fn keys_are_cleaned_like_the_go_store() {
        assert_eq!(Key::parse("lease/a").unwrap().as_str(), "lease/a");
        assert_eq!(Key::parse("a//b/./c/").unwrap().as_str(), "a/b/c");
        assert_eq!(Key::parse("foo/../bar").unwrap().as_str(), "bar");
        for bad in [
            "",
            "/abs",
            "../escape",
            "foo/../../escape",
            "a\0b",
            ".",
            "a/..",
            "..foo",
        ] {
            assert_eq!(Key::parse(bad), Err(Error::BadKey), "{bad:?}");
        }
    }
}

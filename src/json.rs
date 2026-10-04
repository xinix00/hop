//! Precies zoveel JSON als de tunnel leest en schrijft.
//!
//! Bezit een lezer zonder boom: het token (`{"a","s","t"}`), de config-push
//! van de edge (`{"version", "config": {"ingress": [...]}}`) en
//! `TUNNEL_INGRESS` worden in één doorgang gelezen, met een terugroep per
//! veld; wat de tunnel niet kent (`warp-routing`, `originRequest`) slaat
//! hij over zonder het te bewaren. En een schrijver voor één tekenreeks met
//! de juiste escapes, voor het antwoord op een push. Geen getallen met
//! breuk of exponent als waarde die gelezen wordt: de enige die er toe doet
//! is een versie, een geheel getal.
//!
//! De diepte is begrensd ([`MAX_DEPTH`]): dit zijn bytes van het netwerk,
//! en een recursieve lezer zonder grens is een stack die de afzender kiest.

#![forbid(unsafe_code)]

use alloc::string::String;
use core::fmt;

/// De diepste nesting die de lezer aanneemt. Een config-push is vier diep
/// (`config`, `ingress`, een regel, `originRequest`); 32 is ruim.
pub(crate) const MAX_DEPTH: u8 = 32;

/// Waarom het lezen faalde, met de byte-offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    /// Iets anders dan verwacht op deze plek.
    Syntax {
        /// De byte-offset.
        at: usize,
    },
    /// Een ander type dan het veld hoort te hebben.
    Type {
        /// De byte-offset.
        at: usize,
        /// Wat er had moeten staan.
        want: &'static str,
    },
    /// Dieper genest dan [`MAX_DEPTH`].
    Depth,
    /// Een geheel getal buiten `i64`.
    Range {
        /// De byte-offset.
        at: usize,
    },
    /// De heap weigerde een tekenreeks.
    OutOfMemory,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax { at } => write!(f, "json syntax error at byte {at}"),
            Self::Type { at, want } => write!(f, "json: expected {want} at byte {at}"),
            Self::Depth => write!(f, "json nested deeper than {MAX_DEPTH}"),
            Self::Range { at } => write!(f, "json number out of range at byte {at}"),
            Self::OutOfMemory => f.write_str("json: out of memory"),
        }
    }
}

/// Het resultaat van deze module.
pub(crate) type Result<T = (), E = Error> = core::result::Result<T, E>;

/// Een tekenreeks zoals hij in de bron staat, tussen de aanhalingstekens.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Str<'a> {
    /// De ruwe bytes, escapes nog niet vertaald.
    raw: &'a [u8],
    /// Waar de reeks begint, voor een foutmelding.
    at: usize,
}

impl<'a> Str<'a> {
    /// De ruwe bytes als tekst, als er geen escape in staat.
    pub(crate) fn plain(&self) -> Option<&'a str> {
        if self.raw.contains(&b'\\') {
            return None;
        }
        core::str::from_utf8(self.raw).ok()
    }

    /// Roept `f` aan voor elk teken, met de escapes vertaald.
    fn chars(&self, mut f: impl FnMut(char) -> Result) -> Result {
        let s = core::str::from_utf8(self.raw).map_err(|_| Error::Syntax { at: self.at })?;
        let mut it = s.char_indices();
        while let Some((i, c)) = it.next() {
            if c != '\\' {
                f(c)?;
                continue;
            }
            let at = self.at + i;
            let e = it.next().map(|(_, e)| e).ok_or(Error::Syntax { at })?;
            let c = match e {
                '"' => '"',
                '\\' => '\\',
                '/' => '/',
                'b' => '\u{8}',
                'f' => '\u{c}',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'u' => unicode(&mut it, at)?,
                _ => return Err(Error::Syntax { at }),
            };
            f(c)?;
        }
        Ok(())
    }

    /// Of de reeks, vertaald, gelijk is aan `key`.
    pub(crate) fn is(&self, key: &str) -> bool {
        if let Some(p) = self.plain() {
            return p == key;
        }
        let mut want = key.chars();
        let same = self.chars(|c| {
            if want.next() == Some(c) {
                Ok(())
            } else {
                Err(Error::Syntax { at: 0 })
            }
        });
        same.is_ok() && want.next().is_none()
    }

    /// De vertaalde reeks, in een nieuwe `String`.
    pub(crate) fn decode(self) -> Result<String> {
        let mut out = String::new();
        out.try_reserve(self.raw.len())
            .map_err(|_| Error::OutOfMemory)?;
        self.chars(|c| {
            out.push(c);
            Ok(())
        })?;
        Ok(out)
    }
}

/// Leest de vier hexcijfers na `\u`, met een surrogaatpaar als dat volgt.
fn unicode(it: &mut core::str::CharIndices<'_>, at: usize) -> Result<char> {
    let hex4 = |it: &mut core::str::CharIndices<'_>| -> Result<u32> {
        let mut v = 0u32;
        for _ in 0..4 {
            let d = it
                .next()
                .and_then(|(_, c)| c.to_digit(16))
                .ok_or(Error::Syntax { at })?;
            v = (v << 4) | d;
        }
        Ok(v)
    };
    let hi = hex4(it)?;
    if !(0xd800..0xdc00).contains(&hi) {
        return char::from_u32(hi).ok_or(Error::Syntax { at });
    }
    // Een hoge surrogaat moet door een lage gevolgd worden.
    let (Some((_, '\\')), Some((_, 'u'))) = (it.next(), it.next()) else {
        return Err(Error::Syntax { at });
    };
    let lo = hex4(it)?;
    if !(0xdc00..0xe000).contains(&lo) {
        return Err(Error::Syntax { at });
    }
    char::from_u32(0x10000 + ((hi - 0xd800) << 10) + (lo - 0xdc00)).ok_or(Error::Syntax { at })
}

/// Een lezer over één JSON-document.
pub(crate) struct Reader<'a> {
    /// De bron.
    s: &'a [u8],
    /// De volgende ongelezen byte.
    i: usize,
    /// Hoe diep de lezer nu zit.
    depth: u8,
}

impl<'a> Reader<'a> {
    /// Een lezer aan het begin van `s`.
    pub(crate) fn new(s: &'a [u8]) -> Self {
        Self { s, i: 0, depth: 0 }
    }

    /// Slaat witruimte over en geeft de volgende byte zonder hem te lezen.
    fn peek(&mut self) -> Option<u8> {
        while let Some(&c) = self.s.get(self.i) {
            if !matches!(c, b' ' | b'\t' | b'\n' | b'\r') {
                return Some(c);
            }
            self.i += 1;
        }
        None
    }

    /// Eist byte `c` als volgende.
    fn expect(&mut self, c: u8) -> Result {
        if self.peek() == Some(c) {
            self.i += 1;
            Ok(())
        } else {
            Err(Error::Syntax { at: self.i })
        }
    }

    /// Een typefout op de huidige plek.
    fn want(&self, want: &'static str) -> Error {
        Error::Type { at: self.i, want }
    }

    /// Eén niveau dieper, met de grens.
    fn enter(&mut self) -> Result {
        if self.depth >= MAX_DEPTH {
            return Err(Error::Depth);
        }
        self.depth += 1;
        Ok(())
    }

    /// Of de volgende waarde `null` is; zo ja, dan is hij gelezen.
    pub(crate) fn null(&mut self) -> bool {
        if self.peek() == Some(b'n') && self.s.get(self.i..self.i + 4) == Some(b"null") {
            self.i += 4;
            return true;
        }
        false
    }

    /// Leest een object en roept `f` per veld aan; `f` moet de waarde lezen
    /// (of [`Reader::skip`] aanroepen).
    pub(crate) fn object(&mut self, mut f: impl FnMut(&mut Self, Str<'a>) -> Result) -> Result {
        if self.peek() != Some(b'{') {
            return Err(self.want("an object"));
        }
        self.i += 1;
        self.enter()?;
        if self.peek() == Some(b'}') {
            self.i += 1;
            self.depth -= 1;
            return Ok(());
        }
        loop {
            let key = self.string()?;
            self.expect(b':')?;
            f(self, key)?;
            match self.peek() {
                Some(b',') => self.i += 1,
                Some(b'}') => {
                    self.i += 1;
                    self.depth -= 1;
                    return Ok(());
                }
                _ => return Err(Error::Syntax { at: self.i }),
            }
        }
    }

    /// Leest een array en roept `f` per element aan; `f` moet het element
    /// lezen.
    pub(crate) fn array(&mut self, mut f: impl FnMut(&mut Self) -> Result) -> Result {
        if self.peek() != Some(b'[') {
            return Err(self.want("an array"));
        }
        self.i += 1;
        self.enter()?;
        if self.peek() == Some(b']') {
            self.i += 1;
            self.depth -= 1;
            return Ok(());
        }
        loop {
            f(self)?;
            match self.peek() {
                Some(b',') => self.i += 1,
                Some(b']') => {
                    self.i += 1;
                    self.depth -= 1;
                    return Ok(());
                }
                _ => return Err(Error::Syntax { at: self.i }),
            }
        }
    }

    /// Leest een tekenreeks.
    pub(crate) fn string(&mut self) -> Result<Str<'a>> {
        if self.peek() != Some(b'"') {
            return Err(self.want("a string"));
        }
        let start = self.i + 1;
        let mut i = start;
        loop {
            match self.s.get(i) {
                None => return Err(Error::Syntax { at: i }),
                Some(b'"') => break,
                Some(b'\\') => i += 2,
                Some(&c) if c < 0x20 => return Err(Error::Syntax { at: i }),
                Some(_) => i += 1,
            }
        }
        let raw = self.s.get(start..i).ok_or(Error::Syntax { at: i })?;
        self.i = i + 1;
        Ok(Str { raw, at: start })
    }

    /// Leest een geheel getal.
    pub(crate) fn int(&mut self) -> Result<i64> {
        if !matches!(self.peek(), Some(b'-' | b'0'..=b'9')) {
            return Err(self.want("an integer"));
        }
        let at = self.i;
        let neg = self.s.get(self.i) == Some(&b'-');
        if neg {
            self.i += 1;
        }
        let mut v: i64 = 0;
        let mut digits = 0;
        while let Some(&c @ b'0'..=b'9') = self.s.get(self.i) {
            v = v
                .checked_mul(10)
                .and_then(|v| v.checked_add(i64::from(c - b'0')))
                .ok_or(Error::Range { at })?;
            self.i += 1;
            digits += 1;
        }
        if digits == 0 || matches!(self.s.get(self.i), Some(b'.' | b'e' | b'E')) {
            return Err(Error::Type {
                at,
                want: "an integer",
            });
        }
        Ok(if neg { -v } else { v })
    }

    /// Slaat één waarde over, van elk type.
    pub(crate) fn skip(&mut self) -> Result {
        match self.peek() {
            Some(b'{') => self.object(|r, _| r.skip()),
            Some(b'[') => self.array(Self::skip),
            Some(b'"') => self.string().map(|_| ()),
            Some(b't') => self.word(b"true"),
            Some(b'f') => self.word(b"false"),
            Some(b'n') => self.word(b"null"),
            Some(b'-' | b'0'..=b'9') => {
                // Een getal in elke vorm; alleen de tekens tellen hier.
                while let Some(b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9') = self.s.get(self.i)
                {
                    self.i += 1;
                }
                Ok(())
            }
            _ => Err(Error::Syntax { at: self.i }),
        }
    }

    /// Leest een vast woord (`true`, `false`, `null`).
    fn word(&mut self, w: &[u8]) -> Result {
        if self.s.get(self.i..self.i + w.len()) == Some(w) {
            self.i += w.len();
            Ok(())
        } else {
            Err(Error::Syntax { at: self.i })
        }
    }

    /// Eist dat er na de waarde alleen witruimte staat.
    pub(crate) fn end(&mut self) -> Result {
        match self.peek() {
            None => Ok(()),
            Some(_) => Err(Error::Syntax { at: self.i }),
        }
    }

    /// De ruwe bytes van de volgende waarde, zonder hem te vertalen.
    pub(crate) fn raw(&mut self) -> Result<&'a [u8]> {
        self.peek();
        let start = self.i;
        self.skip()?;
        self.s.get(start..self.i).ok_or(Error::Syntax { at: start })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec::Vec;

    #[test]
    fn reads_an_object_with_unknown_fields() {
        let src = br#" {"version": 7, "config": {"ingress": [{"hostname": "a.nl", "service": "http://x:80"}, {"service": "http_status:404"}], "warp-routing": {"enabled": false}, "n": [1.5e3, -2, null, true]}} "#;
        let mut r = Reader::new(src);
        let mut version = 0;
        let mut services = Vec::new();
        r.object(|r, k| {
            if k.is("version") {
                version = r.int()?;
                return Ok(());
            }
            if !k.is("config") {
                return r.skip();
            }
            r.object(|r, k| {
                if !k.is("ingress") {
                    return r.skip();
                }
                r.array(|r| {
                    r.object(|r, k| {
                        if k.is("service") {
                            services.push(r.string()?.decode()?);
                            Ok(())
                        } else {
                            r.skip()
                        }
                    })
                })
            })
        })
        .unwrap();
        r.end().unwrap();
        assert_eq!(version, 7);
        assert_eq!(services, ["http://x:80", "http_status:404"]);
    }

    #[test]
    fn escapes_are_translated() {
        let mut r = Reader::new(br#""a\"b\\c\/d\n\u00e9\ud83d\ude00""#);
        let s = r.string().unwrap();
        assert_eq!(s.plain(), None);
        assert_eq!(s.decode().unwrap(), "a\"b\\c/d\n\u{e9}\u{1f600}");
        let mut r = Reader::new(br#""\u0073ervice""#);
        assert!(r.string().unwrap().is("service"));
    }

    #[test]
    fn refusals_carry_the_offset() {
        assert_eq!(
            Reader::new(b"{\"a\" 1}").object(|r, _| r.skip()),
            Err(Error::Syntax { at: 5 })
        );
        assert!(matches!(
            Reader::new(b"[1]").object(|r, _| r.skip()),
            Err(Error::Type { at: 0, .. })
        ));
        assert_eq!(
            Reader::new(b"99999999999999999999").int(),
            Err(Error::Range { at: 0 })
        );
        assert!(matches!(Reader::new(b"1.5").int(), Err(Error::Type { .. })));
        assert!(Reader::new(b"\"open").string().is_err());
        assert!(
            Reader::new(br#""\ud800x""#)
                .string()
                .unwrap()
                .decode()
                .is_err()
        );
        let deep: Vec<u8> = core::iter::repeat_n(b'[', 40).collect();
        assert_eq!(Reader::new(&deep).skip(), Err(Error::Depth));
        let mut r = Reader::new(b"{} x");
        r.skip().unwrap();
        assert_eq!(r.end(), Err(Error::Syntax { at: 3 }));
    }

    #[test]
    fn raw_gives_the_value_bytes() {
        let mut r = Reader::new(br#"{"config": {"a": [1, 2]}, "b": 1}"#);
        let mut got = None;
        r.object(|r, k| {
            if k.is("config") {
                got = Some(r.raw()?);
                Ok(())
            } else {
                r.skip()
            }
        })
        .unwrap();
        assert_eq!(got, Some(&br#"{"a": [1, 2]}"#[..]));
    }
}

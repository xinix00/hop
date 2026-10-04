//! Een klein JSON-waardetype met een begrensde parser en een schrijver.
//!
//! Geen serde: nul externe crates (handboek §8). Wat Hop aan JSON ziet is
//! klein en plat (een jobspec is kilobytes, een snapshot tientallen), dus
//! een boom van [`Value`]s is genoeg, en elke knoop wordt faalbaar
//! gealloceerd.
//!
//! De grenzen zijn de verdediging tegen invoer van buiten: [`MAX_INPUT`]
//! bytes en [`MAX_DEPTH`] niveaus. Een parser die recursief afdaalt zonder
//! diepte-grens is een stack-overflow die op een verzoek wacht.

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::Write as _;

use crate::{Error, Name, Result, TryClone, try_push, try_push_str};

/// De grootste invoer die de parser aanneemt: 1 MiB. Dat is de body-grens
/// van de HTTP-server (leanhttp, 1 MiB) en ruim boven een snapshot van
/// honderd jobs (gemeten 15-07: 127 jobs, 61 KB).
pub const MAX_INPUT: usize = 1 << 20;

/// De diepste nesting die de parser aanneemt. Een jobspec nest vier diep
/// (job, artifacts, artifact, headers); een snapshot vijf. 32 laat ruimte
/// voor wat nog komt en houdt de recursie op de stack klein.
pub const MAX_DEPTH: usize = 32;

/// Een JSON-getal, zonder verlies: gehele getallen blijven geheel.
///
/// `memory_limit` is een `u64` in bytes; via `f64` zou alles boven 2^53
/// stil afronden.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Number {
    /// Een negatief geheel getal.
    Int(i64),
    /// Een niet-negatief geheel getal.
    Uint(u64),
    /// Een getal met breuk of exponent, of te groot voor 64 bits.
    Float(f64),
}

impl Number {
    /// Het getal als `i64`, als het geheel is en past.
    pub fn as_i64(self) -> Option<i64> {
        match self {
            Self::Int(v) => Some(v),
            Self::Uint(v) => i64::try_from(v).ok(),
            Self::Float(_) => None,
        }
    }

    /// Het getal als `u64`, als het geheel en niet negatief is.
    pub fn as_u64(self) -> Option<u64> {
        match self {
            Self::Uint(v) => Some(v),
            Self::Int(_) | Self::Float(_) => None,
        }
    }

    /// Het getal als `f64` (met verlies boven 2^53).
    pub fn as_f64(self) -> f64 {
        match self {
            // Bewust met verlies: dit is de float-lezing van een getal.
            Self::Int(v) => v as f64,
            Self::Uint(v) => v as f64,
            Self::Float(v) => v,
        }
    }
}

/// Een JSON-object: sleutel-waardeparen in documentvolgorde.
///
/// De volgorde blijft staan zodat wat Hop schrijft er in de bucket uitziet
/// zoals de Go-versie het schreef (velden in struct-volgorde).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Object {
    pairs: Vec<(String, Value)>,
}

impl Object {
    /// Een leeg object.
    pub const fn new() -> Self {
        Self { pairs: Vec::new() }
    }

    /// De waarde bij `key`.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    /// Voegt een paar achteraan toe. De aanroeper zorgt dat de sleutel nieuw is.
    pub fn push(&mut self, key: &str, value: Value) -> Result {
        let key = crate::try_string(key)?;
        try_push(&mut self.pairs, (key, value))
    }

    /// Loopt de paren af in documentvolgorde.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.pairs.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Het aantal paren.
    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    /// Of het object leeg is.
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }
}

/// Een JSON-waarde.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Value {
    /// `null`.
    #[default]
    Null,
    /// `true` of `false`.
    Bool(bool),
    /// Een getal.
    Number(Number),
    /// Een string.
    String(String),
    /// Een array.
    Array(Vec<Value>),
    /// Een object.
    Object(Object),
}

impl Value {
    /// De string, als dit er een is.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    /// Het object, als dit er een is.
    pub fn as_object(&self) -> Option<&Object> {
        match self {
            Self::Object(o) => Some(o),
            _ => None,
        }
    }

    /// De array, als dit er een is.
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }

    /// De bool, als dit er een is.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Het getal als `i64`, als het geheel is en past.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Number(n) => n.as_i64(),
            _ => None,
        }
    }

    /// Het getal als `u64`, als het geheel en niet negatief is.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Number(n) => n.as_u64(),
            _ => None,
        }
    }

    /// Of dit `null` is.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Een string-waarde, faalbaar gekopieerd.
    pub fn string(s: &str) -> Result<Self> {
        Ok(Self::String(crate::try_string(s)?))
    }

    /// Een geheel getal.
    pub fn int(v: i64) -> Self {
        match u64::try_from(v) {
            Ok(u) => Self::Number(Number::Uint(u)),
            Err(_) => Self::Number(Number::Int(v)),
        }
    }

    /// Een niet-negatief geheel getal.
    pub fn uint(v: u64) -> Self {
        Self::Number(Number::Uint(v))
    }
}

impl TryClone for Object {
    fn try_clone(&self) -> Result<Self> {
        let mut pairs = Vec::new();
        pairs
            .try_reserve_exact(self.pairs.len())
            .map_err(|_| Error::OutOfMemory)?;
        for (k, v) in &self.pairs {
            pairs.push((k.try_clone()?, v.try_clone()?));
        }
        Ok(Self { pairs })
    }
}

impl TryClone for Value {
    fn try_clone(&self) -> Result<Self> {
        Ok(match self {
            Self::Null => Self::Null,
            Self::Bool(b) => Self::Bool(*b),
            Self::Number(n) => Self::Number(*n),
            Self::String(s) => Self::String(s.try_clone()?),
            Self::Array(a) => Self::Array(a.try_clone()?),
            Self::Object(o) => Self::Object(o.try_clone()?),
        })
    }
}

/// Leest één JSON-document uit `input`.
///
/// Weigert invoer boven [`MAX_INPUT`], nesting boven [`MAX_DEPTH`], dubbele
/// sleutels, ongeldige UTF-8 en alles wat na het document nog staat.
pub fn parse(input: &[u8]) -> Result<Value> {
    parse_with_limit(input, MAX_INPUT)
}

/// Parse met een expliciet bytebudget; alle structuurcontroles blijven gelden.
pub fn parse_with_limit(input: &[u8], limit: usize) -> Result<Value> {
    if input.len() > limit {
        return Err(Error::TooLarge {
            len: input.len(),
            max: limit,
        });
    }
    let mut p = Parser { b: input, pos: 0 };
    p.skip_ws();
    let v = p.value(0)?;
    p.skip_ws();
    if p.pos < p.b.len() {
        return Err(Error::Trailing { offset: p.pos });
    }
    Ok(v)
}

/// Leest één JSON-document uit een `&str`.
pub fn parse_str(input: &str) -> Result<Value> {
    parse(input.as_bytes())
}

struct Parser<'a> {
    b: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.b.get(self.pos).copied()
    }

    fn err(&self, expected: &'static str) -> Error {
        Error::Syntax {
            offset: self.pos,
            expected,
        }
    }

    fn skip_ws(&mut self) {
        while let Some(b' ' | b'\t' | b'\n' | b'\r') = self.peek() {
            self.pos += 1;
        }
    }

    fn eat(&mut self, c: u8, expected: &'static str) -> Result {
        if self.peek() == Some(c) {
            self.pos += 1;
            Ok(())
        } else {
            Err(self.err(expected))
        }
    }

    fn literal(&mut self, word: &'static [u8], v: Value) -> Result<Value> {
        let end = self.pos.saturating_add(word.len());
        if self.b.get(self.pos..end) == Some(word) {
            self.pos = end;
            Ok(v)
        } else {
            Err(self.err("a value"))
        }
    }

    fn value(&mut self, depth: usize) -> Result<Value> {
        if depth >= MAX_DEPTH {
            return Err(Error::TooDeep {
                offset: self.pos,
                max: MAX_DEPTH,
            });
        }
        match self.peek() {
            Some(b'{') => self.object(depth),
            Some(b'[') => self.array(depth),
            Some(b'"') => Ok(Value::String(self.string()?)),
            Some(b't') => self.literal(b"true", Value::Bool(true)),
            Some(b'f') => self.literal(b"false", Value::Bool(false)),
            Some(b'n') => self.literal(b"null", Value::Null),
            Some(b'-' | b'0'..=b'9') => Ok(Value::Number(self.number()?)),
            _ => Err(self.err("a value")),
        }
    }

    fn object(&mut self, depth: usize) -> Result<Value> {
        self.pos += 1;
        let mut obj = Object::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(Value::Object(obj));
        }
        loop {
            self.skip_ws();
            if self.peek() != Some(b'"') {
                return Err(self.err("a string key"));
            }
            let key = self.string()?;
            if obj.get(&key).is_some() {
                return Err(Error::DuplicateKey {
                    key: Name::new(&key),
                });
            }
            self.skip_ws();
            self.eat(b':', "':'")?;
            self.skip_ws();
            let v = self.value(depth + 1)?;
            try_push(&mut obj.pairs, (key, v))?;
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(Value::Object(obj));
                }
                _ => return Err(self.err("',' or '}'")),
            }
        }
    }

    fn array(&mut self, depth: usize) -> Result<Value> {
        self.pos += 1;
        let mut arr = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(Value::Array(arr));
        }
        loop {
            self.skip_ws();
            let v = self.value(depth + 1)?;
            try_push(&mut arr, v)?;
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    return Ok(Value::Array(arr));
                }
                _ => return Err(self.err("',' or ']'")),
            }
        }
    }

    fn hex4(&mut self) -> Result<u32> {
        let mut v = 0u32;
        for _ in 0..4 {
            let d = match self.peek() {
                Some(c @ b'0'..=b'9') => c - b'0',
                Some(c @ b'a'..=b'f') => c - b'a' + 10,
                Some(c @ b'A'..=b'F') => c - b'A' + 10,
                _ => return Err(self.err("four hex digits")),
            };
            v = (v << 4) | u32::from(d);
            self.pos += 1;
        }
        Ok(v)
    }

    fn escape(&mut self, out: &mut String) -> Result {
        let c = match self.peek() {
            Some(b'"') => '"',
            Some(b'\\') => '\\',
            Some(b'/') => '/',
            Some(b'b') => '\u{8}',
            Some(b'f') => '\u{c}',
            Some(b'n') => '\n',
            Some(b'r') => '\r',
            Some(b't') => '\t',
            Some(b'u') => {
                self.pos += 1;
                return self.unicode(out);
            }
            _ => return Err(self.err("an escape")),
        };
        self.pos += 1;
        push_char(out, c)
    }

    fn unicode(&mut self, out: &mut String) -> Result {
        let hi = self.hex4()?;
        let cp = if (0xD800..0xDC00).contains(&hi) {
            // Een surrogaatpaar: de tweede helft moet direct volgen.
            if self.b.get(self.pos..self.pos.saturating_add(2)) != Some(b"\\u") {
                return Err(self.err("a low surrogate"));
            }
            self.pos += 2;
            let lo = self.hex4()?;
            if !(0xDC00..0xE000).contains(&lo) {
                return Err(self.err("a low surrogate"));
            }
            0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
        } else {
            hi
        };
        // Een losse lage surrogaat is geen teken; Go maakt er U+FFFD van.
        let c = char::from_u32(cp).unwrap_or('\u{FFFD}');
        push_char(out, c)
    }

    fn string(&mut self) -> Result<String> {
        self.pos += 1;
        let mut out = String::new();
        loop {
            let start = self.pos;
            while let Some(c) = self.peek() {
                if c == b'"' || c == b'\\' || c < 0x20 {
                    break;
                }
                self.pos += 1;
            }
            let run = self.b.get(start..self.pos).unwrap_or_default();
            let run = core::str::from_utf8(run).map_err(|e| Error::Syntax {
                offset: start + e.valid_up_to(),
                expected: "UTF-8",
            })?;
            try_push_str(&mut out, run)?;
            match self.peek() {
                Some(b'"') => {
                    self.pos += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.pos += 1;
                    self.escape(&mut out)?;
                }
                _ => return Err(self.err("a closing '\"'")),
            }
        }
    }

    fn digits(&mut self) -> usize {
        let start = self.pos;
        while let Some(b'0'..=b'9') = self.peek() {
            self.pos += 1;
        }
        self.pos - start
    }

    fn number(&mut self) -> Result<Number> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        match self.peek() {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => {
                self.digits();
            }
            _ => return Err(self.err("a digit")),
        }
        let mut float = false;
        if self.peek() == Some(b'.') {
            self.pos += 1;
            float = true;
            if self.digits() == 0 {
                return Err(self.err("a digit"));
            }
        }
        if let Some(b'e' | b'E') = self.peek() {
            self.pos += 1;
            float = true;
            if let Some(b'+' | b'-') = self.peek() {
                self.pos += 1;
            }
            if self.digits() == 0 {
                return Err(self.err("a digit"));
            }
        }
        let text = self.b.get(start..self.pos).unwrap_or_default();
        // Alleen ASCII-cijfers en tekens gezien, dus dit is geldige UTF-8.
        let text = core::str::from_utf8(text).map_err(|_| self.err("a number"))?;
        if !float {
            if let Ok(v) = text.parse::<u64>() {
                return Ok(Number::Uint(v));
            }
            if let Ok(v) = text.parse::<i64>() {
                return Ok(Number::Int(v));
            }
        }
        text.parse::<f64>()
            .map(Number::Float)
            .map_err(|_| Error::Syntax {
                offset: start,
                expected: "a number",
            })
    }
}

fn push_char(out: &mut String, c: char) -> Result {
    out.try_reserve(c.len_utf8())
        .map_err(|_| Error::OutOfMemory)?;
    out.push(c);
    Ok(())
}

/// Schrijft `v` compact (zonder witruimte) achter aan `out`.
pub fn write(v: &Value, out: &mut String) -> Result {
    match v {
        Value::Null => try_push_str(out, "null"),
        Value::Bool(true) => try_push_str(out, "true"),
        Value::Bool(false) => try_push_str(out, "false"),
        Value::Number(n) => write_number(*n, out),
        Value::String(s) => write_string(s, out),
        Value::Array(a) => {
            try_push_str(out, "[")?;
            for (i, item) in a.iter().enumerate() {
                if i > 0 {
                    try_push_str(out, ",")?;
                }
                write(item, out)?;
            }
            try_push_str(out, "]")
        }
        Value::Object(o) => {
            try_push_str(out, "{")?;
            for (i, (k, item)) in o.iter().enumerate() {
                if i > 0 {
                    try_push_str(out, ",")?;
                }
                write_string(k, out)?;
                try_push_str(out, ":")?;
                write(item, out)?;
            }
            try_push_str(out, "}")
        }
    }
}

/// Schrijft `v` naar een nieuwe `String`.
pub fn to_string(v: &Value) -> Result<String> {
    let mut out = String::new();
    write(v, &mut out)?;
    Ok(out)
}

/// Een `fmt::Write` die faalbaar alloceert; `core::fmt` meldt alleen dát
/// het misging, dus de vlag onthoudt of het de heap was.
struct Sink<'a> {
    out: &'a mut String,
    oom: bool,
}

impl core::fmt::Write for Sink<'_> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        if try_push_str(self.out, s).is_err() {
            self.oom = true;
            return Err(core::fmt::Error);
        }
        Ok(())
    }
}

fn write_number(n: Number, out: &mut String) -> Result {
    let mut sink = Sink { out, oom: false };
    let r = match n {
        Number::Int(v) => write!(sink, "{v}"),
        Number::Uint(v) => write!(sink, "{v}"),
        Number::Float(v) if !v.is_finite() => return Err(Error::NotFinite),
        // Display geeft de kortste decimale vorm die terug naar dezelfde
        // f64 leest; dat is ook wat Go schrijft voor gewone groottes.
        Number::Float(v) => write!(sink, "{v}"),
    };
    match r {
        Ok(()) => Ok(()),
        Err(_) => Err(Error::OutOfMemory),
    }
}

/// Schrijft `s` als JSON-string, met de escapes die Go ook schrijft.
///
/// Go ontsnapt `<`, `>` en `&` standaard als `<` enzovoort (HTML-veilig);
/// dat doen we ook, zodat een snapshot byte voor byte vergelijkbaar blijft.
pub fn write_string(s: &str, out: &mut String) -> Result {
    try_push_str(out, "\"")?;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        let esc: Option<&str> = match c {
            '"' => Some("\\\""),
            '\\' => Some("\\\\"),
            '\n' => Some("\\n"),
            '\r' => Some("\\r"),
            '\t' => Some("\\t"),
            _ => None,
        };
        let needs_hex = matches!(
            c,
            '\u{0}'..='\u{1f}' | '<' | '>' | '&' | '\u{2028}' | '\u{2029}'
        );
        if esc.is_none() && !needs_hex {
            continue;
        }
        try_push_str(out, s.get(start..i).unwrap_or_default())?;
        match esc {
            Some(e) => try_push_str(out, e)?,
            None => {
                let mut sink = Sink { out, oom: false };
                if write!(sink, "\\u{:04x}", u32::from(c)).is_err() {
                    return Err(Error::OutOfMemory);
                }
            }
        }
        start = i + c.len_utf8();
    }
    try_push_str(out, s.get(start..).unwrap_or_default())?;
    try_push_str(out, "\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;

    fn roundtrip(s: &str) -> String {
        to_string(&parse_str(s).unwrap()).unwrap()
    }

    #[test]
    fn parses_scalars() {
        assert_eq!(parse_str("null").unwrap(), Value::Null);
        assert_eq!(parse_str(" true ").unwrap(), Value::Bool(true));
        assert_eq!(parse_str("-12").unwrap().as_i64(), Some(-12));
        assert_eq!(
            parse_str("18446744073709551615").unwrap().as_u64(),
            Some(u64::MAX)
        );
        assert_eq!(
            parse_str("1.5e3").unwrap(),
            Value::Number(Number::Float(1500.0))
        );
    }

    #[test]
    fn keeps_large_integers_exact() {
        // 2^53 + 1 is het eerste gehele getal dat een f64 niet kan dragen.
        let v = parse_str("9007199254740993").unwrap();
        assert_eq!(v.as_u64(), Some(9_007_199_254_740_993));
    }

    #[test]
    fn roundtrips_documents() {
        assert_eq!(
            roundtrip(r#"{"a":[1,2,{"b":null}],"c":"x"}"#),
            r#"{"a":[1,2,{"b":null}],"c":"x"}"#
        );
        assert_eq!(roundtrip(" [ ] "), "[]");
        assert_eq!(roundtrip("{}"), "{}");
        assert_eq!(roundtrip("-0.5"), "-0.5");
    }

    #[test]
    fn decodes_escapes_and_surrogates() {
        let v = parse_str(r#""a\"b\\c\/\n\u00e9\ud83d\ude00""#).unwrap();
        assert_eq!(v.as_str(), Some("a\"b\\c/\n\u{e9}\u{1f600}"));
        assert!(parse_str(r#""\ud83d""#).is_err());
        assert!(parse_str(r#""\x""#).is_err());
    }

    #[test]
    fn writes_go_compatible_escapes() {
        let s = to_string(&Value::string("<a&b>\n\u{1}").unwrap()).unwrap();
        assert_eq!(s, r#""\u003ca\u0026b\u003e\n\u0001""#);
    }

    #[test]
    fn rejects_bad_input() {
        for bad in [
            "",
            "{",
            "[1,]",
            "{\"a\":1,}",
            "01",
            "1.",
            "-",
            "tru",
            "\"abc",
            "{\"a\" 1}",
            "[1 2]",
            "\"\u{1}\"",
            "{1:2}",
            "nul",
        ] {
            assert!(parse_str(bad).is_err(), "accepted {bad:?}");
        }
    }

    #[test]
    fn rejects_trailing_document() {
        assert_eq!(
            parse_str(r#"{"a":1} {"a":2}"#),
            Err(Error::Trailing { offset: 8 })
        );
    }

    #[test]
    fn rejects_duplicate_keys() {
        let err = parse_str(r#"{"a":1,"a":2}"#).unwrap_err();
        assert_eq!(err.to_string(), "duplicate key \"a\"");
    }

    #[test]
    fn rejects_invalid_utf8() {
        assert!(matches!(
            parse(b"\"\xff\""),
            Err(Error::Syntax {
                expected: "UTF-8",
                ..
            })
        ));
    }

    #[test]
    fn bounds_depth() {
        let deep: String = "[".repeat(MAX_DEPTH + 1) + &"]".repeat(MAX_DEPTH + 1);
        assert!(matches!(parse_str(&deep), Err(Error::TooDeep { .. })));
        let ok: String = "[".repeat(MAX_DEPTH) + &"]".repeat(MAX_DEPTH);
        assert!(parse_str(&ok).is_ok());
    }

    #[test]
    fn bounds_size() {
        let big = alloc::vec![b' '; MAX_INPUT + 1];
        assert_eq!(
            parse(&big),
            Err(Error::TooLarge {
                len: MAX_INPUT + 1,
                max: MAX_INPUT
            })
        );
    }

    #[test]
    fn explicit_budget_keeps_the_default_and_structural_limits() {
        let mut big = alloc::vec![b' '; MAX_INPUT];
        big.extend_from_slice(b"null");
        assert!(matches!(parse(&big), Err(Error::TooLarge { .. })));
        assert_eq!(parse_with_limit(&big, big.len()), Ok(Value::Null));
        assert!(matches!(
            parse_with_limit(&big, big.len() - 1),
            Err(Error::TooLarge { .. })
        ));
        for bad in [
            b"{\"a\":1,\"a\":2}".as_slice(),
            b"\"\xff\"",
            b"null trailing",
        ] {
            assert!(parse_with_limit(bad, MAX_INPUT * 2).is_err());
        }
        let deep = "[".repeat(MAX_DEPTH + 1) + &"]".repeat(MAX_DEPTH + 1);
        assert!(matches!(
            parse_with_limit(deep.as_bytes(), MAX_INPUT * 2),
            Err(Error::TooDeep { .. })
        ));
    }

    #[test]
    fn refuses_non_finite_numbers() {
        let v = Value::Number(Number::Float(f64::NAN));
        assert_eq!(to_string(&v), Err(Error::NotFinite));
    }

    #[test]
    fn object_keeps_document_order() {
        let v = parse_str(r#"{"z":1,"a":2}"#).unwrap();
        let keys: Vec<_> = v
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, _)| k.to_string())
            .collect();
        assert_eq!(keys, ["z", "a"]);
    }
}

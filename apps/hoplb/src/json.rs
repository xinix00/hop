//! Eén stringveld uit een plat JSON-object: genoeg voor de data van een
//! SSE-gebeurtenis (`{"name":"api"}`, `{"job":"api","event":"started"}`).
//!
//! Waarom geen volledige parser: de watcher leest per gebeurtenis twee
//! velden, en de lijsten van de agent komen via hoplib, dat hop's eigen
//! parser draagt. Dit is dezelfde strengheid als Go's `json.Unmarshal`: een
//! object dat niet helemaal geldig is, geeft geen enkel veld.

use alloc::borrow::Cow;
use alloc::string::String;

/// Hoe diep een waarde mag nesten voor hij onleesbaar heet.
const MAX_DEPTH: usize = 32;

/// De waarde van het stringveld `key` op het bovenste niveau van het object
/// `s`; `None` als `s` geen geldig object is, het veld ontbreekt of geen
/// string is. Bij dubbele sleutels wint de laatste, zoals in Go.
pub(crate) fn string_field<'a>(s: &'a str, key: &str) -> Option<Cow<'a, str>> {
    let mut p = Parser {
        s: s.as_bytes(),
        src: s,
        i: 0,
    };
    p.ws();
    p.eat(b'{')?;
    let mut found = None;
    p.ws();
    if p.peek() == Some(b'}') {
        p.i += 1;
    } else {
        loop {
            p.ws();
            let k = p.string()?;
            p.ws();
            p.eat(b':')?;
            p.ws();
            if k == key && p.peek() == Some(b'"') {
                found = Some(p.string()?);
            } else {
                p.value(0)?;
            }
            p.ws();
            match p.next()? {
                b',' => continue,
                b'}' => break,
                _ => return None,
            }
        }
    }
    p.ws();
    if p.i != p.s.len() {
        return None;
    }
    found
}

struct Parser<'a> {
    s: &'a [u8],
    src: &'a str,
    i: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let b = self.peek()?;
        self.i += 1;
        Some(b)
    }

    fn eat(&mut self, want: u8) -> Option<()> {
        (self.next()? == want).then_some(())
    }

    fn ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.i += 1;
        }
    }

    fn value(&mut self, depth: usize) -> Option<()> {
        if depth > MAX_DEPTH {
            return None;
        }
        match self.peek()? {
            b'"' => self.string().map(drop),
            b'{' => self.container(b'}', depth, true),
            b'[' => self.container(b']', depth, false),
            b't' => self.literal("true"),
            b'f' => self.literal("false"),
            b'n' => self.literal("null"),
            b'-' | b'0'..=b'9' => self.number(),
            _ => None,
        }
    }

    fn container(&mut self, close: u8, depth: usize, object: bool) -> Option<()> {
        self.i += 1;
        self.ws();
        if self.peek() == Some(close) {
            self.i += 1;
            return Some(());
        }
        loop {
            self.ws();
            if object {
                self.string()?;
                self.ws();
                self.eat(b':')?;
                self.ws();
            }
            self.value(depth + 1)?;
            self.ws();
            match self.next()? {
                b',' => {}
                c if c == close => return Some(()),
                _ => return None,
            }
        }
    }

    fn literal(&mut self, word: &str) -> Option<()> {
        let end = self.i.checked_add(word.len())?;
        (self.s.get(self.i..end)? == word.as_bytes()).then(|| self.i = end)
    }

    fn number(&mut self) -> Option<()> {
        let start = self.i;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        match self.next()? {
            b'0' => {}
            b'1'..=b'9' => self.digits(),
            _ => return None,
        }
        if self.peek() == Some(b'.') {
            self.i += 1;
            self.digit()?;
            self.digits();
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.i += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.i += 1;
            }
            self.digit()?;
            self.digits();
        }
        (self.i > start).then_some(())
    }

    fn digit(&mut self) -> Option<()> {
        self.next().filter(u8::is_ascii_digit).map(drop)
    }

    fn digits(&mut self) {
        while self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.i += 1;
        }
    }

    /// Een string; geleend als er geen escapes in staan.
    fn string(&mut self) -> Option<Cow<'a, str>> {
        self.eat(b'"')?;
        let start = self.i;
        loop {
            match self.next()? {
                b'"' => return self.src.get(start..self.i - 1).map(Cow::Borrowed),
                b'\\' => {
                    self.i = start;
                    return self.escaped().map(Cow::Owned);
                }
                c if c < 0x20 => return None,
                _ => {}
            }
        }
    }

    /// Een string met escapes, vanaf het eerste teken na de `"`.
    fn escaped(&mut self) -> Option<String> {
        let mut out = String::new();
        loop {
            let rest = self.src.get(self.i..)?;
            let c = rest.chars().next()?;
            self.i += c.len_utf8();
            match c {
                '"' => return Some(out),
                '\\' => {
                    let e = self.next()?;
                    let ch = match e {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{8}',
                        b'f' => '\u{c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => self.unicode()?,
                        _ => return None,
                    };
                    out.try_reserve(4).ok()?;
                    out.push(ch);
                }
                c if (c as u32) < 0x20 => return None,
                c => {
                    out.try_reserve(4).ok()?;
                    out.push(c);
                }
            }
        }
    }

    fn hex4(&mut self) -> Option<u32> {
        let end = self.i.checked_add(4)?;
        let h = self.src.get(self.i..end)?;
        self.i = end;
        u32::from_str_radix(h, 16).ok()
    }

    fn unicode(&mut self) -> Option<char> {
        let hi = self.hex4()?;
        if (0xD800..0xDC00).contains(&hi) {
            // Een surrogaatpaar; een losse helft wordt U+FFFD, zoals Go.
            if self.s.get(self.i..self.i + 2) == Some(b"\\u") {
                self.i += 2;
                let lo = self.hex4()?;
                if (0xDC00..0xE000).contains(&lo) {
                    let c = 0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00);
                    return char::from_u32(c);
                }
            }
            return Some('\u{FFFD}');
        }
        Some(char::from_u32(hi).unwrap_or('\u{FFFD}'))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_field_of_a_flat_object() {
        assert_eq!(
            string_field(r#"{"name":"api"}"#, "name").as_deref(),
            Some("api")
        );
        assert_eq!(
            string_field(
                r#" { "job" : "a\"bé" , "event":"x", "n": [1, {"a": null}], "f": -1.5e3 } "#,
                "job"
            )
            .as_deref(),
            Some("a\"b\u{e9}")
        );
        assert_eq!(string_field(r#"{"job":1}"#, "job"), None);
        assert_eq!(string_field(r#"{}"#, "job"), None);
        assert_eq!(string_field("not json", "job"), None);
        assert_eq!(string_field(r#"{"job":"a"} x"#, "job"), None);
        assert_eq!(string_field(r#"{"job":"a",}"#, "job"), None);
        assert_eq!(
            string_field(r#"{"job":"a","job":"b"}"#, "job").as_deref(),
            Some("b")
        );
        assert_eq!(
            string_field(r#"{"x":"😀","job":"😀"}"#, "job").as_deref(),
            Some("\u{1F600}")
        );
    }
}

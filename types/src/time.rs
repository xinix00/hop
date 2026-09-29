//! Tijd zonder klok: duren in nanoseconden en tijdstippen als Unix-nanoseconden.
//!
//! Deze crate leest nooit een klok; wie "nu" nodig heeft krijgt het als
//! parameter. Hier staan alleen de rekenregels en de tekstvormen die de
//! Go-generatie op de draad zette: duren als Go ze schrijft (`"1m30s"`) en
//! tijdstippen als RFC 3339 (`"2026-09-29T10:00:00.5Z"`).

use alloc::string::String;
use core::fmt::Write as _;

use crate::{Error, Name, Result};

/// Een duur of tijdstip in nanoseconden.
pub type Nanos = u64;

/// Eén microseconde in nanoseconden.
pub const MICROSECOND: Nanos = 1_000;
/// Eén milliseconde in nanoseconden.
pub const MILLISECOND: Nanos = 1_000_000;
/// Eén seconde in nanoseconden.
pub const SECOND: Nanos = 1_000_000_000;
/// Eén minuut in nanoseconden.
pub const MINUTE: Nanos = 60 * SECOND;
/// Eén uur in nanoseconden.
pub const HOUR: Nanos = 60 * MINUTE;

/// Een tijdstip in nanoseconden sinds 1970-01-01T00:00:00Z; 0 is "geen tijd".
///
/// Go's nul-tijd (`0001-01-01T00:00:00Z`) en het Unix-begin vallen hier
/// samen op 0: Hop heeft nooit iets vóór 1970 te melden, en "niet gezet"
/// moet een waarde hebben.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct Time(pub Nanos);

impl Time {
    /// Het "niet gezet"-tijdstip.
    pub const ZERO: Self = Self(0);

    /// Of dit het "niet gezet"-tijdstip is.
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Hoeveel nanoseconden `self` na `earlier` ligt; 0 als het ervoor ligt.
    pub fn since(self, earlier: Self) -> Nanos {
        self.0.saturating_sub(earlier.0)
    }

    /// Schrijft RFC 3339 met zoveel breukcijfers als nodig (Go's `RFC3339Nano`).
    pub fn write_rfc3339(self, out: &mut String) -> Result {
        if self.is_zero() {
            return crate::try_push_str(out, "0001-01-01T00:00:00Z");
        }
        let secs = self.0 / SECOND;
        let frac = self.0 % SECOND;
        let days = secs / 86_400;
        let rem = secs % 86_400;
        let (y, m, d) = civil_from_days(days);
        let mut buf = FixedBuf::default();
        let r = write!(
            buf,
            "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}",
            rem / 3600,
            (rem / 60) % 60,
            rem % 60
        );
        if r.is_err() {
            return Err(Error::OutOfMemory);
        }
        if frac > 0 {
            let mut digits = [0u8; 9];
            let mut f = frac;
            for slot in digits.iter_mut().rev() {
                *slot = b'0' + (f % 10) as u8;
                f /= 10;
            }
            let mut n = 9;
            while n > 0 && digits.get(n - 1) == Some(&b'0') {
                n -= 1;
            }
            buf.push(b'.');
            for &c in digits.get(..n).unwrap_or_default() {
                buf.push(c);
            }
        }
        buf.push(b'Z');
        crate::try_push_str(out, buf.as_str())
    }

    /// Leest RFC 3339 (met `Z` of een offset als `+02:00`).
    pub fn parse_rfc3339(s: &str) -> Result<Self> {
        let bad = || Error::Invalid {
            field: Name::new("time"),
            why: "not an RFC 3339 timestamp",
        };
        let b = s.as_bytes();
        let num = |from: usize, len: usize| -> Result<u64> {
            let part = b.get(from..from + len).ok_or_else(bad)?;
            let mut v = 0u64;
            for &c in part {
                if !c.is_ascii_digit() {
                    return Err(bad());
                }
                v = v * 10 + u64::from(c - b'0');
            }
            Ok(v)
        };
        if b.len() < 20 || b.get(4) != Some(&b'-') || b.get(7) != Some(&b'-') {
            return Err(bad());
        }
        if !matches!(b.get(10), Some(b'T' | b't'))
            || b.get(13) != Some(&b':')
            || b.get(16) != Some(&b':')
        {
            return Err(bad());
        }
        let (y, mo, d) = (num(0, 4)?, num(5, 2)?, num(8, 2)?);
        let (h, mi, sec) = (num(11, 2)?, num(14, 2)?, num(17, 2)?);
        if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
            return Err(bad());
        }
        let mut i = 19;
        let mut frac = 0u64;
        if b.get(i) == Some(&b'.') {
            i += 1;
            let mut n = 0;
            while let Some(c) = b.get(i).filter(|c| c.is_ascii_digit()) {
                if n < 9 {
                    frac = frac * 10 + u64::from(c - b'0');
                    n += 1;
                }
                i += 1;
            }
            if n == 0 {
                return Err(bad());
            }
            for _ in n..9 {
                frac *= 10;
            }
        }
        let offset: i64 = match b.get(i) {
            Some(b'Z' | b'z') if b.len() == i + 1 => 0,
            Some(sign @ (b'+' | b'-')) if b.len() == i + 6 && b.get(i + 3) == Some(&b':') => {
                let o = (num(i + 1, 2)? * 3600 + num(i + 4, 2)? * 60) as i64;
                if *sign == b'+' { o } else { -o }
            }
            _ => return Err(bad()),
        };
        if y < 1970 {
            // Go's nul-tijd en alles ervoor: "niet gezet".
            return Ok(Self::ZERO);
        }
        let days = days_from_civil(y, mo, d);
        let secs = (days * 86_400 + h * 3600 + mi * 60 + sec) as i64 - offset;
        let secs = u64::try_from(secs).map_err(|_| bad())?;
        let ns = secs
            .checked_mul(SECOND)
            .and_then(|v| v.checked_add(frac))
            .ok_or_else(bad)?;
        Ok(Self(ns))
    }
}

/// Een kleine stackbuffer voor een tijdstempel (maximaal 30 tekens).
struct FixedBuf {
    buf: [u8; 40],
    len: usize,
}

impl Default for FixedBuf {
    fn default() -> Self {
        Self {
            buf: [0; 40],
            len: 0,
        }
    }
}

impl FixedBuf {
    fn push(&mut self, c: u8) {
        if let Some(slot) = self.buf.get_mut(self.len) {
            *slot = c;
            self.len += 1;
        }
    }

    fn as_str(&self) -> &str {
        core::str::from_utf8(self.buf.get(..self.len).unwrap_or_default()).unwrap_or_default()
    }
}

impl core::fmt::Write for FixedBuf {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        if self.len + s.len() > self.buf.len() {
            return Err(core::fmt::Error);
        }
        for &c in s.as_bytes() {
            self.push(c);
        }
        Ok(())
    }
}

/// Dagen sinds 1970-01-01 naar (jaar, maand, dag); Howard Hinnant's algoritme.
fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + u64::from(m <= 2);
    (y, m, d)
}

/// (jaar, maand, dag) naar dagen sinds 1970-01-01; alleen voor jaar >= 1970.
fn days_from_civil(y: u64, m: u64, d: u64) -> u64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe).saturating_sub(719_468)
}

/// Een fout bij het lezen van een duur.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurationError {
    /// De tekst is geen duur (lege tekst, onbekende eenheid, geen eenheid).
    Invalid,
    /// De duur is negatief.
    Negative,
    /// De duur past niet in 64 bits nanoseconden.
    Overflow,
}

/// Leest een duur zoals Go's `time.ParseDuration`: `"30s"`, `"1m30s"`,
/// `"1.5h"`, `"500ms"`, `"0"`.
///
/// Een getal zonder eenheid (behalve `"0"`) is een fout, net als in Go: een
/// `leader_lease` van `"30"` die stil 30 ns wordt, verliest zijn leider
/// duizenden keren per seconde.
pub fn parse_duration(s: &str) -> core::result::Result<Nanos, DurationError> {
    let mut rest = s;
    let neg = rest.starts_with('-');
    if neg || rest.starts_with('+') {
        rest = rest.get(1..).unwrap_or_default();
    }
    if rest == "0" {
        return Ok(0);
    }
    if rest.is_empty() {
        return Err(DurationError::Invalid);
    }
    let mut total: u128 = 0;
    while !rest.is_empty() {
        let int_len = rest.bytes().take_while(u8::is_ascii_digit).count();
        let (int_part, after) = rest.split_at(int_len);
        let (frac_part, after) = match after.strip_prefix('.') {
            Some(a) => {
                let n = a.bytes().take_while(u8::is_ascii_digit).count();
                a.split_at(n)
            }
            None => ("", after),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return Err(DurationError::Invalid);
        }
        let unit_len = after
            .bytes()
            .take_while(|c| !c.is_ascii_digit() && *c != b'.')
            .count();
        let (unit, after) = after.split_at(unit_len);
        let scale: u128 = match unit {
            "ns" => 1,
            "us" | "\u{b5}s" | "\u{3bc}s" => u128::from(MICROSECOND),
            "ms" => u128::from(MILLISECOND),
            "s" => u128::from(SECOND),
            "m" => u128::from(MINUTE),
            "h" => u128::from(HOUR),
            _ => return Err(DurationError::Invalid),
        };
        let mut v: u128 = 0;
        for c in int_part.bytes() {
            v = v
                .checked_mul(10)
                .and_then(|v| v.checked_add(u128::from(c - b'0')))
                .ok_or(DurationError::Overflow)?;
        }
        let mut part = v.checked_mul(scale).ok_or(DurationError::Overflow)?;
        let mut div: u128 = 1;
        let mut frac: u128 = 0;
        for c in frac_part.bytes().take(18) {
            frac = frac * 10 + u128::from(c - b'0');
            div *= 10;
        }
        part += frac * scale / div;
        total = total.checked_add(part).ok_or(DurationError::Overflow)?;
        rest = after;
    }
    if total > u128::from(i64::MAX as u64) {
        return Err(DurationError::Overflow);
    }
    if neg && total > 0 {
        return Err(DurationError::Negative);
    }
    u64::try_from(total).map_err(|_| DurationError::Overflow)
}

/// Schrijft een duur zoals Go's `Duration.String`: `"1m30s"`, `"2.5s"`,
/// `"500ms"`, `"0s"`.
pub fn write_duration(d: Nanos, out: &mut String) -> Result {
    let mut buf = FixedBuf::default();
    let r = if d == 0 {
        write!(buf, "0s")
    } else if d < MICROSECOND {
        write!(buf, "{d}ns")
    } else if d < MILLISECOND {
        write_frac(&mut buf, d, MICROSECOND, "\u{b5}s")
    } else if d < SECOND {
        write_frac(&mut buf, d, MILLISECOND, "ms")
    } else {
        let h = d / HOUR;
        let m = (d / MINUTE) % 60;
        let s_ns = d % MINUTE;
        let mut r = Ok(());
        if h > 0 {
            r = r.and(write!(buf, "{h}h"));
        }
        if h > 0 || m > 0 {
            r = r.and(write!(buf, "{m}m"));
        }
        r.and(write_frac(&mut buf, s_ns, SECOND, "s"))
    };
    if r.is_err() {
        return Err(Error::OutOfMemory);
    }
    crate::try_push_str(out, buf.as_str())
}

fn write_frac(buf: &mut FixedBuf, v: Nanos, unit: Nanos, suffix: &str) -> core::fmt::Result {
    let int = v / unit;
    let mut frac = v % unit;
    write!(buf, "{int}")?;
    if frac > 0 {
        let mut width = 0;
        let mut u = unit;
        while u > 1 {
            u /= 10;
            width += 1;
        }
        while frac.is_multiple_of(10) {
            frac /= 10;
            width -= 1;
        }
        write!(buf, ".{frac:0width$}")?;
    }
    write!(buf, "{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dur(d: Nanos) -> String {
        let mut s = String::new();
        write_duration(d, &mut s).unwrap();
        s
    }

    #[test]
    fn duration_parses_like_go() {
        assert_eq!(parse_duration("30s"), Ok(30 * SECOND));
        assert_eq!(parse_duration("1m30s"), Ok(90 * SECOND));
        assert_eq!(parse_duration("1500ms"), Ok(1500 * MILLISECOND));
        assert_eq!(parse_duration("1.5h"), Ok(90 * MINUTE));
        assert_eq!(parse_duration("0s"), Ok(0));
        assert_eq!(parse_duration("0"), Ok(0));
        assert_eq!(parse_duration("2us"), Ok(2 * MICROSECOND));
        assert_eq!(parse_duration("30"), Err(DurationError::Invalid));
        assert_eq!(parse_duration(""), Err(DurationError::Invalid));
        assert_eq!(parse_duration("5x"), Err(DurationError::Invalid));
        assert_eq!(parse_duration("-30s"), Err(DurationError::Negative));
    }

    #[test]
    fn duration_formats_like_go() {
        assert_eq!(dur(0), "0s");
        assert_eq!(dur(90 * SECOND), "1m30s");
        assert_eq!(dur(2500 * MILLISECOND), "2.5s");
        assert_eq!(dur(500 * MILLISECOND), "500ms");
        assert_eq!(dur(1500 * MICROSECOND), "1.5ms");
        assert_eq!(dur(2 * HOUR), "2h0m0s");
        assert_eq!(dur(7), "7ns");
        for d in [SECOND, 90 * SECOND, 2500 * MILLISECOND, 3 * HOUR + 1] {
            assert_eq!(parse_duration(&dur(d)), Ok(d));
        }
    }

    #[test]
    fn rfc3339_roundtrips() {
        let t = Time::parse_rfc3339("2026-09-29T10:11:12.5Z").unwrap();
        let mut s = String::new();
        t.write_rfc3339(&mut s).unwrap();
        assert_eq!(s, "2026-09-29T10:11:12.5Z");
        let off = Time::parse_rfc3339("2026-09-29T12:11:12.5+02:00").unwrap();
        assert_eq!(off, t);
        assert_eq!(Time::parse_rfc3339("0001-01-01T00:00:00Z"), Ok(Time::ZERO));
        assert!(Time::parse_rfc3339("gisteren").is_err());
        let mut z = String::new();
        Time::ZERO.write_rfc3339(&mut z).unwrap();
        assert_eq!(z, "0001-01-01T00:00:00Z");
    }

    #[test]
    fn civil_dates_match_known_days() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_723), (2024, 1, 1));
        assert_eq!(days_from_civil(2024, 2, 29), 19_782);
    }
}

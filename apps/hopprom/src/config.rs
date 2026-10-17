//! De instellingen van de bewoner uit zijn env, puur en op de host getoetst.
//!
//! Bezit de standaarden en het lezen ervan: `HOPPROM_AGENT` (met de naam
//! `HOP` voor Hop zelf), `HOPPROM_INTERVAL` en `ER_PORT_METRICS`. De
//! bewoner leest de env en logt een weigering; hier staat alleen de regel.

use alloc::string::String;
use core::time::Duration;

use crate::{Error, Result};

/// De agent zonder `HOPPROM_AGENT`: de leader-API van Hop op dezelfde node.
pub const DEFAULT_AGENT: &str = "http://HOP:8080";

/// De hostnaam die voor Hop zelf staat. Hop is altijd de eerste bewoner
/// (slot 1, PORT.md beslissing 1), en het slot-LAN is deterministisch, dus
/// zijn adres is bekend zonder DNS; een A-vraag voor `HOP` zou naar de
/// resolver van het LAN gaan en daar niets vinden.
pub const HOP_HOST: &str = "HOP";

/// Het poll-interval zonder `HOPPROM_INTERVAL`.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);

/// De poort zonder `ER_PORT_METRICS` (een image buiten Hop om).
pub const DEFAULT_PORT: u16 = 9090;

/// Vervangt de host `HOP` (hoofdletterongevoelig) in `url` door `hop_ip`.
///
/// `url` is een `http://`-URL of `host:poort`; al het andere blijft staan.
pub fn with_hop_address(url: &str, hop_ip: [u8; 4]) -> Result<String> {
    let (scheme, rest) = match url.find("://") {
        Some(i) => url.split_at(i + 3),
        None => ("", url),
    };
    let end = rest.find([':', '/']).unwrap_or(rest.len());
    let (host, tail) = rest.split_at(end);
    let mut out = String::new();
    out.try_reserve(url.len() + 15)
        .map_err(|_| Error::OutOfMemory)?;
    out.push_str(scheme);
    if host.eq_ignore_ascii_case(HOP_HOST) {
        let [a, b, c, d] = hop_ip;
        // De ruimte staat er al; schrijven in een String faalt dan niet.
        let _ = core::fmt::write(&mut out, format_args!("{a}.{b}.{c}.{d}"));
    } else {
        out.push_str(host);
    }
    out.push_str(tail);
    Ok(out)
}

/// Het interval uit `HOPPROM_INTERVAL` (een Go-duur: `5s`, `1m30s`);
/// `Err` met de standaard bij onzin of nul.
pub fn interval(env: Option<&str>) -> core::result::Result<Duration, Duration> {
    match env {
        None => Ok(DEFAULT_INTERVAL),
        Some(v) => match types::time::parse_duration(v) {
            Ok(ns) if ns > 0 => Ok(Duration::from_nanos(ns)),
            _ => Err(DEFAULT_INTERVAL),
        },
    }
}

/// De poort uit `ER_PORT_METRICS`; `Err` met de standaard bij onzin.
pub fn port(env: Option<&str>) -> core::result::Result<u16, u16> {
    match env.map(str::parse::<u16>) {
        None => Ok(DEFAULT_PORT),
        Some(Ok(p)) if p != 0 => Ok(p),
        Some(_) => Err(DEFAULT_PORT),
    }
}

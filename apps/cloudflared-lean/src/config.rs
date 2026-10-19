//! De instellingen van de tunnel, uit de env van de jobspec.
//!
//! Bezit het lezen en toetsen van de env, met cloudflared's eigen namen waar
//! die bestaan, zodat hun documentatie blijft gelden:
//!
//! | env | wat |
//! | --- | --- |
//! | `TUNNEL_TOKEN` | verplicht: de named tunnel uit het dashboard |
//! | `TUNNEL_URL` | waar verkeer heen gaat vóór de eerste config-push |
//! | `TUNNEL_CONNECTIONS` | aantal edge-verbindingen, 1 tot 8 (standaard 4) |
//! | `TUNNEL_INGRESS` | een eigen ingress-tabel, als JSON (`{"ingress": [...]}`) |
//! | `TUNNEL_EDGE` | de edge, als namen of adressen met komma's ertussen |
//!
//! Niet de verbindingen zelf (`crate::tunnel`) en niet het token (dat leest
//! [`Token::parse`]). Het geheim uit het token komt nooit in een foutmelding.

#![forbid(unsafe_code)]

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use crate::ingress::{self, Table};
use crate::register::{Token, TokenError};

/// cloudflared's eigen standaard: twee regio's, twee verbindingen elk, zodat
/// één wegvallende edge-machine geen gat maakt.
pub(crate) const DEFAULT_CONNECTIONS: u8 = 4;

/// Het meeste aantal verbindingen; zoveel taken staan klaar.
pub(crate) const MAX_CONNECTIONS: u8 = 8;

/// De vaste ingangen van de edge, die cloudflared zelf ook als regio's kent.
/// cloudflared zoekt eerst het SRV-record `_v2-origintunneld._tcp.argotunnel.com`;
/// de resolver van applib vraagt alleen A-records, en deze twee namen zijn
/// precies waar dat SRV-record naar wijst (gemeten door de Go-voorganger,
/// die bij een mislukte SRV-vraag hierop terugviel).
pub(crate) const DEFAULT_EDGE: &str = "region1.v2.argotunnel.com,region2.v2.argotunnel.com";

/// Het meeste aantal namen in `TUNNEL_EDGE`.
pub(crate) const MAX_EDGES: usize = 8;

/// Wat er zonder `TUNNEL_URL` en `HOPOS_HOST` met een verzoek gebeurt vóór
/// de eerste push: een eerlijke 503 van Cloudflare zelf, in plaats van een
/// verbinding naar een adres dat in een slot niets betekent.
pub(crate) const DEFAULT_FALLBACK: &str = "http_status:503";

/// De instellingen.
#[derive(Debug)]
pub(crate) struct Config {
    /// Het token.
    pub(crate) token: Token,
    /// De tabel van de start: `TUNNEL_INGRESS`, of één regel naar de
    /// terugvaldienst.
    pub(crate) table: Table,
    /// Waar de tabel van de start vandaan komt, voor de logregel.
    pub(crate) table_from: &'static str,
    /// Aantal verbindingen.
    pub(crate) connections: u8,
    /// De edge-namen of -adressen.
    pub(crate) edges: Vec<String>,
}

/// Waarom de env niet bruikbaar is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Error {
    /// `TUNNEL_TOKEN` ontbreekt.
    NoToken,
    /// Het token is kapot.
    Token(TokenError),
    /// `TUNNEL_CONNECTIONS` is geen getal van 1 tot 8.
    Connections,
    /// `TUNNEL_INGRESS` of `TUNNEL_URL` geeft geen bruikbare tabel.
    Ingress(&'static str, ingress::Error),
    /// `TUNNEL_EDGE` is leeg of te lang.
    Edge,
    /// De heap weigerde.
    OutOfMemory,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoToken => f.write_str(
                "TUNNEL_TOKEN is empty: this tunnel needs a named tunnel from the Cloudflare dashboard",
            ),
            Self::Token(e) => write!(f, "TUNNEL_TOKEN: {e}"),
            Self::Connections => write!(f, "TUNNEL_CONNECTIONS must be 1..{MAX_CONNECTIONS}"),
            Self::Ingress(var, e) => write!(f, "{var}: {e}"),
            Self::Edge => write!(f, "TUNNEL_EDGE must name 1..{MAX_EDGES} hosts"),
            Self::OutOfMemory => f.write_str("configuration: out of memory"),
        }
    }
}

impl Config {
    /// Leest de instellingen uit `env`.
    pub(crate) fn from_env<'e>(env: impl Fn(&str) -> Option<&'e str>) -> Result<Self, Error> {
        let raw = env("TUNNEL_TOKEN").map(str::trim).unwrap_or("");
        if raw.is_empty() {
            return Err(Error::NoToken);
        }
        let token = Token::parse(raw).map_err(Error::Token)?;
        let connections = match env("TUNNEL_CONNECTIONS") {
            None => DEFAULT_CONNECTIONS,
            Some(v) => v
                .trim()
                .parse::<u8>()
                .ok()
                .filter(|n| (1..=MAX_CONNECTIONS).contains(n))
                .ok_or(Error::Connections)?,
        };
        let (table, table_from) = match env("TUNNEL_INGRESS").filter(|s| !s.trim().is_empty()) {
            // Versie 0: elke push van het dashboard (versie 1 en hoger) wint.
            Some(json) => (
                Table::parse(0, json.as_bytes())
                    .map_err(|e| Error::Ingress("TUNNEL_INGRESS", e))?,
                "TUNNEL_INGRESS",
            ),
            None => {
                let fb = fallback(env("TUNNEL_URL"), env("HOPOS_HOST"))?;
                (
                    Table::fallback(&fb).map_err(|e| Error::Ingress("TUNNEL_URL", e))?,
                    "TUNNEL_URL",
                )
            }
        };
        let edges = edges(env("TUNNEL_EDGE").unwrap_or(DEFAULT_EDGE))?;
        Ok(Self {
            token,
            table,
            table_from,
            connections,
            edges,
        })
    }
}

/// De terugvaldienst: `TUNNEL_URL`, anders `http://$HOPOS_HOST` (de vorm
/// van de Go-voorganger), anders [`DEFAULT_FALLBACK`].
fn fallback(url: Option<&str>, host: Option<&str>) -> Result<String, Error> {
    let mut out = String::new();
    let (a, b) = match (
        url.map(str::trim).filter(|s| !s.is_empty()),
        host.filter(|s| !s.is_empty()),
    ) {
        (Some(u), _) => ("", u),
        (None, Some(h)) => ("http://", h),
        (None, None) => ("", DEFAULT_FALLBACK),
    };
    out.try_reserve_exact(a.len() + b.len())
        .map_err(|_| Error::OutOfMemory)?;
    out.push_str(a);
    out.push_str(b);
    Ok(out)
}

/// De edge-namen uit een lijst met komma's.
fn edges(list: &str) -> Result<Vec<String>, Error> {
    let mut out = Vec::new();
    for name in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if out.len() >= MAX_EDGES {
            return Err(Error::Edge);
        }
        let mut s = String::new();
        s.try_reserve_exact(name.len())
            .map_err(|_| Error::OutOfMemory)?;
        s.push_str(name);
        out.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
        out.push(s);
    }
    if out.is_empty() {
        return Err(Error::Edge);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Het token uit de Go-toets.
    const TOKEN: &str = "eyJhIjoiOWMyYjY4MGRhNjBhNjU4OTI2YjNmZTViM2JmNWY4ZWUiLCJzIjoiQUFFQ0F3UUZCZ2NJQ1FvTERBME9EeEFSRWhNVUZSWVhHQmthR3h3ZEhoOD0iLCJ0IjoiODcyODc1YmItZTI3OS00YzY5LWE3NjctZjM1Mjg2ZWY5ZDVkIn0=";

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<&'a str> {
        move |k| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| *v)
    }

    fn service(c: &Config) -> alloc::string::String {
        c.table
            .route("h", "/")
            .map(|s| s.as_str().into())
            .unwrap_or_default()
    }

    #[test]
    fn defaults() {
        let c = Config::from_env(env(&[("TUNNEL_TOKEN", TOKEN)])).unwrap();
        assert_eq!(c.connections, 4);
        assert_eq!(
            c.edges,
            ["region1.v2.argotunnel.com", "region2.v2.argotunnel.com"]
        );
        assert_eq!(service(&c), "http_status:503");
        assert_eq!(c.table_from, "TUNNEL_URL");
        let c = Config::from_env(env(&[
            ("TUNNEL_TOKEN", TOKEN),
            ("HOPOS_HOST", "10.100.0.2"),
        ]))
        .unwrap();
        assert_eq!(service(&c), "http://10.100.0.2");
        let c = Config::from_env(env(&[
            ("TUNNEL_TOKEN", TOKEN),
            ("HOPOS_HOST", "10.100.0.2"),
            ("TUNNEL_URL", "http://10.100.0.3:8080"),
            ("TUNNEL_CONNECTIONS", "2"),
            ("TUNNEL_EDGE", " 198.41.192.7 , 198.41.200.7"),
        ]))
        .unwrap();
        assert_eq!(service(&c), "http://10.100.0.3:8080");
        assert_eq!(c.connections, 2);
        assert_eq!(c.edges, ["198.41.192.7", "198.41.200.7"]);
    }

    #[test]
    fn ingress_from_the_env() {
        let c = Config::from_env(env(&[
            ("TUNNEL_TOKEN", TOKEN),
            ("TUNNEL_INGRESS", r#"{"ingress":[{"hostname":"h","service":"http://10.100.0.2:80"},{"service":"http_status:404"}]}"#),
        ]))
        .unwrap();
        assert_eq!(service(&c), "http://10.100.0.2:80");
        assert_eq!(c.table.version(), 0);
        assert_eq!(c.table_from, "TUNNEL_INGRESS");
    }

    #[test]
    fn refusals() {
        assert_eq!(Config::from_env(env(&[])).unwrap_err(), Error::NoToken);
        assert!(matches!(
            Config::from_env(env(&[("TUNNEL_TOKEN", "x!")])),
            Err(Error::Token(_))
        ));
        for n in ["0", "9", "vier"] {
            assert_eq!(
                Config::from_env(env(&[("TUNNEL_TOKEN", TOKEN), ("TUNNEL_CONNECTIONS", n)]))
                    .unwrap_err(),
                Error::Connections
            );
        }
        assert!(matches!(
            Config::from_env(env(&[
                ("TUNNEL_TOKEN", TOKEN),
                ("TUNNEL_INGRESS", r#"{"ingress":[]}"#)
            ])),
            Err(Error::Ingress("TUNNEL_INGRESS", _))
        ));
        assert_eq!(
            Config::from_env(env(&[("TUNNEL_TOKEN", TOKEN), ("TUNNEL_EDGE", " , ")])).unwrap_err(),
            Error::Edge
        );
        // De fout noemt het geheim niet.
        let e = Config::from_env(env(&[("TUNNEL_TOKEN", "eyJhIjoiYSJ9")])).unwrap_err();
        assert!(!alloc::format!("{e}").contains("eyJ"));
    }
}

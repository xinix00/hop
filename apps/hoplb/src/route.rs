//! De routetabel: hostnaam naar route, exact of met één wildcard-niveau
//! (Go: `internal/lb/route.go`).
//!
//! Bezit de patronen en per route de backends en de round-robin-teller. Een
//! tabel wordt nooit ter plekke veranderd: de watcher bouwt een nieuwe en
//! de schil publiceert hem (de host-daemon stuurt elke werker een eigen
//! kopie, de bewoner zet hem in één leesbare tabel). Lezen is dus altijd
//! `&self`; de teller is een atomic, want een teller is geen protocol
//! (handboek §1.3).
//!
//! Beide verzamelingen zijn gesorteerde `Vec`s met binair zoeken: faalbaar
//! te bouwen (handboek §6) en zonder allocatie te lezen. Een wildcard staat
//! op zijn staart (`*.haas.eu` onder `.haas.eu`), zodat `app.haas.eu` zijn
//! route vindt met `host[eerste punt..]` als sleutel, zonder een string te
//! bouwen. Zo blijft wat Go meette overeind: exact eerst, dan één opzoeking
//! voor de wildcard, geen scan over alle routes (BENCHMARKS.md, "O(1)
//! wildcard matching").

use alloc::string::String;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicU64, Ordering::Relaxed};

use crate::error::{Error, Result, try_push, try_string};

/// Eén backend: een draaiende taak op `host:poort`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Backend {
    /// `host:poort`, zoals de proxy hem belt.
    pub address: String,
    /// Of de backend verkeer krijgt. De watcher zet alleen draaiende taken
    /// in de tabel, dus daar is dit altijd `true`; de gezondheid van een
    /// taak is van Hop, niet van hoplb (OLD/CLAUDE.md).
    pub healthy: bool,
}

impl Backend {
    /// Een gezonde backend op `address`.
    pub fn new(address: &str) -> Result<Self> {
        Ok(Self {
            address: try_string(address)?,
            healthy: true,
        })
    }
}

/// Een route: één patroon en zijn backends.
#[derive(Debug)]
pub struct Route {
    /// Het patroon, `api.haas.eu` of `*.haas.eu`.
    pub pattern: String,
    /// De backends, in de volgorde waarin de watcher ze vond.
    pub backends: Vec<Backend>,
    /// De round-robin-teller.
    next: AtomicU64,
}

impl Route {
    /// Een route met teller nul.
    pub fn new(pattern: String, backends: Vec<Backend>) -> Self {
        Self {
            pattern,
            backends,
            next: AtomicU64::new(0),
        }
    }

    /// Een gezonde backend, round-robin (Go: `GetHealthyBackend`).
    ///
    /// Elke aanroep schuift de teller één op en neemt vanaf daar de eerste
    /// gezonde; zonder gezonde backend `None`.
    pub fn healthy_backend(&self) -> Option<&Backend> {
        let n = self.backends.len();
        if n == 0 {
            return None;
        }
        let start = self.next.fetch_add(1, Relaxed).wrapping_add(1);
        let n64 = n as u64;
        (0..n64).find_map(|i| {
            let idx = usize::try_from(start.wrapping_add(i) % n64).ok()?;
            self.backends.get(idx).filter(|b| b.healthy)
        })
    }

    /// Een diepe kopie, met de teller van nu.
    pub fn try_clone(&self) -> Result<Self> {
        let mut backends = Vec::new();
        backends
            .try_reserve_exact(self.backends.len())
            .map_err(|_| Error::OutOfMemory { bytes: 0 })?;
        for b in &self.backends {
            backends.push(Backend {
                address: try_string(&b.address)?,
                healthy: b.healthy,
            });
        }
        Ok(Self {
            pattern: try_string(&self.pattern)?,
            backends,
            next: AtomicU64::new(self.next.load(Relaxed)),
        })
    }

    /// De sleutel in de tabel: het patroon, of bij een wildcard de staat
    /// vanaf de punt.
    fn key(&self) -> &str {
        self.pattern.strip_prefix('*').unwrap_or(&self.pattern)
    }

    /// Is dit een wildcard-patroon (`*.domein`)?
    fn is_wildcard(&self) -> bool {
        self.pattern.starts_with("*.")
    }
}

/// Wat de proxy met een hostnaam doet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pick {
    /// Geen patroon past: 502.
    NoRoute,
    /// Een route zonder gezonde backend: 503.
    NoBackend,
    /// Bel deze backend (`host:poort`).
    Backend(String),
}

/// Alle routes (Go: `RouteTable`).
///
/// # Invariants
///
/// `exact` en `wildcards` zijn gesorteerd op [`Route::key`] zonder dubbelen;
/// in `wildcards` staat alleen `*.`-patronen, in `exact` de rest.
#[derive(Debug, Default)]
pub struct RouteTable {
    exact: Vec<Route>,
    wildcards: Vec<Route>,
}

impl RouteTable {
    /// Een lege tabel; `const`, zodat een bewoner hem in een `static` zet.
    pub const fn new() -> Self {
        Self {
            exact: Vec::new(),
            wildcards: Vec::new(),
        }
    }

    /// Vervangt alle routes in één keer (Go: `Update`).
    ///
    /// Een patroon dat twee keer voorkomt, wordt één route met de backends
    /// van beide, zoals de watcher ze samenvoegt.
    pub fn update(&mut self, routes: Vec<Route>) -> Result {
        let mut exact = Vec::new();
        let mut wildcards = Vec::new();
        for r in routes {
            let into = if r.is_wildcard() {
                &mut wildcards
            } else {
                &mut exact
            };
            insert(into, r)?;
        }
        // INVARIANT: `insert` houdt beide gesorteerd en zonder dubbelen.
        self.exact = exact;
        self.wildcards = wildcards;
        Ok(())
    }

    /// Een tabel uit `routes`.
    pub fn from_routes(routes: Vec<Route>) -> Result<Self> {
        let mut t = Self::new();
        t.update(routes)?;
        Ok(t)
    }

    /// De route voor `host` (Go: `Match`).
    ///
    /// Een poort (`:443`) valt eraf. Eerst exact, dan de wildcard van het
    /// eerste niveau: `*.domein.com` past op `app.domein.com`, niet op
    /// `domein.com` en niet op `a.b.domein.com` (de staart vanaf de eerste
    /// punt moet precies het patroon zijn).
    pub fn match_host(&self, host: &str) -> Option<&Route> {
        let host = strip_port(host);
        if let Some(r) = find(&self.exact, host) {
            return Some(r);
        }
        let dot = host.find('.')?;
        find(&self.wildcards, host.get(dot..)?)
    }

    /// Kiest voor `host`: geen route, geen gezonde backend, of een adres.
    pub fn pick(&self, host: &str) -> Pick {
        match self.match_host(host) {
            None => Pick::NoRoute,
            Some(r) => match r.healthy_backend() {
                None => Pick::NoBackend,
                Some(b) => match try_string(&b.address) {
                    Ok(a) => Pick::Backend(a),
                    // Zonder geheugen voor een adres is er ook geen
                    // verbinding te maken; 503 zegt dat eerlijk.
                    Err(_) => Pick::NoBackend,
                },
            },
        }
    }

    /// Het aantal patronen.
    pub fn len(&self) -> usize {
        self.exact.len() + self.wildcards.len()
    }

    /// Of de tabel leeg is.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Het aantal backends over alle routes.
    pub fn backends(&self) -> usize {
        self.routes().map(|r| r.backends.len()).sum()
    }

    /// Alle routes: eerst exact, dan de wildcards, elk gesorteerd.
    pub fn routes(&self) -> impl Iterator<Item = &Route> {
        self.exact.iter().chain(self.wildcards.iter())
    }

    /// Een diepe kopie; de schil van de host-daemon geeft elke werker er één.
    pub fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            exact: clone_all(&self.exact)?,
            wildcards: clone_all(&self.wildcards)?,
        })
    }
}

/// Zoekt `key` in een gesorteerde lijst.
fn find<'a>(list: &'a [Route], key: &str) -> Option<&'a Route> {
    list.binary_search_by(|r| r.key().cmp(key))
        .ok()
        .and_then(|i| list.get(i))
}

/// Zet `r` gesorteerd in `list`; een bestaand patroon krijgt de backends erbij.
fn insert(list: &mut Vec<Route>, mut r: Route) -> Result {
    match list.binary_search_by(|x| x.key().cmp(r.key())) {
        Ok(i) => {
            let Some(have) = list.get_mut(i) else {
                return Ok(());
            };
            have.backends
                .try_reserve(r.backends.len())
                .map_err(|_| Error::OutOfMemory { bytes: 0 })?;
            have.backends.append(&mut r.backends);
            Ok(())
        }
        Err(i) => crate::error::try_insert(list, i, r),
    }
}

fn clone_all(list: &[Route]) -> Result<Vec<Route>> {
    let mut out = Vec::new();
    out.try_reserve_exact(list.len())
        .map_err(|_| Error::OutOfMemory { bytes: 0 })?;
    for r in list {
        try_push(&mut out, r.try_clone()?)?;
    }
    Ok(out)
}

/// `host` zonder poort. Go knipt na de laatste `:`; een IPv6-literal
/// (`[::1]:80`) houdt hier zijn haken, want anders knipt dat midden in het
/// adres.
pub(crate) fn strip_port(host: &str) -> &str {
    if host.starts_with('[') {
        return match host.find(']') {
            Some(i) => host.get(..=i).unwrap_or(host),
            None => host,
        };
    }
    match host.rfind(':') {
        Some(i) => host.get(..i).unwrap_or(host),
        None => host,
    }
}

#[cfg(test)]
mod tests;

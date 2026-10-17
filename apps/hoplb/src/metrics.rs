//! De metrics van het verkeer: verzoeken per statuscode en latentie per
//! domein en backend, en de Prometheus-tekst daarvan (Go:
//! `internal/metrics/metrics.go` en `exporter.go`).
//!
//! Bezit per paar (domein, backend) een reeks: de tellers per statuscode,
//! een rollend venster van de laatste [`MAX_SAMPLES`] latenties, hun som en
//! een gesorteerde kopie die blijft staan tot er een meting bij komt. Die
//! kopie is waarom een scrape goedkoop is: Go mat 23 ns voor een percentiel
//! uit 10.000 metingen tegen 96 µs zonder (BENCHMARKS.md, "sorted cache").
//!
//! Eén eigenaar schrijft en leest: alles is `&mut self`, geen slot. In de
//! host-daemon is dat de eigenaar-thread (de werkers sturen een [`Record`]),
//! in de bewoner de metrics-taak (idem, over een brievenbus).
//!
//! Begrensd, anders dan Go: het domein is de `Host`-kop van de client, en
//! wie elke keer een andere naam stuurt, liet de Go-versie onbeperkt reeksen
//! aanmaken. Boven [`MAX_SERIES`] reeksen komt elke nieuwe onder domein
//! [`OVERFLOW_DOMAIN`] en telt [`Metrics::folded`]; de export zegt dat dan
//! met één extra regel.

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::{self, Write};

use crate::error::{Error, Result, try_insert, try_push, try_string};

/// Zoveel latenties per reeks voor de percentielen (Go: `maxSamples`).
pub const MAX_SAMPLES: usize = 10_000;

/// Zoveel reeksen (paren domein, backend) op zijn hoogst, op de host.
pub const MAX_SERIES: usize = 512;

/// Het domein waaronder reeksen boven de grens vallen.
pub const OVERFLOW_DOMAIN: &str = "_other";

/// De kwantielen van de export (Go: `quantiles`).
pub const QUANTILES: [f64; 4] = [0.5, 0.9, 0.95, 0.99];

/// Het `Content-Type` van `/metrics` (Prometheus-tekst, versie 0.0.4).
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4";

/// Eén afgehandeld verzoek, zoals een werker het aan de eigenaar stuurt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    /// De `Host`-kop van de client (met poort, zoals Go `r.Host` gebruikt).
    pub domain: String,
    /// `host:poort` van de backend; leeg als er geen gekozen werd.
    pub backend: String,
    /// De statuscode die de client kreeg.
    pub code: u16,
    /// Van het lezen van de kop tot het laatste byte van het antwoord.
    pub nanos: u64,
}

/// Eén reeks: één domein, één backend.
#[derive(Debug)]
struct Series {
    domain: String,
    backend: String,
    /// Statuscode naar aantal, gesorteerd op code.
    codes: Vec<(u16, u64)>,
    /// Het venster, als ring zodra hij vol is.
    samples: Vec<f64>,
    /// Waar de volgende meting komt als het venster vol is.
    head: usize,
    /// De som van alle latenties ooit, in seconden (niet alleen het venster).
    sum: f64,
    /// Het venster gesorteerd; geldig zolang `dirty` onwaar is.
    sorted: Vec<f64>,
    dirty: bool,
}

impl Series {
    fn new(domain: &str, backend: &str) -> Result<Self> {
        Ok(Self {
            domain: try_string(domain)?,
            backend: try_string(backend)?,
            codes: Vec::new(),
            samples: Vec::new(),
            head: 0,
            sum: 0.0,
            sorted: Vec::new(),
            dirty: false,
        })
    }

    fn count(&mut self, code: u16) -> Result {
        match self.codes.binary_search_by_key(&code, |(c, _)| *c) {
            Ok(i) => {
                if let Some((_, n)) = self.codes.get_mut(i) {
                    *n = n.wrapping_add(1);
                }
                Ok(())
            }
            Err(i) => try_insert(&mut self.codes, i, (code, 1)),
        }
    }

    fn sample(&mut self, secs: f64, max: usize) -> Result {
        self.sum += secs;
        self.dirty = true;
        if self.samples.len() < max {
            return try_push(&mut self.samples, secs);
        }
        // Vol: de oudste eruit. De volgorde in de ring doet er niet toe,
        // de percentielen komen uit de gesorteerde kopie.
        if let Some(slot) = self.samples.get_mut(self.head) {
            *slot = secs;
        }
        self.head = (self.head + 1) % max.max(1);
        Ok(())
    }

    /// De gesorteerde kopie, zo nodig opnieuw gemaakt.
    fn sorted(&mut self) -> &[f64] {
        if self.dirty || self.sorted.len() != self.samples.len() {
            self.sorted.clear();
            if self.sorted.try_reserve(self.samples.len()).is_err() {
                // Zonder geheugen geen percentielen; de tellers blijven.
                return &[];
            }
            self.sorted.extend_from_slice(&self.samples);
            self.sorted.sort_unstable_by(f64::total_cmp);
            self.dirty = false;
        }
        &self.sorted
    }
}

/// De metrics van één hoplb (Go: `Metrics`).
///
/// # Invariants
///
/// `series` is gesorteerd op (domein, backend) zonder dubbelen, en telt
/// hoogstens `max_series + 1` reeksen (de extra is [`OVERFLOW_DOMAIN`]).
#[derive(Debug)]
pub struct Metrics {
    series: Vec<Series>,
    max_samples: usize,
    max_series: usize,
    folded: u64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Metrics met de grenzen van de host: [`MAX_SAMPLES`], [`MAX_SERIES`].
    pub fn new() -> Self {
        Self::with_limits(MAX_SAMPLES, MAX_SERIES)
    }

    /// Metrics met eigen grenzen; de bewoner neemt kleinere (zijn heap is
    /// een deel van zijn partitie).
    pub fn with_limits(max_samples: usize, max_series: usize) -> Self {
        Self {
            series: Vec::new(),
            max_samples: max_samples.max(1),
            max_series,
            folded: 0,
        }
    }

    /// Telt een verzoek (Go: `RecordRequest`).
    pub fn record_request(&mut self, domain: &str, backend: &str, code: u16, nanos: u64) -> Result {
        let max = self.max_samples;
        let s = self.series_mut(domain, backend)?;
        s.count(code)?;
        s.sample(nanos as f64 / 1e9, max)
    }

    /// Telt een [`Record`].
    pub fn record(&mut self, r: &Record) -> Result {
        self.record_request(&r.domain, &r.backend, r.code, r.nanos)
    }

    /// Hoeveel reeksen er onder [`OVERFLOW_DOMAIN`] vielen.
    pub fn folded(&self) -> u64 {
        self.folded
    }

    /// Hoeveel verzoeken (domein, backend) met `code` kreeg.
    pub fn request_count(&self, domain: &str, backend: &str, code: u16) -> u64 {
        self.series(domain, backend)
            .and_then(|s| s.codes.iter().find(|(c, _)| *c == code))
            .map_or(0, |(_, n)| *n)
    }

    /// Alle tellers: (domein, backend, code, aantal), gesorteerd (Go:
    /// `RequestCounts`, zonder de diepe kopie: de eigenaar leest zelf).
    pub fn request_counts(&self) -> impl Iterator<Item = (&str, &str, u16, u64)> {
        self.series.iter().flat_map(|s| {
            s.codes
                .iter()
                .map(move |(c, n)| (s.domain.as_str(), s.backend.as_str(), *c, *n))
        })
    }

    /// Het percentiel `p` (0.0 tot 1.0) van het venster (Go: `Percentile`);
    /// 0 zonder metingen.
    pub fn percentile(&mut self, domain: &str, backend: &str, p: f64) -> f64 {
        let [v] = self.percentiles(domain, backend, &[p]);
        v
    }

    /// Meer percentielen uit één gesorteerde kopie (Go: `Percentiles`).
    ///
    /// De kopie blijft staan tot de volgende meting, dus een tweede scrape
    /// zonder nieuw verkeer sorteert niets.
    pub fn percentiles<const N: usize>(
        &mut self,
        domain: &str,
        backend: &str,
        ps: &[f64; N],
    ) -> [f64; N] {
        let mut out = [0.0; N];
        let Some(i) = self.index(domain, backend).ok() else {
            return out;
        };
        let Some(s) = self.series.get_mut(i) else {
            return out;
        };
        let sorted = s.sorted();
        if sorted.is_empty() {
            return out;
        }
        for (o, p) in out.iter_mut().zip(ps) {
            *o = at(sorted, *p);
        }
        out
    }

    /// Alle domeinen, gesorteerd en elk één keer (Go: `AllDomains`).
    pub fn all_domains(&self) -> impl Iterator<Item = &str> {
        let mut last: Option<&str> = None;
        self.series.iter().filter_map(move |s| {
            let d = s.domain.as_str();
            if last == Some(d) {
                return None;
            }
            last = Some(d);
            Some(d)
        })
    }

    /// Alle backends van `domain`, gesorteerd (Go: `AllBackends`).
    pub fn all_backends<'a>(&'a self, domain: &'a str) -> impl Iterator<Item = &'a str> {
        self.series
            .iter()
            .filter(move |s| s.domain == domain)
            .map(|s| s.backend.as_str())
    }

    /// De som van alle latenties, in seconden (Go: `LatencySum`).
    pub fn latency_sum(&self, domain: &str, backend: &str) -> f64 {
        self.series(domain, backend).map_or(0.0, |s| s.sum)
    }

    /// Het aantal metingen in het venster (Go: `SampleCount`).
    pub fn sample_count(&self, domain: &str, backend: &str) -> usize {
        self.series(domain, backend).map_or(0, |s| s.samples.len())
    }

    /// Schrijft de Prometheus-tekst achter `out` (Go: `Exporter.ServeHTTP`).
    ///
    /// Dezelfde regels als Go, met de statuscodes gesorteerd (Go liep over
    /// een map en had dus geen vaste volgorde).
    pub fn render(&mut self, out: &mut String) -> Result {
        let mut w = Out(out);
        self.render_counts(&mut w).map_err(oom)?;
        let mut i = 0;
        while let Some(s) = self.series.get_mut(i) {
            if i == 0 {
                w.write_str(DURATION_HEAD).map_err(oom)?;
            }
            render_series(s, &mut w).map_err(oom)?;
            i += 1;
        }
        if self.series.is_empty() {
            w.write_str(DURATION_HEAD).map_err(oom)?;
        }
        if self.folded > 0 {
            write!(
                w,
                "\n# HELP hoplb_series_folded_total Series folded into domain \"{OVERFLOW_DOMAIN}\" (over {} series)\n\
                 # TYPE hoplb_series_folded_total counter\nhoplb_series_folded_total {}\n",
                self.max_series, self.folded
            )
            .map_err(oom)?;
        }
        Ok(())
    }

    fn render_counts(&self, w: &mut Out<'_>) -> fmt::Result {
        w.write_str("# HELP hoplb_requests_total Total HTTP requests\n")?;
        w.write_str("# TYPE hoplb_requests_total counter\n")?;
        for (d, b, code, n) in self.request_counts() {
            w.write_str("hoplb_requests_total{domain=")?;
            quote(w, d)?;
            w.write_str(",backend=")?;
            quote(w, b)?;
            writeln!(w, ",code=\"{code}\"}} {n}")?;
        }
        w.write_str("\n")
    }

    fn index(&self, domain: &str, backend: &str) -> core::result::Result<usize, usize> {
        self.series
            .binary_search_by(|s| (s.domain.as_str(), s.backend.as_str()).cmp(&(domain, backend)))
    }

    fn series(&self, domain: &str, backend: &str) -> Option<&Series> {
        self.index(domain, backend)
            .ok()
            .and_then(|i| self.series.get(i))
    }

    /// De reeks van (domein, backend), zo nodig nieuw; boven de grens die
    /// van [`OVERFLOW_DOMAIN`].
    fn series_mut(&mut self, domain: &str, backend: &str) -> Result<&mut Series> {
        let i = match self.index(domain, backend) {
            Ok(i) => i,
            Err(_) if self.series.len() >= self.max_series => {
                self.folded = self.folded.wrapping_add(1);
                match self.index(OVERFLOW_DOMAIN, "") {
                    Ok(i) => i,
                    Err(i) => {
                        try_insert(&mut self.series, i, Series::new(OVERFLOW_DOMAIN, "")?)?;
                        i
                    }
                }
            }
            Err(i) => {
                // INVARIANT: `i` is de plek die de sortering houdt.
                try_insert(&mut self.series, i, Series::new(domain, backend)?)?;
                i
            }
        };
        self.series
            .get_mut(i)
            .ok_or(Error::OutOfMemory { bytes: 0 })
    }
}

/// De kop van het tweede blok van de export.
const DURATION_HEAD: &str = "# HELP hoplb_request_duration_seconds Request duration percentiles\n\
                             # TYPE hoplb_request_duration_seconds summary\n";

/// De regels van één reeks: vier kwantielen, `_count` en `_sum`.
fn render_series(s: &mut Series, w: &mut Out<'_>) -> fmt::Result {
    let count = s.samples.len();
    if count == 0 {
        return Ok(());
    }
    let sum = s.sum;
    let mut values = [0.0; QUANTILES.len()];
    {
        let sorted = s.sorted();
        if !sorted.is_empty() {
            for (v, q) in values.iter_mut().zip(QUANTILES) {
                *v = at(sorted, q);
            }
        }
    }
    for (q, v) in QUANTILES.iter().zip(values) {
        w.write_str("hoplb_request_duration_seconds{domain=")?;
        quote(w, &s.domain)?;
        w.write_str(",backend=")?;
        quote(w, &s.backend)?;
        writeln!(w, ",quantile=\"{q:.2}\"}} {v:.6}")?;
    }
    w.write_str("hoplb_request_duration_seconds_count{domain=")?;
    quote(w, &s.domain)?;
    w.write_str(",backend=")?;
    quote(w, &s.backend)?;
    writeln!(w, "}} {count}")?;
    w.write_str("hoplb_request_duration_seconds_sum{domain=")?;
    quote(w, &s.domain)?;
    w.write_str(",backend=")?;
    quote(w, &s.backend)?;
    writeln!(w, "}} {sum:.6}")
}

/// Het element op percentiel `p` van een gesorteerde, niet-lege lijst
/// (Go: `int(float64(n-1) * p)`, geklemd).
fn at(sorted: &[f64], p: f64) -> f64 {
    let n = sorted.len();
    let idx = ((n.saturating_sub(1)) as f64 * p) as usize;
    sorted
        .get(idx.min(n.saturating_sub(1)))
        .copied()
        .unwrap_or(0.0)
}

/// Een labelwaarde tussen aanhalingstekens, geëscapet zoals Prometheus
/// het wil: `\\`, `\"` en `\n`. (Go gebruikte `%q`, dat voor gewone namen
/// hetzelfde geeft.)
fn quote(w: &mut Out<'_>, s: &str) -> fmt::Result {
    w.write_char('"')?;
    for c in s.chars() {
        match c {
            '\\' => w.write_str("\\\\")?,
            '"' => w.write_str("\\\"")?,
            '\n' => w.write_str("\\n")?,
            c => w.write_char(c)?,
        }
    }
    w.write_char('"')
}

/// Een `fmt::Write` die faalbaar groeit: zonder geheugen een fout, geen abort.
struct Out<'a>(&'a mut String);

impl Write for Out<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.0.try_reserve(s.len()).map_err(|_| fmt::Error)?;
        self.0.push_str(s);
        Ok(())
    }
}

fn oom(_: fmt::Error) -> Error {
    Error::OutOfMemory { bytes: 0 }
}

#[cfg(test)]
mod tests;

//! De routeertabel die Cloudflare naar ons duwt.
//!
//! Bezit de regels (hostname, pad, dienst), het lezen van een config-push en
//! het kiezen van een dienst voor een verzoek. Niet de opslag van de levende
//! tabel: die staat in `crate::tunnel`, als leesbare tabel van één core
//! (handboek §1.1), en wisselt in zijn geheel om, nooit half.
//!
//! Waarom geen `leanhttp::Mux`: die routeert op pad, kiest de meest
//! specifieke route los van de volgorde, en staat vast zodra de server
//! draait. Cloudflare doet precies de andere drie dingen: hij routeert eerst
//! op hostname (exact of `*.achtervoegsel`), zijn pad is een reguliere
//! expressie ([`crate::regex`]), hij neemt de eerste passende regel, en zijn
//! config komt binnen terwijl we draaien. Vier verschillen, dus een eigen
//! tabel; hij is kleiner dan het verschil zou zijn.

#![forbid(unsafe_code)]

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use crate::json;
use crate::regex::{self, Regex};

/// De langste dienst-URL die een regel mag hebben. Bij het routeren gaat de
/// dienst als waarde mee over de `.await`s van het verzoek ([`Service`]),
/// zonder heap en zonder een lening op de tabel; 256 bytes is ruim voor
/// `http://host:poort/voorvoegsel`.
pub(crate) const SERVICE_CAP: usize = 256;

/// Het meeste aantal regels. Een dashboard met honderd hostnamen op één
/// tunnel is al veel; het plafond houdt een push begrensd.
pub(crate) const MAX_RULES: usize = 256;

/// Waarom een config niet toegepast werd. De edge hoort de tekst terug.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Error {
    /// De JSON is onleesbaar.
    Json(json::Error),
    /// Regel `rule` heeft een pad dat niet compileert.
    Path {
        /// Het regelnummer, vanaf 0.
        rule: usize,
        /// De reden.
        cause: regex::Error,
    },
    /// Regel `rule` heeft geen dienst.
    NoService {
        /// Het regelnummer.
        rule: usize,
    },
    /// Regel `rule` heeft een dienst langer dan [`SERVICE_CAP`].
    ServiceTooLong {
        /// Het regelnummer.
        rule: usize,
    },
    /// Geen enkele regel.
    Empty,
    /// Meer dan [`MAX_RULES`] regels.
    TooMany,
    /// Een push groter dan de tunnel leest.
    TooLarge {
        /// De grens in bytes.
        limit: usize,
    },
    /// De stream met de push brak af.
    Body,
    /// De heap weigerde.
    OutOfMemory,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(e) => write!(f, "configuration is not readable: {e}"),
            Self::Path { rule, cause } => write!(f, "rule {rule} has an unusable path: {cause}"),
            Self::NoService { rule } => write!(f, "rule {rule} has no service"),
            Self::ServiceTooLong { rule } => {
                write!(
                    f,
                    "rule {rule} has a service longer than {SERVICE_CAP} bytes"
                )
            }
            Self::Empty => f.write_str("configuration carries no ingress rules"),
            Self::TooMany => write!(f, "configuration carries more than {MAX_RULES} rules"),
            Self::TooLarge { limit } => write!(f, "configuration larger than {limit} bytes"),
            Self::Body => f.write_str("configuration stream broke off"),
            Self::OutOfMemory => f.write_str("configuration: out of memory"),
        }
    }
}

impl From<json::Error> for Error {
    fn from(e: json::Error) -> Self {
        Self::Json(e)
    }
}

/// Eén regel: waarheen met wat.
#[derive(Debug, Clone)]
pub(crate) struct Rule {
    /// Leeg of `*` is elke host; `*.x` is een achtervoegsel; anders exact.
    hostname: String,
    /// `None` is elk pad.
    path: Option<Regex>,
    /// `http://10.100.0.2:7080`, `http_status:404`, ...
    service: String,
}

impl Rule {
    /// Cloudflare's eigen regel (`ingress/rule.go`): een lege hostname of `*`
    /// past overal, `*.example.com` op achtervoegsel, anders exact (zonder
    /// kast); het pad past als er geen patroon is of als het ergens past.
    ///
    /// Het achtervoegsel is hier ook zonder kast, anders dan in de
    /// Go-voorganger: een hostname is dat altijd.
    fn matches(&self, host: &str, path: &str) -> bool {
        let h = self.hostname.as_str();
        let host_ok = if h.is_empty() || h == "*" {
            true
        } else if let Some(suffix) = h.strip_prefix('*') {
            host.len() > suffix.len()
                && host
                    .get(host.len() - suffix.len()..)
                    .is_some_and(|t| t.eq_ignore_ascii_case(suffix))
        } else {
            h.eq_ignore_ascii_case(host)
        };
        host_ok && self.path.as_ref().is_none_or(|re| re.is_match(path))
    }
}

/// De dienst van een verzoek, als waarde: een kopie uit de tabel, zodat een
/// verzoek geen lening op de tabel over zijn `.await`s houdt en een push
/// midden in een verzoek dat verzoek niet raakt.
#[derive(Clone, Copy)]
pub(crate) struct Service {
    /// De bytes.
    buf: [u8; SERVICE_CAP],
    /// De lengte.
    len: usize,
}

impl Service {
    /// Kopieert `s`; `None` boven [`SERVICE_CAP`].
    pub(crate) fn new(s: &str) -> Option<Self> {
        let mut buf = [0u8; SERVICE_CAP];
        buf.get_mut(..s.len())?.copy_from_slice(s.as_bytes());
        Some(Self { buf, len: s.len() })
    }

    /// De dienst.
    pub(crate) fn as_str(&self) -> &str {
        core::str::from_utf8(self.buf.get(..self.len).unwrap_or(&[])).unwrap_or("")
    }
}

impl fmt::Debug for Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

/// Een momentopname van de regels plus de versie waarmee ze kwamen.
#[derive(Debug, Clone)]
pub(crate) struct Table {
    /// De versie van de push; 0 is de tabel van de start.
    version: i32,
    /// De regels, in volgorde.
    rules: Vec<Rule>,
}

/// Een `String`-kopie van `s`, of een weigering van de heap.
fn owned(s: &str) -> Result<String, Error> {
    let mut out = String::new();
    out.try_reserve_exact(s.len())
        .map_err(|_| Error::OutOfMemory)?;
    out.push_str(s);
    Ok(out)
}

impl Table {
    /// Een tabel zonder regels (vóór de start); elk verzoek is dan 404.
    pub(crate) const fn empty() -> Self {
        Self {
            version: 0,
            rules: Vec::new(),
        }
    }

    /// Eén regel: alles naar `fallback`. Zo werkt de tunnel al vóór de
    /// eerste push; anders zit er een gat tussen "verbonden" en
    /// "geconfigureerd" waarin bezoekers een fout krijgen.
    pub(crate) fn fallback(fallback: &str) -> Result<Self, Error> {
        if fallback.len() > SERVICE_CAP {
            return Err(Error::ServiceTooLong { rule: 0 });
        }
        let mut rules = Vec::new();
        rules.try_reserve_exact(1).map_err(|_| Error::OutOfMemory)?;
        rules.push(Rule {
            hostname: String::new(),
            path: None,
            service: owned(fallback)?,
        });
        Ok(Self { version: 0, rules })
    }

    /// Leest een config (`{"ingress": [...], ...}`) als tabel met `version`.
    /// Een kapotte regel weigert de hele config: half toepassen zou betekenen
    /// dat het dashboard iets anders zegt dan er draait.
    pub(crate) fn parse(version: i32, config: &[u8]) -> Result<Self, Error> {
        let mut rules: Vec<Rule> = Vec::new();
        let mut err: Option<Error> = None;
        let mut r = json::Reader::new(config);
        r.object(|r, k| {
            if !k.is("ingress") {
                return r.skip();
            }
            if r.null() {
                return Ok(());
            }
            r.array(|r| {
                let n = rules.len();
                match read_rule(r, n) {
                    Ok(rule) if err.is_none() => {
                        if n >= MAX_RULES {
                            err = Some(Error::TooMany);
                        } else if rules.try_reserve(1).is_err() {
                            err = Some(Error::OutOfMemory);
                        } else {
                            rules.push(rule);
                        }
                        Ok(())
                    }
                    Ok(_) => Ok(()),
                    Err(RuleError::Json(e)) => Err(e),
                    Err(RuleError::Rule(e)) => {
                        err.get_or_insert(e);
                        Ok(())
                    }
                }
            })
        })?;
        r.end()?;
        if let Some(e) = err {
            return Err(e);
        }
        if rules.is_empty() {
            return Err(Error::Empty);
        }
        Ok(Self { version, rules })
    }

    /// De versie.
    pub(crate) fn version(&self) -> i32 {
        self.version
    }

    /// Het aantal regels.
    pub(crate) fn len(&self) -> usize {
        self.rules.len()
    }

    /// De dienst voor een verzoek, of `None` als geen regel past (dat kan
    /// alleen als de laatste regel geen vangnet is).
    pub(crate) fn route(&self, host: &str, path: &str) -> Option<Service> {
        let rule = self.rules.iter().find(|r| r.matches(host, path))?;
        Service::new(&rule.service)
    }

    /// Roept `f` aan met elke regel als leesbare regel, voor de log bij de
    /// start en na elke push. Zonder dit is "de tunnel draait" niet te
    /// onderscheiden van "de tunnel draait en stuurt alles de verkeerde kant
    /// op".
    pub(crate) fn describe(&self, mut f: impl FnMut(Line<'_>)) {
        for r in &self.rules {
            f(Line(r));
        }
    }
}

/// Eén regel als tekst: `host [path re] -> dienst`.
pub(crate) struct Line<'a>(&'a Rule);

impl fmt::Display for Line<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let host = if self.0.hostname.is_empty() {
            "*"
        } else {
            &self.0.hostname
        };
        f.write_str(host)?;
        if let Some(re) = &self.0.path {
            write!(f, " path {}", re.as_str())?;
        }
        write!(f, " -> {}", self.0.service)
    }
}

/// Een regel die niet gelezen kon worden: de JSON zelf (dan stopt het
/// lezen), of de inhoud (dan leest de lezer door en weigert daarna).
enum RuleError {
    Json(json::Error),
    Rule(Error),
}

impl From<json::Error> for RuleError {
    fn from(e: json::Error) -> Self {
        Self::Json(e)
    }
}

/// Leest één regel (`{"hostname", "path", "service", ...}`).
fn read_rule(r: &mut json::Reader<'_>, n: usize) -> Result<Rule, RuleError> {
    let (mut host, mut path, mut service) = (None, None, None);
    r.object(|r, k| {
        let slot = if k.is("hostname") {
            &mut host
        } else if k.is("path") {
            &mut path
        } else if k.is("service") {
            &mut service
        } else {
            return r.skip();
        };
        if !r.null() {
            *slot = Some(r.string()?.decode()?);
        }
        Ok(())
    })?;
    let service = service.unwrap_or_default();
    if service.is_empty() {
        return Err(RuleError::Rule(Error::NoService { rule: n }));
    }
    if service.len() > SERVICE_CAP {
        return Err(RuleError::Rule(Error::ServiceTooLong { rule: n }));
    }
    let path = match path.filter(|p| !p.is_empty()) {
        Some(p) => {
            Some(Regex::new(&p).map_err(|cause| RuleError::Rule(Error::Path { rule: n, cause }))?)
        }
        None => None,
    };
    Ok(Rule {
        hostname: host.unwrap_or_default(),
        path,
        service,
    })
}

/// Een config-push van de edge: `{"version": N, "config": {...}}`. Geeft de
/// versie en de ruwe config.
pub(crate) fn parse_push(body: &[u8]) -> Result<(i32, &[u8]), Error> {
    let mut version: Option<i64> = None;
    let mut config: Option<&[u8]> = None;
    let mut r = json::Reader::new(body);
    r.object(|r, k| {
        if k.is("version") {
            version = Some(r.int()?);
            Ok(())
        } else if k.is("config") {
            config = Some(r.raw()?);
            Ok(())
        } else {
            r.skip()
        }
    })?;
    r.end()?;
    let version = i32::try_from(version.unwrap_or(0))
        .map_err(|_| Error::Json(json::Error::Range { at: 0 }))?;
    Ok((version, config.unwrap_or(b"{}")))
}

/// Wat een push met de tabel doet.
#[derive(Debug)]
pub(crate) enum Update {
    /// Ouder of gelijk aan wat er staat: genegeerd. Dat is Cloudflare's eigen
    /// regel, en hij houdt een late push van een andere verbinding uit een
    /// nieuwere tabel.
    Stale,
    /// Een nieuwe tabel om in te zetten.
    New(Table),
}

/// Beslist over een push van `version` met `config` tegen de huidige versie.
pub(crate) fn update(current: i32, version: i32, config: &[u8]) -> Result<Update, Error> {
    if version <= current {
        return Ok(Update::Stale);
    }
    Table::parse(version, config).map(Update::New)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::format;
    use alloc::string::ToString;

    /// Past een push toe zoals de tunnel dat doet.
    fn push(t: &mut Table, version: i32, config: &str) -> Result<i32, Error> {
        match update(t.version(), version, config.as_bytes())? {
            Update::Stale => Ok(t.version()),
            Update::New(n) => {
                *t = n;
                Ok(version)
            }
        }
    }

    fn route(t: &Table, host: &str, path: &str) -> Option<String> {
        t.route(host, path).map(|s| s.as_str().to_string())
    }

    // De vier verschillen met een gewone mux, elk met een toets (uit de Go-
    // toetsen van internal/ingress).
    #[test]
    fn ordered_first_match_wins() {
        let mut t = Table::fallback("http://val:80").unwrap();
        push(
            &mut t,
            1,
            r#"{"ingress":[
            {"hostname":"demo.example.com","path":"^/api","service":"http://api:8080"},
            {"hostname":"demo.example.com","service":"http://web:80"},
            {"service":"http://val:80"}]}"#,
        )
        .unwrap();
        for (host, path, want) in [
            ("demo.example.com", "/api/v1/devices", "http://api:8080"),
            ("demo.example.com", "/", "http://web:80"),
            ("iets.anders.nl", "/api", "http://val:80"),
        ] {
            assert_eq!(route(&t, host, path).as_deref(), Some(want), "{host}{path}");
        }
    }

    #[test]
    fn hostname_glob() {
        let mut t = Table::fallback("http://val:80").unwrap();
        push(
            &mut t,
            1,
            r#"{"ingress":[
            {"hostname":"*.example.com","service":"http://wild:80"},
            {"hostname":"exact.nl","service":"http://exact:80"},
            {"service":"http://val:80"}]}"#,
        )
        .unwrap();
        for (host, want) in [
            ("a.example.com", "http://wild:80"),
            ("diep.genest.example.com", "http://wild:80"),
            ("A.EXAMPLE.COM", "http://wild:80"),
            ("example.com", "http://val:80"),
            ("EXACT.NL", "http://exact:80"),
            ("nietexact.nl", "http://val:80"),
        ] {
            assert_eq!(route(&t, host, "/").as_deref(), Some(want), "{host}");
        }
    }

    #[test]
    fn path_is_a_regex() {
        let mut t = Table::fallback("http://val:80").unwrap();
        push(
            &mut t,
            1,
            r#"{"ingress":[
            {"hostname":"h","path":"\\.(jpg|png)$","service":"http://beeld:80"},
            {"hostname":"h","service":"http://web:80"}]}"#,
        )
        .unwrap();
        for (path, want) in [
            ("/camera/voordeur.jpg", "http://beeld:80"),
            ("/logo.png", "http://beeld:80"),
            ("/jpg/index.html", "http://web:80"),
        ] {
            assert_eq!(route(&t, "h", path).as_deref(), Some(want), "{path}");
        }
        assert_eq!(route(&t, "ander", "/"), None, "geen vangnet: geen dienst");
    }

    #[test]
    fn version_is_monotonic() {
        let mut t = Table::fallback("http://val:80").unwrap();
        push(&mut t, 5, r#"{"ingress":[{"service":"http://nieuw:80"}]}"#).unwrap();
        assert_eq!(
            push(&mut t, 3, r#"{"ingress":[{"service":"http://oud:80"}]}"#).unwrap(),
            5
        );
        assert_eq!(route(&t, "h", "/").as_deref(), Some("http://nieuw:80"));
    }

    #[test]
    fn bad_rule_keeps_old_table() {
        let mut t = Table::fallback("http://val:80").unwrap();
        push(&mut t, 1, r#"{"ingress":[{"service":"http://goed:80"}]}"#).unwrap();
        for bad in [
            r#"{"ingress":[{"path":"(ongeldig","service":"http://x:80"}]}"#,
            r#"{"ingress":[{"hostname":"h"}]}"#,
            r#"{"ingress":[]}"#,
            "niet json",
        ] {
            assert!(push(&mut t, 2, bad).is_err(), "{bad}");
            assert_eq!(route(&t, "h", "/").as_deref(), Some("http://goed:80"));
            assert_eq!(t.version(), 1);
        }
    }

    #[test]
    fn fallback_before_first_push() {
        let t = Table::fallback("http://stulp:80").unwrap();
        assert_eq!(
            route(&t, "wat.dan.ook", "/pad").as_deref(),
            Some("http://stulp:80")
        );
        let mut lines = Vec::new();
        t.describe(|l| lines.push(format!("{l}")));
        assert_eq!(lines, ["* -> http://stulp:80"]);
        assert_eq!(route(&Table::empty(), "h", "/"), None);
    }

    // De vorm die de edge echt stuurt: de ingress naast warp-routing en
    // originRequest, per regel ook originRequest.
    #[test]
    fn push_as_the_edge_sends_it() {
        let body = br#"{"version":3,"config":{"ingress":[{"hostname":"stulp.example.nl","originRequest":{"noTLSVerify":true},"service":"http://10.100.0.2:80","path":null},{"service":"http_status:404"}],"warp-routing":{"enabled":false},"originRequest":{}}}"#;
        let (version, config) = parse_push(body).unwrap();
        assert_eq!(version, 3);
        let t = Table::parse(version, config).unwrap();
        assert_eq!(t.len(), 2);
        assert_eq!(
            route(&t, "stulp.example.nl", "/").as_deref(),
            Some("http://10.100.0.2:80")
        );
        assert_eq!(route(&t, "x.nl", "/").as_deref(), Some("http_status:404"));
        let mut lines = Vec::new();
        t.describe(|l| lines.push(format!("{l}")));
        assert_eq!(
            lines,
            [
                "stulp.example.nl -> http://10.100.0.2:80",
                "* -> http_status:404"
            ]
        );
    }

    #[test]
    fn limits() {
        let long = "x".repeat(SERVICE_CAP + 1);
        let cfg = format!(r#"{{"ingress":[{{"service":"{long}"}}]}}"#);
        assert_eq!(
            Table::parse(1, cfg.as_bytes()).unwrap_err(),
            Error::ServiceTooLong { rule: 0 }
        );
        let many: Vec<String> = (0..=MAX_RULES)
            .map(|_| r#"{"service":"http://a"}"#.to_string())
            .collect();
        let cfg = format!(r#"{{"ingress":[{}]}}"#, many.join(","));
        assert_eq!(Table::parse(1, cfg.as_bytes()).unwrap_err(), Error::TooMany);
        assert!(parse_push(br#"{"version":99999999999}"#).is_err());
    }
}

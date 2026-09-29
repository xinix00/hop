//! De jobspec: wat de gebruiker wil draaien, en de JSON-vorm ervan.
//!
//! Een job heeft geen id: de naam is de sleutel (upsert op naam). De velden
//! en hun JSON-namen zijn die van `OLD/internal/types/types.go`; wat daar
//! `omitempty` was, wordt hier ook weggelaten bij het schrijven.

use alloc::string::String;
use alloc::vec::Vec;

use crate::de::{self, ObjectBuilder};
use crate::json::{self, Value};
use crate::time::Nanos;
use crate::{Error, Map, Name, Result, TryClone};

/// Hoeveel sleutels een map in een job (env, tags, affinity, volumes,
/// headers) hoogstens heeft. Een echte job heeft er een handvol; 256 is de
/// grens waarboven het een fout of een aanval is.
pub const MAX_MAP_ENTRIES: usize = 256;

/// Hoeveel artifacts een job hoogstens heeft. Eén per architectuur is het
/// gebruik (arm64, amd64, riscv64); 16 laat ruim ruimte.
pub const MAX_ARTIFACTS: usize = 16;

/// Hoeveel benoemde poorten een job hoogstens heeft.
pub const MAX_PORTS: usize = 64;

/// Welke runner een job uitvoert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Driver {
    /// Een proces op de host (`"exec"`, de standaard).
    Exec,
    /// Een Docker-container (`"docker"`).
    Docker,
    /// Een app-image op een HopOS-core (`"hop"`).
    Hop,
}

impl Driver {
    /// De naam op de draad.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exec => "exec",
            Self::Docker => "docker",
            Self::Hop => "hop",
        }
    }

    /// Leest de naam op de draad.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "exec" => Some(Self::Exec),
            "docker" => Some(Self::Docker),
            "hop" => Some(Self::Hop),
            _ => None,
        }
    }

    /// De driver die bij een image hoort: een image betekent Docker (Go's `DriverFor`).
    pub fn for_image(image: &str) -> Self {
        if image.is_empty() {
            Self::Exec
        } else {
            Self::Docker
        }
    }
}

/// Hoe een update van een bestaande job wordt uitgerold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UpdatePolicy {
    /// Eén voor één vervangen, zonder downtime (de standaard).
    #[default]
    Rolling,
    /// Alles stoppen, dan alles nieuw starten.
    Recreate,
    /// Alles nieuw naast het oude, dan het oude stoppen (2x capaciteit).
    BlueGreen,
}

impl UpdatePolicy {
    /// De naam op de draad.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rolling => "rolling",
            Self::Recreate => "recreate",
            Self::BlueGreen => "blue-green",
        }
    }

    /// Leest de naam op de draad.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "rolling" => Some(Self::Rolling),
            "recreate" => Some(Self::Recreate),
            "blue-green" => Some(Self::BlueGreen),
            _ => None,
        }
    }
}

/// Het soort health check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CheckType {
    /// HTTP GET, 200 tot en met 399 is gezond (de standaard).
    #[default]
    Http,
    /// Een TCP-connect die lukt is gezond.
    Tcp,
    /// Een bestand waarvan de mtime sinds de vorige check veranderde is gezond.
    File,
}

impl CheckType {
    /// De naam op de draad.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Tcp => "tcp",
            Self::File => "file",
        }
    }

    /// Leest de naam op de draad.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "http" => Some(Self::Http),
            "tcp" => Some(Self::Tcp),
            "file" => Some(Self::File),
            _ => None,
        }
    }
}

/// Waar de applicatie vandaan komt: één download, met de nodes waarvoor hij geldt.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Artifact {
    /// De URL; het schema kiest de downloader (`http://`, `https://`, `s3://`).
    pub url: String,
    /// Node-attributen die moeten kloppen; leeg is "past overal".
    pub matches: Map<String>,
    /// HTTP-headers voor de download.
    pub headers: Map<String>,
    /// Overige credentials (S3: `access_key`, `secret_key`, `region`).
    pub auth: Map<String>,
    /// `"tar.gz"`, `"tar.bz2"`, `"zip"`, of leeg voor een kaal bestand.
    pub extract: String,
    /// De bestandsnaam voor een kale download (standaard de basename van de URL).
    pub filename: String,
}

/// Een health check op een job.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct HealthCheck {
    /// Het soort check; `None` is de standaard (HTTP) en wordt niet geschreven.
    pub kind: Option<CheckType>,
    /// HTTP: het pad; file: het absolute pad.
    pub path: String,
    /// HTTP/TCP: de benoemde poort (standaard `"http"`).
    pub port: String,
    /// Hoe vaak er gecheckt wordt (0 = standaard, 10 s).
    pub interval: Nanos,
    /// HTTP/TCP: de timeout per poging (0 = standaard, 5 s).
    pub timeout: Nanos,
    /// Hoe lang een nieuwe taak mag doen over gezond worden (0 = standaard, 30 s).
    pub initial_timeout: Nanos,
    /// Opeenvolgende mislukkingen voordat de taak ongezond is (0 = standaard, 3).
    pub failure_threshold: i64,
}

/// Wat de gebruiker wil draaien. De naam is de unieke sleutel.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Job {
    /// De unieke naam.
    pub name: String,
    /// Node-attributen die allemaal moeten kloppen (AND, gelijkheid).
    pub affinity: Map<String>,
    /// De runner; `None` is afgeleid van `image` (zie [`Job::driver`]).
    pub driver: Option<Driver>,
    /// Het Docker-image (alleen voor Docker).
    pub image: String,
    /// De downloads; de agent kiest de eerste die bij zijn node past.
    pub artifacts: Vec<Artifact>,
    /// Draai als deze gebruiker (standaard: die van de agent).
    pub user: String,
    /// Het commando.
    pub command: String,
    /// Het aantal instanties: 0 is 1, -1 is "op elke agent".
    pub count: i64,
    /// Poortnaam naar hostpoort (0 = dynamisch).
    pub ports: Map<u16>,
    /// Relatieve CPU-prioriteit (0 = niet begrensd).
    pub cpu_shares: i64,
    /// Geheugengrens in bytes (0 = niet begrensd).
    pub memory_limit: u64,
    /// Extra omgevingsvariabelen.
    pub env: Map<String>,
    /// Labels voor discovery en groepering.
    pub tags: Map<String>,
    /// Hostpad naar taakpad.
    pub volumes: Map<String>,
    /// De health check, als die er is.
    pub health_check: Option<HealthCheck>,
    /// `None` = standaard (onbeperkt met backoff), 0 = nooit, -1 = onbeperkt,
    /// N = opgeven na N binnen `restart_window`.
    pub max_restarts: Option<i64>,
    /// Het venster waarin herstarts tellen (0 = standaard, 5 min).
    pub restart_window: Nanos,
    /// Hoe een update uitrolt; `None` is rolling.
    pub update_policy: Option<UpdatePolicy>,
    /// `None` = automatisch (achteraan), 0 = bovenaan, N = N-de plek.
    pub priority: Option<i64>,
    /// Waar terwijl een update uitrolt. Blijft staan als de uitrol faalt of
    /// de leader halverwege sterft: dat is de eerlijke waarheid ("de uitrol
    /// is niet af, de vloot kan gemengd zijn") in plaats van een valse
    /// "gezond". Opnieuw toepassen wist hem.
    pub deploying: bool,
}

impl Job {
    /// De runner: de gezette driver, of afgeleid van `image`.
    pub fn driver(&self) -> Driver {
        self.driver.unwrap_or(Driver::for_image(&self.image))
    }

    /// Of dit een daemon is (`count == -1`: één op elke agent).
    pub fn is_daemon(&self) -> bool {
        self.count == -1
    }

    /// Het gewenste aantal instanties van een gewone job (0 of minder is 1).
    ///
    /// Voor een daemon betekent dit getal niets; zie [`Job::is_daemon`].
    pub fn desired(&self) -> usize {
        usize::try_from(self.count).unwrap_or(0).max(1)
    }

    /// De update-policy, met rolling als standaard.
    pub fn policy(&self) -> UpdatePolicy {
        self.update_policy.unwrap_or_default()
    }

    /// Vult de boot-config-afkorting in: precies één artifact en geen
    /// commando of image kan alleen de hop-driver betekenen.
    ///
    /// Dat scheelt 15 bytes per entry, en de bootargs-buffer van de firmware
    /// is hard ~1 KB (gemeten 19-07: entry vier viel van elke cmdline af). De
    /// API en de init-jobs moeten het eens zijn, dus de regel staat hier.
    pub fn apply_hop_shorthand(&mut self) {
        if self.driver.is_none()
            && self.command.is_empty()
            && self.image.is_empty()
            && self.artifacts.len() == 1
        {
            self.driver = Some(Driver::Hop);
        }
    }

    /// Toetst of er iets te draaien valt: een commando, een image, of de
    /// hop-driver met minstens één artifact (meer artifacts is hoe één job
    /// meerdere architecturen overspant; de agent kiest per node).
    pub fn check_runnable(&self) -> Result {
        let hop_image = self.driver == Some(Driver::Hop) && !self.artifacts.is_empty();
        if self.command.is_empty() && self.image.is_empty() && !hop_image {
            return Err(Error::Invalid {
                field: Name::new(&self.name),
                why: "command or image required (or driver \"hop\" with at least one artifact)",
            });
        }
        Ok(())
    }

    /// Leest een job uit een JSON-waarde.
    ///
    /// `strict` weigert onbekende sleutels (de init-jobs in de config: een
    /// typefout daar is een instelling die niets doet). De API is laks,
    /// zoals Go's standaard-decoder.
    pub fn from_value(v: &Value, strict: bool) -> Result<Self> {
        let obj = de::object(v, "job")?;
        let mut job = Self::default();
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            match k {
                "name" => job.name = de::string(v, k)?,
                "affinity" => job.affinity = de::str_map(v, k, MAX_MAP_ENTRIES)?,
                "driver" => job.driver = parse_opt(v, k, Driver::parse)?,
                "image" => job.image = de::string(v, k)?,
                "artifacts" => {
                    job.artifacts =
                        de::list(v, k, MAX_ARTIFACTS, |a| Artifact::from_value(a, strict))?;
                }
                "user" => job.user = de::string(v, k)?,
                "command" => job.command = de::string(v, k)?,
                "count" => job.count = de::int(v, k)?,
                "ports" => job.ports = ports(v, k)?,
                "cpu_shares" => job.cpu_shares = de::int(v, k)?,
                "memory_limit" => job.memory_limit = de::uint(v, k)?,
                "env" => job.env = de::str_map(v, k, MAX_MAP_ENTRIES)?,
                "tags" => job.tags = de::str_map(v, k, MAX_MAP_ENTRIES)?,
                "volumes" => job.volumes = de::str_map(v, k, MAX_MAP_ENTRIES)?,
                "health_check" => job.health_check = Some(HealthCheck::from_value(v, strict)?),
                "max_restarts" => job.max_restarts = Some(de::int(v, k)?),
                "restart_window" => job.restart_window = de::duration(v, k)?,
                "update_policy" => job.update_policy = parse_opt(v, k, UpdatePolicy::parse)?,
                "priority" => job.priority = Some(de::int(v, k)?),
                "deploying" => job.deploying = de::boolean(v, k)?,
                _ => de::unknown(k, strict)?,
            }
        }
        Ok(job)
    }

    /// Leest een job uit JSON-tekst (laks, zoals de API).
    pub fn from_json(input: &[u8]) -> Result<Self> {
        Self::from_value(&json::parse(input)?, false)
    }

    /// De job als JSON-waarde, met Go's `omitempty`-regels.
    pub fn to_value(&self) -> Result<Value> {
        let mut o = ObjectBuilder::new();
        o.str("name", &self.name)?;
        o.map_opt("affinity", &self.affinity)?;
        if let Some(d) = self.driver {
            o.str("driver", d.as_str())?;
        }
        o.str_opt("image", &self.image)?;
        if !self.artifacts.is_empty() {
            let mut arr = Vec::new();
            arr.try_reserve_exact(self.artifacts.len())
                .map_err(|_| Error::OutOfMemory)?;
            for a in &self.artifacts {
                arr.push(a.to_value()?);
            }
            o.field("artifacts", Value::Array(arr))?;
        }
        o.str_opt("user", &self.user)?;
        o.str_opt("command", &self.command)?;
        o.int_opt("count", self.count)?;
        if !self.ports.is_empty() {
            let mut p = ObjectBuilder::new();
            for (k, v) in self.ports.iter() {
                p.field(k, Value::uint(u64::from(*v)))?;
            }
            o.field("ports", p.build())?;
        }
        o.int_opt("cpu_shares", self.cpu_shares)?;
        o.uint_opt("memory_limit", self.memory_limit)?;
        o.map_opt("env", &self.env)?;
        o.map_opt("tags", &self.tags)?;
        o.map_opt("volumes", &self.volumes)?;
        if let Some(hc) = &self.health_check {
            o.field("health_check", hc.to_value()?)?;
        }
        if let Some(n) = self.max_restarts {
            o.field("max_restarts", Value::int(n))?;
        }
        o.uint_opt("restart_window", self.restart_window)?;
        if let Some(p) = self.update_policy {
            o.str("update_policy", p.as_str())?;
        }
        if let Some(p) = self.priority {
            o.field("priority", Value::int(p))?;
        }
        if self.deploying {
            o.field("deploying", Value::Bool(true))?;
        }
        Ok(o.build())
    }

    /// De job als compacte JSON-tekst.
    pub fn to_json(&self) -> Result<String> {
        json::to_string(&self.to_value()?)
    }
}

impl Artifact {
    /// Leest een artifact uit een JSON-waarde.
    pub fn from_value(v: &Value, strict: bool) -> Result<Self> {
        let obj = de::object(v, "artifacts")?;
        let mut a = Self::default();
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            match k {
                "url" => a.url = de::string(v, k)?,
                "match" => a.matches = de::str_map(v, k, MAX_MAP_ENTRIES)?,
                "headers" => a.headers = de::str_map(v, k, MAX_MAP_ENTRIES)?,
                "auth" => a.auth = de::str_map(v, k, MAX_MAP_ENTRIES)?,
                "extract" => a.extract = de::string(v, k)?,
                "filename" => a.filename = de::string(v, k)?,
                _ => de::unknown(k, strict)?,
            }
        }
        Ok(a)
    }

    /// Het artifact als JSON-waarde.
    pub fn to_value(&self) -> Result<Value> {
        let mut o = ObjectBuilder::new();
        o.str("url", &self.url)?;
        o.map_opt("match", &self.matches)?;
        o.map_opt("headers", &self.headers)?;
        o.map_opt("auth", &self.auth)?;
        o.str_opt("extract", &self.extract)?;
        o.str_opt("filename", &self.filename)?;
        Ok(o.build())
    }
}

impl HealthCheck {
    /// Leest een health check uit een JSON-waarde.
    pub fn from_value(v: &Value, strict: bool) -> Result<Self> {
        let obj = de::object(v, "health_check")?;
        let mut hc = Self::default();
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            match k {
                "type" => hc.kind = parse_opt(v, k, CheckType::parse)?,
                "path" => hc.path = de::string(v, k)?,
                "port" => hc.port = de::string(v, k)?,
                "interval" => hc.interval = de::duration(v, k)?,
                "timeout" => hc.timeout = de::duration(v, k)?,
                "initial_timeout" => hc.initial_timeout = de::duration(v, k)?,
                "failure_threshold" => hc.failure_threshold = de::int(v, k)?,
                _ => de::unknown(k, strict)?,
            }
        }
        Ok(hc)
    }

    /// De health check als JSON-waarde.
    pub fn to_value(&self) -> Result<Value> {
        let mut o = ObjectBuilder::new();
        if let Some(t) = self.kind {
            o.str("type", t.as_str())?;
        }
        // `path` had in Go geen omitempty.
        o.str("path", &self.path)?;
        o.str_opt("port", &self.port)?;
        o.uint_opt("interval", self.interval)?;
        o.uint_opt("timeout", self.timeout)?;
        o.uint_opt("initial_timeout", self.initial_timeout)?;
        o.int_opt("failure_threshold", self.failure_threshold)?;
        Ok(o.build())
    }
}

/// Een optionele enum uit een string; leeg is `None`, onbekend is een fout.
fn parse_opt<T>(v: &Value, field: &str, parse: fn(&str) -> Option<T>) -> Result<Option<T>> {
    let s = v.as_str().ok_or(Error::WrongType {
        field: Name::new(field),
        want: "a string",
    })?;
    if s.is_empty() {
        return Ok(None);
    }
    parse(s).map(Some).ok_or(Error::Invalid {
        field: Name::new(field),
        why: "unknown value",
    })
}

/// De poortenmap: naam naar poort, elk getal in 0..=65535.
fn ports(v: &Value, field: &str) -> Result<Map<u16>> {
    let obj = de::object(v, field)?;
    if obj.len() > MAX_PORTS {
        return Err(Error::TooMany {
            field: Name::new(field),
            max: MAX_PORTS,
        });
    }
    let mut m = Map::new();
    for (k, item) in obj.iter() {
        let n = de::uint(item, field)?;
        let port = u16::try_from(n).map_err(|_| Error::OutOfRange {
            field: Name::new(field),
        })?;
        m.insert(crate::try_string(k)?, port)?;
    }
    Ok(m)
}

impl TryClone for Artifact {
    fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            url: self.url.try_clone()?,
            matches: self.matches.try_clone()?,
            headers: self.headers.try_clone()?,
            auth: self.auth.try_clone()?,
            extract: self.extract.try_clone()?,
            filename: self.filename.try_clone()?,
        })
    }
}

impl TryClone for HealthCheck {
    fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            kind: self.kind,
            path: self.path.try_clone()?,
            port: self.port.try_clone()?,
            interval: self.interval,
            timeout: self.timeout,
            initial_timeout: self.initial_timeout,
            failure_threshold: self.failure_threshold,
        })
    }
}

impl TryClone for Job {
    fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            name: self.name.try_clone()?,
            affinity: self.affinity.try_clone()?,
            driver: self.driver,
            image: self.image.try_clone()?,
            artifacts: self.artifacts.try_clone()?,
            user: self.user.try_clone()?,
            command: self.command.try_clone()?,
            count: self.count,
            ports: self.ports.try_clone()?,
            cpu_shares: self.cpu_shares,
            memory_limit: self.memory_limit,
            env: self.env.try_clone()?,
            tags: self.tags.try_clone()?,
            volumes: self.volumes.try_clone()?,
            health_check: self.health_check.try_clone()?,
            max_restarts: self.max_restarts,
            restart_window: self.restart_window,
            update_policy: self.update_policy,
            priority: self.priority,
            deploying: self.deploying,
        })
    }
}

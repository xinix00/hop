//! Node- en clusterconfiguratie: de velden, de defaults, het lezen en toetsen.
//!
//! Deze crate bezit de vorm van de configuratie en de regels om hem uit
//! JSON te lezen; hij leest geen bestand. De host-daemon leest de bytes en
//! geeft ze aan [`Config::from_json`]; bestaat het bestand niet, dan is
//! [`Config::default`] de configuratie (net als `pkg/config.Load` in Go).
//!
//! Het formaat is JSON en niet YAML, en de reden is niet smaak: de Go-kern
//! van HopOS importeerde dit pakket, en `gopkg.in/yaml.v3` kostte hem 3.440
//! bytes symbolen plus de hele regexp-machine (62.576 bytes), omdat yaml's
//! package-init `regexp.MustCompile` aanroept. Alles wat een job of een
//! lease beschrijft was al JSON, dus het formaat werd hetzelfde.
//!
//! Onbekende sleutels zijn een fout: een typefout in een sleutel is anders
//! een instelling die er lijkt te staan en niets doet, en dat kost een avond.

#![cfg_attr(not(test), no_std)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]
#![forbid(unsafe_code)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use types::de::{self, ObjectBuilder};
use types::json::{self, Object, Value};
use types::time::{Nanos, SECOND, write_duration};
use types::{Map, Name, TryClone};

/// Hoeveel init-jobs een config hoogstens noemt. De firmware-bootargs van
/// een HopOS-board zijn hard ~1 KB (gemeten 19-07: de vierde entry viel van
/// elke cmdline af), dus in de praktijk zijn het er een paar; 64 is de grens
/// waarboven het een vergissing is.
pub const MAX_INIT_JOBS: usize = 64;

/// Hoeveel eigen node-attributen een config hoogstens noemt.
pub const MAX_ATTRIBUTES: usize = 64;

/// Wat er mis kan gaan bij het lezen van een configuratie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// De inhoud begint niet met `{`: waarschijnlijk een oud YAML-bestand.
    NotJson {
        /// De eerste bytes van het bestand, voor de melding.
        head: Name,
    },
    /// Een fout in de JSON of in een veld.
    Json(types::Error),
}

impl From<types::Error> for Error {
    fn from(e: types::Error) -> Self {
        Self::Json(e)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Zonder deze melding is de fout "invalid JSON at byte 0" op een
            // YAML-bestand, en dat leest niemand als "het formaat is gewisseld".
            Self::NotJson { head } => write!(
                f,
                "config is JSON, and this is not (it starts with {head:?}): a YAML config \
                 converts to JSON one-to-one: the same keys, in braces, with durations \
                 quoted (\"30s\")"
            ),
            Self::Json(e) => write!(f, "{e}"),
        }
    }
}

impl core::error::Error for Error {}

/// Het resultaat van elke faalbare handeling in deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

/// De hele configuratie van een node.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Config {
    /// Deze node.
    pub node: NodeConfig,
    /// De cluster waar hij bij hoort.
    pub cluster: ClusterConfig,
    /// Hoeveel CPU en geheugen Hop hier mag vergeven.
    pub capacity: CapacityConfig,
    /// Paden op het bestandssysteem.
    pub paths: PathsConfig,
    /// De runners.
    pub runner: RunnerConfig,
    /// Timeouts.
    pub timeouts: TimeoutsConfig,
    /// De gedeelde HMAC-sleutel (`X-Hop-Auth`); leeg is geen authenticatie.
    pub api_key: String,
}

/// De instellingen van deze node.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeConfig {
    /// Het node-id; leeg is "genereer en bewaar in `data/node-id`".
    pub id: String,
    /// Het IP om te adverteren; leeg is automatisch.
    pub ip: String,
    /// De agent-poort; de leader luistert op deze plus 1000.
    pub port: u16,
    /// Een CIDR (`"10.0.0.0/24"`) waarbinnen het interface-IP gekozen wordt
    /// als `ip` leeg is; voor clusters die op een eigen LAN of VPN adverteren.
    pub network: String,
    /// Eigen node-attributen, samengevoegd met de automatisch gevonden.
    pub attributes: Map<String>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            ip: String::new(),
            port: 8080,
            network: String::new(),
            attributes: Map::new(),
        }
    }
}

/// Clusterbrede instellingen.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterConfig {
    /// De clusternaam (standaard `"default"`).
    pub name: String,
    /// De backend voor de leader-lease.
    pub lock: LockConfig,
    /// Jobs die een schone boot eenmalig zaait (geen snapshot, lege store).
    ///
    /// Ze blijven ruwe JSON-objecten met de veldnamen van `POST /v1/jobs`,
    /// zodat een spec kopieerbaar is tussen config en API; de leader leest
    /// ze strikt als [`types::Job`].
    pub init_jobs: Vec<Value>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            name: String::from("default"),
            lock: LockConfig::default(),
            init_jobs: Vec::new(),
        }
    }
}

/// De hoplock-backend voor de leaderverkiezing; leeg is standalone.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LockConfig {
    /// `""` of `"hoplockserver"`, `"s3"`, of `"mem"`.
    pub kind: String,
    /// De basis-URL van de hoplockserver.
    pub url: String,
    /// De sleutel van het lease-object (standaard `clusters/<naam>/lease.json`).
    pub key: String,
    /// De `X-API-Key` voor de hoplockserver.
    pub api_key: String,
    /// De S3-instellingen als `kind` `"s3"` is.
    pub s3: S3LockConfig,
}

/// Een S3-compatibele object-store voor lease en snapshot.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct S3LockConfig {
    /// Het endpoint.
    pub endpoint: String,
    /// De bucket.
    pub bucket: String,
    /// De regio.
    pub region: String,
    /// De access key.
    pub access_key_id: String,
    /// De secret key.
    pub secret_access_key: String,
    /// Een sessietoken, als die er is.
    pub session_token: String,
    /// Pad-stijl adressering (MinIO en dergelijke).
    pub use_path_style: bool,
}

/// Een plafond onder de hardware; 0 is "gebruik wat er gemeten wordt".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CapacityConfig {
    /// CPU-shares (1024 per core).
    pub cpu_shares: u64,
    /// Geheugen in bytes.
    pub memory: u64,
}

/// Paden op het bestandssysteem.
#[derive(Debug, Clone, PartialEq)]
pub struct PathsConfig {
    /// De lokale staat in standalone-modus.
    pub state_file: String,
    /// Waar taakmappen komen.
    pub rootfs_base: String,
}

impl Default for PathsConfig {
    fn default() -> Self {
        Self {
            state_file: String::from("./data/state.json"),
            rootfs_base: String::from("/tmp/hop"),
        }
    }
}

/// De runners.
#[derive(Debug, Clone, PartialEq)]
pub struct RunnerConfig {
    /// Procesisolatie (chroot op Linux, sandbox op macOS); standaard aan.
    pub isolate: bool,
    /// Het pad van de Docker-socket.
    pub docker_socket: String,
    /// Hoeveel regels stdout/stderr per taak in geheugen blijven. Standaard
    /// 50, klein met opzet: Hop draait op boards met een paar honderd MB.
    pub log_tail_lines: u64,
    /// Hoe lang de staart van een gestopte taak leesbaar blijft (seconden),
    /// zodat een crash achteraf te lezen is. Standaard 300.
    pub log_keep_seconds: u64,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            isolate: true,
            docker_socket: String::from("/var/run/docker.sock"),
            log_tail_lines: 50,
            log_keep_seconds: 300,
        }
    }
}

/// Timeouts, als duren in nanoseconden; in JSON als Go-duurstrings (`"30s"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutsConfig {
    /// Hoe vaak een health check loopt.
    pub health_check_interval: Nanos,
    /// Hoe lang één health check mag duren.
    pub health_check_timeout: Nanos,
    /// Na hoe lang zonder heartbeat een agent dood is.
    pub node_dead_threshold: Nanos,
    /// Hoe lang een leader-lease geldt.
    pub leader_lease: Nanos,
}

impl Default for TimeoutsConfig {
    fn default() -> Self {
        Self {
            health_check_interval: 5 * SECOND,
            health_check_timeout: 5 * SECOND,
            node_dead_threshold: 30 * SECOND,
            leader_lease: 30 * SECOND,
        }
    }
}

impl Config {
    /// Leest een configuratie uit JSON, bovenop de defaults.
    ///
    /// Wat niet genoemd wordt houdt zijn default, ook binnen een blok dat
    /// wel genoemd wordt. Onbekende sleutels, een duur als getal, een
    /// tweede document en een niet-JSON-bestand zijn fouten.
    pub fn from_json(data: &[u8]) -> Result<Self> {
        let head = trim_start(data);
        if !head.is_empty() && head.first() != Some(&b'{') {
            let n = head.len().min(24);
            let text = core::str::from_utf8(head.get(..n).unwrap_or_default()).unwrap_or("?");
            return Err(Error::NotJson {
                head: Name::new(text),
            });
        }
        let mut cfg = Self::default();
        if head.is_empty() {
            return Ok(cfg);
        }
        let v = json::parse(data)?;
        cfg.apply(de::object(&v, "config")?)?;
        Ok(cfg)
    }

    fn apply(&mut self, obj: &Object) -> Result {
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            match k {
                "node" => self.node.apply(de::object(v, k)?)?,
                "cluster" => self.cluster.apply(de::object(v, k)?)?,
                "capacity" => self.capacity.apply(de::object(v, k)?)?,
                "paths" => self.paths.apply(de::object(v, k)?)?,
                "runner" => self.runner.apply(de::object(v, k)?)?,
                "timeouts" => self.timeouts.apply(de::object(v, k)?)?,
                "api_key" => self.api_key = de::string(v, k)?,
                _ => de::unknown(k, true)?,
            }
        }
        Ok(())
    }

    /// De configuratie als JSON-waarde, in een vorm die [`Config::from_json`]
    /// weer leest.
    pub fn to_value(&self) -> Result<Value> {
        let mut o = ObjectBuilder::new();
        o.field("node", self.node.to_value()?)?;
        o.field("cluster", self.cluster.to_value()?)?;
        let mut c = ObjectBuilder::new();
        c.field("cpu_shares", Value::uint(self.capacity.cpu_shares))?;
        c.field("memory", Value::uint(self.capacity.memory))?;
        o.field("capacity", c.build())?;
        let mut p = ObjectBuilder::new();
        p.str("state_file", &self.paths.state_file)?;
        p.str("rootfs_base", &self.paths.rootfs_base)?;
        o.field("paths", p.build())?;
        let mut r = ObjectBuilder::new();
        r.field("isolate", Value::Bool(self.runner.isolate))?;
        r.str("docker_socket", &self.runner.docker_socket)?;
        r.field("log_tail_lines", Value::uint(self.runner.log_tail_lines))?;
        r.field(
            "log_keep_seconds",
            Value::uint(self.runner.log_keep_seconds),
        )?;
        o.field("runner", r.build())?;
        o.field("timeouts", self.timeouts.to_value()?)?;
        o.str("api_key", &self.api_key)?;
        Ok(o.build())
    }

    /// De configuratie als compacte JSON-tekst.
    pub fn to_json(&self) -> Result<String> {
        Ok(json::to_string(&self.to_value()?)?)
    }
}

impl NodeConfig {
    fn apply(&mut self, obj: &Object) -> Result {
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            match k {
                "id" => self.id = de::string(v, k)?,
                "ip" => self.ip = de::string(v, k)?,
                "port" => {
                    self.port =
                        u16::try_from(de::uint(v, k)?).map_err(|_| types::Error::OutOfRange {
                            field: Name::new("node.port"),
                        })?;
                }
                "network" => self.network = de::string(v, k)?,
                "attributes" => self.attributes = de::str_map(v, k, MAX_ATTRIBUTES)?,
                _ => de::unknown(k, true)?,
            }
        }
        Ok(())
    }

    fn to_value(&self) -> Result<Value> {
        let mut o = ObjectBuilder::new();
        o.str("id", &self.id)?;
        o.str("ip", &self.ip)?;
        o.field("port", Value::uint(u64::from(self.port)))?;
        o.str("network", &self.network)?;
        o.field("attributes", de::str_map_value(&self.attributes)?)?;
        Ok(o.build())
    }
}

impl ClusterConfig {
    fn apply(&mut self, obj: &Object) -> Result {
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            match k {
                "name" => self.name = de::string(v, k)?,
                "lock" => self.lock.apply(de::object(v, k)?)?,
                "init_jobs" => {
                    self.init_jobs = de::list(v, k, MAX_INIT_JOBS, |item| {
                        de::object(item, "init_jobs")?;
                        item.try_clone()
                    })?;
                }
                _ => de::unknown(k, true)?,
            }
        }
        Ok(())
    }

    fn to_value(&self) -> Result<Value> {
        let mut o = ObjectBuilder::new();
        o.str("name", &self.name)?;
        o.field("lock", self.lock.to_value()?)?;
        o.field("init_jobs", Value::Array(self.init_jobs.try_clone()?))?;
        Ok(o.build())
    }
}

impl LockConfig {
    fn apply(&mut self, obj: &Object) -> Result {
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            match k {
                "type" => self.kind = de::string(v, k)?,
                "url" => self.url = de::string(v, k)?,
                "key" => self.key = de::string(v, k)?,
                "api_key" => self.api_key = de::string(v, k)?,
                "s3" => self.s3.apply(de::object(v, k)?)?,
                _ => de::unknown(k, true)?,
            }
        }
        Ok(())
    }

    fn to_value(&self) -> Result<Value> {
        let mut o = ObjectBuilder::new();
        o.str("type", &self.kind)?;
        o.str("url", &self.url)?;
        o.str("key", &self.key)?;
        o.str("api_key", &self.api_key)?;
        let s = &self.s3;
        let mut s3 = ObjectBuilder::new();
        s3.str("endpoint", &s.endpoint)?;
        s3.str("bucket", &s.bucket)?;
        s3.str("region", &s.region)?;
        s3.str("access_key_id", &s.access_key_id)?;
        s3.str("secret_access_key", &s.secret_access_key)?;
        s3.str("session_token", &s.session_token)?;
        s3.field("use_path_style", Value::Bool(s.use_path_style))?;
        o.field("s3", s3.build())?;
        Ok(o.build())
    }
}

impl S3LockConfig {
    fn apply(&mut self, obj: &Object) -> Result {
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            match k {
                "endpoint" => self.endpoint = de::string(v, k)?,
                "bucket" => self.bucket = de::string(v, k)?,
                "region" => self.region = de::string(v, k)?,
                "access_key_id" => self.access_key_id = de::string(v, k)?,
                "secret_access_key" => self.secret_access_key = de::string(v, k)?,
                "session_token" => self.session_token = de::string(v, k)?,
                "use_path_style" => self.use_path_style = de::boolean(v, k)?,
                _ => de::unknown(k, true)?,
            }
        }
        Ok(())
    }
}

impl CapacityConfig {
    fn apply(&mut self, obj: &Object) -> Result {
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            match k {
                "cpu_shares" => self.cpu_shares = de::uint(v, k)?,
                "memory" => self.memory = de::uint(v, k)?,
                _ => de::unknown(k, true)?,
            }
        }
        Ok(())
    }
}

impl PathsConfig {
    fn apply(&mut self, obj: &Object) -> Result {
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            match k {
                "state_file" => self.state_file = de::string(v, k)?,
                "rootfs_base" => self.rootfs_base = de::string(v, k)?,
                _ => de::unknown(k, true)?,
            }
        }
        Ok(())
    }
}

impl RunnerConfig {
    fn apply(&mut self, obj: &Object) -> Result {
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            match k {
                "isolate" => self.isolate = de::boolean(v, k)?,
                "docker_socket" => self.docker_socket = de::string(v, k)?,
                "log_tail_lines" => self.log_tail_lines = de::uint(v, k)?,
                "log_keep_seconds" => self.log_keep_seconds = de::uint(v, k)?,
                _ => de::unknown(k, true)?,
            }
        }
        Ok(())
    }
}

impl TimeoutsConfig {
    /// Leest de duren als strings. Een getal is bewust een fout en niet "dan
    /// zijn het nanoseconden": een `leader_lease` van 30 die stil 30 ns wordt,
    /// is een cluster dat zijn leider duizenden keren per seconde verliest.
    fn apply(&mut self, obj: &Object) -> Result {
        for (k, v) in obj.iter() {
            if v.is_null() {
                continue;
            }
            let slot = match k {
                "health_check_interval" => &mut self.health_check_interval,
                "health_check_timeout" => &mut self.health_check_timeout,
                "node_dead_threshold" => &mut self.node_dead_threshold,
                "leader_lease" => &mut self.leader_lease,
                _ => {
                    de::unknown(k, true)?;
                    continue;
                }
            };
            let mut path = String::new();
            types::try_push_str(&mut path, "timeouts.")?;
            types::try_push_str(&mut path, k)?;
            let Some(s) = v.as_str() else {
                return Err(types::Error::Invalid {
                    field: Name::new(&path),
                    why: "durations are strings like \"30s\"",
                }
                .into());
            };
            *slot = de::duration_str(s, &path)?;
        }
        Ok(())
    }

    fn to_value(self) -> Result<Value> {
        let mut o = ObjectBuilder::new();
        for (k, d) in [
            ("health_check_interval", self.health_check_interval),
            ("health_check_timeout", self.health_check_timeout),
            ("node_dead_threshold", self.node_dead_threshold),
            ("leader_lease", self.leader_lease),
        ] {
            let mut s = String::new();
            write_duration(d, &mut s)?;
            o.field(k, Value::String(s))?;
        }
        Ok(o.build())
    }
}

fn trim_start(data: &[u8]) -> &[u8] {
    let n = data
        .iter()
        .take_while(|c| matches!(c, b' ' | b'\t' | b'\r' | b'\n'))
        .count();
    data.get(n..).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    //! De tests van `OLD/pkg/config/config_test.go`. Go las een bestand; hier
    //! krijgt de lezer de bytes, dus "geen bestand" is de default zelf.

    use super::*;
    use alloc::string::ToString;
    use types::time::MILLISECOND;

    fn load(s: &str) -> Result<Config> {
        Config::from_json(s.as_bytes())
    }

    #[test]
    fn load_zonder_bestand_geeft_defaults() {
        // Geen bestand: de host geeft dan Config::default(). Een leeg
        // bestand is hetzelfde geval, en dat is de gewone weg voor een node
        // die alles auto-detecteert.
        let cfg = load("").unwrap();
        assert_eq!(cfg.node.port, 8080);
        assert_eq!(cfg.cluster.name, "default");
        assert_eq!(cfg, Config::default());
    }

    #[test]
    fn load_overschrijft_alleen_wat_er_staat() {
        let cfg = load(
            r#"{
	  "node": {"id": "node-1", "port": 9000, "attributes": {"core-class": "big"}},
	  "cluster": {"name": "dev", "lock": {"type": "s3", "s3": {"bucket": "hop-leases", "use_path_style": true}}},
	  "api_key": "geheim",
	  "timeouts": {"leader_lease": "45s"}
	}"#,
        )
        .unwrap();
        assert_eq!(cfg.node.id, "node-1");
        assert_eq!(cfg.node.port, 9000);
        assert_eq!(cfg.node.attributes.get("core-class").unwrap(), "big");
        assert_eq!(cfg.cluster.lock.kind, "s3");
        assert_eq!(cfg.cluster.lock.s3.bucket, "hop-leases");
        assert!(cfg.cluster.lock.s3.use_path_style);
        assert_eq!(cfg.api_key, "geheim");
        assert_eq!(cfg.timeouts.leader_lease, 45 * SECOND);
        // Niet genoemd = default blijft staan, ook binnen een blok dat wél
        // genoemd werd (timeouts noemde alleen leader_lease).
        assert_eq!(cfg.timeouts.health_check_interval, 5 * SECOND);
        assert_eq!(cfg.paths.state_file, "./data/state.json");
        assert!(cfg.runner.isolate);
    }

    #[test]
    fn load_duren() {
        let cfg = load(
            r#"{"timeouts": {
	  "health_check_interval": "10s", "health_check_timeout": "1500ms",
	  "node_dead_threshold": "1m30s", "leader_lease": "0s"}}"#,
        )
        .unwrap();
        assert_eq!(
            cfg.timeouts,
            TimeoutsConfig {
                health_check_interval: 10 * SECOND,
                health_check_timeout: 1500 * MILLISECOND,
                node_dead_threshold: 90 * SECOND,
                leader_lease: 0,
            }
        );
    }

    #[test]
    fn load_weigert() {
        let cases = [
            ("onbekende sleutel", r#"{"node": {"prt": 8080}}"#, "prt"),
            (
                "onbekende sleutel in timeouts",
                r#"{"timeouts": {"leader_leas": "30s"}}"#,
                "leader_leas",
            ),
            (
                "duur als getal",
                r#"{"timeouts": {"leader_lease": 30}}"#,
                "durations are strings",
            ),
            (
                "duur zonder eenheid",
                r#"{"timeouts": {"leader_lease": "30"}}"#,
                "not a duration",
            ),
            (
                "negatieve duur",
                r#"{"timeouts": {"leader_lease": "-30s"}}"#,
                "negative",
            ),
            ("geen JSON", "node:\n  id: node-1\n", "config is JSON"),
            (
                "YAML-comment bovenaan",
                "# Hop config\nnode:\n  id: x\n",
                "config is JSON",
            ),
            (
                "twee documenten",
                r#"{"node": {"id": "a"}} {"node": {"id": "b"}}"#,
                "more than one",
            ),
            ("stuk JSON", r#"{"node": {"id": "#, ""),
            ("verkeerd type", r#"{"node": {"port": "8080"}}"#, ""),
        ];
        for (naam, inhoud, bevat) in cases {
            let err = load(inhoud).expect_err(naam);
            assert!(
                err.to_string().contains(bevat),
                "{naam}: fout {err} noemt {bevat:?} niet"
            );
        }
    }

    // De YAML-hint is er voor precies één moment: iemand die na de wissel
    // zijn oude bestand meegeeft. Die moet lezen wat er aan de hand is.
    #[test]
    fn yaml_hint_noemt_het_formaat() {
        let err = load("cluster:\n  name: haas-prod\n")
            .unwrap_err()
            .to_string();
        for woord in ["JSON", "YAML", "cluster:"] {
            assert!(err.contains(woord), "melding {err:?} mist {woord:?}");
        }
    }

    // Wat Load leest moet de schrijver weer kunnen schrijven: anders is een
    // configuratie die Hop zelf uitschrijft niet terug te lezen.
    #[test]
    fn rondje_marshal() {
        let mut cfg = Config::default();
        cfg.cluster.name = "haas-prod".to_string();
        cfg.timeouts.health_check_timeout = 2500 * MILLISECOND;
        cfg.node
            .attributes
            .insert("k".to_string(), "v".to_string())
            .unwrap();
        let b = cfg.to_json().unwrap();
        let terug = load(&b).unwrap();
        assert_eq!(terug, cfg);
    }

    #[test]
    fn init_jobs_blijven_ruw_json() {
        // init_jobs volgen het job-schema van POST /v1/jobs; ze blijven dus
        // ruwe objecten en moeten copy-pasteable blijven.
        let cfg = load(
            r#"{"cluster": {"init_jobs": [
	  {"name": "welcome", "driver": "hop", "count": -1, "artifacts": [{"url": "http://x/app.elf"}]}]}}"#,
        )
        .unwrap();
        assert_eq!(cfg.cluster.init_jobs.len(), 1);
        let job = cfg.cluster.init_jobs[0].as_object().unwrap();
        assert_eq!(job.get("name").unwrap().as_str(), Some("welcome"));
        assert_eq!(job.get("count").unwrap().as_i64(), Some(-1));
        // En ze lezen als echte job.
        let parsed = types::Job::from_value(&cfg.cluster.init_jobs[0], true).unwrap();
        assert_eq!(parsed.driver, Some(types::Driver::Hop));
    }
}

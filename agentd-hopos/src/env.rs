//! De configuratie die de kern in de env van het slot van Hop zet.
//!
//! In de Go-generatie las de kern `hopos.cfg` en de bootparams
//! (`hopos.node`, `hopos.apikey`, `hopos.cluster`, `hopos.insecure`,
//! `hopos.s3.*`, `hopos.init[]`) en gaf ze in-proces aan `agentboot`. Nu is
//! Hop een app; de kern geeft dezelfde keuzes als env-blob op de
//! control-page, onder deze namen:
//!
//! | Variabele | Betekenis | Standaard |
//! | --- | --- | --- |
//! | `HOPOS_NODE` | het node-id | `hopos-<slot>` |
//! | `HOPOS_APIKEY` | de HMAC-sleutel van de API (`X-Hop-Auth`) | geen: de API start niet |
//! | `HOPOS_INSECURE` | `1`: bewust zonder sleutel (bank, dev) | uit |
//! | `HOPOS_CLUSTER` | de clusternaam | `hopos` |
//! | `HOPOS_NODE_IP` | het LAN-adres in het endpoint dat de leader ziet | het slot-IP |
//! | `HOPOS_PORT` | de agent-poort; de leader luistert op poort + 1000 | `8080` |
//! | `HOPOS_CORES` | de app-cores waar Hop tegen plant | `1` |
//! | `HOPOS_MEMORY` | het app-geheugen in bytes waar Hop tegen plant | 256 MiB |
//! | `HOPOS_S3_ENDPOINT`, `_BUCKET`, `_REGION`, `_KEY`, `_SECRET`, `_PATHSTYLE` | de bucket van de node: de object-store van de apps, en met `HOPOS_LOCK_TYPE=s3` de lock van de cluster | geen |
//! | `HOPOS_INIT_JOBS` | de init-jobs als één JSON-array (`hopos.init[]`) | [`INIT_JOBS_FILE`] als die er is |
//!
//! De sleutel wint: met `HOPOS_APIKEY` én `HOPOS_INSECURE=1` authenticeert
//! de API (zoals de Go-kern), en de bewoner zegt dat de vlag genegeerd is.
//!
//! De init-jobs staan in de env zolang ze passen (de kern kiest dat:
//! `hopos/src/config.rs` in HopOS); past het niet, dan laat de kern ze weg
//! en leest de bewoner [`INIT_JOBS_FILE`] in zijn volume. Ze worden alleen
//! bij een schone boot gezaaid ([`crate::Node::seed_init_jobs`]).
//!
//! De lock van de cluster (`HOPOS_LOCK_*`, `HOPOS_LEASE_TTL`,
//! `HOPOS_ADVERTISE`) leest [`crate::lock`]; zonder lock is de node de
//! standalone leader.
//!
//! Geheimen (`HOPOS_APIKEY`, `HOPOS_S3_SECRET`) komen nooit op het log: de
//! `Debug` van [`BootConfig`] en [`S3Config`] toont alleen hun lengte.

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

/// Het node-id.
pub const ENV_NODE: &str = "HOPOS_NODE";
/// De HMAC-sleutel.
pub const ENV_APIKEY: &str = "HOPOS_APIKEY";
/// `1` is een open API met opzet.
pub const ENV_INSECURE: &str = "HOPOS_INSECURE";
/// De clusternaam.
pub const ENV_CLUSTER: &str = "HOPOS_CLUSTER";
/// Het LAN-adres van de node.
pub const ENV_NODE_IP: &str = "HOPOS_NODE_IP";
/// De agent-poort.
pub const ENV_PORT: &str = "HOPOS_PORT";
/// De app-cores.
pub const ENV_CORES: &str = "HOPOS_CORES";
/// Het app-geheugen.
pub const ENV_MEMORY: &str = "HOPOS_MEMORY";
/// De init-jobs als JSON-array.
pub const ENV_INIT_JOBS: &str = "HOPOS_INIT_JOBS";
/// Het S3-endpoint.
pub const ENV_S3_ENDPOINT: &str = "HOPOS_S3_ENDPOINT";
/// De S3-bucket.
pub const ENV_S3_BUCKET: &str = "HOPOS_S3_BUCKET";
/// De S3-regio.
pub const ENV_S3_REGION: &str = "HOPOS_S3_REGION";
/// Het S3-sleutel-id.
pub const ENV_S3_KEY: &str = "HOPOS_S3_KEY";
/// Het S3-geheim.
pub const ENV_S3_SECRET: &str = "HOPOS_S3_SECRET";
/// `1`: S3 met path-style adressen.
pub const ENV_S3_PATHSTYLE: &str = "HOPOS_S3_PATHSTYLE";

/// De init-jobs als bestand, in het volume van Hop, als de env ze niet
/// droeg.
pub const INIT_JOBS_FILE: &str = "/hop/init-jobs.json";

/// Het geheugen waar Hop tegen plant als de kern het niet zegt.
pub const DEFAULT_MEMORY: u64 = 256 << 20;

/// De S3-instellingen van de cluster.
#[derive(Clone, PartialEq, Eq)]
pub struct S3Config {
    /// Het endpoint (`https://...`).
    pub endpoint: String,
    /// De bucket.
    pub bucket: String,
    /// De regio; leeg mag.
    pub region: String,
    /// Het sleutel-id.
    pub key: String,
    /// Het geheim; nooit op het log.
    pub secret: String,
    /// Path-style adressen.
    pub path_style: bool,
}

impl fmt::Debug for S3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("key", &self.key)
            .field("secret", &Redacted(self.secret.len()))
            .field("path_style", &self.path_style)
            .finish()
    }
}

/// Een geheim in `Debug`: alleen zijn lengte.
struct Redacted(usize);

impl fmt::Debug for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{} bytes>", self.0)
    }
}

/// Wat de bewoner uit de env leest.
#[derive(Clone, PartialEq, Eq)]
pub struct BootConfig {
    /// Het node-id.
    pub node_id: String,
    /// De HMAC-sleutel; leeg alleen met `insecure`.
    pub api_key: Vec<u8>,
    /// Bewust zonder sleutel: `HOPOS_INSECURE=1` en geen sleutel.
    pub insecure: bool,
    /// `HOPOS_INSECURE=1` stond er, maar de sleutel wint.
    pub insecure_ignored: bool,
    /// De clusternaam.
    pub cluster: String,
    /// Het LAN-adres in het endpoint.
    pub node_ip: String,
    /// De agent-poort; de leader op poort + 1000.
    pub port: u16,
    /// De app-cores.
    pub cores: u32,
    /// Het app-geheugen in bytes.
    pub memory: u64,
    /// Of `HOPOS_MEMORY` ontbrak (de bewoner meldt dat luid).
    pub memory_defaulted: bool,
    /// De clusteropslag, als endpoint en bucket er allebei zijn.
    pub s3: Option<S3Config>,
    /// De init-jobs uit de env (de ruwe JSON-array), als ze er zijn.
    pub init_jobs: Option<String>,
}

impl fmt::Debug for BootConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BootConfig")
            .field("node_id", &self.node_id)
            .field("api_key", &Redacted(self.api_key.len()))
            .field("insecure", &self.insecure)
            .field("insecure_ignored", &self.insecure_ignored)
            .field("cluster", &self.cluster)
            .field("node_ip", &self.node_ip)
            .field("port", &self.port)
            .field("cores", &self.cores)
            .field("memory", &self.memory)
            .field("memory_defaulted", &self.memory_defaulted)
            .field("s3", &self.s3)
            .field("init_jobs", &self.init_jobs.as_ref().map(String::len))
            .finish()
    }
}

/// Waarom de bewoner niet start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootError {
    /// Geen sleutel en geen `HOPOS_INSECURE=1`.
    ///
    /// Fail closed, zoals de Go-kern: een lege sleutel zet de HMAC-toets uit,
    /// en een API op het LAN zonder toets is code-uitvoering voor iedereen.
    NoKey,
    /// Een variabele met een waarde die geen getal of poort is.
    Bad {
        /// De variabele.
        var: &'static str,
        /// De waarde.
        value: String,
    },
}

impl fmt::Display for BootError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BootError::NoKey => write!(
                f,
                "no {ENV_APIKEY}: the API would accept unauthenticated job dispatch; set {ENV_APIKEY}, or {ENV_INSECURE}=1 on purpose"
            ),
            BootError::Bad { var, value } => write!(f, "{var}={value:?} is not valid"),
        }
    }
}

/// De S3-instellingen, als endpoint en bucket er allebei zijn (zoals Go:
/// allebei nodig om S3 te kiezen).
fn s3(get: &impl Fn(&str) -> Option<String>) -> Option<S3Config> {
    let endpoint = get(ENV_S3_ENDPOINT).filter(|v| !v.is_empty())?;
    let bucket = get(ENV_S3_BUCKET).filter(|v| !v.is_empty())?;
    Some(S3Config {
        endpoint,
        bucket,
        region: get(ENV_S3_REGION).unwrap_or_default(),
        key: get(ENV_S3_KEY).unwrap_or_default(),
        secret: get(ENV_S3_SECRET).unwrap_or_default(),
        path_style: get(ENV_S3_PATHSTYLE).as_deref() == Some("1"),
    })
}

/// Een getal uit de env, of `default` als hij ontbreekt.
fn number<T: core::str::FromStr>(
    get: &impl Fn(&str) -> Option<String>,
    var: &'static str,
    default: T,
) -> Result<T, BootError> {
    match get(var) {
        None => Ok(default),
        Some(v) => v
            .trim()
            .parse()
            .map_err(|_| BootError::Bad { var, value: v }),
    }
}

impl BootConfig {
    /// Leest de config met `get` (de env van het slot); `slot_ip` is het adres als `HOPOS_NODE_IP` ontbreekt.
    pub fn from_env(
        get: impl Fn(&str) -> Option<String>,
        slot: u64,
        slot_ip: &str,
    ) -> Result<Self, BootError> {
        let api_key = get(ENV_APIKEY).unwrap_or_default().into_bytes();
        let flag = get(ENV_INSECURE).as_deref() == Some("1");
        if api_key.is_empty() && !flag {
            return Err(BootError::NoKey);
        }
        let port: u16 = number(&get, ENV_PORT, 8080)?;
        if port == 0 || port > u16::MAX - 1000 {
            return Err(BootError::Bad {
                var: ENV_PORT,
                value: alloc::format!("{port}"),
            });
        }
        let memory_raw = get(ENV_MEMORY);
        Ok(Self {
            node_id: get(ENV_NODE).unwrap_or_else(|| alloc::format!("hopos-{slot}")),
            insecure: flag && api_key.is_empty(),
            insecure_ignored: flag && !api_key.is_empty(),
            api_key,
            cluster: get(ENV_CLUSTER).unwrap_or_else(|| String::from("hopos")),
            node_ip: get(ENV_NODE_IP).unwrap_or_else(|| String::from(slot_ip)),
            port,
            cores: number(&get, ENV_CORES, 1)?.max(1),
            memory: number(&get, ENV_MEMORY, DEFAULT_MEMORY)?,
            memory_defaulted: memory_raw.is_none(),
            s3: s3(&get),
            init_jobs: get(ENV_INIT_JOBS).filter(|j| !j.trim().is_empty()),
        })
    }

    /// De leader-poort: agent-poort plus 1000, zoals in Go.
    pub fn leader_port(&self) -> u16 {
        self.port.saturating_add(1000)
    }
}

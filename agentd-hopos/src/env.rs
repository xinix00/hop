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
//!
//! Nog niet gelezen (een volgende stap, met de clusterstaat op S3):
//! `HOPOS_S3_ENDPOINT`, `HOPOS_S3_BUCKET`, `HOPOS_S3_REGION`,
//! `HOPOS_S3_KEY`, `HOPOS_S3_SECRET`, en de init-jobs. Een env-blob is
//! hoogstens ~3,8 KB (`CTRL_ENV_MAX`), dus init-jobs horen eerder in een
//! bestand op hopfs dan in de env.

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

/// Het geheugen waar Hop tegen plant als de kern het niet zegt.
pub const DEFAULT_MEMORY: u64 = 256 << 20;

/// Wat de bewoner uit de env leest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootConfig {
    /// Het node-id.
    pub node_id: String,
    /// De HMAC-sleutel; leeg alleen met `insecure`.
    pub api_key: Vec<u8>,
    /// Bewust zonder sleutel.
    pub insecure: bool,
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
        let insecure = get(ENV_INSECURE).as_deref() == Some("1");
        if api_key.is_empty() && !insecure {
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
            api_key,
            insecure,
            cluster: get(ENV_CLUSTER).unwrap_or_else(|| String::from("hopos")),
            node_ip: get(ENV_NODE_IP).unwrap_or_else(|| String::from(slot_ip)),
            port,
            cores: number(&get, ENV_CORES, 1)?.max(1),
            memory: number(&get, ENV_MEMORY, DEFAULT_MEMORY)?,
            memory_defaulted: memory_raw.is_none(),
        })
    }

    /// De leader-poort: agent-poort plus 1000, zoals in Go.
    pub fn leader_port(&self) -> u16 {
        self.port.saturating_add(1000)
    }
}

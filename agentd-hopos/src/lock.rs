//! De lock van de cluster op HopOS: welke opslag de lease en de clusterstaat krijgen, en de async contracten ervan.
//!
//! In Go deed een HopOS-node alleen de S3-lock (`hopos.s3.*`, zie
//! `pkg/agentboot` op github.com/xinix00/hop, tag v1.0.7). Deze bewoner
//! spreekt beide protocollen van de host (`store`): een hoplockserver
//! (`HOPOS_LOCK_URL`, het CAS-protocol over http) en S3 (`HOPOS_S3_*`,
//! leans3 over leanhttps met de wortels). De keuze is die van de daemon
//! (`store::open_lease`): het type van de lock kiest de lease, zonder type
//! is een URL een hoplockserver, en zonder lock blijft de node de
//! standalone leader die hij was. De staat (`state/<cluster>`) gaat waar
//! [`discovery::state_store_for`] hem zet, dezelfde poort als de daemon,
//! zodat een host-agent en een HopOS-node van één cluster dezelfde staat
//! lezen.
//!
//! S3 alleen maakt geen cluster: `hopos.s3.*` is ook de object-store van de
//! apps (de store-ops), en een node die daarvoor een bucket krijgt, is niet
//! vanzelf een geclusterde node. De S3-lock vraagt `HOPOS_LOCK_TYPE=s3`,
//! zoals `cluster.lock.type` op de host.
//!
//! | Variabele | Betekenis | Standaard |
//! | --- | --- | --- |
//! | `HOPOS_LOCK_TYPE` | `hoplockserver`, `s3` of `mem` (`hopos.lock.type`) | een URL is een hoplockserver |
//! | `HOPOS_LOCK_URL` | de basis-URL van de hoplockserver (`hopos.lock.url`) | geen |
//! | `HOPOS_LOCK_KEY` | de sleutel van het lease-object (`hopos.lock.key`) | `leases/<cluster>` |
//! | `HOPOS_LOCK_APIKEY` | de `X-API-Key` van de hoplockserver (`hopos.lock.apikey`) | geen |
//! | `HOPOS_LEASE_TTL` | de lease in seconden (`hopos.lease_ttl`) | 30 |
//! | `HOPOS_ADVERTISE` | `ip` of `ip:poort` zoals de andere nodes deze node zien (`hopos.advertise`) | `HOPOS_NODE_IP` en `HOPOS_PORT` |
//!
//! `HOPOS_ADVERTISE` is er voor een node achter een NAT (QEMU slirp: de host
//! ziet de gast als `127.0.0.1` op een doorgezette poort): het endpoint van de
//! agent en het adres in de lease gebruiken dat adres, de listeners blijven op
//! `HOPOS_PORT` en poort + 1000. De leader-poort is ook hier poort + 1000.
//!
//! Het lease-object is dat van de host (`discovery::wire`): een host-agent
//! en een HopOS-node delen één lease op één server of bucket.
//!
//! De contracten ([`LeaseBackend`], [`StateBackend`]) zijn die van
//! `discovery::Backend` en `store::StateStore`, maar `async`: een aanroep
//! wacht op het net en geeft de core terug. Er is geen trait-object (een
//! methode die een future geeft, kan niet in een vtable); [`Lease`] en
//! [`State`] zijn enums over de twee backends.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::future::Future;

use discovery::{LeaseState, StateStoreKind};

use crate::env::{BootConfig, BootError, S3Config};
use crate::fetch::{Connect, Resolve};
use crate::hoplock::{HoplockLease, HoplockState};
use crate::s3::{S3Lease, S3State};

/// Het type van de lock: `hoplockserver`, `s3` of `mem`.
pub const ENV_LOCK_TYPE: &str = "HOPOS_LOCK_TYPE";
/// De basis-URL van de hoplockserver.
pub const ENV_LOCK_URL: &str = "HOPOS_LOCK_URL";
/// De sleutel van het lease-object.
pub const ENV_LOCK_KEY: &str = "HOPOS_LOCK_KEY";
/// De `X-API-Key` van de hoplockserver.
pub const ENV_LOCK_APIKEY: &str = "HOPOS_LOCK_APIKEY";
/// De lease in seconden.
pub const ENV_LEASE_TTL: &str = "HOPOS_LEASE_TTL";
/// Het adres zoals de andere nodes deze node zien.
pub const ENV_ADVERTISE: &str = "HOPOS_ADVERTISE";

/// `cfg` met het adres uit [`ENV_ADVERTISE`] als endpoint en lease-adres.
///
/// De kopie is voor de [`crate::Node`] (endpoint, eigen leader-adres) en de
/// lease; de binary bindt zijn poorten op de `cfg` van de env.
pub fn advertised(
    cfg: &BootConfig,
    get: impl Fn(&str) -> Option<String>,
) -> Result<BootConfig, BootError> {
    let Some(v) = get(ENV_ADVERTISE).filter(|v| !v.trim().is_empty()) else {
        return Ok(cfg.clone());
    };
    let bad = || BootError::Bad {
        var: ENV_ADVERTISE,
        value: v.clone(),
    };
    let (ip, port) = match v.trim().rsplit_once(':') {
        Some((ip, p)) => (ip, p.parse::<u16>().map_err(|_| bad())?),
        None => (v.trim(), cfg.port),
    };
    if ip.is_empty() || port == 0 || port > u16::MAX - 1000 {
        return Err(bad());
    }
    let mut out = cfg.clone();
    out.node_ip = String::from(ip);
    out.port = port;
    Ok(out)
}

/// De lease als niets hem zet: 30 s, de standaard van de daemon
/// (`config::TimeoutsConfig::leader_lease`).
pub const DEFAULT_LEASE_TTL_MS: u64 = 30_000;

/// De kortste lease die de bewoner aanneemt.
///
/// De elector antwoordt "wie leidt" uit de laatste lees zolang die jonger is
/// dan één lease, en de verkiezing vraagt het elke 10 s: met een lease
/// onder de tik is het antwoord bij elke vraag verlopen, leest de elector
/// opnieuw, en registreert een agent nooit (gemeten 30-09 op QEMU met 9 s:
/// elke tik "geen leader", vier tikken later een vergeefse overname). 15 s
/// laat een tik en een trage lees ruimte.
pub const MIN_LEASE_TTL_MS: u64 = 15_000;

/// Welke opslag de lock is.
#[derive(Clone, PartialEq, Eq)]
pub enum LockKind {
    /// Een hoplockserver.
    Hoplock {
        /// De basis-URL.
        url: String,
        /// De `X-API-Key`; leeg is zonder.
        api_key: String,
    },
    /// Een S3-compatibele bucket.
    S3(S3Config),
}

impl fmt::Debug for LockKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // De sleutel blijft weg: alleen of hij er is.
            Self::Hoplock { url, api_key } => f
                .debug_struct("Hoplock")
                .field("url", &crate::client::redact(url))
                .field("api_key", &(!api_key.is_empty()).then_some("<set>"))
                .finish(),
            Self::S3(s3) => f.debug_tuple("S3").field(s3).finish(),
        }
    }
}

/// De clusterconfig van de bewoner: de lock, de sleutels en de lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterConfig {
    /// De opslag van de lease.
    pub lock: LockKind,
    /// De opslag van de gecommitte clusterstaat ([`discovery::state_store_for`]).
    pub state: LockKind,
    /// De sleutel van het lease-object.
    pub lease_key: String,
    /// De sleutel van de clusterstaat (`state/<cluster>`).
    pub state_key: String,
    /// De lease in milliseconden.
    pub ttl_ms: u64,
}

impl ClusterConfig {
    /// Leest de lock uit de env; `Ok(None)` is standalone (geen lock-config).
    ///
    /// `s3` is de S3-sectie die [`crate::BootConfig`] al las (endpoint en
    /// bucket allebei gezet).
    pub fn from_env(
        get: impl Fn(&str) -> Option<String>,
        cluster: &str,
        s3: Option<&S3Config>,
    ) -> Result<Option<Self>, BootError> {
        let kind = get(ENV_LOCK_TYPE).unwrap_or_default();
        let url = get(ENV_LOCK_URL).unwrap_or_default();
        let hoplock = || -> Result<LockKind, BootError> {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(BootError::Bad {
                    var: ENV_LOCK_URL,
                    value: crate::client::redact(&url),
                });
            }
            Ok(LockKind::Hoplock {
                url: url.clone(),
                api_key: get(ENV_LOCK_APIKEY).unwrap_or_default(),
            })
        };
        // De lease: het type kiest, zoals `store::open_lease` op de host.
        let lock = match kind.trim() {
            "" if url.is_empty() => return Ok(None),
            "mem" => return Ok(None),
            "" | "hoplockserver" => hoplock()?,
            "s3" => match s3 {
                Some(s) => LockKind::S3(s.clone()),
                None => {
                    return Err(BootError::Bad {
                        var: ENV_LOCK_TYPE,
                        value: String::from("s3 without HOPOS_S3_ENDPOINT and HOPOS_S3_BUCKET"),
                    });
                }
            },
            _ => {
                return Err(BootError::Bad {
                    var: ENV_LOCK_TYPE,
                    value: kind,
                });
            }
        };
        // De staat: de ene poort van daemon en bewoner.
        let (endpoint, bucket) = s3.map_or(("", ""), |s| (s.endpoint.as_str(), s.bucket.as_str()));
        let state = match discovery::state_store_for(false, kind.trim(), &url, endpoint, bucket) {
            StateStoreKind::S3 => s3.map_or_else(|| lock.clone(), |s| LockKind::S3(s.clone())),
            StateStoreKind::HoplockServer | StateStoreKind::File => lock.clone(),
        };
        let ttl_ms = match get(ENV_LEASE_TTL) {
            None => DEFAULT_LEASE_TTL_MS,
            Some(v) => match v.trim().parse::<u64>() {
                Ok(s) if s.saturating_mul(1000) >= MIN_LEASE_TTL_MS => s.saturating_mul(1000),
                _ => {
                    return Err(BootError::Bad {
                        var: ENV_LEASE_TTL,
                        value: v,
                    });
                }
            },
        };
        let lease_key = get(ENV_LOCK_KEY)
            .filter(|k| !k.trim().is_empty())
            .unwrap_or_else(|| format!("leases/{cluster}"));
        Ok(Some(Self {
            lock,
            state,
            lease_key,
            state_key: format!("state/{cluster}"),
            ttl_ms,
        }))
    }

    /// Het tijdsbudget per backend-aanroep, zoals op de host.
    pub fn call_timeout_ms(&self) -> u64 {
        discovery::backend_timeout_for(self.ttl_ms)
    }

    /// Een korte omschrijving voor de opstartregel, zonder geheimen.
    pub fn label(&self) -> String {
        match &self.lock {
            LockKind::Hoplock { url, .. } => {
                format!("hoplockserver ({})", crate::client::redact(url))
            }
            LockKind::S3(s3) => {
                format!("s3 ({}/{})", crate::client::redact(&s3.endpoint), s3.bucket)
            }
        }
    }
}

/// De lease-opslag, async: het contract van `discovery::Backend`.
///
/// Elke schrijf is voorwaardelijk op de vorige handle, en een lege handle
/// betekent "alleen als er nog niets is".
pub trait LeaseBackend {
    /// Leest de lease en zijn handle; `NoLease` als er geen is.
    fn read(&mut self) -> impl Future<Output = discovery::Result<(LeaseState, String)>>;
    /// Schrijft de lease als de huidige handle `prev` is en geeft de nieuwe.
    fn write(
        &mut self,
        prev: &str,
        state: &LeaseState,
    ) -> impl Future<Output = discovery::Result<String>>;
    /// Verwijdert de lease als de huidige handle `handle` is.
    fn delete(&mut self, handle: &str) -> impl Future<Output = discovery::Result>;
    /// Waarom de laatste aanroep `Unreachable` gaf, voor de logregel.
    fn last_error(&self) -> Option<&str>;
}

/// De gecommitte clusterstaat, async: het contract van `store::StateStore`.
///
/// Geen CAS: de schrijver is per constructie de huidige leaseholder.
pub trait StateBackend {
    /// Overschrijft de snapshot.
    fn save(&mut self, snapshot: &[u8]) -> impl Future<Output = Result<(), String>>;
    /// Leest de snapshot; `None` is "er is nog niets", een schone start.
    fn load(&mut self) -> impl Future<Output = Result<Option<Vec<u8>>, String>>;
    /// Waar de staat woont, voor de opstartregel; zonder geheimen.
    fn describe(&self) -> String;
}

/// De lease-backend van een geclusterde node.
pub enum Lease<C, R> {
    /// Op een hoplockserver.
    Hoplock(HoplockLease<C, R>),
    /// In een S3-bucket.
    S3(S3Lease<C, R>),
}

impl<C: Connect, R: Resolve> LeaseBackend for Lease<C, R> {
    async fn read(&mut self) -> discovery::Result<(LeaseState, String)> {
        match self {
            Self::Hoplock(b) => b.read().await,
            Self::S3(b) => b.read().await,
        }
    }

    async fn write(&mut self, prev: &str, state: &LeaseState) -> discovery::Result<String> {
        match self {
            Self::Hoplock(b) => b.write(prev, state).await,
            Self::S3(b) => b.write(prev, state).await,
        }
    }

    async fn delete(&mut self, handle: &str) -> discovery::Result {
        match self {
            Self::Hoplock(b) => b.delete(handle).await,
            Self::S3(b) => b.delete(handle).await,
        }
    }

    fn last_error(&self) -> Option<&str> {
        match self {
            Self::Hoplock(b) => b.last_error(),
            Self::S3(b) => b.last_error(),
        }
    }
}

/// De staat-backend van een geclusterde node.
pub enum State<C, R> {
    /// Op de hoplockserver van de lease.
    Hoplock(HoplockState<C, R>),
    /// In de bucket van de lease.
    S3(S3State<C, R>),
}

impl<C: Connect, R: Resolve> StateBackend for State<C, R> {
    async fn save(&mut self, snapshot: &[u8]) -> Result<(), String> {
        match self {
            Self::Hoplock(b) => b.save(snapshot).await,
            Self::S3(b) => b.save(snapshot).await,
        }
    }

    async fn load(&mut self) -> Result<Option<Vec<u8>>, String> {
        match self {
            Self::Hoplock(b) => b.load().await,
            Self::S3(b) => b.load().await,
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::Hoplock(b) => b.describe(),
            Self::S3(b) => b.describe(),
        }
    }
}

/// Opent de lease-backend van `cfg` over `client` (dial, wortels, willekeur).
pub fn open_lease<C: Connect, R: Resolve>(
    cfg: &ClusterConfig,
    client: crate::client::Client<C, R>,
    wall_secs: fn() -> u64,
) -> Lease<C, R> {
    let timeout = core::time::Duration::from_millis(cfg.call_timeout_ms());
    match &cfg.lock {
        LockKind::Hoplock { url, api_key } => Lease::Hoplock(HoplockLease::new(
            client,
            url,
            api_key,
            &cfg.lease_key,
            timeout,
        )),
        LockKind::S3(s3) => Lease::S3(S3Lease::new(
            client,
            s3,
            &cfg.lease_key,
            timeout,
            cfg.ttl_ms,
            wall_secs,
        )),
    }
}

/// Opent de staat-backend van `cfg` over `client`.
pub fn open_state<C: Connect, R: Resolve>(
    cfg: &ClusterConfig,
    client: crate::client::Client<C, R>,
    wall_secs: fn() -> u64,
) -> State<C, R> {
    let timeout = core::time::Duration::from_millis(cfg.call_timeout_ms());
    match &cfg.state {
        LockKind::Hoplock { url, api_key } => State::Hoplock(HoplockState::new(
            client,
            url,
            api_key,
            &cfg.state_key,
            timeout,
        )),
        LockKind::S3(s3) => State::S3(S3State::new(client, s3, &cfg.state_key, timeout, wall_secs)),
    }
}

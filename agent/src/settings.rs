//! Wat de agent van zijn node moet weten.
//!
//! Bezit alleen waarden; het lezen van de config-file is van `config`, het
//! vertalen naar deze waarden staat in [`Settings::from_config`], en wat
//! alleen het ijzer weet (cores, geheugen, architectuur, entropie) vult de
//! executor die hem bouwt.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::Write as _;

use types::{Nanos, try_string};

use crate::{Error, Result};

/// De vaste gegevens van een node.
#[derive(Clone, Debug, Default)]
pub struct Settings {
    /// Het node-id (blijft staan over herstarts).
    pub id: String,
    /// Het HTTP-endpoint van de agent (`http://ip:poort`).
    pub endpoint: String,
    /// De node-attributen voor affinity (`node.id`, `node.arch`, `node.os`, ...).
    pub attributes: BTreeMap<String, String>,
    /// Het aantal cores dat de node heeft (op HopOS: de app-cores).
    pub cpu_cores: u32,
    /// Het geheugen dat de node heeft, in bytes.
    pub memory_bytes: u64,
    /// Een lagere CPU-grens in shares als de node gedeeld wordt; 0 = geen.
    pub cap_cpu_shares: i64,
    /// Een lagere geheugengrens in bytes; 0 = geen.
    pub cap_memory: u64,
    /// De sharegroups op een core die de node niet uitdeelt: hun leden
    /// kosten geen CPU uit de boekhouding en passen altijd (op HopOS
    /// [`crate::HOP_GROUP`], en [`crate::SYSTEM_GROUP`] als de kern zijn
    /// core deelt). Leeg buiten HopOS.
    pub free_groups: Vec<String>,
    /// Hoe vaak de monitor taken nakijkt; 0 = 5 s.
    pub monitor_interval: Nanos,
    /// De standaard-timeout van een probe; 0 = 5 s.
    pub health_timeout: Nanos,
    /// Het zaad voor taak-id's en jitter (de executor geeft entropie mee).
    pub seed: u64,
}

impl Settings {
    /// Bouwt de instellingen uit de node-config, zoals Go's `agent.New` ze uit `*config.Config` las.
    ///
    /// De config weet niet alles, en dat is de bedoeling: `cpu_cores`,
    /// `memory_bytes` en `seed` blijven 0 en de attributen `node.arch`,
    /// `node.os` en `node.docker` ontbreken, want die meet de executor op de
    /// node zelf (Go: `GetSystemInfo` en `runtime.GOARCH`). Een leeg
    /// `node.id` blijft leeg (de executor genereert en bewaart het in
    /// `data/node-id`), en zonder `node.ip` blijft het endpoint leeg tot de
    /// executor het interface-IP gekozen heeft: `http://:8080` is geen adres.
    ///
    /// De attributen uit de config gaan over `node.id` heen, net als in Go:
    /// de operator heeft het laatste woord over zijn eigen labels.
    pub fn from_config(cfg: &config::Config) -> Result<Self> {
        let node = &cfg.node;
        let mut endpoint = String::new();
        if !node.ip.is_empty() {
            // "http://" + ip + ":" + hoogstens vijf cijfers: vooraf gereserveerd,
            // zodat het schrijven hieronder niet meer alloceert.
            endpoint
                .try_reserve_exact(node.ip.len() + 13)
                .map_err(|_| Error::Alloc)?;
            write!(endpoint, "http://{}:{}", node.ip, node.port).map_err(|_| Error::Alloc)?;
        }
        // Begrensd: `config` weigert meer dan `config::MAX_ATTRIBUTES` sleutels.
        let mut attributes = BTreeMap::new();
        if !node.id.is_empty() {
            attributes.insert(try_string("node.id")?, try_string(&node.id)?);
        }
        for (k, v) in node.attributes.iter() {
            attributes.insert(try_string(k)?, try_string(v)?);
        }
        Ok(Self {
            id: try_string(&node.id)?,
            endpoint,
            attributes,
            cpu_cores: 0,
            memory_bytes: 0,
            cap_cpu_shares: i64::try_from(cfg.capacity.cpu_shares).unwrap_or(i64::MAX),
            cap_memory: cfg.capacity.memory,
            free_groups: Vec::new(),
            monitor_interval: cfg.timeouts.health_check_interval,
            health_timeout: cfg.timeouts.health_check_timeout,
            seed: 0,
        })
    }

    /// De CPU waar de node tegen plant: de lagere van grens en ijzer.
    pub fn effective_cpu_shares(&self) -> i64 {
        let detected = i64::from(self.cpu_cores).saturating_mul(1024);
        if self.cap_cpu_shares > 0 && self.cap_cpu_shares < detected {
            self.cap_cpu_shares
        } else {
            detected
        }
    }

    /// Het geheugen waar de node tegen plant: de lagere van grens en ijzer.
    pub fn effective_memory_bytes(&self) -> u64 {
        if self.cap_memory > 0 && self.cap_memory < self.memory_bytes {
            self.cap_memory
        } else {
            self.memory_bytes
        }
    }

    /// Het monitor-interval met de standaard.
    pub fn monitor_interval(&self) -> Nanos {
        if self.monitor_interval > 0 {
            self.monitor_interval
        } else {
            5 * types::time::SECOND
        }
    }

    /// De probe-timeout met de standaard.
    pub fn health_timeout(&self) -> Nanos {
        if self.health_timeout > 0 {
            self.health_timeout
        } else {
            5 * types::time::SECOND
        }
    }
}

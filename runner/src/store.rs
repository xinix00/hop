//! De store-kant van de system-API: de opdrachten die apps via de kern aan Hop geven.
//!
//! Een app op HopOS kopieert op afroep tussen zijn eigen map in de
//! object-store en zijn hopfs-zicht (`OP_STORE_*`). De kern heeft geen S3,
//! geen sleutels en geen TLS; Hop wel. Dus zet de kern de call in een rij en
//! haalt Hop hem op ([`crate::SystemApi::next_store`]), doet de S3-kant, en
//! verplaatst de bytes met [`crate::SystemApi::store_read`] en
//! [`crate::SystemApi::store_write`]: die lezen en schrijven het bestand van
//! het slot van de opdracht, door de mount-tabel van dát slot. Afmelden is
//! [`crate::SystemApi::store_done`].
//!
//! Deze module bezit alleen de typen; de draad is `hopos-runner`, de S3-kant
//! de bewoner (`agentd-hopos`).

use alloc::string::String;

use crate::Slot;

/// Wat een app vraagt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreOp {
    /// Object naar het lokale pad (vervangend).
    Pull,
    /// Het lokale pad naar het object (vervangend).
    Push,
    /// De namen onder een prefix in de eigen map.
    List,
    /// Het object weg (idempotent).
    Drop,
}

impl StoreOp {
    /// De naam voor een logregel.
    pub fn name(self) -> &'static str {
        match self {
            StoreOp::Pull => "pull",
            StoreOp::Push => "push",
            StoreOp::List => "list",
            StoreOp::Drop => "drop",
        }
    }
}

/// Eén opdracht uit de rij van de kern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreTask {
    /// Het ticket: de naam van de opdracht bij de kern.
    pub ticket: u64,
    /// Het slot van de app.
    pub slot: Slot,
    /// De op.
    pub op: StoreOp,
    /// De jobnaam van het slot (van de kern, niet van de app): de
    /// naamruimte `apps/<cluster>/<job>/`.
    pub job: String,
    /// De objectnaam binnen de eigen map, genormaliseerd tot `/a/b` (bij
    /// list de prefix; `/` is de hele map).
    pub key: String,
    /// Het lokale pad zoals de app het gaf; de kern vergelijkt het bij elke
    /// lees en schrijf.
    pub path: String,
}

/// De uitkomst van een opdracht, zoals de app hem krijgt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreStatus {
    /// Gelukt.
    Ok,
    /// Mislukt; de tekst zegt waarom.
    Error,
    /// Het object bestaat niet.
    NotFound,
    /// Geweigerd.
    Denied,
}

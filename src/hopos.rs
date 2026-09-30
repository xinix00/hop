//! De bron in een HopOS-slot: de Hop-API over `hoplib::hopos` (async op de
//! executor van de app-core, leanhttp over applib's `TcpConn`).
//!
//! Bezit één [`hoplib::hopos::Agent`]. De naam `HOP` in het adres wordt
//! vóór hoplib het slot-adres van Hop ([`crate::config::with_hop_address`]):
//! Hop is altijd slot 1 en het slot-LAN is deterministisch
//! (`abi::layout::slot_ip4`), dus daar is geen DNS voor nodig.

use alloc::string::String;
use alloc::vec::Vec;

use applib::rt::Exec;
use hoplib::hopos::Agent as HopAgent;
use hoplib::{Client, Error};
use types::{Agent, Job};

use crate::config;
use crate::poll::{AgentTasks, Capacity, Leader, Source, Status};

/// Het slot-adres van Hop: slot 1 op het slot-LAN (10.100.0.2).
pub fn hop_ip() -> [u8; 4] {
    abi::layout::slot_ip4(abi::layout::Slot::FIRST).to_be_bytes()
}

/// De Hop-API van één agent, vanuit een slot.
pub struct SlotSource {
    agent: HopAgent,
}

impl core::fmt::Debug for SlotSource {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SlotSource")
            .field("base", self.base())
            .finish_non_exhaustive()
    }
}

impl SlotSource {
    /// Een bron voor de agent op `url` (met `HOP` voor Hop zelf), met
    /// sleutel `api_key` (leeg of `None`: geen handtekening).
    pub fn new(url: &str, api_key: Option<&str>, exec: &'static Exec) -> Result<Self, Error> {
        let base = config::with_hop_address(url, hop_ip()).map_err(|_| Error::OutOfMemory)?;
        let key = api_key.filter(|k| !k.is_empty());
        let client = Client::new(&base, key)?;
        Ok(Self {
            agent: HopAgent::new(client, exec),
        })
    }

    /// Het basisadres, zoals hoplib het normaliseerde.
    pub fn base(&self) -> &String {
        &self.agent.client().base
    }
}

impl Source for SlotSource {
    type Error = Error;

    async fn agents(&mut self) -> Result<Vec<Agent>, Error> {
        self.agent.agents().await
    }

    async fn jobs(&mut self) -> Result<Vec<Job>, Error> {
        self.agent.jobs().await
    }

    async fn tasks(&mut self) -> Result<Vec<AgentTasks>, Error> {
        self.agent.tasks().await
    }

    async fn capacity(&mut self, agent: &Agent) -> Result<Capacity, Error> {
        self.agent.capacity(agent).await
    }

    async fn status(&mut self) -> Result<Status, Error> {
        self.agent.status().await
    }

    async fn leader(&mut self) -> Result<Leader, Error> {
        self.agent.leader().await
    }

    fn out_of_memory(&self) -> Error {
        Error::OutOfMemory
    }
}

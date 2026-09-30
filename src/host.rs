//! De bron op de host: de Hop-API over `hoplib::host` (blokkerend, over
//! hop's `hostnet`).
//!
//! Bezit één [`hoplib::host::Agent`]: het adres, de sleutel (elke aanroep
//! draagt `X-Hop-Auth` als er een is) en de HTTP-client. hoplib dialt per
//! aanroep vers; vijf aanroepen per vijf seconden hebben geen pool nodig.
//!
//! De methodes van [`Source`] zijn `async` voor de vorm; ze blokkeren en
//! zijn in één poll klaar. De host-bin draait de ronde met
//! [`hostnet::block_on`].

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hoplib::host::Agent as HopAgent;
use hoplib::{Client, Error};
use types::{Agent, Job, Time};

use crate::poll::{AgentTasks, Capacity, Leader, Source, Status};

/// De termijn van een aanroep. hoplib's standaard is 30 s; een ronde van
/// vijf seconden wacht liever niet zo lang op één antwoord.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// De Hop-API van één agent, vanaf de host.
#[derive(Debug)]
pub struct HostSource {
    agent: HopAgent,
}

impl HostSource {
    /// Een bron voor de agent op `base` (`http://127.0.0.1:8080` of
    /// `host:poort`), met sleutel `api_key` (leeg: geen handtekening).
    pub fn new(base: &str, api_key: &str) -> Result<Self, Error> {
        let key = (!api_key.is_empty()).then_some(api_key);
        let client = Client::new(base, key)?;
        Ok(Self {
            agent: HopAgent::new(client).with_timeout(CALL_TIMEOUT),
        })
    }

    /// Het basisadres, zoals hoplib het normaliseerde.
    pub fn base(&self) -> &str {
        &self.agent.client().base
    }
}

impl Source for HostSource {
    type Error = Error;

    async fn agents(&mut self) -> Result<Vec<Agent>, Error> {
        self.agent.agents()
    }

    async fn jobs(&mut self) -> Result<Vec<Job>, Error> {
        self.agent.jobs()
    }

    async fn tasks(&mut self) -> Result<Vec<AgentTasks>, Error> {
        self.agent.tasks()
    }

    async fn capacity(&mut self, agent: &Agent) -> Result<Capacity, Error> {
        self.agent.capacity(agent)
    }

    async fn status(&mut self) -> Result<Status, Error> {
        self.agent.status()
    }

    async fn leader(&mut self) -> Result<Leader, Error> {
        self.agent.leader()
    }

    fn out_of_memory(&self) -> Error {
        Error::OutOfMemory
    }
}

/// De wandklok van de host.
pub fn now() -> Option<Time> {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(d.as_nanos()).ok().map(Time)
}

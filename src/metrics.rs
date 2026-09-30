//! De metrics van één ronde: wat [`crate::render`] op de draad zet.
//!
//! Een [`Metrics`] is een waarde, geen handvat: de collector bouwt er per
//! ronde een nieuwe en geeft hem weg (de host-bin stuurt hem naar zijn
//! serverthread, de bewoner zet hem in zijn leesbare tabel). Kopiëren kan
//! falen en heet daarom [`types::TryClone`].

use alloc::string::String;
use alloc::vec::Vec;

use types::{Map, Time, TryClone};

/// De capaciteit van één agent, zoals hopprom hem exporteert.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CapacityMetric {
    /// CPU-cores.
    pub cpu_cores: u64,
    /// Geheugen in bytes.
    pub memory_bytes: u64,
    /// Gereserveerde CPU-shares (1024 per core).
    pub cpu_used_shares: i64,
    /// Gereserveerd geheugen in bytes.
    pub memory_used_bytes: u64,
}

impl From<&hoplib::Capacity> for CapacityMetric {
    fn from(c: &hoplib::Capacity) -> Self {
        Self {
            cpu_cores: c.cpu_cores,
            memory_bytes: c.memory_bytes,
            cpu_used_shares: c.cpu_used_shares,
            memory_used_bytes: c.memory_used_bytes,
        }
    }
}

impl TryClone for CapacityMetric {
    fn try_clone(&self) -> types::Result<Self> {
        Ok(*self)
    }
}

/// Wat hopprom van één agent exporteert.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AgentMetric {
    /// De laatst bekende capaciteit; `None` als die nog nooit gelezen werd.
    pub capacity: Option<CapacityMetric>,
    /// De CPU-temperatuur in milligraden Celsius uit de heartbeat; 0 is onbekend.
    pub temp_milli_c: i64,
}

impl TryClone for AgentMetric {
    fn try_clone(&self) -> types::Result<Self> {
        Ok(*self)
    }
}

/// Wat hopprom van één job exporteert.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct JobMetric {
    /// Draaiende taken van deze job.
    pub placed: u64,
    /// Gewenste instanties: `count`, 0 is 1, -1 is het aantal agents.
    pub expected: u64,
    /// `placed >= expected`.
    pub healthy: bool,
    /// Gemiddeld CPU-gebruik van de draaiende taken, in procenten van de toewijzing.
    pub cpu_percent: f64,
    /// Gemiddeld geheugengebruik van de draaiende taken, in procenten van de toewijzing.
    pub mem_percent: f64,
}

impl TryClone for JobMetric {
    fn try_clone(&self) -> types::Result<Self> {
        Ok(*self)
    }
}

/// De metrics van één ronde, de tellers inbegrepen.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Metrics {
    /// Geregistreerde agents.
    pub agents_total: u64,
    /// Agents met een heartbeat binnen [`crate::HEALTHY_WITHIN`].
    pub agents_healthy: u64,
    /// Per agent-id.
    pub agents: Map<AgentMetric>,
    /// Taken per staat (de staat zoals de node hem meldt).
    pub tasks_by_state: Map<u64>,
    /// Draaiende taken per job.
    pub tasks_placed: Map<u64>,
    /// De opgetelde `restart_count` per job, over alle taken.
    pub task_restarts: Map<u64>,
    /// Alle jobs.
    pub jobs_total: u64,
    /// Per jobnaam.
    pub jobs: Map<JobMetric>,
    /// Teller: taken die voor het eerst gezien werden, per job.
    pub starts_total: Map<u64>,
    /// Teller: taken die naar `failed` gingen, per job.
    pub failures_total: Map<u64>,
    /// Teller: herstarts (een `restart_count` die groeide), per job.
    pub restarts_total: Map<u64>,
    /// Het adres van de leader; leeg als er geen bekend is.
    pub leader: String,
    /// Wanneer de lease van de lokale leader verloopt; nul op een volger.
    pub lease_expires_at: Time,
    /// Teller: leaderwissels sinds de start van hopprom.
    pub leader_changes: u64,
    /// Een nieuwe leader wacht nog op zijn eerste reconcile.
    pub settling: bool,
    /// Jobs waarvan de laatste uitrol niet afliep.
    pub deploying: Vec<String>,
}

impl TryClone for Metrics {
    fn try_clone(&self) -> types::Result<Self> {
        Ok(Self {
            agents_total: self.agents_total,
            agents_healthy: self.agents_healthy,
            agents: self.agents.try_clone()?,
            tasks_by_state: self.tasks_by_state.try_clone()?,
            tasks_placed: self.tasks_placed.try_clone()?,
            task_restarts: self.task_restarts.try_clone()?,
            jobs_total: self.jobs_total,
            jobs: self.jobs.try_clone()?,
            starts_total: self.starts_total.try_clone()?,
            failures_total: self.failures_total.try_clone()?,
            restarts_total: self.restarts_total.try_clone()?,
            leader: self.leader.try_clone()?,
            lease_expires_at: self.lease_expires_at,
            leader_changes: self.leader_changes,
            settling: self.settling,
            deploying: self.deploying.try_clone()?,
        })
    }
}

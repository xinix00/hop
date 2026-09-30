//! De collector: van een [`Poll`] naar [`Metrics`], met de tellers als
//! delta tussen twee rondes.
//!
//! Bezit de staat van de vorige ronde (welke taak in welke staat met welke
//! `restart_count`), de tellers, de laatst bekende capaciteit per agent en
//! de leader. Meer niet: de gauges komen elke ronde vers uit de API. Een
//! herstart van hopprom zet de tellers op nul; Prometheus' `rate()` kent
//! die reset (zoals in Go).
//!
//! De tellers, per job:
//!
//! - **starts**: een taak-id dat de vorige ronde niet had. De eerste ronde
//!   telt dus elke taak die hij vindt, zoals in Go.
//! - **failures**: een bekende taak die sinds de vorige ronde naar `failed`
//!   ging. Een taak die al `failed` binnenkomt, is een start en geen
//!   failure (Go deed hetzelfde).
//! - **restarts**: een `restart_count` die groeide. Hop v3 geeft een
//!   herstarte taak een NIEUW id met de teller van zijn voorganger plus één
//!   (`agent/src/node.rs`, `restart_fire`); een nieuw id met een
//!   `restart_count` boven nul is dus één herstart. Op hetzelfde id telt het
//!   verschil. Go keek alleen naar hetzelfde id dat van `running` wegging,
//!   en zag daardoor bij v3 bijna nooit een herstart; de README beloofde
//!   "restartCount increases", en dat doet deze regel.
//!
//! De taken van een agent die de leader niet bereikte, blijven in de vorige
//! ronde staan: komt de agent terug, dan zijn zijn taken geen nieuwe starts.
//!
//! Een ronde is alles of niets: alles wordt eerst in lokale waarden
//! gebouwd en pas aan het eind vastgelegd, zodat een lege heap halverwege
//! geen halve staat achterlaat.

use alloc::string::String;
use alloc::vec::Vec;

use types::time::SECOND;
use types::{Agent, Map, Nanos, Task, TaskState, Time, TryClone, try_string};

use crate::metrics::{AgentMetric, CapacityMetric, JobMetric, Metrics};
use crate::poll::{AgentTasks, Poll};
use crate::{Result, bump, push};

/// Een agent is gezond als zijn laatste heartbeat hooguit zo oud is (Go's
/// `agentHealthyThreshold`).
pub const HEALTHY_WITHIN: Nanos = 60 * SECOND;

/// Een taak zoals de vorige ronde hem zag.
#[derive(Debug, PartialEq)]
struct Seen {
    id: String,
    job: String,
    agent: String,
    state: TaskState,
    restarts: i64,
}

impl Seen {
    fn of(agent: &str, t: &Task) -> Result<Self> {
        Ok(Self {
            id: try_string(&t.id)?,
            job: try_string(&t.job_name)?,
            agent: try_string(agent)?,
            state: t.state,
            restarts: t.restart_count,
        })
    }

    fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            id: try_string(&self.id)?,
            job: try_string(&self.job)?,
            agent: try_string(&self.agent)?,
            state: self.state,
            restarts: self.restarts,
        })
    }
}

/// De som van de draaiende taken van één job, voor de gemiddelden.
#[derive(Clone, Copy, Debug, Default)]
struct Sum {
    cpu: f64,
    mem: f64,
    n: u64,
}

/// De tellers per job.
#[derive(Debug, Default, PartialEq)]
struct Counters {
    starts: Map<u64>,
    failures: Map<u64>,
    restarts: Map<u64>,
}

impl Counters {
    fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            starts: self.starts.try_clone()?,
            failures: self.failures.try_clone()?,
            restarts: self.restarts.try_clone()?,
        })
    }
}

/// De gauges van de taken van één ronde.
#[derive(Default)]
struct TaskGauges {
    by_state: Map<u64>,
    placed: Map<u64>,
    restarts: Map<u64>,
    sums: Map<Sum>,
}

/// Van rondes naar metrics. Zie de moduledoc voor de regels.
#[derive(Debug, Default)]
pub struct Collector {
    /// De taken van de vorige ronde, gesorteerd op id.
    seen: Vec<Seen>,
    counters: Counters,
    /// De laatst gelezen capaciteit per agent-id.
    capacity: Map<CapacityMetric>,
    leader: String,
    lease_expires_at: Time,
    leader_changes: u64,
    settling: bool,
    deploying: Vec<String>,
    rounds: u64,
}

impl Collector {
    /// Een collector zonder vorige ronde.
    pub fn new() -> Self {
        Self::default()
    }

    /// Hoeveel rondes er vastgelegd zijn.
    pub fn rounds(&self) -> u64 {
        self.rounds
    }

    /// Verwerkt één ronde en geeft de metrics ervan.
    ///
    /// `now` is de wandklok; `None` (een HopOS-node vóór zijn eerste SNTP)
    /// meet de gezondheid tegen de jongste heartbeat in de ronde, en de
    /// lease als onbekend.
    pub fn observe(&mut self, poll: &Poll, now: Option<Time>) -> Result<Metrics> {
        let mut counters = self.counters.try_clone()?;
        let mut seen = Vec::new();
        let gauges = self.tasks(&poll.tasks, &mut counters, &mut seen)?;
        let capacity = self.capacities(poll)?;
        let agents = agent_metrics(&poll.agents, &capacity)?;
        let jobs = job_metrics(poll, &gauges)?;

        let mut leader = self.leader.try_clone()?;
        let mut lease = self.lease_expires_at;
        let mut changes = self.leader_changes;
        if let Some(l) = &poll.leader {
            if l.leader != leader && !leader.is_empty() {
                changes = changes.saturating_add(1);
            }
            leader = l.leader.try_clone()?;
            lease = l.lease_expires_at;
        }
        let (settling, deploying) = match &poll.status {
            Some(s) => (s.settling, s.deploying.try_clone()?),
            None => (self.settling, self.deploying.try_clone()?),
        };

        let metrics = Metrics {
            agents_total: count(poll.agents.len()),
            agents_healthy: healthy(&poll.agents, now),
            agents,
            tasks_by_state: gauges.by_state,
            tasks_placed: gauges.placed,
            task_restarts: gauges.restarts,
            jobs_total: count(poll.jobs.len()),
            jobs,
            starts_total: counters.starts.try_clone()?,
            failures_total: counters.failures.try_clone()?,
            restarts_total: counters.restarts.try_clone()?,
            leader: leader.try_clone()?,
            lease_expires_at: lease,
            leader_changes: changes,
            settling,
            deploying: deploying.try_clone()?,
        };

        // Vastleggen: vanaf hier kan niets meer falen.
        self.seen = seen;
        self.counters = counters;
        self.capacity = capacity;
        self.leader = leader;
        self.lease_expires_at = lease;
        self.leader_changes = changes;
        self.settling = settling;
        self.deploying = deploying;
        self.rounds = self.rounds.saturating_add(1);
        Ok(metrics)
    }

    /// De taken van de ronde: de gauges, de tellers en de nieuwe `seen`.
    fn tasks(
        &self,
        per_agent: &[AgentTasks],
        counters: &mut Counters,
        seen: &mut Vec<Seen>,
    ) -> Result<TaskGauges> {
        let mut g = TaskGauges::default();
        for at in per_agent {
            let Some(tasks) = &at.tasks else {
                // Onbereikbaar: zijn taken blijven zoals de vorige ronde ze zag.
                for s in self.seen.iter().filter(|s| s.agent == at.agent) {
                    push(seen, s.try_clone()?)?;
                }
                continue;
            };
            for t in tasks {
                gauge(&mut g, t)?;
                self.delta(t, counters)?;
                push(seen, Seen::of(&at.agent, t)?)?;
            }
        }
        seen.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        Ok(g)
    }

    /// De tellers voor één taak, tegen de vorige ronde.
    fn delta(&self, t: &Task, c: &mut Counters) -> Result {
        let job = t.job_name.as_str();
        let before = self
            .seen
            .binary_search_by(|s| s.id.as_str().cmp(&t.id))
            .ok()
            .and_then(|i| self.seen.get(i));
        match before {
            None => {
                bump(&mut c.starts, job, 1)?;
                if t.restart_count > 0 {
                    bump(&mut c.restarts, job, 1)?;
                }
            }
            Some(prev) => {
                if t.state == TaskState::Failed && prev.state != TaskState::Failed {
                    bump(&mut c.failures, job, 1)?;
                }
                let grew = t.restart_count.saturating_sub(prev.restarts);
                if grew > 0 {
                    bump(&mut c.restarts, job, u64::try_from(grew).unwrap_or(0))?;
                }
            }
        }
        Ok(())
    }

    /// De capaciteit per agent: de nieuwe waar gelezen, de vorige bij een
    /// misser, en weg voor een agent die er niet meer is.
    fn capacities(&self, poll: &Poll) -> Result<Map<CapacityMetric>> {
        let mut out = Map::new();
        for a in &poll.agents {
            let fresh = poll
                .capacities
                .iter()
                .find(|(id, _)| *id == a.id)
                .and_then(|(_, c)| c.as_ref())
                .map(CapacityMetric::from);
            if let Some(c) = fresh.or_else(|| self.capacity.get(&a.id).copied()) {
                out.insert(try_string(&a.id)?, c)?;
            }
        }
        Ok(out)
    }
}

/// Telt één taak in de gauges.
fn gauge(g: &mut TaskGauges, t: &Task) -> Result {
    let job = t.job_name.as_str();
    bump(&mut g.by_state, t.state.as_str(), 1)?;
    if t.state == TaskState::Running {
        bump(&mut g.placed, job, 1)?;
        match g.sums.get_mut(job) {
            Some(s) => {
                s.cpu += t.cpu_percent;
                s.mem += t.mem_percent;
                s.n = s.n.saturating_add(1);
            }
            None => {
                let s = Sum {
                    cpu: t.cpu_percent,
                    mem: t.mem_percent,
                    n: 1,
                };
                g.sums.insert(try_string(job)?, s)?;
            }
        }
    }
    if t.restart_count > 0 {
        bump(
            &mut g.restarts,
            job,
            u64::try_from(t.restart_count).unwrap_or(0),
        )?;
    }
    Ok(())
}

/// De metrics per agent: temperatuur uit de heartbeat, capaciteit uit de map.
fn agent_metrics(agents: &[Agent], capacity: &Map<CapacityMetric>) -> Result<Map<AgentMetric>> {
    let mut out = Map::new();
    for a in agents {
        let m = AgentMetric {
            capacity: capacity.get(&a.id).copied(),
            temp_milli_c: a.temp_milli_c,
        };
        out.insert(try_string(&a.id)?, m)?;
    }
    Ok(out)
}

/// De metrics per job.
fn job_metrics(poll: &Poll, g: &TaskGauges) -> Result<Map<JobMetric>> {
    let agents = count(poll.agents.len());
    let mut out = Map::new();
    for job in &poll.jobs {
        let placed = g.placed.get(&job.name).copied().unwrap_or(0);
        let expected = if job.is_daemon() {
            agents
        } else {
            count(job.desired())
        };
        let (cpu, mem) = match g.sums.get(&job.name) {
            Some(s) if s.n > 0 => (s.cpu / s.n as f64, s.mem / s.n as f64),
            _ => (0.0, 0.0),
        };
        let m = JobMetric {
            placed,
            expected,
            healthy: placed >= expected,
            cpu_percent: cpu,
            mem_percent: mem,
        };
        out.insert(try_string(&job.name)?, m)?;
    }
    Ok(out)
}

/// Agents met een heartbeat binnen [`HEALTHY_WITHIN`] van `now` (of, zonder
/// wandklok, van de jongste heartbeat).
fn healthy(agents: &[Agent], now: Option<Time>) -> u64 {
    let reference = now.unwrap_or_else(|| {
        agents
            .iter()
            .map(|a| a.last_seen)
            .max()
            .unwrap_or(Time::ZERO)
    });
    let n = agents
        .iter()
        .filter(|a| !a.last_seen.is_zero() && reference.since(a.last_seen) < HEALTHY_WITHIN)
        .count();
    count(n)
}

/// Een aantal als `u64`.
fn count(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

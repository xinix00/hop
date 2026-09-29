//! Nep-agents voor de tests: de Rust-vorm van `mockAgent` uit
//! `OLD/internal/leader/failover_test.go`.
//!
//! Go startte per agent een `httptest`-server; hier is het een tabel achter
//! de [`Transport`]-trait, per endpoint. Het gedrag is dat van de Go-mock:
//! `/run` weigert met 503 bij vol of `fail_runs`, met 406 bij
//! `reject_affinity`, en `?replace=1` telt de eigen voorgangers niet mee.

use std::collections::BTreeMap;
use std::format;
use std::string::{String, ToString};
use std::vec::Vec;

use types::{Agent, Job, Map, Nanos, Task, TaskState, Time};

use crate::{Leader, MemStore, RunReply, Transport};

/// Een vast "nu" voor de tests: 1 september 2026, 00:00 UTC.
pub(crate) const NOW: Time = Time(1_788_220_800 * types::time::SECOND);

/// Eén nep-agent.
#[derive(Default)]
pub(crate) struct MockAgent {
    pub(crate) tasks: Vec<Task>,
    pub(crate) jobs: BTreeMap<String, Job>,
    pub(crate) run_calls: usize,
    pub(crate) task_seq: usize,
    pub(crate) fail_runs: bool,
    pub(crate) reject_affinity: bool,
    pub(crate) fail_stops: bool,
    /// > 0: weiger met 503 zodra er zoveel taken zijn.
    pub(crate) max_capacity: usize,
    /// Een vaste status voor elke `/run` (de Go-tests met een eigen handler).
    pub(crate) run_status: Option<u16>,
    /// Onbereikbaar: niets antwoordt.
    pub(crate) down: bool,
    pub(crate) deletes: Vec<String>,
    pub(crate) stops: Vec<String>,
    pub(crate) stop_tasks: Vec<String>,
}

impl MockAgent {
    pub(crate) fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub(crate) fn tasks_for_job(&self, name: &str) -> usize {
        self.tasks.iter().filter(|t| t.job_name == name).count()
    }

    pub(crate) fn task_ids(&self, name: &str) -> Vec<String> {
        self.tasks
            .iter()
            .filter(|t| t.job_name == name)
            .map(|t| t.id.clone())
            .collect()
    }

    /// Vult de agent met taken, alsof een vorige leider ze plaatste.
    pub(crate) fn add_tasks(&mut self, name: &str, count: usize) {
        for _ in 0..count {
            self.task_seq += 1;
            self.tasks.push(Task {
                id: format!("task-{name}-{}", self.task_seq),
                job_name: name.to_string(),
                state: TaskState::Running,
                ..Task::default()
            });
        }
    }

    /// De `placed`-tellingen voor registratie.
    pub(crate) fn placed_counts(&self) -> Map<u32> {
        let mut m = Map::new();
        for t in &self.tasks {
            match m.get_mut(&t.job_name) {
                Some(n) => *n += 1,
                None => {
                    m.insert(t.job_name.clone(), 1).unwrap();
                }
            }
        }
        m
    }

    /// Het aantal taken dat nog draait (niet `failed`): Go's `RunningTaskCount`.
    pub(crate) fn running_count(&self) -> usize {
        self.tasks
            .iter()
            .filter(|t| t.state == TaskState::Running)
            .count()
    }

    /// Zet de staat van één taak, alsof zijn herstartbudget op is.
    pub(crate) fn mark_task_state(&mut self, id: &str, state: TaskState) {
        for t in self.tasks.iter_mut().filter(|t| t.id == id) {
            t.state = state;
        }
    }

    /// Zet de staat van alle taken van een job.
    pub(crate) fn mark_job_tasks_state(&mut self, name: &str, state: TaskState) {
        for t in self.tasks.iter_mut().filter(|t| t.job_name == name) {
            t.state = state;
        }
    }

    pub(crate) fn clear_tasks(&mut self) {
        self.tasks.clear();
        self.jobs.clear();
    }

    fn run(&mut self, job: &Job, replace: bool) -> RunReply {
        if self.down {
            return RunReply::Unreachable;
        }
        if let Some(s) = self.run_status {
            if (200..=202).contains(&s) {
                self.accept(job);
            }
            return RunReply::from_status(s);
        }
        if self.reject_affinity {
            return RunReply::AffinityMismatch;
        }
        if self.fail_runs {
            return RunReply::NoCapacity;
        }
        let mut occupied = self.tasks.len();
        if replace {
            occupied -= self.tasks_for_job(&job.name);
        }
        if self.max_capacity > 0 && occupied >= self.max_capacity {
            return RunReply::NoCapacity;
        }
        if replace {
            self.tasks.retain(|t| t.job_name != job.name);
        }
        self.accept(job);
        RunReply::Accepted
    }

    fn accept(&mut self, job: &Job) {
        self.jobs.insert(job.name.clone(), job.clone());
        self.run_calls += 1;
        self.task_seq += 1;
        self.tasks.push(Task {
            id: format!("task-{}-{}", job.name, self.task_seq),
            job_name: job.name.clone(),
            state: TaskState::Running,
            ..Task::default()
        });
    }
}

/// Alle nep-agents, per endpoint, plus de pauzes die een update vroeg.
#[derive(Default)]
pub(crate) struct FakeNet {
    pub(crate) agents: BTreeMap<String, MockAgent>,
    pub(crate) paused: Nanos,
}

impl FakeNet {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Voegt een nep-agent toe en geeft zijn endpoint.
    pub(crate) fn add(&mut self, id: &str) -> String {
        let ep = format!("http://{id}");
        self.agents.insert(ep.clone(), MockAgent::default());
        ep
    }

    pub(crate) fn get(&mut self, id: &str) -> &mut MockAgent {
        self.agents.get_mut(&format!("http://{id}")).unwrap()
    }

    pub(crate) fn total_tasks(&self) -> usize {
        self.agents.values().map(MockAgent::task_count).sum()
    }

    pub(crate) fn total_for_job(&self, name: &str) -> usize {
        self.agents.values().map(|a| a.tasks_for_job(name)).sum()
    }

    fn agent(&mut self, a: &Agent) -> Option<&mut MockAgent> {
        self.agents.get_mut(&a.endpoint).filter(|m| !m.down)
    }
}

impl Transport for FakeNet {
    fn run(&mut self, agent: &Agent, job: &Job, replace: bool) -> RunReply {
        match self.agents.get_mut(&agent.endpoint) {
            Some(m) => m.run(job, replace),
            None => RunReply::Unreachable,
        }
    }

    fn stop_job(&mut self, agent: &Agent, job: &str) -> bool {
        let Some(m) = self.agent(agent) else {
            return false;
        };
        m.stops.push(job.to_string());
        if m.fail_stops {
            return false;
        }
        m.tasks.retain(|t| t.job_name != job);
        true
    }

    fn stop_task(&mut self, agent: &Agent, task_id: &str) {
        if let Some(m) = self.agent(agent) {
            m.stop_tasks.push(task_id.to_string());
            m.tasks.retain(|t| t.id != task_id);
        }
    }

    fn delete_job(&mut self, agent: &Agent, job: &str) {
        if let Some(m) = self.agent(agent) {
            m.deletes.push(job.to_string());
            m.tasks.retain(|t| t.job_name != job);
            m.jobs.remove(job);
        }
    }

    fn tasks(&mut self, agent: &Agent) -> Option<Vec<Task>> {
        self.agent(agent).map(|m| m.tasks.clone())
    }

    fn pause(&mut self, d: Nanos) {
        self.paused += d;
    }
}

/// Een leider op agent `local`, zonder settle.
pub(crate) fn leader() -> Leader<MemStore> {
    Leader::new("local-agent".to_string(), MemStore::new())
}

/// Een job met naam en aantal.
pub(crate) fn job(name: &str, count: i64) -> Job {
    Job {
        name: name.to_string(),
        command: "echo".to_string(),
        count,
        ..Job::default()
    }
}

/// Een job met naam, aantal en prioriteit.
pub(crate) fn job_prio(name: &str, count: i64, priority: i64) -> Job {
    Job {
        priority: Some(priority),
        ..job(name, count)
    }
}

/// Een agent-waarde voor registratie.
pub(crate) fn agent(id: &str, endpoint: &str) -> Agent {
    Agent {
        id: id.to_string(),
        endpoint: endpoint.to_string(),
        ..Agent::default()
    }
}

/// Maakt een nep-agent en registreert hem (zonder placed) op tijd `now`.
pub(crate) fn join(l: &mut Leader<MemStore>, net: &mut FakeNet, id: &str, now: Time) {
    let ep = net.add(id);
    assert!(
        l.register_agent(agent(id, &ep), Map::new(), now, net)
            .unwrap()
    );
}

/// Registreert een bestaande nep-agent opnieuw, met zijn eigen taken als placed.
pub(crate) fn rejoin(l: &mut Leader<MemStore>, net: &mut FakeNet, id: &str, now: Time) -> bool {
    let ep = format!("http://{id}");
    let placed = net.get(id).placed_counts();
    l.register_agent(agent(id, &ep), placed, now, net).unwrap()
}

/// Het totaal aantal geplaatste instanties van `name` volgens de leider.
pub(crate) fn placed_total(l: &Leader<MemStore>, name: &str) -> u32 {
    l.placed(name).unwrap().iter().map(|(_, n)| *n).sum()
}

/// Tijd `secs` seconden na [`NOW`].
pub(crate) fn at(secs: u64) -> Time {
    Time(NOW.0 + secs * types::time::SECOND)
}

/// Tijd `ms` milliseconden na [`NOW`]: de Go-tests sliepen in milliseconden.
pub(crate) fn at_ms(ms: u64) -> Time {
    Time(NOW.0 + ms * types::time::MILLISECOND)
}

/// Zet de plaatsing met de hand, zoals de Go-tests met `l.do(...)` deden.
pub(crate) fn force_placed(l: &mut Leader<MemStore>, agent_id: &str, name: &str, n: u32) {
    if l.placed.get(agent_id).is_none() {
        l.placed.insert(agent_id.to_string(), Map::new()).unwrap();
    }
    l.placed
        .get_mut(agent_id)
        .unwrap()
        .insert(name.to_string(), n)
        .unwrap();
}

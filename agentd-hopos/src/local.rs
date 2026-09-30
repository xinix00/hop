//! De verbinding van de leader met zijn agents: in-proces voor die van deze node, de [`Relay`] voor de rest.
//!
//! In de standalone-cluster is de enige agent die van deze node. Wat in Go
//! een ondertekende `POST /run` naar het eigen adres was, is hier een
//! methode-aanroep op de [`Agent`] die de [`crate::Node`] bezit; de
//! vertaling van de uitkomst is die van de `/run`-handler in `api`, zodat de
//! leader hetzelfde ziet als over de draad.
//!
//! In een cluster (`HOPOS_LOCK_URL` of `HOPOS_S3_*`) staan de andere agents
//! op andere nodes. De leader-transport is synchroon, het net niet: een
//! aanroep bij zo'n agent antwoordt uit de boeken van de [`Relay`] en gaat
//! daarna over het LAN (zie [`crate::relay`]). Zonder relay (standalone)
//! kent deze leader geen andere agent, en die is dan onbereikbaar.

use alloc::vec::Vec;

use agent::{Agent as Node, Error as AgentError};
use leader::{RunReply, Transport};
use types::{Agent, Job, Nanos, Task, TryClone};

use crate::relay::Relay;

/// De agent van deze node (en de relay naar de rest) als [`Transport`] van de leader, geleend voor één aanroep.
pub(crate) struct Local<'a> {
    pub(crate) agent: &'a mut Node,
    pub(crate) now: Nanos,
    pub(crate) pool_largest: Option<u64>,
    /// De boeken van de agents op andere nodes; `None` standalone.
    pub(crate) relay: Option<&'a mut Relay>,
}

impl Local<'_> {
    /// Is dit verzoek voor ons? Een andere agent kent deze leader niet.
    fn ours(&self, agent: &Agent) -> bool {
        agent.id == self.agent.id()
    }
}

impl Transport for Local<'_> {
    fn run(&mut self, agent: &Agent, job: &Job, replace: bool) -> RunReply {
        if !self.ours(agent) {
            return match self.relay.as_deref_mut() {
                Some(r) => r.run(self.now, agent, job, replace),
                None => RunReply::Unreachable,
            };
        }
        let Ok(job) = job.try_clone() else {
            return RunReply::Rejected(500);
        };
        match self.agent.run(self.now, job, replace, self.pool_largest) {
            Ok(_) => RunReply::Accepted,
            Err(AgentError::AffinityMismatch) => RunReply::AffinityMismatch,
            Err(AgentError::NoCapacity | AgentError::TooManyTasks | AgentError::TooManyJobs) => {
                RunReply::NoCapacity
            }
            Err(_) => RunReply::Rejected(500),
        }
    }

    fn stop_job(&mut self, agent: &Agent, job: &str) -> bool {
        if !self.ours(agent) {
            return self
                .relay
                .as_deref_mut()
                .is_some_and(|r| r.stop_job(agent, job));
        }
        self.agent.stop_job_tasks(job);
        true
    }

    fn stop_task(&mut self, agent: &Agent, task_id: &str) {
        if self.ours(agent) {
            self.agent.stop_task(task_id);
        } else if let Some(r) = self.relay.as_deref_mut() {
            r.stop_task(agent, task_id);
        }
    }

    fn delete_job(&mut self, agent: &Agent, job: &str) {
        if self.ours(agent) {
            self.agent.delete_job(self.now, job);
        } else if let Some(r) = self.relay.as_deref_mut() {
            r.delete_job(agent, job);
        }
    }

    fn tasks(&mut self, agent: &Agent) -> Option<Vec<Task>> {
        if !self.ours(agent) {
            return self.relay.as_deref().and_then(|r| r.tasks(agent));
        }
        let mut out = Vec::new();
        for t in self.agent.tasks() {
            out.try_reserve(1).ok()?;
            out.push(t.clone());
        }
        Some(out)
    }
}

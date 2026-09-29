//! De verbinding van de leader met de agent op dezelfde node: in-proces, geen HTTP.
//!
//! In de standalone-cluster van fase 1 is de enige agent die van deze node.
//! Wat in Go een ondertekende `POST /run` naar het eigen adres was, is hier
//! een methode-aanroep op de [`Agent`] die de [`crate::Node`] bezit; de
//! vertaling van de uitkomst is die van de `/run`-handler in `api`, zodat de
//! leader hetzelfde ziet als over de draad.

use alloc::vec::Vec;

use agent::{Agent as Node, Error as AgentError};
use leader::{RunReply, Transport};
use types::{Agent, Job, Nanos, Task, TryClone};

/// De agent van deze node als [`Transport`] van de leader, geleend voor één aanroep.
pub(crate) struct Local<'a> {
    pub(crate) agent: &'a mut Node,
    pub(crate) now: Nanos,
    pub(crate) pool_largest: Option<u64>,
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
            return RunReply::Unreachable;
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
            return false;
        }
        self.agent.stop_job_tasks(job);
        true
    }

    fn stop_task(&mut self, agent: &Agent, task_id: &str) {
        if self.ours(agent) {
            self.agent.stop_task(task_id);
        }
    }

    fn delete_job(&mut self, agent: &Agent, job: &str) {
        if self.ours(agent) {
            self.agent.delete_job(self.now, job);
        }
    }

    fn tasks(&mut self, agent: &Agent) -> Option<Vec<Task>> {
        if !self.ours(agent) {
            return None;
        }
        let mut out = Vec::new();
        for t in self.agent.tasks() {
            out.try_reserve(1).ok()?;
            out.push(t.clone());
        }
        Some(out)
    }
}

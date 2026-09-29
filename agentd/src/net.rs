//! De verbinding van de leader met zijn agents: in-proces voor de eigen agent, HTTP voor de rest.
//!
//! Bezit niets: hij leent de eigen [`Agent`] en de client voor één aanroep
//! van de leader. De eigen agent gaat in-proces, en dat is geen optimalisatie:
//! de eigenaar-thread die de leader draait, is dezelfde die de agent-API
//! beantwoordt, en een HTTP-aanroep naar zichzelf zou op zichzelf wachten.
//! De vertaling van de uitkomst is die van de `/run`-handler in `api`, zodat
//! de leader hetzelfde ziet als over de draad.
//!
//! De termijnen zijn die van Go: 5 s voor `/run` en `/tasks`, 60 s voor
//! stoppen en verwijderen (docker: 10 s SIGTERM plus 10 s SIGKILL).

use std::time::Duration;

use agent::{Agent as Node, Error as AgentError};
use hostnet::{Call, Http};
use leader::{RunReply, Transport};
use types::json;
use types::{Agent, Job, Nanos, Task, TryClone};

/// De termijn van `/run` en `/tasks`.
const QUICK: Duration = Duration::from_secs(5);

/// De termijn van stoppen en verwijderen.
const SLOW: Duration = Duration::from_secs(60);

/// De grootste takenlijst die de leader van een agent leest.
const MAX_TASKS_BODY: usize = 8 << 20;

/// De transport van de leader, geleend voor één aanroep.
pub(crate) struct Net<'a> {
    pub(crate) agent: &'a mut Node,
    pub(crate) now: Nanos,
    pub(crate) http: &'a Http,
    pub(crate) key: &'a [u8],
}

impl Net<'_> {
    /// Is dit de agent van deze node?
    fn ours(&self, agent: &Agent) -> bool {
        agent.id == self.agent.id()
    }

    /// Een ondertekende aanroep bij `agent`; de status en body, of `None` als hij niet antwoordde.
    fn call(
        &self,
        agent: &Agent,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
        timeout: Duration,
    ) -> Option<(u16, Vec<u8>)> {
        let url = format!("{}{path}", agent.endpoint.trim_end_matches('/'));
        let sig = auth::sign_call(self.key, method, &url, body.unwrap_or_default())
            .map(|s| String::from_utf8_lossy(&s).into_owned());
        let mut headers: Vec<(&str, &str)> = Vec::new();
        if let Some(s) = &sig {
            headers.push((auth::AUTH_HEADER, s));
        }
        if body.is_some() {
            headers.push(("Content-Type", "application/json"));
        }
        let call = Call {
            method,
            url: &url,
            headers: &headers,
            body,
            timeout,
        };
        self.http
            .request(&call, MAX_TASKS_BODY)
            .ok()
            .map(|r| (r.status, r.body))
    }
}

impl Transport for Net<'_> {
    fn run(&mut self, agent: &Agent, job: &Job, replace: bool) -> RunReply {
        if self.ours(agent) {
            let Ok(job) = job.try_clone() else {
                return RunReply::Rejected(500);
            };
            return match self.agent.run(self.now, job, replace, None) {
                Ok(_) => RunReply::Accepted,
                Err(AgentError::AffinityMismatch) => RunReply::AffinityMismatch,
                Err(
                    AgentError::NoCapacity | AgentError::TooManyTasks | AgentError::TooManyJobs,
                ) => RunReply::NoCapacity,
                Err(_) => RunReply::Rejected(500),
            };
        }
        let Ok(body) = job.to_json() else {
            return RunReply::Rejected(500);
        };
        let path = if replace { "/run?replace=1" } else { "/run" };
        match self.call(agent, "POST", path, Some(body.as_bytes()), QUICK) {
            Some((status, _)) => RunReply::from_status(status),
            None => RunReply::Unreachable,
        }
    }

    fn stop_job(&mut self, agent: &Agent, job: &str) -> bool {
        if self.ours(agent) {
            self.agent.stop_job_tasks(job);
            return true;
        }
        let path = format!("/stop/{job}");
        matches!(
            self.call(agent, "POST", &path, Some(b""), SLOW),
            Some((200, _))
        )
    }

    fn stop_task(&mut self, agent: &Agent, task_id: &str) {
        if self.ours(agent) {
            self.agent.stop_task(task_id);
            return;
        }
        let path = format!("/stop-task/{task_id}");
        let _ = self.call(agent, "POST", &path, Some(b""), SLOW);
    }

    fn delete_job(&mut self, agent: &Agent, job: &str) {
        if self.ours(agent) {
            self.agent.delete_job(self.now, job);
            return;
        }
        let path = format!("/delete/{job}");
        let _ = self.call(agent, "DELETE", &path, None, SLOW);
    }

    fn tasks(&mut self, agent: &Agent) -> Option<Vec<Task>> {
        if self.ours(agent) {
            let mut out = Vec::new();
            for t in self.agent.tasks() {
                out.try_reserve(1).ok()?;
                out.push(t.clone());
            }
            return Some(out);
        }
        let (status, body) = self.call(agent, "GET", "/tasks", None, QUICK)?;
        if status != 200 {
            return None;
        }
        let v = json::parse(&body).ok()?;
        v.as_array()?
            .iter()
            .map(|t| Task::from_value(t).ok())
            .collect()
    }

    fn pause(&mut self, d: Nanos) {
        // De pauze tussen twee stappen van een rolling update (2 s): de API
        // antwoordt pas na de uitrol, zoals in Go.
        std::thread::sleep(Duration::from_nanos(d));
    }
}

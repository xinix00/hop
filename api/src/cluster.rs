//! De brug van de leader-API naar de echte leader: [`LeaderCluster`].
//!
//! Bezit niets: hij leent de [`Leader`] en de [`Transport`] voor de duur van
//! één verzoek. De trait [`Cluster`] kent geen transport, en de leider houdt
//! er bewust geen vast (hij krijgt hem per aanroep, zodat de tests nep-agents
//! geven en de host HTTP); daarom is de brug een paar leningen en geen
//! `impl Cluster for Leader`.
//!
//! De trait geeft waarden terug zonder foutkanaal. Waar de leider een fout
//! geeft die de trait niet kan dragen (vrijwel altijd een lege heap in de
//! reconcile ná de eigenlijke handeling), zegt de brug wat er wél gebeurd is
//! in plaats van te doen alsof niets lukte.

use alloc::string::String;
use alloc::vec::Vec;

use leader::{JobStore, Leader, Transport};
use types::{Agent, Job, Map, Nanos, Telemetry, Time, TryClone, try_string};

use crate::{Cluster, ClusterError, EventLog};

/// Een leider plus zijn verbinding met de agents, als [`Cluster`] voor [`crate::LeaderApi`].
pub struct LeaderCluster<'a, S, T> {
    leader: &'a mut Leader<S>,
    net: &'a mut T,
    events: Option<&'a mut EventLog>,
}

impl<'a, S: JobStore, T: Transport> LeaderCluster<'a, S, T> {
    /// Leent `leader` en `net` voor één verzoek.
    pub fn new(leader: &'a mut Leader<S>, net: &'a mut T) -> Self {
        Self {
            leader,
            net,
            events: None,
        }
    }

    /// Stuurt de meldingen van `POST /v1/notify` naar `log` in plaats van
    /// naar de rij van de leider.
    ///
    /// Waarom: de leider bewaart van `job:<naam>:<event>` alleen de naam
    /// (een [`leader::Event`] zegt wát er veranderde), maar `/v1/events`
    /// geeft zoals in Go een `task`-gebeurtenis met het event erbij. De
    /// eigen meldingen van de leider (registraties, plaatsingen) komen via
    /// [`Leader::drain_events`] in dezelfde rij.
    #[must_use]
    pub fn with_events(mut self, log: &'a mut EventLog) -> Self {
        self.events = Some(log);
        self
    }
}

/// Kopieert een lijst; zonder geheugen een lege lijst, want de trait heeft
/// geen foutkanaal en een half gevulde lijst zou een verkeerde telling zijn.
fn copy_all<X: TryClone>(xs: &[X]) -> Vec<X> {
    let mut out = Vec::new();
    if out.try_reserve_exact(xs.len()).is_err() {
        return Vec::new();
    }
    for x in xs {
        match x.try_clone() {
            Ok(c) => out.push(c),
            Err(_) => return Vec::new(),
        }
    }
    out
}

fn text(e: &leader::Error) -> String {
    alloc::format!("{e}")
}

impl<S: JobStore, T: Transport> Cluster for LeaderCluster<'_, S, T> {
    fn agents(&self) -> Vec<Agent> {
        copy_all(self.leader.agents())
    }

    fn register_agent(
        &mut self,
        now: Nanos,
        id: &str,
        endpoint: &str,
        version: &str,
        placed: &[(String, i64)],
    ) -> bool {
        let (Ok(id_s), Ok(endpoint), Ok(version)) =
            (try_string(id), try_string(endpoint), try_string(version))
        else {
            return false;
        };
        let mut counts = Map::new();
        for (name, n) in placed {
            // Een negatieve of absurde telling van de agent is geen plaatsing.
            let Ok(n) = u32::try_from(*n) else {
                continue;
            };
            let Ok(name) = try_string(name) else {
                return false;
            };
            if n > 0 && counts.insert(name, n).is_err() {
                return false;
            }
        }
        let agent = Agent {
            id: id_s,
            endpoint,
            version,
            ..Agent::default()
        };
        match self
            .leader
            .register_agent(agent, counts, Time(now), self.net)
        {
            Ok(registered) => registered,
            // De registratie kan gelukt zijn terwijl de reconcile erna faalde;
            // dan is de agent bekend en moet hij geen 409 krijgen.
            Err(_) => self.leader.agent(id).is_some(),
        }
    }

    fn heartbeat(&mut self, now: Nanos, id: &str, version: &str, telemetry: Telemetry) -> bool {
        self.leader.heartbeat(id, version, telemetry, Time(now))
    }

    fn unregister_agent(&mut self, id: &str) {
        // De agent is weg, ook als de reconcile erna geen geheugen had; de
        // volgende tik probeert het opnieuw.
        let _ = self.leader.unregister_agent(id, self.net);
    }

    fn jobs(&self) -> Vec<Job> {
        copy_all(self.leader.jobs())
    }

    fn has_job(&self, name: &str) -> bool {
        self.leader.job(name).is_some()
    }

    fn update_job(&mut self, _now: Nanos, job: Job) -> Result<(), ClusterError> {
        // Nooit `Locked`: met één eigenaar lopen er geen twee updates tegelijk.
        self.leader
            .update_job(job, self.net)
            .map_err(|e| ClusterError::Failed(text(&e)))
    }

    fn dispatch_job(&mut self, _now: Nanos, job: Job) -> Result<(), String> {
        self.leader
            .dispatch_job(job, self.net)
            .map_err(|e| text(&e))
    }

    fn next_priority(&self) -> i64 {
        self.leader.next_priority()
    }

    fn patch_priority(&mut self, _now: Nanos, name: &str, priority: i64) -> bool {
        // Alleen een onbekende job is een 404; een fout in de reconcile na
        // het herschrijven laat de nieuwe volgorde staan.
        !matches!(
            self.leader.patch_job_priority(name, priority, self.net),
            Err(leader::Error::NotFound { .. })
        )
    }

    fn delete_job(&mut self, _now: Nanos, name: &str) {
        let _ = self.leader.delete_job(name, self.net);
    }

    fn placed_counts(&self) -> Vec<(String, i64)> {
        let Ok(counts) = self.leader.placed_counts() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for (name, n) in counts.iter() {
            let Ok(name) = try_string(name) else {
                return Vec::new();
            };
            if out.try_reserve(1).is_err() {
                return Vec::new();
            }
            out.push((name, i64::from(*n)));
        }
        out
    }

    fn placed_agents(&self, name: &str) -> Vec<Agent> {
        let Ok(on) = self.leader.placed(name) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for a in self.leader.agents() {
            if on.get(&a.id).is_none() {
                continue;
            }
            match a.try_clone() {
                Ok(c) if out.try_reserve(1).is_ok() => out.push(c),
                _ => return Vec::new(),
            }
        }
        out
    }

    fn is_settled(&self) -> bool {
        self.leader.is_settled()
    }

    fn state_time(&self) -> Time {
        self.leader.state_time()
    }

    fn mark_unplaced(&mut self, agent: &str, job: &str) {
        let _ = self.leader.mark_unplaced(agent, job, self.net);
    }

    fn notify(&mut self, topic: &str) {
        match self.events.as_deref_mut() {
            Some(log) => log.push_topic(topic),
            None => self.leader.notify(topic),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Eén verzoek van begin tot eind: HTTP-vorm in, de echte leider erachter,
    //! een nep-agent op de draad.

    use alloc::string::ToString;
    use alloc::vec::Vec;

    use leader::{Event, MemStore, RunReply};
    use types::json::Value;
    use types::{Task, TaskState};

    use super::*;
    use crate::{LeaderApi, Method, Request, Response};

    const T0: Nanos = 1_788_220_800 * types::time::SECOND;

    /// Eén nep-agent die alles aanneemt en zijn taken bijhoudt.
    #[derive(Default)]
    struct Net {
        runs: Vec<(String, String)>,
        tasks: Vec<Task>,
    }

    impl Transport for Net {
        fn run(&mut self, agent: &Agent, job: &Job, _replace: bool) -> RunReply {
            self.runs.push((agent.id.clone(), job.name.clone()));
            self.tasks.push(Task {
                id: alloc::format!("task-{}", self.runs.len()),
                job_name: job.name.clone(),
                state: TaskState::Running,
                ..Task::default()
            });
            RunReply::Accepted
        }
        fn stop_job(&mut self, _: &Agent, job: &str) -> bool {
            self.tasks.retain(|t| t.job_name != job);
            true
        }
        fn stop_task(&mut self, _: &Agent, task_id: &str) {
            self.tasks.retain(|t| t.id != task_id);
        }
        fn delete_job(&mut self, _: &Agent, job: &str) {
            self.tasks.retain(|t| t.job_name != job);
        }
        fn tasks(&mut self, _: &Agent) -> Option<Vec<Task>> {
            Some(self.tasks.clone())
        }
    }

    fn call(
        l: &mut Leader<MemStore>,
        net: &mut Net,
        method: Method,
        target: &str,
        body: &str,
    ) -> Response {
        let mut c = LeaderCluster::new(l, net);
        LeaderApi::new(b"", "test-cluster")
            .handle(&mut c, T0, &Request::new(method, target, body.as_bytes()))
            .0
    }

    fn get<'a>(v: &'a Value, k: &str) -> &'a Value {
        v.as_object().unwrap().get(k).unwrap()
    }

    #[test]
    fn register_dispatch_status_through_real_leader() {
        let mut l = Leader::new("a1".to_string(), MemStore::new());
        let mut net = Net::default();

        // Registreren, met een telling voor een job die de leider nog niet kent.
        let r = call(
            &mut l,
            &mut net,
            Method::Post,
            "/v1/agents",
            r#"{"id":"a1","endpoint":"http://a1:8080","version":"3.0.0","placed":{}}"#,
        );
        assert_eq!(r.status, 200);
        assert_eq!(
            get(&r.json_body().unwrap(), "status").as_str(),
            Some("registered")
        );
        assert_eq!(l.agent("a1").unwrap().version, "3.0.0");

        let r = call(
            &mut l,
            &mut net,
            Method::Post,
            "/v1/heartbeat",
            r#"{"id":"a1","endpoint":"http://a1:8080","temp_milli_c":41000}"#,
        );
        assert_eq!(r.status, 200);
        assert_eq!(l.agent("a1").unwrap().telemetry.temp_milli_c, 41_000);

        // Dispatch: de leider plaatst twee instanties via de transport.
        let r = call(
            &mut l,
            &mut net,
            Method::Post,
            "/v1/jobs",
            r#"{"name":"web","command":"./web","count":2}"#,
        );
        assert_eq!(r.status, 201);
        assert_eq!(
            get(&r.json_body().unwrap(), "status").as_str(),
            Some("dispatched")
        );
        assert_eq!(net.runs.len(), 2);
        assert!(net.runs.iter().all(|(a, j)| a == "a1" && j == "web"));
        // Zonder eigen prioriteit achteraan: de eerste job krijgt 0.
        assert_eq!(l.job("web").unwrap().priority, Some(0));

        // Status uit de placed-tellers van de echte leider.
        let r = call(&mut l, &mut net, Method::Get, "/v1/status", "");
        assert_eq!(r.status, 200);
        let v = r.json_body().unwrap();
        assert_eq!(get(&v, "cluster_name").as_str(), Some("test-cluster"));
        assert_eq!(get(&v, "agents").as_u64(), Some(1));
        assert_eq!(get(&v, "jobs").as_u64(), Some(1));
        assert_eq!(get(&v, "total_placed").as_i64(), Some(2));
        assert_eq!(get(get(&v, "placed"), "web").as_i64(), Some(2));
        assert_eq!(get(&v, "settling"), &Value::Bool(false));

        // Een tweede POST is een update (rolling): de telling blijft 2.
        let r = call(
            &mut l,
            &mut net,
            Method::Post,
            "/v1/jobs",
            r#"{"name":"web","command":"./web-v2","count":2}"#,
        );
        assert_eq!(r.status, 200);
        assert_eq!(
            get(&r.json_body().unwrap(), "status").as_str(),
            Some("updated")
        );
        assert_eq!(l.job("web").unwrap().command, "./web-v2");
        assert_eq!(net.tasks.len(), 2);

        // Een hand-back boekt af en de reconcile zet hem terug.
        let before = net.runs.len();
        let r = call(
            &mut l,
            &mut net,
            Method::Post,
            "/v1/notify",
            r#"{"job":"web","event":"unplaceable","agent":"a1"}"#,
        );
        assert_eq!(r.status, 204);
        assert_eq!(net.runs.len(), before + 1);
        assert!(l.drain_events().contains(&Event::Job("web".to_string())));

        // Verwijderen via de API haalt hem uit de store en van de agent.
        let r = call(&mut l, &mut net, Method::Delete, "/v1/jobs/web", "");
        assert_eq!(r.status, 204);
        assert!(l.job("web").is_none());
        assert!(net.tasks.is_empty());

        // Een onbekende agent krijgt 404 op zijn heartbeat.
        let r = call(
            &mut l,
            &mut net,
            Method::Post,
            "/v1/heartbeat",
            r#"{"id":"ghost","endpoint":"http://ghost"}"#,
        );
        assert_eq!(r.status, 404);
    }

    #[test]
    fn register_conflict_and_priority_patch_through_real_leader() {
        let mut l = Leader::new("a1".to_string(), MemStore::new());
        let mut net = Net::default();
        let reg = |id: &str, ep: &str| alloc::format!(r#"{{"id":"{id}","endpoint":"{ep}"}}"#);
        assert_eq!(
            call(
                &mut l,
                &mut net,
                Method::Post,
                "/v1/agents",
                &reg("a1", "http://a1")
            )
            .status,
            200
        );
        // Hetzelfde id, levend op een ander adres: 409.
        assert_eq!(
            call(
                &mut l,
                &mut net,
                Method::Post,
                "/v1/agents",
                &reg("a1", "http://other")
            )
            .status,
            409
        );
        for name in ["a", "b"] {
            let body = alloc::format!(r#"{{"name":"{name}","command":"x"}}"#);
            assert_eq!(
                call(&mut l, &mut net, Method::Post, "/v1/jobs", &body).status,
                201
            );
        }
        let r = call(
            &mut l,
            &mut net,
            Method::Patch,
            "/v1/jobs/b/priority",
            r#"{"priority":0}"#,
        );
        assert_eq!(r.status, 204);
        assert_eq!(l.job("b").unwrap().priority, Some(0));
        assert_eq!(l.job("a").unwrap().priority, Some(1));
        let r = call(
            &mut l,
            &mut net,
            Method::Patch,
            "/v1/jobs/nope/priority",
            r#"{"priority":0}"#,
        );
        assert_eq!(r.status, 404);
    }
}

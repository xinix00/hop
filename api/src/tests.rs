//! De tests uit `internal/agent/handlers_test.go` (de HTTP-kant) en
//! `internal/api/server_test.go`, zelfde namen en bedoeling.

use super::*;
use agent::{Action, Agent, Settings, StartOk};
use alloc::string::ToString;
use types::{Driver, Map};

const S: u64 = types::time::SECOND;
const T0: u64 = 1_000 * S;

fn agent() -> Agent {
    let mut attrs = alloc::collections::BTreeMap::new();
    attrs.insert("node.os".to_string(), "linux".to_string());
    attrs.insert("node.id".to_string(), "test-agent".to_string());
    Agent::new(Settings {
        id: "test-agent".into(),
        endpoint: "http://127.0.0.1:8080".into(),
        attributes: attrs,
        cpu_cores: 8,
        memory_bytes: 16 << 30,
        cap_cpu_shares: 1000,
        cap_memory: 1 << 30,
        seed: 7,
        ..Settings::default()
    })
}

fn node() -> NodeApi {
    NodeApi::new(b"", false)
}

fn call(a: &mut Agent, method: Method, target: &str, body: &str) -> (Response, Effect) {
    node().handle(a, T0, None, &Request::new(method, target, body.as_bytes()))
}

fn body(r: &Response) -> types::json::Value {
    r.json_body().unwrap()
}

fn get<'a>(v: &'a types::json::Value, k: &str) -> &'a types::json::Value {
    v.as_object().unwrap().get(k).unwrap()
}

// ---- handlers_test.go ----------------------------------------------------------

#[test]
fn handle_health() {
    let (r, _) = call(&mut agent(), Method::Get, "/health", "");
    assert_eq!(r.status, 200);
    assert_eq!(get(&body(&r), "status").as_str(), Some("ok"));
}

#[test]
fn handle_tasks() {
    let mut a = agent();
    call(
        &mut a,
        Method::Post,
        "/run",
        r#"{"name":"t","command":"x"}"#,
    );
    let (r, _) = call(&mut a, Method::Get, "/tasks", "");
    assert_eq!(body(&r).as_array().unwrap().len(), 1);
}

#[test]
fn handle_run_success() {
    let mut a = agent();
    let (r, _) = call(
        &mut a,
        Method::Post,
        "/run",
        r#"{"name":"test-job","command":"echo hi"}"#,
    );
    assert_eq!(r.status, 202);
    assert_eq!(get(&body(&r), "status").as_str(), Some("accepted"));
    assert!(matches!(&a.take_actions()[..], [Action::Start { .. }]));
}

#[test]
fn handle_run_method_not_allowed() {
    assert_eq!(call(&mut agent(), Method::Get, "/run", "").0.status, 405);
}

#[test]
fn handle_run_invalid_json() {
    assert_eq!(
        call(&mut agent(), Method::Post, "/run", "{nope").0.status,
        400
    );
}

#[test]
fn handle_run_insufficient_capacity() {
    let (r, _) = call(
        &mut agent(),
        Method::Post,
        "/run",
        r#"{"name":"big","command":"x","cpu_shares":999999}"#,
    );
    assert_eq!(r.status, 503);
    assert_eq!(
        get(&body(&r), "error").as_str(),
        Some("insufficient capacity")
    );
}

#[test]
fn handle_run_affinity_mismatch() {
    let (r, _) = call(
        &mut agent(),
        Method::Post,
        "/run",
        r#"{"name":"p","command":"x","affinity":{"node.os":"darwin"}}"#,
    );
    assert_eq!(r.status, 406);
}

#[test]
fn handle_run_affinity_match() {
    let (r, _) = call(
        &mut agent(),
        Method::Post,
        "/run",
        r#"{"name":"p","command":"x","affinity":{"node.os":"linux"}}"#,
    );
    assert_eq!(r.status, 202);
}

#[test]
fn handle_run_no_affinity() {
    assert_eq!(
        call(
            &mut agent(),
            Method::Post,
            "/run",
            r#"{"name":"p","command":"x"}"#
        )
        .0
        .status,
        202
    );
}

#[test]
fn handle_run_empty_json() {
    assert_eq!(call(&mut agent(), Method::Post, "/run", "{}").0.status, 202);
}

#[test]
fn handle_run_with_ports() {
    let mut a = agent();
    call(
        &mut a,
        Method::Post,
        "/run",
        r#"{"name":"p","command":"x","ports":{"http":0}}"#,
    );
    match &a.take_actions()[..] {
        [Action::Start { job, .. }] => assert_eq!(job.ports.get("http"), Some(&0)),
        other => panic!("{other:?}"),
    }
}

#[test]
fn handle_delete_success() {
    let mut a = agent();
    call(
        &mut a,
        Method::Post,
        "/run",
        r#"{"name":"d","command":"x"}"#,
    );
    let (r, _) = call(&mut a, Method::Delete, "/delete/d", "");
    assert_eq!(r.status, 200);
    assert_eq!(get(&body(&r), "deleted").as_u64(), Some(1));
}

#[test]
fn handle_delete_method_not_allowed() {
    assert_eq!(
        call(&mut agent(), Method::Get, "/delete/d", "").0.status,
        405
    );
}

#[test]
fn handle_delete_missing_job_id() {
    assert_eq!(
        call(&mut agent(), Method::Delete, "/delete/", "").0.status,
        400
    );
}

#[test]
fn handle_delete_non_existent_job() {
    let (r, _) = call(&mut agent(), Method::Delete, "/delete/nope", "");
    assert_eq!(get(&body(&r), "deleted").as_u64(), Some(0));
}

#[test]
fn handle_logs_invalid_path() {
    assert_eq!(
        call(&mut agent(), Method::Get, "/logs/only-id", "")
            .0
            .status,
        400
    );
}

#[test]
fn handle_logs_invalid_stream() {
    assert_eq!(
        call(&mut agent(), Method::Get, "/logs/id/stdin", "")
            .0
            .status,
        400
    );
}

#[test]
fn handle_logs_stdout_stream() {
    let (r, e) = call(&mut agent(), Method::Get, "/logs/abc/stdout", "");
    assert_eq!(r.header("Content-Type"), Some("text/event-stream"));
    assert_eq!(
        e,
        Effect::Logs {
            task_id: "abc".into(),
            stream: LogStream::Stdout
        }
    );
}

#[test]
fn handle_capacity() {
    let mut a = agent();
    let (r, _) = call(&mut a, Method::Get, "/capacity", "");
    let v = body(&r);
    assert_eq!(get(&v, "memory_bytes").as_u64(), Some(1 << 30));
    assert_eq!(
        get(get(&v, "attributes"), "node.os").as_str(),
        Some("linux")
    );
}

#[test]
fn capacity_includes_attributes() {
    let (r, _) = call(&mut agent(), Method::Get, "/capacity", "");
    assert!(body(&r).as_object().unwrap().get("attributes").is_some());
}

#[test]
fn handle_leader() {
    let (r, _) = call(&mut agent(), Method::Get, "/leader", "");
    assert_eq!(get(&body(&r), "leader").as_str(), Some(""));
}

#[test]
fn handle_leader_from_state() {
    let mut a = agent();
    a.set_leader_addr("10.0.0.1:9080");
    a.set_lease_expires_at(types::Time(T0));
    let (r, _) = call(&mut a, Method::Get, "/leader", "");
    let v = body(&r);
    assert_eq!(get(&v, "leader").as_str(), Some("10.0.0.1:9080"));
    assert!(v.as_object().unwrap().get("lease_expires_at").is_some());
}

#[test]
fn proxy_to_leader_no_leader() {
    let (r, e) = call(&mut agent(), Method::Get, "/v1/jobs", "");
    assert_eq!(r.status, 503);
    assert_eq!(e, Effect::None);
}

#[test]
fn proxy_to_leader_success() {
    let mut a = agent();
    a.set_leader_addr("10.0.0.1:9080");
    let (_, e) = call(&mut a, Method::Post, "/v1/jobs", r#"{"name":"x"}"#);
    assert_eq!(
        e,
        Effect::Proxy {
            leader: "10.0.0.1:9080".into(),
            stream: false
        }
    );
    let (_, e) = call(&mut a, Method::Get, "/v1/events", "");
    assert!(matches!(e, Effect::Proxy { stream: true, .. }));
}

#[test]
fn stop_during_start_does_not_resurrect_task() {
    let mut a = agent();
    call(
        &mut a,
        Method::Post,
        "/run",
        r#"{"name":"race","command":"x"}"#,
    );
    let id = a.tasks().next().unwrap().id.clone();
    a.take_actions();
    let (r, _) = call(&mut a, Method::Post, "/stop/race", "");
    assert_eq!(get(&body(&r), "stopped").as_u64(), Some(1));
    a.on_started(
        T0,
        &id,
        Driver::Exec,
        Ok(StartOk {
            pid: 3,
            ports: Map::new(),
        }),
    );
    assert!(a.task(&id).is_none());
}

#[test]
fn handle_flip() {
    let mut a = agent();
    let off = NodeApi::new(b"", false);
    let req = |b: &str| Request::new(Method::Post, "/flip", b.as_bytes());
    let sum = "a".repeat(64);
    let good = alloc::format!(r#"{{"url":"https://x/k.bin","sha256":"{sum}"}}"#);
    assert_eq!(off.handle(&mut a, T0, None, &req(&good)).0.status, 501);
    let on = NodeApi::new(b"", true);
    assert_eq!(on.handle(&mut a, T0, None, &req("{x")).0.status, 400);
    assert_eq!(
        on.handle(&mut a, T0, None, &req(r#"{"url":"x","sha256":""}"#))
            .0
            .status,
        400
    );
    let (r, e) = on.handle(&mut a, T0, None, &req(&good));
    assert_eq!(r.status, 202);
    assert!(matches!(e, Effect::Flip { .. }));
}

#[test]
fn auth_rejects_unsigned_and_accepts_signed() {
    let mut a = agent();
    let api = NodeApi::new(b"secret", false);
    let mut req = Request::new(Method::Get, "/tasks", b"");
    assert_eq!(api.handle(&mut a, T0, None, &req).0.status, 401);
    let sig = auth::sign(b"secret", "GET", "/tasks", b"");
    req.headers.push((
        auth::AUTH_HEADER.into(),
        core::str::from_utf8(&sig).unwrap().into(),
    ));
    assert_eq!(api.handle(&mut a, T0, None, &req).0.status, 200);
    // /health blijft publiek.
    assert_eq!(
        api.handle(&mut a, T0, None, &Request::new(Method::Get, "/health", b""))
            .0
            .status,
        200
    );
}

#[test]
fn cors_preflight_and_private_network() {
    let mut req = Request::new(Method::Options, "/run", b"");
    req.headers.push((
        "Access-Control-Request-Private-Network".into(),
        "true".into(),
    ));
    let (r, _) = node().handle(&mut agent(), T0, None, &req);
    assert_eq!(r.status, 200);
    assert_eq!(
        r.header("Access-Control-Allow-Private-Network"),
        Some("true")
    );
    assert_eq!(r.header("Access-Control-Allow-Origin"), Some("*"));
}

// ---- server_test.go ------------------------------------------------------------

/// Een leader in het geheugen, net genoeg voor de handlers.
#[derive(Default)]
struct FakeCluster {
    agents: Vec<types::Agent>,
    jobs: Vec<types::Job>,
    topics: Vec<String>,
    unplaced: Vec<(String, String)>,
    no_agents_for_dispatch: bool,
    /// `(agent, job)`: waar een job staat, voor `/v1/jobs/{naam}/status`.
    placed_on: Vec<(String, String)>,
}

impl Cluster for FakeCluster {
    fn agents(&self) -> Vec<types::Agent> {
        self.agents.clone()
    }
    fn register_agent(
        &mut self,
        _: u64,
        id: &str,
        endpoint: &str,
        version: &str,
        _: &[(String, i64)],
    ) -> bool {
        if let Some(a) = self.agents.iter().find(|a| a.id == id) {
            return a.endpoint == endpoint;
        }
        self.agents.push(types::Agent {
            id: id.into(),
            endpoint: endpoint.into(),
            version: version.into(),
            ..types::Agent::default()
        });
        true
    }
    fn heartbeat(&mut self, _: u64, id: &str, _: &str, _: types::Telemetry) -> bool {
        self.agents.iter().any(|a| a.id == id)
    }
    fn unregister_agent(&mut self, id: &str) {
        self.agents.retain(|a| a.id != id);
    }
    fn jobs(&self) -> Vec<types::Job> {
        self.jobs.clone()
    }
    fn has_job(&self, name: &str) -> bool {
        self.jobs.iter().any(|j| j.name == name)
    }
    fn update_job(&mut self, _: u64, job: types::Job) -> core::result::Result<(), ClusterError> {
        self.jobs.retain(|j| j.name != job.name);
        self.jobs.push(job);
        Ok(())
    }
    fn dispatch_job(&mut self, _: u64, job: types::Job) -> core::result::Result<(), String> {
        self.jobs.push(job);
        if self.no_agents_for_dispatch {
            return Err("no agents available".into());
        }
        Ok(())
    }
    fn next_priority(&self) -> i64 {
        i64::try_from(self.jobs.len()).unwrap()
    }
    fn patch_priority(&mut self, _: u64, name: &str, p: i64) -> bool {
        match self.jobs.iter_mut().find(|j| j.name == name) {
            Some(j) => {
                j.priority = Some(p);
                true
            }
            None => false,
        }
    }
    fn delete_job(&mut self, _: u64, name: &str) {
        self.jobs.retain(|j| j.name != name);
    }
    fn placed_counts(&self) -> Vec<(String, i64)> {
        self.jobs.iter().map(|j| (j.name.clone(), 1)).collect()
    }
    fn placed_agents(&self, name: &str) -> Vec<types::Agent> {
        self.agents
            .iter()
            .filter(|a| {
                self.placed_on
                    .iter()
                    .any(|(ag, j)| *ag == a.id && j == name)
            })
            .cloned()
            .collect()
    }
    fn is_settled(&self) -> bool {
        true
    }
    fn state_time(&self) -> types::Time {
        types::Time(T0)
    }
    fn mark_unplaced(&mut self, agent: &str, job: &str) {
        self.unplaced.push((agent.into(), job.into()));
    }
    fn notify(&mut self, topic: &str) {
        self.topics.push(topic.into());
    }
}

fn lcall(c: &mut FakeCluster, method: Method, target: &str, body: &str) -> Response {
    leffect(c, method, target, body).0
}

fn leffect(
    c: &mut FakeCluster,
    method: Method,
    target: &str,
    body: &str,
) -> (Response, LeaderEffect) {
    LeaderApi::new(b"", "test-cluster").handle(
        c,
        T0,
        &Request::new(method, target, body.as_bytes()),
    )
}

#[test]
fn health_endpoint() {
    assert_eq!(
        lcall(&mut FakeCluster::default(), Method::Get, "/health", "").status,
        200
    );
}

#[test]
fn get_agents_empty() {
    let r = lcall(&mut FakeCluster::default(), Method::Get, "/v1/agents", "");
    assert_eq!(body(&r).as_array().unwrap().len(), 0);
}

#[test]
fn get_agents_with_registered() {
    let mut c = FakeCluster::default();
    lcall(
        &mut c,
        Method::Post,
        "/v1/agents",
        r#"{"id":"a1","endpoint":"http://a1"}"#,
    );
    let r = lcall(&mut c, Method::Get, "/v1/agents", "");
    assert_eq!(body(&r).as_array().unwrap().len(), 1);
}

#[test]
fn heartbeat_success() {
    let mut c = FakeCluster::default();
    lcall(
        &mut c,
        Method::Post,
        "/v1/agents",
        r#"{"id":"a1","endpoint":"http://a1"}"#,
    );
    assert_eq!(
        lcall(
            &mut c,
            Method::Post,
            "/v1/heartbeat",
            r#"{"id":"a1","endpoint":"http://a1"}"#
        )
        .status,
        200
    );
}

#[test]
fn heartbeat_missing_fields() {
    assert_eq!(
        lcall(
            &mut FakeCluster::default(),
            Method::Post,
            "/v1/heartbeat",
            r#"{"id":"a1"}"#
        )
        .status,
        400
    );
}

#[test]
fn heartbeat_invalid_json() {
    assert_eq!(
        lcall(
            &mut FakeCluster::default(),
            Method::Post,
            "/v1/heartbeat",
            "{x"
        )
        .status,
        400
    );
}

#[test]
fn heartbeat_registers_new_agent() {
    // Een onbekende agent krijgt 404 en herregistreert dan zelf.
    let r = lcall(
        &mut FakeCluster::default(),
        Method::Post,
        "/v1/heartbeat",
        r#"{"id":"n","endpoint":"http://n"}"#,
    );
    assert_eq!(r.status, 404);
}

#[test]
fn get_jobs_empty() {
    let r = lcall(&mut FakeCluster::default(), Method::Get, "/v1/jobs", "");
    assert_eq!(body(&r).as_array().unwrap().len(), 0);
}

#[test]
fn get_jobs_with_stored() {
    let mut c = FakeCluster::default();
    lcall(
        &mut c,
        Method::Post,
        "/v1/jobs",
        r#"{"name":"j","command":"x"}"#,
    );
    assert_eq!(
        body(&lcall(&mut c, Method::Get, "/v1/jobs", ""))
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn run_job_missing_name() {
    assert_eq!(
        lcall(
            &mut FakeCluster::default(),
            Method::Post,
            "/v1/jobs",
            r#"{"command":"x"}"#
        )
        .status,
        400
    );
}

#[test]
fn run_job_missing_command() {
    assert_eq!(
        lcall(
            &mut FakeCluster::default(),
            Method::Post,
            "/v1/jobs",
            r#"{"name":"j"}"#
        )
        .status,
        400
    );
}

#[test]
fn run_job_invalid_json() {
    assert_eq!(
        lcall(&mut FakeCluster::default(), Method::Post, "/v1/jobs", "{x").status,
        400
    );
}

#[test]
fn run_job_creates_new() {
    let mut c = FakeCluster::default();
    let r = lcall(
        &mut c,
        Method::Post,
        "/v1/jobs",
        r#"{"name":"j","command":"x"}"#,
    );
    assert_eq!(r.status, 201);
    assert_eq!(get(&body(&r), "status").as_str(), Some("dispatched"));
    assert_eq!(c.jobs[0].priority, Some(0));
}

#[test]
fn run_job_no_agents_available() {
    let mut c = FakeCluster {
        no_agents_for_dispatch: true,
        ..FakeCluster::default()
    };
    let r = lcall(
        &mut c,
        Method::Post,
        "/v1/jobs",
        r#"{"name":"j","command":"x"}"#,
    );
    assert_eq!(r.status, 201);
    assert_eq!(get(&body(&r), "status").as_str(), Some("pending"));
}

#[test]
fn run_job_update_existing() {
    let mut c = FakeCluster::default();
    lcall(
        &mut c,
        Method::Post,
        "/v1/jobs",
        r#"{"name":"j","command":"x"}"#,
    );
    let r = lcall(
        &mut c,
        Method::Post,
        "/v1/jobs",
        r#"{"name":"j","command":"y"}"#,
    );
    assert_eq!(r.status, 200);
    let v = body(&r);
    assert_eq!(get(&v, "status").as_str(), Some("updated"));
    assert_eq!(get(&v, "policy").as_str(), Some("rolling"));
}

#[test]
fn delete_job_by_name() {
    let mut c = FakeCluster::default();
    lcall(
        &mut c,
        Method::Post,
        "/v1/jobs",
        r#"{"name":"j","command":"x"}"#,
    );
    assert_eq!(lcall(&mut c, Method::Delete, "/v1/jobs/j", "").status, 204);
    assert!(c.jobs.is_empty());
}

#[test]
fn delete_job_empty_name() {
    assert_eq!(
        lcall(&mut FakeCluster::default(), Method::Delete, "/v1/jobs/", "").status,
        400
    );
}

#[test]
fn unregister_agent() {
    let mut c = FakeCluster::default();
    lcall(
        &mut c,
        Method::Post,
        "/v1/agents",
        r#"{"id":"a1","endpoint":"http://a1"}"#,
    );
    assert_eq!(
        lcall(&mut c, Method::Delete, "/v1/agents/a1", "").status,
        204
    );
    assert!(c.agents.is_empty());
}

#[test]
fn unregister_agent_empty_id() {
    assert_eq!(
        lcall(
            &mut FakeCluster::default(),
            Method::Delete,
            "/v1/agents/",
            ""
        )
        .status,
        400
    );
}

#[test]
fn status_endpoint_empty() {
    let v = body(&lcall(
        &mut FakeCluster::default(),
        Method::Get,
        "/v1/status",
        "",
    ));
    assert_eq!(get(&v, "cluster_name").as_str(), Some("test-cluster"));
    assert_eq!(get(&v, "agents").as_u64(), Some(0));
    assert_eq!(get(&v, "settling").as_bool(), Some(false));
}

#[test]
fn status_endpoint_with_agents() {
    let mut c = FakeCluster::default();
    lcall(
        &mut c,
        Method::Post,
        "/v1/agents",
        r#"{"id":"a1","endpoint":"http://a1"}"#,
    );
    lcall(
        &mut c,
        Method::Post,
        "/v1/jobs",
        r#"{"name":"j","command":"x"}"#,
    );
    let v = body(&lcall(&mut c, Method::Get, "/v1/status", ""));
    assert_eq!(get(&v, "agents").as_u64(), Some(1));
    assert_eq!(get(&v, "total_placed").as_i64(), Some(1));
}

#[test]
fn notify_endpoint_with_event() {
    let mut c = FakeCluster::default();
    let r = lcall(
        &mut c,
        Method::Post,
        "/v1/notify",
        r#"{"job":"j","event":"crash","agent":"a1"}"#,
    );
    assert_eq!(r.status, 204);
    assert_eq!(c.topics, ["job:j:crash"]);
}

#[test]
fn notify_endpoint_without_event() {
    let mut c = FakeCluster::default();
    lcall(&mut c, Method::Post, "/v1/notify", r#"{"job":"j"}"#);
    lcall(&mut c, Method::Post, "/v1/notify", "{}");
    assert_eq!(c.topics, ["job:j", ""]);
}

#[test]
fn notify_event_types() {
    let mut c = FakeCluster::default();
    lcall(
        &mut c,
        Method::Post,
        "/v1/notify",
        r#"{"job":"j","event":"unplaceable","agent":"a1"}"#,
    );
    assert_eq!(c.unplaced, [("a1".to_string(), "j".to_string())]);
}

#[test]
fn run_job_hop_driver_multiple_artifacts() {
    let mut c = FakeCluster::default();
    let r = lcall(
        &mut c,
        Method::Post,
        "/v1/jobs",
        r#"{"name":"h","driver":"hop","artifacts":[{"url":"http://a","match":{"node.arch":"arm64"}},{"url":"http://b"}]}"#,
    );
    assert_eq!(r.status, 201);
}

#[test]
fn run_job_hop_driver_without_artifacts() {
    let r = lcall(
        &mut FakeCluster::default(),
        Method::Post,
        "/v1/jobs",
        r#"{"name":"h","driver":"hop"}"#,
    );
    assert_eq!(r.status, 400);
}

#[test]
fn contract_status_shape() {
    let v = body(&lcall(
        &mut FakeCluster::default(),
        Method::Get,
        "/v1/status",
        "",
    ));
    for k in [
        "cluster_name",
        "agents",
        "jobs",
        "total_placed",
        "settling",
        "placed",
        "deploying",
    ] {
        assert!(v.as_object().unwrap().get(k).is_some(), "{k}");
    }
}

// ---- /v1/events, /v1/tasks en de doorgifte naar een agent ----

/// Twee agents, zoals `/v1/agents` ze zou geven.
fn two_agents() -> FakeCluster {
    let mut c = FakeCluster::default();
    for (id, ep) in [
        ("a1", "http://10.0.0.1:8080"),
        ("a2", "http://10.0.0.2:8080"),
    ] {
        c.agents.push(types::Agent {
            id: id.into(),
            endpoint: ep.into(),
            ..types::Agent::default()
        });
    }
    c
}

// TestSSEEventFormat: de eerste gebeurtenis is een ping met `{}`, en een
// melding `job:my-api:started` wordt `event: task` met job en event.
#[test]
fn sse_event_format() {
    let mut c = FakeCluster::default();
    let (r, effect) = leffect(&mut c, Method::Get, "/v1/events", "");
    assert_eq!(r.status, 200);
    assert_eq!(r.header("Content-Type"), Some("text/event-stream"));
    assert_eq!(effect, LeaderEffect::Events);
    assert!(PING.contains("event: ping\ndata: {}\n\n"));
    let mut log = EventLog::new();
    let start = log.seq();
    log.push_topic("job:my-api:started");
    let mut out = String::new();
    let next = log.since(start, &mut out);
    assert_eq!(next, start + 1);
    assert!(out.contains("event: task\n"), "{out}");
    assert!(out.contains(r#""event":"started""#), "{out}");
    assert!(out.contains(r#""job":"my-api""#), "{out}");
    // Wie bij is, krijgt niets.
    let mut none = String::new();
    assert_eq!(log.since(next, &mut none), next);
    assert!(none.is_empty());
}

// TestNotifyWithEventType: de melding van een agent komt met zijn event in
// de rij van de stroom, niet zonder (de leider zelf onthoudt alleen de naam).
#[test]
fn notify_with_event_type() {
    let mut leader = ::leader::Leader::new(String::from("me"), ::leader::MemStore::new());
    let mut net = NoNet;
    let mut log = EventLog::new();
    let start = log.seq();
    let mut cluster = LeaderCluster::new(&mut leader, &mut net).with_events(&mut log);
    let (r, _) = LeaderApi::new(b"", "c").handle(
        &mut cluster,
        T0,
        &Request::new(
            Method::Post,
            "/v1/notify",
            br#"{"job":"my-api","event":"started"}"#,
        ),
    );
    assert_eq!(r.status, 204);
    let mut out = String::new();
    log.since(start, &mut out);
    assert_eq!(
        out,
        "event: task\ndata: {\"job\":\"my-api\",\"event\":\"started\"}\n\n"
    );
    // En de rij van de leider bleef leeg: geen dubbele melding.
    assert!(leader.drain_events().is_empty());
}

/// Een transport zonder agents: de notify-route raakt het net niet.
struct NoNet;

impl ::leader::Transport for NoNet {
    fn run(&mut self, _: &types::Agent, _: &types::Job, _: bool) -> ::leader::RunReply {
        ::leader::RunReply::Unreachable
    }
    fn stop_job(&mut self, _: &types::Agent, _: &str) -> bool {
        false
    }
    fn stop_task(&mut self, _: &types::Agent, _: &str) {}
    fn delete_job(&mut self, _: &types::Agent, _: &str) {}
    fn tasks(&mut self, _: &types::Agent) -> Option<Vec<types::Task>> {
        None
    }
}

#[test]
fn event_frames_match_go_per_topic() {
    let mut log = EventLog::new();
    for t in ["agent:n1", "job:web", "job:web:crash", "", "settled"] {
        log.push_topic(t);
    }
    log.push(&::leader::Event::Agent("n2".into()));
    log.push(&::leader::Event::Job("db".into()));
    log.push(&::leader::Event::Status);
    let mut out = String::new();
    log.since(0, &mut out);
    let frames: Vec<&str> = out.split("\n\n").filter(|f| !f.is_empty()).collect();
    assert_eq!(
        frames,
        [
            "event: agent\ndata: {\"id\":\"n1\"}",
            "event: job\ndata: {\"name\":\"web\"}",
            "event: task\ndata: {\"job\":\"web\",\"event\":\"crash\"}",
            "event: status\ndata: {}",
            "event: status\ndata: {}",
            "event: agent\ndata: {\"id\":\"n2\"}",
            "event: job\ndata: {\"name\":\"db\"}",
            "event: status\ndata: {}",
        ]
    );
    // Een naam met een aanhalingsteken blijft geldige JSON.
    let mut q = String::new();
    topic_frame("job:a\"b", &mut q);
    assert!(q.contains(r#"{"name":"a\"b"}"#), "{q}");
}

#[test]
fn a_reader_that_fell_behind_gets_one_status() {
    let mut log = EventLog::new();
    let start = log.seq();
    for i in 0..(EVENT_LOG_CAP + 3) {
        log.push_topic(&format!("job:j{i}"));
    }
    let mut out = String::new();
    let next = log.since(start, &mut out);
    assert!(out.starts_with("event: status\ndata: {}\n\n"), "{out}");
    assert_eq!(out.matches("event: job").count(), EVENT_LOG_CAP);
    assert_eq!(next, log.seq());
    // Een nummer uit een vorige rij (een nieuwe leider): ook `status`.
    let mut again = String::new();
    assert_eq!(EventLog::new().since(next, &mut again), 0);
    assert_eq!(again, "event: status\ndata: {}\n\n");
}

#[test]
fn log_lines_are_data_frames() {
    let mut out = String::new();
    data_frame("hello", &mut out);
    data_frame("two\nlines\r", &mut out);
    assert_eq!(out, "data: hello\n\ndata: two\ndata: lines\n\n");
    assert!(is_follow(&Request::new(
        Method::Get,
        "/logs/t/stdout?follow=1",
        b""
    )));
    assert!(!is_follow(&Request::new(
        Method::Get,
        "/logs/t/stdout",
        b""
    )));
}

#[test]
fn tasks_asks_every_agent() {
    let mut c = two_agents();
    let (r, effect) = leffect(&mut c, Method::Get, "/v1/tasks", "");
    assert_eq!(r.status, 200);
    assert_eq!(
        effect,
        LeaderEffect::Tasks {
            agents: vec![
                ("a1".into(), "http://10.0.0.1:8080".into()),
                ("a2".into(), "http://10.0.0.2:8080".into()),
            ],
            scope: TasksScope::All {
                agents: c.agents.clone()
            },
        }
    );
}

#[test]
fn tasks_reply_keys_by_agent_and_names_the_silent() {
    let t = types::Task {
        id: "t1".into(),
        job_name: "web".into(),
        ..types::Task::default()
    };
    // a1 meldde zijn kern en Hop, a2 ook, maar a2 antwoordde niet.
    let mut a1 = types::Agent {
        id: "a1".into(),
        ..types::Agent::default()
    };
    a1.telemetry.kern = types::SysUsage {
        cpu_percent: Some(2.0),
        mem_bytes: 1 << 20,
        ram_bytes: 8 << 20,
        core: Some(0),
    };
    a1.telemetry.hop.mem_bytes = 1 << 20;
    let a2 = types::Agent {
        id: "a2".into(),
        telemetry: a1.telemetry,
        ..types::Agent::default()
    };
    let r = tasks_reply(
        &[a1, a2],
        &[("a1".into(), Some(vec![t])), ("a2".into(), None)],
    );
    assert_eq!(r.status, 200);
    let v = body(&r);
    let by = get(&v, "tasks_by_agent").as_object().unwrap();
    let a1 = by.get("a1").unwrap().as_array().unwrap();
    assert_eq!(a1.len(), 3);
    assert_eq!(get(&a1[0], "id").as_str(), Some("t1"));
    // De systeemtaken achter de echte, met hun slot als pid.
    assert_eq!(get(&a1[1], "job_name").as_str(), Some("kern"));
    assert_eq!(get(&a1[1], "state").as_str(), Some("system"));
    assert_eq!(get(&a1[1], "pid").as_i64(), Some(0));
    let mem = types::de::float(get(&a1[1], "mem_percent"), "mem_percent").unwrap();
    assert_eq!(mem, 12.5);
    assert_eq!(get(&a1[2], "job_name").as_str(), Some("hop"));
    assert_eq!(get(&a1[2], "pid").as_i64(), Some(1));
    assert!(by.get("a2").is_none());
    let silent = get(&v, "unreachable").as_array().unwrap();
    assert_eq!(silent[0].as_str(), Some("a2"));
}

#[test]
fn agent_logs_and_capacity_go_to_that_agent() {
    let mut c = two_agents();
    let (_, e) = leffect(
        &mut c,
        Method::Get,
        "/v1/agents/a2/logs/t9/stderr?follow=1",
        "",
    );
    assert_eq!(
        e,
        LeaderEffect::Agent {
            endpoint: "http://10.0.0.2:8080".into(),
            path: "/logs/t9/stderr?follow=1".into(),
            stream: true,
        }
    );
    // Zonder query is de leader-route de levende tail (Go, het dashboard);
    // `follow=0` blijft de momentopname.
    let (_, e) = leffect(&mut c, Method::Get, "/v1/agents/a2/logs/t9/stdout", "");
    assert!(
        matches!(&e, LeaderEffect::Agent { path, stream: true, .. } if path == "/logs/t9/stdout?follow=1"),
        "{e:?}"
    );
    let (_, e) = leffect(
        &mut c,
        Method::Get,
        "/v1/agents/a2/logs/t9/stdout?follow=0",
        "",
    );
    assert!(
        matches!(&e, LeaderEffect::Agent { path, .. } if path == "/logs/t9/stdout?follow=0"),
        "{e:?}"
    );
    let (_, e) = leffect(&mut c, Method::Get, "/v1/agents/a1/capacity", "");
    assert_eq!(
        e,
        LeaderEffect::Agent {
            endpoint: "http://10.0.0.1:8080".into(),
            path: "/capacity".into(),
            stream: false,
        }
    );
    let (r, e) = leffect(&mut c, Method::Get, "/v1/agents/zz/capacity", "");
    assert_eq!((r.status, e), (404, LeaderEffect::None));
    let (r, _) = leffect(&mut c, Method::Get, "/v1/agents/a1/logs/t9/stdin", "");
    assert_eq!(r.status, 400);
    let (r, _) = leffect(&mut c, Method::Get, "/v1/agents/a1/logs/t9", "");
    assert_eq!(r.status, 400);
    // Een ander pad onder een agent blijft een 404, geen doorgifte.
    let (r, e) = leffect(&mut c, Method::Get, "/v1/agents/a1/other", "");
    assert_eq!((r.status, e), (404, LeaderEffect::None));
}

// ---- contract_test.go: /v1/jobs/{naam}/status ------------------------------------

// TestContract_JobStatusHasTasksByAgent: tasks_by_agent staat hier, ook
// voor een job die niet bestaat (200, zoals Go).
#[test]
fn contract_job_status_has_tasks_by_agent() {
    let r = lcall(
        &mut FakeCluster::default(),
        Method::Get,
        "/v1/jobs/anything/status",
        "",
    );
    assert_eq!(r.status, 200);
    let text = core::str::from_utf8(&r.body).unwrap();
    assert!(text.contains("tasks_by_agent"), "{text}");
    // Go's vorm op de byte: een onbekende job heeft nil-kaarten.
    assert_eq!(text, r#"{"agents":null,"tasks_by_agent":null}"#);
}

fn job_named(name: &str) -> types::Job {
    types::Job {
        name: name.into(),
        command: "sleep 1".into(),
        ..types::Job::default()
    }
}

#[test]
fn job_status_asks_only_the_agents_that_have_it() {
    let mut c = two_agents();
    c.jobs.push(job_named("web"));
    c.placed_on.push(("a2".into(), "web".into()));
    let (r, e) = leffect(&mut c, Method::Get, "/v1/jobs/web/status", "");
    assert_eq!(r.status, 200);
    let LeaderEffect::Tasks { agents, scope } = e else {
        panic!("no round: {e:?}");
    };
    assert_eq!(agents, vec![("a2".into(), "http://10.0.0.2:8080".into())]);
    let TasksScope::Job { name, agents } = &scope else {
        panic!("wrong scope: {scope:?}");
    };
    assert_eq!((name.as_str(), agents.len()), ("web", 1));
}

#[test]
fn job_status_of_a_job_placed_nowhere_has_an_empty_map() {
    let mut c = two_agents();
    c.jobs.push(job_named("web"));
    let (r, e) = leffect(&mut c, Method::Get, "/v1/jobs/web/status", "");
    assert_eq!(e, LeaderEffect::None);
    assert_eq!(
        core::str::from_utf8(&r.body).unwrap(),
        r#"{"agents":null,"tasks_by_agent":{}}"#
    );
    let (r, _) = leffect(&mut c, Method::Get, "/v1/jobs//status", "");
    assert_eq!(r.status, 400);
}

#[test]
fn job_status_reply_keeps_only_the_tasks_of_the_job() {
    let task = |id: &str, job: &str| types::Task {
        id: id.into(),
        job_name: job.into(),
        ..types::Task::default()
    };
    let agents = [types::Agent {
        id: "a1".into(),
        endpoint: "http://10.0.0.1:8080".into(),
        ..types::Agent::default()
    }];
    let results = [
        ("a1".into(), Some(vec![task("t1", "web"), task("t2", "db")])),
        ("a2".into(), None),
        ("a3".into(), Some(vec![task("t3", "db")])),
    ];
    let r = job_status_reply("web", &agents, &results);
    let v = body(&r);
    let by = get(&v, "tasks_by_agent").as_object().unwrap();
    let a1 = by.get("a1").unwrap().as_array().unwrap();
    assert_eq!(a1.len(), 1);
    assert_eq!(get(&a1[0], "id").as_str(), Some("t1"));
    // Stil, of zonder taak van deze job: weg, zoals Go.
    assert!(by.get("a2").is_none() && by.get("a3").is_none());
    let listed = get(&v, "agents").as_array().unwrap();
    assert_eq!(
        get(&listed[0], "endpoint").as_str(),
        Some("http://10.0.0.1:8080")
    );
    // En de scope geeft hetzelfde antwoord als de functie.
    let scope = TasksScope::Job {
        name: "web".into(),
        agents: agents.to_vec(),
    };
    assert_eq!(scope.reply(&results), r);
}

// ---- PATCH /v1/jobs/{naam}/priority: de sleepvolgorde van het dashboard -------

#[test]
fn patch_job_priority() {
    let mut c = FakeCluster::default();
    c.jobs.push(job_named("web"));
    let r = lcall(
        &mut c,
        Method::Patch,
        "/v1/jobs/web/priority",
        r#"{"priority":3}"#,
    );
    assert_eq!(r.status, 204);
    assert_eq!(c.jobs[0].priority, Some(3));
    let r = lcall(
        &mut c,
        Method::Patch,
        "/v1/jobs/nope/priority",
        r#"{"priority":1}"#,
    );
    assert_eq!(r.status, 404);
    assert_eq!(get(&body(&r), "error").as_str(), Some("job nope not found"));
    let r = lcall(&mut c, Method::Patch, "/v1/jobs/web/priority", "{");
    assert_eq!(r.status, 400);
}

// ---- CORS: de browser praat direct met de agent ----------------------------------

#[test]
fn cors_on_every_agent_answer_even_a_refusal() {
    let mut a = agent();
    let api = NodeApi::new(b"secret", false);
    // Ongetekend: 401, en toch met de koppen, anders ziet de browser alleen
    // "CORS error" in plaats van de weigering.
    let (r, _) = api.handle(
        &mut a,
        T0,
        None,
        &Request::new(Method::Get, "/v1/jobs", b""),
    );
    assert_eq!(r.status, 401);
    assert_eq!(r.header("Access-Control-Allow-Origin"), Some("*"));
    // De preflight heeft geen handtekening nodig, en noemt PATCH, DELETE,
    // X-Hop-Auth en Content-Type.
    let (r, e) = api.handle(
        &mut a,
        T0,
        None,
        &Request::new(Method::Options, "/v1/jobs/web/priority", b""),
    );
    assert_eq!((r.status, e), (200, Effect::None));
    let methods = r.header("Access-Control-Allow-Methods").unwrap();
    assert!(
        methods.contains("PATCH") && methods.contains("DELETE"),
        "{methods}"
    );
    let headers = r.header("Access-Control-Allow-Headers").unwrap();
    assert!(
        headers.contains("X-Hop-Auth") && headers.contains("Content-Type"),
        "{headers}"
    );
    // Een privé-netwerkkop alleen op verzoek.
    assert!(r.header("Access-Control-Allow-Private-Network").is_none());
    let mut req = Request::new(Method::Get, "/v1/events", b"");
    req.headers.push((
        "Access-Control-Request-Private-Network".into(),
        "true".into(),
    ));
    let mut resp = Response::empty(200);
    cors(&req, &mut resp);
    assert_eq!(
        resp.header("Access-Control-Allow-Private-Network"),
        Some("true")
    );
}

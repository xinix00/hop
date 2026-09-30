//! De tests: tegen de bytes van hop's eigen `api`-crate waar dat kan.
//!
//! De Go-hoplib had geen tests; de referentie is daarom de server zelf. De
//! verzoeken van [`Client`] gaan door `api::LeaderApi` (met de HMAC-toets
//! erin, dus een fout ondertekend verzoek is een 401 en geen groene test),
//! en de SSE-bytes komen uit `api::EventLog`. Waar `api` een antwoord niet
//! zonder de hele agent kan maken (`/capacity`, `/leader`), staan de bytes
//! letterlijk zoals `api/src/node.rs` ze schrijft.

use super::*;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::time::Duration;

use api::{Cluster, ClusterError, LeaderApi, LeaderEffect, Method};

const KEY: &str = "secret123";

/// Een cluster zoals de tests van `api` hem nep maken.
#[derive(Default)]
struct FakeCluster {
    agents: Vec<types::Agent>,
    jobs: Vec<types::Job>,
    no_agents: bool,
}

impl Cluster for FakeCluster {
    fn agents(&self) -> Vec<types::Agent> {
        self.agents.clone()
    }
    fn register_agent(&mut self, _: u64, _: &str, _: &str, _: &str, _: &[(String, i64)]) -> bool {
        true
    }
    fn heartbeat(&mut self, _: u64, _: &str, _: &str, _: i64) -> bool {
        true
    }
    fn unregister_agent(&mut self, _: &str) {}
    fn jobs(&self) -> Vec<types::Job> {
        self.jobs.clone()
    }
    fn has_job(&self, name: &str) -> bool {
        self.jobs.iter().any(|j| j.name == name)
    }
    fn update_job(&mut self, _: u64, job: types::Job) -> core::result::Result<(), ClusterError> {
        if job.name == "locked" {
            return Err(ClusterError::Locked);
        }
        self.jobs.retain(|j| j.name != job.name);
        self.jobs.push(job);
        Ok(())
    }
    fn dispatch_job(&mut self, _: u64, job: types::Job) -> core::result::Result<(), String> {
        self.jobs.push(job);
        if self.no_agents {
            return Err("no agents available".into());
        }
        Ok(())
    }
    fn next_priority(&self) -> i64 {
        0
    }
    fn patch_priority(&mut self, _: u64, _: &str, _: i64) -> bool {
        true
    }
    fn delete_job(&mut self, _: u64, name: &str) {
        self.jobs.retain(|j| j.name != name);
    }
    fn placed_counts(&self) -> Vec<(String, i64)> {
        self.jobs.iter().map(|j| (j.name.clone(), 2)).collect()
    }
    fn is_settled(&self) -> bool {
        false
    }
    fn state_time(&self) -> types::Time {
        types::Time(0)
    }
    fn mark_unplaced(&mut self, _: &str, _: &str) {}
    fn notify(&mut self, _: &str) {}
}

fn cluster() -> FakeCluster {
    FakeCluster {
        agents: vec![
            types::Agent {
                id: "node-1".into(),
                endpoint: "http://10.0.0.1:7878".into(),
                version: "3.0.0".into(),
                last_seen: types::Time(1_790_000_000_000_000_000),
                temp_milli_c: 51_000,
            },
            types::Agent {
                id: "node-2".into(),
                endpoint: "http://10.0.0.2:7878".into(),
                ..types::Agent::default()
            },
        ],
        jobs: vec![
            Job::from_json(
                br#"{"name":"api","command":"./api","count":2,"tags":{"hoplb":"api.example"}}"#,
            )
            .unwrap(),
            Job::from_json(br#"{"name":"db","image":"postgres:16"}"#).unwrap(),
        ],
        no_agents: false,
    }
}

fn task(id: &str, job: &str, port: u16) -> Task {
    let mut t = Task {
        id: id.into(),
        job_name: job.into(),
        driver: "exec".into(),
        state: TaskState::Running,
        ..Task::default()
    };
    t.ports.insert("http".into(), port).unwrap();
    t
}

/// Een verzoek van [`Client`] zoals de adapter van hop het leest.
fn to_api(req: &Request) -> api::Request {
    let path = req.path();
    let query = req.url.split_once('?').map(|(_, q)| q).unwrap_or("");
    let mut target = String::from(path);
    if !query.is_empty() {
        target.push('?');
        target.push_str(query);
    }
    let mut r = api::Request::new(Method::parse(req.method), &target, &req.body);
    r.headers = req.headers.clone();
    r
}

/// Het antwoord van de echte leader-API (met taken voor een rondgang).
fn leader(c: &mut FakeCluster, key: &str, req: &Request) -> Response {
    let api = LeaderApi::new(key.as_bytes(), "prod");
    let (r, effect) = api.handle(c, 0, &to_api(req));
    let r = match effect {
        LeaderEffect::Tasks { agents } => {
            let results: Vec<(String, Option<Vec<Task>>)> = agents
                .into_iter()
                .map(|(id, _)| {
                    let tasks = (id == "node-1")
                        .then(|| vec![task("t1", "api", 31000), task("t2", "db", 31001)]);
                    (id, tasks)
                })
                .collect();
            api::tasks_reply(&results)
        }
        _ => r,
    };
    Response {
        status: r.status,
        body: r.body,
    }
}

fn client() -> Client {
    Client::new("127.0.0.1:7878", Some(KEY)).unwrap()
}

fn reply(status: u16, body: &str) -> Response {
    Response {
        status,
        body: body.as_bytes().to_vec(),
    }
}

// --- De client: adres, ondertekening, verzoeken. ---

#[test]
fn base_is_normalized() {
    assert_eq!(client().base, "http://127.0.0.1:7878");
    let c = Client::new("http://node:7878/", None).unwrap();
    assert_eq!(c.base, "http://node:7878");
    assert_eq!(c.api_key, None);
    assert_eq!(
        Client::new("https://h:1", None).unwrap().base,
        "https://h:1"
    );
    for bad in ["", "ftp://x", "http://h/prefix", "h/p", "http://"] {
        assert_eq!(Client::new(bad, None), Err(Error::BadUrl), "{bad}");
    }
}

#[test]
fn sign_matches_hops_vectors() {
    // De vectoren van hop's auth-tests (Python hmac/hashlib, 29-09-2026).
    let (h, v) = client().sign("GET", "/v1/jobs", b"").unwrap();
    assert_eq!(h, "X-Hop-Auth");
    assert_eq!(
        v,
        "205db2f439beceb0a19264c65fc29ea421e2b33fe7827b30fc80520af0e57338"
    );
    let c = Client::new("h:1", Some("your-secret-key")).unwrap();
    let (_, v) = c
        .sign(
            "POST",
            "http://h:1/v1/jobs?x=1",
            br#"{"name":"api","count":3}"#,
        )
        .unwrap();
    assert_eq!(
        v,
        "54f6f8899ba8af25f69825f54c0c52a61f3ea224b6dc5542f4646d936f746f59"
    );
    // Zonder sleutel, of met een lege: geen kop.
    assert_eq!(
        Client::new("h:1", None).unwrap().sign("GET", "/", b""),
        None
    );
    assert_eq!(
        Client::new("h:1", Some("")).unwrap().sign("GET", "/", b""),
        None
    );
}

#[test]
fn requests_have_the_right_shape() {
    let c = client();
    let r = c.status().unwrap();
    let req = r.request();
    assert_eq!(req.method, "GET");
    assert_eq!(req.url, "http://127.0.0.1:7878/v1/status");
    assert_eq!(req.path(), "/v1/status");
    assert!(req.body.is_empty());
    assert!(req.header("content-type").is_none());
    assert_eq!(
        req.header("x-hop-auth"),
        c.sign("GET", "/v1/status", b"").map(|(_, v)| v).as_deref()
    );

    let job = Job::from_json(br#"{"name":"web","command":"./web"}"#).unwrap();
    let a = c.apply(&job).unwrap();
    let req = a.request();
    assert_eq!(req.method, "POST");
    assert_eq!(req.body, job.to_json().unwrap().into_bytes());
    assert_eq!(req.header("Content-Type"), Some("application/json"));

    let d = c.delete("web").unwrap();
    assert_eq!(d.request().method, "DELETE");
    assert_eq!(d.request().url, "http://127.0.0.1:7878/v1/jobs/web");

    let e = c.events().unwrap();
    assert_eq!(e.url, "http://127.0.0.1:7878/v1/events");
    assert_eq!(e.header("Accept"), Some("text/event-stream"));

    let open = Client::new("h:1", None).unwrap();
    assert!(open.status().unwrap().request().headers.is_empty());

    let agent = &cluster().agents[0];
    let t = c.agent_tasks(agent).unwrap();
    assert_eq!(t.request().url, "http://10.0.0.1:7878/tasks");
    let cap = c.capacity(agent).unwrap();
    assert_eq!(
        cap.request().url,
        "http://127.0.0.1:7878/v1/agents/node-1/capacity"
    );
    let js = c.job_status("api").unwrap();
    assert_eq!(js.request().path(), "/v1/jobs/api/status");
}

// --- Tegen de echte leader-API. ---

#[test]
fn status_from_the_leader_api() {
    let mut cl = cluster();
    let call = client().status().unwrap();
    let st = call.parse(&leader(&mut cl, KEY, call.request())).unwrap();
    assert_eq!(st.cluster_name, "prod");
    assert_eq!(st.agents, 2);
    assert_eq!(st.jobs, 2);
    assert_eq!(st.total_placed, 4);
    assert!(st.settling);
    assert_eq!(st.placed.get("api"), Some(&2));
    assert!(st.deploying.is_empty());
}

#[test]
fn a_wrong_key_is_a_401_with_the_reason() {
    let mut cl = cluster();
    let bad = Client::new("h:1", Some("wrong")).unwrap();
    let call = bad.jobs().unwrap();
    let err = call
        .parse(&leader(&mut cl, KEY, call.request()))
        .unwrap_err();
    let Error::Status { code, reason } = err else {
        panic!("{err:?}");
    };
    assert_eq!(code, 401);
    assert_eq!(reason.as_str(), "unauthorized");
    assert_eq!(err.to_string(), "status 401: unauthorized");
}

#[test]
fn agents_jobs_and_one_job() {
    let mut cl = cluster();
    let c = client();
    let call = c.agents().unwrap();
    let agents = call.parse(&leader(&mut cl, KEY, call.request())).unwrap();
    assert_eq!(agents, cluster().agents);

    let call = c.jobs().unwrap();
    let jobs = call.parse(&leader(&mut cl, KEY, call.request())).unwrap();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].tags.get("hoplb").unwrap(), "api.example");

    let call = c.job("db").unwrap();
    let db = call.parse(&leader(&mut cl, KEY, call.request())).unwrap();
    assert_eq!(db.unwrap().image, "postgres:16");
    let call = c.job("nope").unwrap();
    assert_eq!(
        call.parse(&leader(&mut cl, KEY, call.request())).unwrap(),
        None
    );
    // Go schreef `null` voor een lege lijst.
    assert!(call.parse(&reply(200, "null")).unwrap().is_none());
}

#[test]
fn tasks_by_agent_and_the_unreachable() {
    let mut cl = cluster();
    let call = client().tasks().unwrap();
    let all = call
        .parse(&leader(&mut cl, KEY, call.request()))
        .unwrap()
        .unwrap();
    let n1 = all.iter().find(|a| a.agent == "node-1").unwrap();
    let tasks = n1.tasks.as_ref().unwrap();
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].ports.get("http"), Some(&31000));
    let n2 = all.iter().find(|a| a.agent == "node-2").unwrap();
    assert_eq!(n2.tasks, None);
    // Een leader zonder /v1/tasks: de terugval.
    let old = reply(404, r#"{"error":"not found"}"#);
    assert_eq!(call.parse(&old).unwrap(), None);
}

#[test]
fn job_status_and_its_fallback() {
    // De vorm van Go's handleJobStatus (en van hop na alpha.10).
    let call = client().job_status("api").unwrap();
    let body = r#"{"agents":[{"id":"node-1","endpoint":"http://10.0.0.1:7878","version":"3.0.0","last_seen":"0001-01-01T00:00:00Z"}],"tasks_by_agent":{"node-1":[{"id":"t1","job_name":"api","driver":"exec","ports":{"http":31000},"pid":0,"state":"running","started_at":"0001-01-01T00:00:00Z","restart_count":0,"last_failed_at":"0001-01-01T00:00:00Z","next_restart_at":"0001-01-01T00:00:00Z","cpu_percent":0,"mem_percent":0}]}}"#;
    let st = call.parse(&reply(200, body)).unwrap().unwrap();
    assert_eq!(st.agents[0].id, "node-1");
    assert_eq!(st.tasks[0].tasks.as_ref().unwrap()[0].id, "t1");
    // Een onbekende job: beide null, zoals Go.
    let st = call
        .parse(&reply(200, r#"{"agents":null,"tasks_by_agent":null}"#))
        .unwrap()
        .unwrap();
    assert!(st.agents.is_empty() && st.tasks.is_empty());
    // Hop tot en met alpha.10: 404, en de terugval filtert /v1/tasks.
    let mut cl = cluster();
    let r = leader(&mut cl, KEY, call.request());
    assert_eq!(r.status, 404);
    assert_eq!(call.parse(&r).unwrap(), None);
    let t = client().tasks().unwrap();
    let all = t
        .parse(&leader(&mut cl, KEY, t.request()))
        .unwrap()
        .unwrap();
    let st = JobStatus::from_tasks("api", cluster().agents, all);
    assert_eq!(st.agents.len(), 1);
    assert_eq!(st.agents[0].id, "node-1");
    let mine = st.tasks[0].tasks.as_ref().unwrap();
    assert_eq!(mine.len(), 1);
    assert_eq!(mine[0].job_name, "api");
}

#[test]
fn apply_create_update_pending_locked() {
    let mut cl = cluster();
    let c = client();
    let web = Job::from_json(br#"{"name":"web","command":"./web"}"#).unwrap();
    let call = c.apply(&web).unwrap();
    let a = call.parse(&leader(&mut cl, KEY, call.request())).unwrap();
    assert_eq!((a.name.as_str(), a.status.as_str()), ("web", "dispatched"));

    let call = c.apply(&web).unwrap();
    let a = call.parse(&leader(&mut cl, KEY, call.request())).unwrap();
    assert_eq!(
        (a.status.as_str(), a.policy.as_str()),
        ("updated", "rolling")
    );

    cl.no_agents = true;
    let lone = Job::from_json(br#"{"name":"lone","command":"x"}"#).unwrap();
    let call = c.apply(&lone).unwrap();
    let a = call.parse(&leader(&mut cl, KEY, call.request())).unwrap();
    assert_eq!(a.status, "pending");
    assert_eq!(a.error, "no agents available");

    cl.jobs
        .push(Job::from_json(br#"{"name":"locked","command":"x"}"#).unwrap());
    let locked = Job::from_json(br#"{"name":"locked","command":"y"}"#).unwrap();
    let call = c.apply(&locked).unwrap();
    let err = call
        .parse(&leader(&mut cl, KEY, call.request()))
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "status 409: job is locked by another operation"
    );

    let empty = Job::default();
    let call = c.apply(&empty).unwrap();
    let err = call
        .parse(&leader(&mut cl, KEY, call.request()))
        .unwrap_err();
    assert!(matches!(err, Error::Status { code: 400, .. }));
}

#[test]
fn delete_is_a_204() {
    let mut cl = cluster();
    let call = client().delete("db").unwrap();
    let r = leader(&mut cl, KEY, call.request());
    assert_eq!(r.status, 204);
    assert_eq!(call.parse(&r), Ok(()));
    assert!(!cl.has_job("db"));
}

// --- De agent-routes, letterlijk zoals api/src/node.rs ze schrijft. ---

#[test]
fn capacity_direct_and_via_the_leader() {
    let body = r#"{"cpu_cores":4,"memory_bytes":8589934592,"cpu_used_shares":1024,"memory_used_bytes":536870912,"tasks_running":2,"attributes":{"node.arch":"arm64"}}"#;
    let a = &cluster().agents[0];
    let direct = client().agent_capacity(a).unwrap();
    let cap = direct.parse(&reply(200, body)).unwrap();
    assert_eq!(cap.cpu_cores, 4);
    assert_eq!(cap.memory_bytes, 8 << 30);
    assert_eq!(cap.cpu_used_shares, 1024);
    assert_eq!(cap.tasks_running, 2);
    assert_eq!(cap.attributes.get("node.arch").unwrap(), "arm64");

    let via = client().capacity(a).unwrap();
    assert_eq!(via.parse(&reply(200, body)).unwrap(), Some(cap));
    // Een oude leader: terugval. Een onbekende agent: een fout.
    assert_eq!(
        via.parse(&reply(404, r#"{"error":"not found"}"#)).unwrap(),
        None
    );
    let err = via
        .parse(&reply(404, r#"{"error":"agent not found"}"#))
        .unwrap_err();
    assert_eq!(err.to_string(), "status 404: agent not found");
}

#[test]
fn leader_with_and_without_lease() {
    let call = client().leader().unwrap();
    let l = call
        .parse(&reply(
            200,
            r#"{"leader":"10.0.0.1:8878","lease_expires_at":"2026-09-30T12:00:00Z"}"#,
        ))
        .unwrap();
    assert_eq!(l.leader, "10.0.0.1:8878");
    assert_eq!(
        l.lease_expires_at,
        Time::parse_rfc3339("2026-09-30T12:00:00Z").unwrap()
    );
    let l = call.parse(&reply(200, r#"{"leader":""}"#)).unwrap();
    assert!(l.leader.is_empty() && l.lease_expires_at.is_zero());
}

#[test]
fn a_status_without_json_keeps_the_code() {
    let call = client().jobs().unwrap();
    let err = call.parse(&reply(503, "Service Unavailable")).unwrap_err();
    assert_eq!(err.to_string(), "status 503");
    let err = call.parse(&reply(200, "{not json")).unwrap_err();
    assert!(matches!(err, Error::Json(_)));
}

// --- SSE: de bytes van api::EventLog. ---

/// Alles wat er uit `bytes` komt, hap voor hap van `step` bytes.
fn read_all(bytes: &[u8], step: usize) -> Vec<Event> {
    let mut rd = Events::new();
    let mut out = Vec::new();
    for chunk in bytes.chunks(step) {
        rd.push(chunk).unwrap();
        while let Some(e) = rd.next().unwrap() {
            out.push(e);
        }
    }
    out
}

fn event_log_bytes() -> String {
    let mut log = api::EventLog::new();
    let seq = log.seq();
    log.push_topic("job:api");
    log.push_topic("job:api:started");
    log.push_topic("agent:node-2");
    log.push_topic("");
    let mut wire = String::from(api::PING);
    wire.push_str(api::KEEPALIVE);
    log.since(seq, &mut wire);
    wire
}

#[test]
fn sse_from_the_event_log_in_any_chunking() {
    let wire = event_log_bytes();
    let want = [
        ("ping", "{}"),
        ("job", r#"{"name":"api"}"#),
        ("task", r#"{"job":"api","event":"started"}"#),
        ("agent", r#"{"id":"node-2"}"#),
        ("status", "{}"),
    ];
    for step in [1, 2, 3, 7, 64, wire.len()] {
        let got = read_all(wire.as_bytes(), step);
        let got: Vec<(&str, &str)> = got
            .iter()
            .map(|e| (e.kind.as_str(), e.data.as_str()))
            .collect();
        assert_eq!(got, want, "step {step}");
    }
}

#[test]
fn sse_topics_and_the_go_job_parser() {
    let got = read_all(event_log_bytes().as_bytes(), 5);
    assert_eq!(got[0].topic().unwrap(), Topic::Ping);
    assert!(got[0].is_resync());
    assert_eq!(got[1].topic().unwrap(), Topic::Job { name: "api".into() });
    assert_eq!(
        got[2].topic().unwrap(),
        Topic::Task {
            job: "api".into(),
            event: "started".into()
        }
    );
    assert_eq!(
        got[3].topic().unwrap(),
        Topic::Agent {
            id: "node-2".into()
        }
    );
    assert_eq!(got[4].topic().unwrap(), Topic::Status);
    assert!(got[4].is_resync());
    // Go's ParseJobFromSSE: de naam van een job-melding, de job van een taak.
    assert_eq!(got[1].job().as_deref(), Some("api"));
    assert_eq!(got[2].job().as_deref(), Some("api"));
    assert_eq!(got[3].job(), None);
    let odd = Event {
        kind: "future".into(),
        data: "x".into(),
    };
    assert_eq!(odd.topic().unwrap(), Topic::Other);
}

#[test]
fn sse_log_lines_crlf_comments_and_multiline() {
    // Een logregel met een regeleinde erin: twee data-regels, één gebeurtenis.
    let mut wire = String::new();
    api::data_frame("first\nsecond", &mut wire);
    wire.push_str(": comment only\n\n");
    wire.push_str("event: job\r\ndata: {\"name\":\"x\"}\r\n\r\n");
    wire.push_str("event:tight\ndata:no-space\n\n");
    let got = read_all(wire.as_bytes(), 4);
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].kind, "");
    assert_eq!(got[0].data, "first\nsecond");
    assert_eq!(got[1].kind, "job");
    assert_eq!(got[1].data, r#"{"name":"x"}"#);
    assert_eq!(
        (got[2].kind.as_str(), got[2].data.as_str()),
        ("tight", "no-space")
    );
}

#[test]
fn sse_refuses_an_endless_event_and_reset_drops_a_half() {
    let mut rd = Events::new();
    let big = vec![b'a'; MAX_EVENT_BYTES + 1];
    assert_eq!(
        rd.push(&big),
        Err(Error::TooLarge {
            len: MAX_EVENT_BYTES + 1,
            max: MAX_EVENT_BYTES
        })
    );
    assert_eq!(rd.next(), Ok(None));
    // Een half binnengekomen gebeurtenis van een verbroken stroom.
    rd.push(b"event: job\ndata: {\"na").unwrap();
    rd.reset();
    rd.push(api::PING.as_bytes()).unwrap();
    assert_eq!(rd.next().unwrap().unwrap().kind, "ping");
    assert_eq!(rd.next(), Ok(None));
}

// --- De backoff en de reden. ---

#[test]
fn backoff_doubles_to_the_ceiling_and_resets() {
    let mut b = Backoff::default();
    let got: Vec<u64> = (0..7).map(|_| b.next_delay().as_secs()).collect();
    assert_eq!(got, [1, 2, 4, 8, 16, 30, 30]);
    assert_eq!(b.peek(), BACKOFF_MAX);
    b.reset();
    assert_eq!(b.next_delay(), BACKOFF_MIN);
    let odd = Backoff::new(Duration::from_secs(5), Duration::from_secs(1));
    assert_eq!(odd.peek(), Duration::from_secs(5));
}

#[test]
fn reason_truncates_on_a_char_boundary() {
    let long = "\u{e9}".repeat(REASON_BYTES);
    let r = Reason::new(&long);
    assert_eq!(r.as_str().len(), REASON_BYTES);
    let odd = alloc::format!("x{long}");
    assert_eq!(Reason::new(&odd).as_str().len(), REASON_BYTES - 1);
    assert_eq!(Reason::of(&Error::Closed).as_str(), "stream closed");
}

#[cfg(feature = "std")]
mod host;

#[cfg(feature = "hopos")]
mod hopos;

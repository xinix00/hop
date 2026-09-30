//! De reeks van het dashboard (hop-gui) tegen een echte daemon: alles op de
//! agent-poort, zoals de browser het doet, met `X-Hop-Auth` en een
//! `Origin`, en op elk antwoord de CORS-koppen.
//!
//! `app.js` praat direct met een agent: de preflight (zonder handtekening),
//! `/leader`, `/v1/status`, `/v1/agents`, `/v1/jobs` (GET en POST), de
//! jobstatus met de takentabel, de capaciteit van een agent, de sleep-
//! volgorde (`PATCH .../priority`), `/v1/events` als SSE, de log van een
//! taak (zonder query: levend, zoals in Go) en `DELETE`. De `/v1/...` gaan
//! op de agent-poort als doorgifte naar de leader (hier in-proces), en
//! juist die antwoorden moeten de koppen ook dragen.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "een test faalt luid"
)]

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use hostnet::{Call, Http, Open, Reply};
use types::json::{self, Value};

const KEY: &str = "dashboard-secret";
const T: Duration = Duration::from_secs(10);
const ORIGIN: &str = "http://localhost:3000";

/// Twee vrije poorten P en P + 1000, in een eigen reeks.
fn ports() -> u16 {
    let start = 30_000 + (std::process::id() % 80) as u16 * 100;
    for p in (start..39_000).chain(30_000..start) {
        if TcpListener::bind(("127.0.0.1", p)).is_ok()
            && TcpListener::bind(("127.0.0.1", p + 1000)).is_ok()
        {
            return p;
        }
    }
    panic!("no free port pair");
}

struct Daemon {
    child: Child,
    log: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Daemon {
    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

fn start(dir: &Path, port: u16) -> Daemon {
    let cfg = dir.join("hop.json");
    std::fs::write(
        &cfg,
        format!(
            r#"{{"node": {{"id": "g1", "ip": "127.0.0.1", "port": {port}}},
                "cluster": {{"name": "dash"}},
                "paths": {{"state_file": "{}", "rootfs_base": "{}"}},
                "runner": {{"isolate": false}},
                "api_key": "{KEY}"}}"#,
            dir.join("state.json").display(),
            dir.join("tasks").display()
        ),
    )
    .unwrap();
    let log = dir.join("agentd.log");
    let child = Command::new(env!("CARGO_BIN_EXE_agentd"))
        .args(["--config", cfg.to_str().unwrap()])
        .current_dir(dir)
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    Daemon { child, log }
}

/// De koppen van de browser: de handtekening van Web Crypto
/// (`hex(HMAC-SHA256(key, METHOD\nPATH\nhex(sha256(body))))`), de origin,
/// en bij een body het content-type.
fn browser(method: &str, url: &str, body: Option<&[u8]>) -> Vec<(&'static str, String)> {
    let sig = auth::sign_call(KEY.as_bytes(), method, url, body.unwrap_or_default())
        .map(|s| String::from_utf8(s.to_vec()).unwrap())
        .unwrap();
    let mut h = vec![(auth::AUTH_HEADER, sig), ("Origin", String::from(ORIGIN))];
    if body.is_some() {
        h.push(("Content-Type", String::from("application/json")));
    }
    h
}

fn request(method: &str, url: &str, headers: &[(&str, String)], body: Option<&[u8]>) -> Reply {
    let headers: Vec<(&str, &str)> = headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let c = Call {
        method,
        url,
        headers: &headers,
        body,
        timeout: T,
    };
    Http::new().request(&c, 1 << 20).unwrap()
}

/// Een aanroep van het dashboard; de CORS-koppen zijn er altijd.
fn call(method: &str, url: &str, body: Option<&[u8]>) -> Reply {
    let r = request(method, url, &browser(method, url, body), body);
    assert_eq!(
        r.header("Access-Control-Allow-Origin"),
        Some("*"),
        "{method} {url}: no CORS on {} {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );
    r
}

/// Een stroom van het dashboard (SSE, een log-tail); de kop draagt CORS.
fn open(url: &str) -> Open {
    let h = browser("GET", url, None);
    let headers: Vec<(&str, &str)> = h.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let c = Call {
        method: "GET",
        url,
        headers: &headers,
        body: None,
        timeout: Duration::from_secs(30),
    };
    let o = Http::new().open(&c).unwrap();
    assert_eq!(o.status(), 200, "{url}");
    assert_eq!(o.header("content-type"), Some("text/event-stream"), "{url}");
    assert_eq!(o.header("access-control-allow-origin"), Some("*"), "{url}");
    o
}

fn read_until(o: &mut Open, d: &Daemon, what: &str, done: impl Fn(&str) -> bool) -> String {
    let until = Instant::now() + Duration::from_secs(20);
    let mut seen = String::new();
    let mut buf = [0u8; 4096];
    while Instant::now() < until {
        let n = o.read(&mut buf).unwrap();
        assert!(n > 0, "stream ended before {what}: {seen}\n{}", d.log());
        seen.push_str(&String::from_utf8_lossy(&buf[..n]));
        if done(&seen) {
            return seen;
        }
    }
    panic!("timed out waiting for {what}: {seen}\n{}", d.log());
}

fn wait_for(what: &str, d: &Daemon, mut ok: impl FnMut() -> bool) {
    let until = Instant::now() + Duration::from_secs(20);
    while Instant::now() < until {
        if ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("timed out waiting for {what}; daemon log:\n{}", d.log());
}

fn field<'a>(v: &'a Value, k: &str) -> &'a Value {
    v.as_object().unwrap().get(k).unwrap_or(&Value::Null)
}

fn body(r: &Reply) -> Value {
    json::parse(&r.body).unwrap()
}

fn last_tick(s: &str) -> Option<u64> {
    s.split("tick ")
        .skip(1)
        .filter_map(|r| r.split(|c: char| !c.is_ascii_digit()).next()?.parse().ok())
        .max()
}

#[test]
fn the_dashboard_sequence_on_the_agent_port() {
    let dir = std::env::temp_dir().join(format!("agentd-dashboard-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port = ports();
    let d = start(&dir, port);
    let agent = format!("http://127.0.0.1:{port}");
    wait_for("the leader", &d, || d.log().contains("HOP_LEADER"));
    wait_for("the agent registered", &d, || {
        let r = request(
            "GET",
            &format!("{agent}/v1/agents"),
            &browser("GET", &format!("{agent}/v1/agents"), None),
            None,
        );
        r.status == 200 && String::from_utf8_lossy(&r.body).contains(r#""id":"g1""#)
    });

    // De preflight: geen handtekening, wel PATCH, DELETE en de twee koppen.
    for path in ["/v1/jobs", "/v1/jobs/ticker/priority", "/v1/events"] {
        let r = request(
            "OPTIONS",
            &format!("{agent}{path}"),
            &[
                ("Origin", String::from(ORIGIN)),
                ("Access-Control-Request-Method", String::from("PATCH")),
                (
                    "Access-Control-Request-Headers",
                    String::from("content-type,x-hop-auth"),
                ),
            ],
            None,
        );
        assert_eq!(r.status, 200, "OPTIONS {path}");
        assert_eq!(r.header("Access-Control-Allow-Origin"), Some("*"));
        let m = r.header("Access-Control-Allow-Methods").unwrap_or("");
        assert!(m.contains("PATCH") && m.contains("DELETE"), "{m}");
        let h = r.header("Access-Control-Allow-Headers").unwrap_or("");
        assert!(
            h.contains("X-Hop-Auth") && h.contains("Content-Type"),
            "{h}"
        );
    }

    // Een ongetekend verzoek: 401, en toch met CORS (de browser toont dan
    // de weigering in plaats van een CORS-fout).
    let r = request(
        "GET",
        &format!("{agent}/v1/jobs"),
        &[("Origin", String::from(ORIGIN))],
        None,
    );
    assert_eq!(r.status, 401);
    assert_eq!(r.header("Access-Control-Allow-Origin"), Some("*"));

    // refresh(): status, jobs, agents, leader.
    let st = body(&call("GET", &format!("{agent}/v1/status"), None));
    assert_eq!(field(&st, "cluster_name").as_str(), Some("dash"));
    for k in ["agents", "jobs", "total_placed", "settling", "placed"] {
        assert!(st.as_object().unwrap().get(k).is_some(), "status lacks {k}");
    }
    let agents = body(&call("GET", &format!("{agent}/v1/agents"), None));
    let a = &agents.as_array().unwrap()[0];
    assert_eq!(field(a, "endpoint").as_str(), Some(agent.as_str()));
    let l = body(&call("GET", &format!("{agent}/leader"), None));
    assert!(!field(&l, "leader").as_str().unwrap_or("").is_empty());

    // De capaciteit per agent (via de leader naar die agent).
    let cap = body(&call(
        "GET",
        &format!("{agent}/v1/agents/g1/capacity"),
        None,
    ));
    for k in [
        "cpu_cores",
        "memory_bytes",
        "cpu_used_shares",
        "memory_used_bytes",
        "tasks_running",
    ] {
        assert!(
            cap.as_object().unwrap().get(k).is_some(),
            "capacity lacks {k}"
        );
    }

    // De stroom van het dashboard staat vóór er iets gebeurt.
    let mut ev = open(&format!("{agent}/v1/events"));
    read_until(&mut ev, &d, "the ping", |s| s.contains("event: ping"));

    // startJob(): een job die elke 200 ms een regel schrijft.
    let spec = br#"{"name":"ticker","command":"i=0; while true; do echo tick $i; i=$((i+1)); sleep 0.2; done"}"#;
    let r = call("POST", &format!("{agent}/v1/jobs"), Some(spec));
    assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
    read_until(&mut ev, &d, "an event for ticker", |s| s.contains("ticker"));

    // refreshJobDetail(): de takentabel uit de jobstatus.
    let mut task = String::new();
    wait_for("ticker running in the job status", &d, || {
        let v = body(&call(
            "GET",
            &format!("{agent}/v1/jobs/ticker/status"),
            None,
        ));
        let Some(ts) = field(field(&v, "tasks_by_agent"), "g1").as_array() else {
            return false;
        };
        let Some(t) = ts.first() else { return false };
        task = field(t, "id").as_str().unwrap_or("").to_string();
        field(t, "state").as_str() == Some("running")
    });
    let v = body(&call(
        "GET",
        &format!("{agent}/v1/jobs/ticker/status"),
        None,
    ));
    assert_eq!(
        field(&field(&v, "agents").as_array().unwrap()[0], "id").as_str(),
        Some("g1")
    );

    // startLogStream(): zonder query, en levend.
    let mut tail = open(&format!("{agent}/v1/agents/g1/logs/{task}/stdout"));
    let first = read_until(&mut tail, &d, "a tick", |s| last_tick(s).is_some());
    let then = last_tick(&first).unwrap();
    read_until(&mut tail, &d, "a newer tick", |s| {
        last_tick(s).is_some_and(|n| n > then + 2)
    });

    // onDrop(): de sleepvolgorde. Het dashboard stuurt de doelplek; de
    // leader nummert dicht (0..N-1), zoals Go.
    let idle = br#"{"name":"idle","command":"sleep 60"}"#;
    let r = call("POST", &format!("{agent}/v1/jobs"), Some(idle));
    assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
    let prio = |name: &str| {
        let jobs = body(&call("GET", &format!("{agent}/v1/jobs"), None));
        let j = jobs
            .as_array()
            .unwrap()
            .iter()
            .find(|j| field(j, "name").as_str() == Some(name))
            .cloned()
            .unwrap();
        field(&j, "priority").as_i64()
    };
    assert_eq!((prio("ticker"), prio("idle")), (Some(0), Some(1)));
    let r = call(
        "PATCH",
        &format!("{agent}/v1/jobs/idle/priority"),
        Some(br#"{"priority":0}"#),
    );
    assert_eq!(r.status, 204, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!((prio("idle"), prio("ticker")), (Some(0), Some(1)));
    let r = call("DELETE", &format!("{agent}/v1/jobs/idle"), None);
    assert_eq!(r.status, 204);

    // deleteJob(): weg, en de levende tail eindigt vanzelf.
    let r = call("DELETE", &format!("{agent}/v1/jobs/ticker"), None);
    assert_eq!(r.status, 204);
    let until = Instant::now() + Duration::from_secs(20);
    let mut buf = [0u8; 4096];
    loop {
        assert!(Instant::now() < until, "tail did not end\n{}", d.log());
        match tail.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }
    drop(ev);
    drop(d);
    std::fs::remove_dir_all(&dir).unwrap();
}

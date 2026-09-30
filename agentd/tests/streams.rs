//! De stromen van de daemon van begin tot eind: `/v1/events` als SSE, de
//! rondgang van `/v1/tasks`, en de log van een taak via de leader, eerst
//! als momentopname (`?follow=0`) en dan levend (`?follow=1`) tot de taak
//! stopt.
//!
//! Alles gaat naar de leader-poort, zoals `hop jobs`, `hop logs` en `hop
//! events` zonder `--agent` doen.

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

const KEY: &str = "stream-secret";
const T: Duration = Duration::from_secs(10);

/// Twee vrije poorten P en P + 1000, in een andere reeks dan `e2e.rs`.
fn ports() -> u16 {
    let start = 41_000 + (std::process::id() % 100) as u16 * 100;
    for p in (start..58_000).chain(41_000..start) {
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
            r#"{{"node": {{"id": "s1", "ip": "127.0.0.1", "port": {port}}},
                "cluster": {{"name": "streams"}},
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

fn signed(method: &str, url: &str, body: Option<&[u8]>) -> Vec<(&'static str, String)> {
    let sig = auth::sign_call(KEY.as_bytes(), method, url, body.unwrap_or_default())
        .map(|s| String::from_utf8(s.to_vec()).unwrap())
        .unwrap();
    vec![(auth::AUTH_HEADER, sig)]
}

fn call(method: &str, url: &str, body: Option<&[u8]>) -> Reply {
    let h = signed(method, url, body);
    let mut headers: Vec<(&str, &str)> = h.iter().map(|(k, v)| (*k, v.as_str())).collect();
    headers.push(("Content-Type", "application/json"));
    let c = Call {
        method,
        url,
        headers: &headers,
        body,
        timeout: T,
    };
    Http::new().request(&c, 1 << 20).unwrap()
}

fn open(url: &str) -> Open {
    let h = signed("GET", url, None);
    let headers: Vec<(&str, &str)> = h.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let c = Call {
        method: "GET",
        url,
        headers: &headers,
        body: None,
        timeout: Duration::from_secs(30),
    };
    Http::new().open(&c).unwrap()
}

/// Leest van `o` tot `done(alles tot nu)` of de termijn; geeft wat er kwam.
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

/// Het hoogste `tick N` in een stuk log.
fn last_tick(s: &str) -> Option<u64> {
    s.split("tick ")
        .skip(1)
        .filter_map(|r| r.split(|c: char| !c.is_ascii_digit()).next()?.parse().ok())
        .max()
}

#[test]
fn events_tasks_and_a_live_log_through_the_leader() {
    let dir = std::env::temp_dir().join(format!("agentd-streams-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port = ports();
    let d = start(&dir, port);
    let leader = format!("http://127.0.0.1:{}", port + 1000);
    let agent = format!("http://127.0.0.1:{port}");
    wait_for("the leader", &d, || {
        d.log().contains("HOP_LEADER")
            && Http::new()
                .request(&Call::get(&format!("{leader}/health"), T), 1024)
                .is_ok_and(|r| r.status == 200)
    });

    // hop events: de stroom staat (ping) vóór er iets gebeurt.
    let mut ev = open(&format!("{leader}/v1/events"));
    assert_eq!(ev.status(), 200);
    assert_eq!(ev.header("content-type"), Some("text/event-stream"));
    read_until(&mut ev, &d, "the ping", |s| {
        s.contains("event: ping\ndata: {}")
    });

    // Een taak die elke 200 ms een regel schrijft.
    let spec = br#"{"name":"ticker","command":"i=0; while true; do echo tick $i; i=$((i+1)); sleep 0.2; done"}"#;
    let r = call("POST", &format!("{leader}/v1/jobs"), Some(spec));
    assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
    // De plaatsing komt als melding over de stroom (een `task`-gebeurtenis
    // met het event, of de job zelf).
    read_until(&mut ev, &d, "an event for ticker", |s| {
        s.contains(r#""job":"ticker""#) || s.contains(r#""name":"ticker""#)
    });

    // hop jobs via de leader: /v1/tasks met de taak per agent.
    let mut task = String::new();
    wait_for("ticker running in /v1/tasks", &d, || {
        let r = call("GET", &format!("{leader}/v1/tasks"), None);
        let v = json::parse(&r.body).unwrap();
        let Some(ts) = field(field(&v, "tasks_by_agent"), "s1").as_array() else {
            return false;
        };
        ts.iter().any(|t| {
            task = field(t, "id").as_str().unwrap_or("").to_string();
            field(t, "job_name").as_str() == Some("ticker")
                && field(t, "state").as_str() == Some("running")
        })
    });
    // Dezelfde route via de agent-poort (de proxy, in-proces).
    let r = call("GET", &format!("{agent}/v1/tasks"), None);
    assert!(String::from_utf8_lossy(&r.body).contains(&task));

    // hop logs: een momentopname via de doorgifte van de leader
    // (`follow=0`; zonder query volgt deze route, zoals in Go).
    let snap_url = format!("{leader}/v1/agents/s1/logs/{task}/stdout");
    let mut snap = String::new();
    wait_for("a few ticks in the snapshot", &d, || {
        let r = call("GET", &format!("{snap_url}?follow=0"), None);
        snap = String::from_utf8_lossy(&r.body).into_owned();
        r.status == 200 && last_tick(&snap).is_some_and(|n| n >= 2)
    });
    assert!(snap.contains("data: tick 0\n\n"), "{snap}");
    let then = last_tick(&snap).unwrap();

    // hop logs --follow: de tail begint met wat er al was en groeit door.
    let mut tail = open(&format!("{snap_url}?follow=1"));
    assert_eq!(tail.status(), 200);
    let got = read_until(&mut tail, &d, "a newer tick", |s| {
        last_tick(s).is_some_and(|n| n > then + 3)
    });
    assert!(got.contains("data: tick 0\n\n"), "{got}");

    // Een agent die de leader niet kent: 404, geen doorgifte.
    let r = call("GET", &format!("{leader}/v1/agents/nope/capacity"), None);
    assert_eq!(r.status, 404);
    // De capaciteit via de leader.
    let r = call("GET", &format!("{leader}/v1/agents/s1/capacity"), None);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert!(String::from_utf8_lossy(&r.body).contains("cpu_cores"));

    // Stop de job: de ring gaat dicht en de levende tail eindigt vanzelf.
    let r = call("DELETE", &format!("{leader}/v1/jobs/ticker"), None);
    assert_eq!(r.status, 204);
    let until = Instant::now() + Duration::from_secs(20);
    let mut buf = [0u8; 4096];
    loop {
        assert!(Instant::now() < until, "tail did not end\n{}", d.log());
        match tail.read(&mut buf) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => panic!("tail broke instead of ending: {e}"),
        }
    }

    // Het plafond: de open stromen (de events-stroom telt mee) houden
    // threads vast; boven het plafond weigert de node luid.
    let mut held = Vec::new();
    let mut refused = None;
    for _ in 0..8 {
        let o = open(&format!("{leader}/v1/events"));
        if o.status() == 503 {
            refused = Some(o);
            break;
        }
        held.push(o);
    }
    let mut refused = refused.expect("no stream was refused");
    let body = refused.read_to_end(4096).unwrap();
    assert!(
        String::from_utf8_lossy(&body).contains("too many open streams"),
        "{}",
        String::from_utf8_lossy(&body)
    );
    drop(held);
    drop(ev);
    drop(d);
    std::fs::remove_dir_all(&dir).unwrap();
}

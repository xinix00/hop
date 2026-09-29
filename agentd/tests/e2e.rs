//! De daemon van begin tot eind op deze host: `agentd` standalone met de
//! file-store, een proces-job (`sleep 30`) toepassen, zien dat hij draait,
//! hem verwijderen, en zien dat het proces weg is.
//!
//! De verzoeken zijn die van `hop` (apply, jobs, delete), ondertekend met
//! dezelfde HMAC; `tools/e2e-host.sh` doet hetzelfde met de echte `hop`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "een test faalt luid"
)]

use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use hostnet::{Call, Http, Reply};
use types::json::{self, Value};

const KEY: &str = "e2e-secret";
const T: Duration = Duration::from_secs(10);

/// Twee vrije poorten P en P + 1000. Niet uit de ephemere reeks: macOS
/// deelt die oplopend uit vanaf 49152 en zit na een dag testen boven de
/// 60000, waarna P + 1000 nooit meer past (29-09: "no free port pair" na
/// vijftig pogingen). Daarom een vaste lage reeks, begonnen op een plek
/// die van de pid afhangt, zodat twee tests naast elkaar niet botsen.
fn ports() -> u16 {
    let start = 20_000 + (std::process::id() % 200) as u16 * 100;
    for p in (start..40_000).chain(20_000..start) {
        if TcpListener::bind(("127.0.0.1", p)).is_ok()
            && TcpListener::bind(("127.0.0.1", p + 1000)).is_ok()
        {
            return p;
        }
    }
    panic!("no free port pair");
}

/// De daemon; Drop doodt hem, ook als de test halverwege faalt.
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

fn call(method: &str, url: &str, body: Option<&[u8]>, key: &str) -> Reply {
    let sig = auth::sign_call(key.as_bytes(), method, url, body.unwrap_or_default())
        .map(|s| String::from_utf8(s.to_vec()).unwrap());
    let mut headers = vec![("Content-Type", "application/json")];
    if let Some(s) = &sig {
        headers.push((auth::AUTH_HEADER, s.as_str()));
    }
    let c = Call {
        method,
        url,
        headers: &headers,
        body,
        timeout: T,
    };
    Http::new().request(&c, 1 << 20).unwrap()
}

fn tasks(agent: &str) -> Vec<Value> {
    let r = call("GET", &format!("{agent}/tasks"), None, KEY);
    assert_eq!(r.status, 200);
    json::parse(&r.body).unwrap().as_array().unwrap().to_vec()
}

fn field<'a>(v: &'a Value, k: &str) -> &'a Value {
    v.as_object().unwrap().get(k).unwrap_or(&Value::Null)
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

fn is_alive(pid: i64) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn start(dir: &Path, port: u16) -> Daemon {
    let cfg = dir.join("hop.json");
    let state = dir.join("data").join("state.json");
    std::fs::write(
        &cfg,
        format!(
            r#"{{"node": {{"id": "e2e", "ip": "127.0.0.1", "port": {port}}},
                "cluster": {{"name": "e2e"}},
                "paths": {{"state_file": "{}", "rootfs_base": "{}"}},
                "runner": {{"isolate": false}},
                "api_key": "{KEY}"}}"#,
            state.display(),
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

#[test]
fn standalone_apply_jobs_delete() {
    let dir = std::env::temp_dir().join(format!("agentd-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let port = ports();
    let d = start(&dir, port);
    let agent = format!("http://127.0.0.1:{port}");
    let leader = format!("http://127.0.0.1:{}", port + 1000);

    wait_for("HOP_UP and the leader", &d, || {
        d.log().contains("HOP_UP")
            && Http::new()
                .request(&Call::get(&format!("{leader}/health"), T), 1024)
                .is_ok_and(|r| r.status == 200)
    });
    assert!(d.log().contains("HOP_LEADER"), "{}", d.log());

    // Zonder handtekening: 401.
    let r = call("GET", &format!("{leader}/v1/jobs"), None, "");
    assert_eq!(r.status, 401);

    // hop apply: een proces-job.
    let spec = br#"{"name":"sleeper","command":"sleep 30"}"#;
    let r = call("POST", &format!("{leader}/v1/jobs"), Some(spec), KEY);
    assert_eq!(r.status, 201, "{}", String::from_utf8_lossy(&r.body));
    let v = json::parse(&r.body).unwrap();
    assert_eq!(field(&v, "status").as_str(), Some("dispatched"));

    // hop jobs: de job, en zijn taak draait.
    let r = call("GET", &format!("{leader}/v1/jobs"), None, KEY);
    assert!(String::from_utf8_lossy(&r.body).contains("sleeper"));
    let mut pid = 0;
    wait_for("the task to run", &d, || {
        tasks(&agent).iter().any(|t| {
            pid = field(t, "pid").as_i64().unwrap_or(0);
            field(t, "job_name").as_str() == Some("sleeper")
                && field(t, "state").as_str() == Some("running")
                && pid > 0
        })
    });
    assert!(is_alive(pid), "sleep {pid} not alive");
    let r = call("GET", &format!("{leader}/v1/status"), None, KEY);
    let v = json::parse(&r.body).unwrap();
    assert_eq!(field(field(&v, "placed"), "sleeper").as_i64(), Some(1));
    // Via de agent-API naar de leader (de proxy in-proces): hetzelfde.
    let r = call("GET", &format!("{agent}/v1/jobs"), None, KEY);
    assert_eq!(r.status, 200);

    // De file-store: na de debounce staat de job in het staatbestand.
    let state = dir.join("data").join("state.json");
    wait_for("the committed state", &d, || {
        std::fs::read_to_string(&state).is_ok_and(|s| s.contains("sleeper"))
    });

    // hop delete: de job en zijn proces weg.
    let r = call("DELETE", &format!("{leader}/v1/jobs/sleeper"), None, KEY);
    assert_eq!(r.status, 204);
    wait_for("the task to go", &d, || tasks(&agent).is_empty());
    wait_for("the process to die", &d, || !is_alive(pid));
    wait_for("the state without the job", &d, || {
        std::fs::read_to_string(&state).is_ok_and(|s| !s.contains("sleeper"))
    });

    drop(d);
    let mut s = String::new();
    let _ = std::fs::File::open(dir.join("agentd.log")).map(|mut f| f.read_to_string(&mut s));
    assert!(s.contains("HOP_JOB_PLACED"), "{s}");
    std::fs::remove_dir_all(&dir).unwrap();
}

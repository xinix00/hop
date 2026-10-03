//! De woordenschat van Hop: jobspec, taakstaat, node-identiteit, poorten, artifacts.
//!
//! Deze crate bezit de typen en hun JSON-vorm, en niets anders: geen I/O,
//! geen klok, geen staat. Elke andere crate van Hop spreekt deze woorden,
//! zodat een job die de API binnenkomt byte voor byte dezelfde is als die de
//! leader naar een agent stuurt en die in de gecommitte snapshot staat.
//!
//! De JSON-vorm volgt de Go-generatie (github.com/xinix00/hop, tag v1.0.7,
//! `internal/types`) veld voor veld, zodat de GUI, de CLI en bestaande
//! snapshots blijven werken.
//!
//! Alles is `no_std` met `alloc`, en elke allocatie is faalbaar: een job komt
//! van buiten, en een te grote job is een fout, geen afgebroken programma.

#![cfg_attr(not(test), no_std)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod de;
mod error;
pub mod job;
pub mod json;
mod map;
pub mod task;
pub mod time;

pub use error::{Error, NAME_BYTES, Name};
pub use job::{Artifact, CheckType, Driver, HealthCheck, Job, UpdatePolicy};
pub use map::Map;
pub use task::{Agent, HOP_SLOT, KERN_SLOT, SysUsage, Task, TaskState, Telemetry};
pub use time::{Nanos, Time};

/// Het resultaat van elke faalbare handeling in deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

/// Een kopie die kan falen wanneer de heap op is.
///
/// `Clone` op een `String` of `Vec` breekt het programma af bij OOM; dit is
/// de vorm die het handboek (§6) vraagt voor data die van buiten komt.
pub trait TryClone: Sized {
    /// Maakt een diepe kopie, of geeft [`Error::OutOfMemory`].
    fn try_clone(&self) -> Result<Self>;
}

macro_rules! copy_try_clone {
    ($($t:ty),*) => {$(
        impl TryClone for $t {
            fn try_clone(&self) -> Result<Self> {
                Ok(*self)
            }
        }
    )*};
}
copy_try_clone!(bool, u8, u16, u32, u64, usize, i32, i64, f64);

impl TryClone for alloc::string::String {
    fn try_clone(&self) -> Result<Self> {
        try_string(self)
    }
}

impl<T: TryClone> TryClone for Option<T> {
    fn try_clone(&self) -> Result<Self> {
        match self {
            Some(v) => Ok(Some(v.try_clone()?)),
            None => Ok(None),
        }
    }
}

impl<T: TryClone> TryClone for alloc::vec::Vec<T> {
    fn try_clone(&self) -> Result<Self> {
        let mut out = alloc::vec::Vec::new();
        out.try_reserve_exact(self.len())
            .map_err(|_| Error::OutOfMemory)?;
        for v in self {
            out.push(v.try_clone()?);
        }
        Ok(out)
    }
}

/// Kopieert een `&str` naar een nieuwe `String`, faalbaar.
pub fn try_string(s: &str) -> Result<alloc::string::String> {
    let mut out = alloc::string::String::new();
    out.try_reserve_exact(s.len())
        .map_err(|_| Error::OutOfMemory)?;
    out.push_str(s);
    Ok(out)
}

/// Voegt `v` achteraan toe, faalbaar: eerst ruimte, dan de push.
pub fn try_push<T>(vec: &mut alloc::vec::Vec<T>, v: T) -> Result {
    vec.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
    vec.push(v);
    Ok(())
}

/// Voegt `s` achter aan `out` toe, faalbaar.
pub fn try_push_str(out: &mut alloc::string::String, s: &str) -> Result {
    out.try_reserve(s.len()).map_err(|_| Error::OutOfMemory)?;
    out.push_str(s);
    Ok(())
}

#[cfg(test)]
mod tests {
    //! De tests van `internal/types/types_test.go` uit de Go-generatie
    //! (github.com/xinix00/hop, tag v1.0.7), plus de jobspecs uit `jobs/`
    //! daar als vectoren, letterlijk in de toetsen overgenomen.

    use super::*;
    use crate::time::SECOND;
    use alloc::format;
    use alloc::string::ToString;

    fn map(pairs: &[(&str, &str)]) -> Map<String> {
        let mut m = Map::new();
        for (k, v) in pairs {
            m.insert(k.to_string(), v.to_string()).unwrap();
        }
        m
    }

    #[test]
    fn task_state_constants() {
        assert_eq!(TaskState::Running.as_str(), "running");
        assert_eq!(TaskState::Failed.as_str(), "failed");
        assert_eq!(TaskState::Queued.as_str(), "queued");
        assert_eq!(TaskState::Downloading.as_str(), "downloading");
        assert_eq!(TaskState::Stopping.as_str(), "stopping");
        assert_eq!(TaskState::System.as_str(), "system");
        assert_eq!(TaskState::parse("system"), Some(TaskState::System));
    }

    #[test]
    fn job_json_roundtrip() {
        let mut ports = Map::new();
        ports.insert("http".to_string(), 0).unwrap();
        ports.insert("grpc".to_string(), 0).unwrap();
        let job = Job {
            name: "my-app".to_string(),
            command: "echo hello".to_string(),
            count: 3,
            ports,
            cpu_shares: 100,
            memory_limit: 512 * 1024 * 1024,
            env: map(&[("FOO", "bar")]),
            tags: map(&[("env", "prod")]),
            artifacts: alloc::vec![
                Artifact {
                    url: "https://example.com/app-arm64.tar.gz".to_string(),
                    matches: map(&[("node.arch", "arm64")]),
                    headers: map(&[("Authorization", "Bearer token")]),
                    ..Artifact::default()
                },
                Artifact {
                    url: "https://example.com/app-amd64.tar.gz".to_string(),
                    matches: map(&[("node.arch", "amd64")]),
                    ..Artifact::default()
                },
            ],
            health_check: Some(HealthCheck {
                path: "/health".to_string(),
                port: "http".to_string(),
                interval: 10 * SECOND,
                timeout: 5 * SECOND,
                ..HealthCheck::default()
            }),
            max_restarts: Some(5),
            ..Job::default()
        };
        let data = job.to_json().unwrap();
        let decoded = Job::from_json(data.as_bytes()).unwrap();
        assert_eq!(decoded, job);
        assert_eq!(
            decoded.artifacts[0].matches.get("node.arch").unwrap(),
            "arm64"
        );
        assert_eq!(decoded.health_check.unwrap().path, "/health");
    }

    #[test]
    fn task_json_roundtrip() {
        let mut ports = Map::new();
        ports.insert("http".to_string(), 8080).unwrap();
        ports.insert("grpc".to_string(), 9090).unwrap();
        let task = Task {
            id: "task-456".to_string(),
            job_name: "my-app".to_string(),
            ports,
            pid: 12345,
            state: TaskState::Running,
            started_at: Time(1_790_000_000 * SECOND),
            restart_count: 2,
            cores: 2,
            core: Some(0),
            ..Task::default()
        };
        let data = json::to_string(&task.to_value().unwrap()).unwrap();
        assert!(data.contains(r#""cores":2"#), "{data}");
        // Core 0 (de OS-core) is een meting en staat erin.
        assert!(data.contains(r#""core":0"#), "{data}");
        let decoded = Task::from_value(&json::parse_str(&data).unwrap()).unwrap();
        assert_eq!(decoded, task);
        assert_eq!(decoded.ports.get("http"), Some(&8080));
        // 0 cores is onbekend: het veld ontbreekt, en een taak zonder leest als 0.
        // Geen core is onbekend: dan ontbreekt "core" ook.
        let none = Task {
            cores: 0,
            core: None,
            ..task
        };
        let data = json::to_string(&none.to_value().unwrap()).unwrap();
        assert!(!data.contains("cores") && !data.contains("core"), "{data}");
        let old =
            Task::from_value(&json::parse_str(r#"{"id":"t","cores":null}"#).unwrap()).unwrap();
        assert_eq!((old.cores, old.core), (0, None));
    }

    #[test]
    fn agent_json_roundtrip() {
        let agent = Agent {
            id: "agent-789".to_string(),
            endpoint: "http://192.168.1.10:8080".to_string(),
            last_seen: Time(1_790_000_000 * SECOND),
            ..Agent::default()
        };
        let data = agent.to_json().unwrap();
        let decoded = Agent::from_value(&json::parse_str(&data).unwrap()).unwrap();
        assert_eq!(decoded, agent);
    }

    /// De telemetrie van een heartbeat: plat op de draad, wat niet gemeten
    /// is ontbreekt, en een cpu van 0 is een meting.
    #[test]
    fn agent_telemetry_roundtrip() {
        let agent = Agent {
            id: "n1".to_string(),
            telemetry: Telemetry {
                temp_milli_c: 59_800,
                kern: SysUsage {
                    cpu_percent: Some(0.0),
                    mem_bytes: 3 << 20,
                    ram_bytes: 64 << 20,
                    core: Some(0),
                },
                hop: SysUsage {
                    cpu_percent: None,
                    mem_bytes: 5 << 20,
                    ram_bytes: 0,
                    core: None,
                },
            },
            ..Agent::default()
        };
        let data = agent.to_json().unwrap();
        assert!(data.contains(r#""kern_cpu_percent":0"#), "{data}");
        assert!(data.contains(r#""kern_mem_bytes":3145728"#), "{data}");
        assert!(data.contains(r#""kern_ram_bytes":67108864"#), "{data}");
        assert!(data.contains(r#""hop_mem_bytes":5242880"#), "{data}");
        assert!(!data.contains("hop_cpu_percent"), "{data}");
        assert!(!data.contains("hop_ram_bytes"), "{data}");
        assert!(data.contains(r#""kern_core":0"#), "{data}");
        assert!(!data.contains("hop_core"), "{data}");
        let decoded = Agent::from_value(&json::parse_str(&data).unwrap()).unwrap();
        assert_eq!(decoded, agent);
        // Een oude agent zonder de velden: niets gemeten.
        let old = Agent::from_value(&json::parse_str(r#"{"id":"n2","temp_milli_c":1}"#).unwrap())
            .unwrap();
        assert_eq!(old.telemetry.kern, SysUsage::default());
        assert!(old.system_tasks().unwrap().is_empty());
        // De heartbeat leest dezelfde sleutels; de rest telt niet.
        let hb = r#"{"id":"n1","version":"3","kern_mem_bytes":7,"hop_cpu_percent":12.5,"hop_core":1,"x":1}"#;
        let t = Telemetry::from_value(&json::parse_str(hb).unwrap()).unwrap();
        assert_eq!((t.kern.mem_bytes, t.hop.cpu_percent), (7, Some(12.5)));
        assert_eq!((t.kern.core, t.hop.core), (None, Some(1)));
    }

    /// `kern` en `hop` als taken: pid is het slot, staat `system`, het
    /// geheugen tegen het RAM met één decimaal; alleen wat gemeten is.
    #[test]
    fn agent_system_tasks() {
        let mut agent = Agent::default();
        agent.telemetry.kern = SysUsage {
            cpu_percent: Some(3.0),
            mem_bytes: 3 << 20,
            ram_bytes: 64 << 20,
            core: Some(0),
        };
        let tasks = agent.system_tasks().unwrap();
        assert_eq!(tasks.len(), 1);
        let k = &tasks[0];
        assert_eq!(
            (k.id.as_str(), k.job_name.as_str(), k.driver.as_str(), k.pid),
            ("kern", "kern", "hop", KERN_SLOT)
        );
        assert_eq!(k.state, TaskState::System);
        assert_eq!((k.cpu_percent, k.mem_percent), (3.0, 4.6));
        // Het cpu-procent van de kern en van Hop slaat op één core.
        assert_eq!(k.cores, 1);
        // De kern leeft op de OS-core.
        assert_eq!(k.core, Some(0));
        agent.telemetry.hop.mem_bytes = 1 << 20;
        let tasks = agent.system_tasks().unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(
            (tasks[1].job_name.as_str(), tasks[1].pid),
            ("hop", HOP_SLOT)
        );
        // Zonder RAM geen noemer: 0, en geen cpu-meting ook 0.
        assert_eq!((tasks[1].cpu_percent, tasks[1].mem_percent), (0.0, 0.0));
        // Hop zonder gemelde core: onbekend, niet 0.
        assert_eq!(tasks[1].core, None);
        let back = Task::from_value(&tasks[0].to_value().unwrap()).unwrap();
        assert_eq!(back, tasks[0]);
    }

    #[test]
    fn artifact_auth_helpers() {
        let artifact = Artifact {
            url: "s3://bucket/key".to_string(),
            auth: map(&[
                ("access_key", "AKIAIOSFODNN7EXAMPLE"),
                ("secret_key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
                ("region", "us-east-1"),
            ]),
            ..Artifact::default()
        };
        let data = json::to_string(&artifact.to_value().unwrap()).unwrap();
        let decoded = Artifact::from_value(&json::parse_str(&data).unwrap(), true).unwrap();
        assert_eq!(
            decoded.auth.get("access_key"),
            artifact.auth.get("access_key")
        );
        assert_eq!(decoded.auth.get("region").unwrap(), "us-east-1");
    }

    #[test]
    fn job_defaults() {
        let job = Job::from_json(br#"{"name": "test", "command": "echo"}"#).unwrap();
        assert_eq!(job.count, 0);
        assert!(job.artifacts.is_empty());
        assert!(job.health_check.is_none());
        assert!(job.ports.is_empty());
        assert_eq!(job.desired(), 1);
        assert_eq!(job.driver(), Driver::Exec);
        assert_eq!(job.policy(), UpdatePolicy::Rolling);
    }

    #[test]
    fn job_vectors_from_old_jobs() {
        // jobs/counter.json en counter-docker.json van v1.0.7, letterlijk.
        let counter = br#"{
  "name": "counter",
  "command": "sh -c 'i=0; while true; do echo counter: $i; i=$((i+1)); sleep 1; done'",
  "count": 100,
  "cpu_shares": 1024,
  "memory_limit": 1073741824
}"#;
        let job = Job::from_json(counter).unwrap();
        assert_eq!(job.name, "counter");
        assert_eq!(job.count, 100);
        assert_eq!(job.cpu_shares, 1024);
        assert_eq!(job.memory_limit, 1 << 30);
        assert_eq!(job.driver(), Driver::Exec);

        let docker = br#"{
  "name": "counter-docker",
  "image": "alpine:latest",
  "command": "sh -c 'i=0; while true; do echo counter: $i; i=$((i+1)); sleep 1; done'",
  "count": 10,
  "cpu_shares": 1024,
  "memory_limit": 1073741824
}"#;
        let job = Job::from_json(docker).unwrap();
        assert_eq!(job.driver(), Driver::Docker);
        // Wat we schrijven is wat Go schreef: velden in struct-volgorde.
        assert_eq!(
            job.to_json().unwrap(),
            r#"{"name":"counter-docker","image":"alpine:latest","command":"sh -c 'i=0; while true; do echo counter: $i; i=$((i+1)); sleep 1; done'","count":10,"cpu_shares":1024,"memory_limit":1073741824}"#
        );
    }

    #[test]
    fn job_readme_spec_parses() {
        // De jobspec uit de README van v1.0.7, met duren als strings zoals daar.
        let spec = br#"{
  "name": "api-service",
  "command": "./server --http=$ER_PORT_HTTP",
  "count": 3,
  "affinity": {"node.arch": "arm64"},
  "artifacts": [
    {"url": "s3://bucket/app-arm64.tar.gz", "match": {"node.arch": "arm64"},
     "auth": {"access_key": "...", "secret_key": "...", "region": "eu-west-1"}, "extract": "tar.gz"}
  ],
  "ports": {"http": 0, "grpc": 0},
  "cpu_shares": 2048,
  "memory_limit": 536870912,
  "env": {"DB_HOST": "postgres.internal"},
  "tags": {"service": "api"},
  "volumes": {"/data/shared": "data"},
  "health_check": {"type": "http", "path": "/health", "port": "http", "timeout": "5s",
                   "initial_timeout": "30s", "failure_threshold": 3},
  "max_restarts": 5,
  "update_policy": "rolling"
}"#;
        let job = Job::from_json(spec).unwrap();
        let hc = job.health_check.as_ref().unwrap();
        assert_eq!(hc.timeout, 5 * SECOND);
        assert_eq!(hc.initial_timeout, 30 * SECOND);
        assert_eq!(hc.kind, Some(CheckType::Http));
        assert_eq!(job.update_policy, Some(UpdatePolicy::Rolling));
        assert_eq!(job.ports.len(), 2);
        // Terug en weer heen: duren gaan als nanoseconden, zoals Go schreef.
        let again = Job::from_json(job.to_json().unwrap().as_bytes()).unwrap();
        assert_eq!(again, job);
    }

    #[test]
    fn job_rejects_bad_fields() {
        assert!(Job::from_json(br#"{"name": 1}"#).is_err());
        assert!(Job::from_json(br#"{"name": "a", "ports": {"http": 70000}}"#).is_err());
        assert!(Job::from_json(br#"{"name": "a", "count": 1.5}"#).is_err());
        assert!(Job::from_json(br#"{"name": "a", "update_policy": "yolo"}"#).is_err());
        assert!(Job::from_json(br#"{"name": "a", "memory_limit": -1}"#).is_err());
        // Laks: een onbekende sleutel telt niet; strikt wel, met de naam erbij.
        let v = json::parse_str(r#"{"name": "a", "comand": "x"}"#).unwrap();
        assert!(Job::from_value(&v, false).is_ok());
        let err = Job::from_value(&v, true).unwrap_err();
        assert_eq!(err.to_string(), "unknown field \"comand\"");
    }

    #[test]
    fn a_fixed_port_recreates_unless_told_to_roll() {
        let job = |s: &str| Job::from_json(s.as_bytes()).unwrap();
        // Zonder policy is een vaste poort recreate; zonder vaste poort rolling.
        let fixed = job(r#"{"name":"web","command":"x","ports":{"http":80}}"#);
        assert_eq!(fixed.policy(), UpdatePolicy::Recreate);
        assert!(fixed.check_rollable().is_ok());
        let dynamic = job(r#"{"name":"web","command":"x","ports":{"http":0}}"#);
        assert_eq!(dynamic.policy(), UpdatePolicy::Rolling);
        // Een expliciete rolling met een vaste poort: nee, met de zin.
        let err = job(r#"{"name":"web","ports":{"http":80},"update_policy":"rolling"}"#)
            .check_rollable()
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "job web: a fixed port (http 80) cannot roll; use update_policy recreate or a dynamic port"
        );
        for policy in ["rolling", "blue-green"] {
            let j = job(&format!(
                r#"{{"name":"web","ports":{{"admin":0,"http":80}},"update_policy":"{policy}"}}"#
            ));
            assert!(
                matches!(
                    j.check_rollable(),
                    Err(Error::FixedPortRolls { number: 80, .. })
                ),
                "{policy}"
            );
        }
        // Recreate, een dynamische poort (0) of geen poort: goed.
        assert!(
            job(r#"{"name":"web","ports":{"http":80},"update_policy":"recreate"}"#)
                .check_rollable()
                .is_ok()
        );
        assert!(
            job(r#"{"name":"web","ports":{"http":0}}"#)
                .check_rollable()
                .is_ok()
        );
        assert!(job(r#"{"name":"web"}"#).check_rollable().is_ok());
    }

    #[test]
    fn job_try_clone_is_deep_and_equal() {
        let job = Job::from_json(br#"{"name":"a","env":{"A":"1"},"priority":0}"#).unwrap();
        assert_eq!(job.try_clone().unwrap(), job);
    }

    #[test]
    fn name_truncates_on_char_boundary() {
        let long = "\u{e9}".repeat(40);
        let n = Name::new(&long);
        assert_eq!(n.as_str().len(), 32);
    }
}

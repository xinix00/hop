//! De woordenschat van Hop: jobspec, taakstaat, node-identiteit, poorten, artifacts.
//!
//! Deze crate bezit de typen en hun JSON-vorm, en niets anders: geen I/O,
//! geen klok, geen staat. Elke andere crate van Hop spreekt deze woorden,
//! zodat een job die de API binnenkomt byte voor byte dezelfde is als die de
//! leader naar een agent stuurt en die in de gecommitte snapshot staat.
//!
//! De JSON-vorm volgt de Go-generatie (`OLD/internal/types`) veld voor veld,
//! zodat de GUI, de CLI en bestaande snapshots blijven werken.
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
pub use task::{Agent, Task, TaskState};
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
    //! De tests van `OLD/internal/types/types_test.go`, plus de jobspecs in
    //! `OLD/jobs/` als vectoren.

    use super::*;
    use crate::time::SECOND;
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
            ..Task::default()
        };
        let data = json::to_string(&task.to_value().unwrap()).unwrap();
        let decoded = Task::from_value(&json::parse_str(&data).unwrap()).unwrap();
        assert_eq!(decoded, task);
        assert_eq!(decoded.ports.get("http"), Some(&8080));
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
        // OLD/jobs/counter.json en counter-docker.json, letterlijk.
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
        // De jobspec uit OLD/README.md, met duren als strings zoals daar.
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

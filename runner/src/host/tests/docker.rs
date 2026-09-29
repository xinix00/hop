//! Docker: de API over de unix-socket tegen een nep-daemon (Go: `docker_test.go`, de `do`-naad).
//!
//! Alles draait zonder Docker. `docker_real_container` praat met de echte
//! daemon en draait alleen met `HOP_TEST_DOCKER=1`.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use super::super::docker::{Demux, Docker, MAX_LOG_FRAME, container_name};
use super::super::{HostConfig, HostError, HostRunner, TaskSpec, prepare};
use super::{Reply, TempDir, config, fake_docker, ms, tick_until};
use crate::{RunState, Stream};

/// Een config met de nep-daemon op `sock`.
fn with_socket(base: &std::path::Path, sock: &std::path::Path) -> HostConfig {
    HostConfig {
        docker_socket: sock.to_path_buf(),
        ..config(base, false)
    }
}

fn docker_spec(id: &str, image: &str) -> TaskSpec {
    TaskSpec {
        task_id: id.into(),
        job_name: id.into(),
        image: image.into(),
        ..TaskSpec::default()
    }
}

/// Een daemon die een container maakt en start, lege logs geeft, en voor de
/// rest 404 zegt (zoals voor een container die weg is).
fn lifecycle(method: &str, path: &str) -> Reply {
    match (method, path) {
        ("GET", "/_ping") => Reply::new(200, "OK"),
        ("POST", p) if p.starts_with("/containers/create") => Reply::new(201, "{}"),
        ("POST", p) if p.ends_with("/start") => Reply::new(204, ""),
        ("GET", p) if p.contains("/logs") => Reply::new(200, ""),
        _ => Reply::new(404, r#"{"message":"not found"}"#),
    }
}

// Go: TestDockerRunnerRequiresImage
#[test]
fn docker_runner_requires_image() {
    let base = TempDir::new("d-noimage");
    let cfg = config(base.path(), false);
    let s = TaskSpec {
        command: "echo hello".into(),
        ..docker_spec("test-task", "")
    };
    assert!(matches!(
        prepare(&cfg, types::Driver::Docker, &s, &mut |_, _| {}),
        Err(HostError::ImageRequired)
    ));
}

// Go: TestDockerRunnerStatusNotFound
#[test]
fn docker_runner_status_not_found() {
    let base = TempDir::new("d-status");
    let (sock, _rx) = fake_docker(base.path(), lifecycle);
    let cfg = with_socket(base.path(), &sock);
    let s = docker_spec("nonexistent-container-id", "nginx:latest");
    let p = prepare(&cfg, types::Driver::Docker, &s, &mut |_, _| {});
    // De pull van de nep-daemon zegt 404; de start zelf gaat buiten de pull om.
    assert!(p.is_err());
    let mut r = HostRunner::new(cfg);
    assert_eq!(
        Docker::new(&sock)
            .inspect(&container_name(&s.task_id))
            .unwrap(),
        None
    );
    // Een container die weg is, is een gefaalde taak; een onbekende ook.
    assert_eq!(r.status(&s.task_id), RunState::Failed);
}

// Go: TestDockerRunnerStopNonExistent
#[test]
fn docker_runner_stop_non_existent() {
    let base = TempDir::new("d-stop404");
    let (sock, _rx) = fake_docker(base.path(), |_, _| {
        Reply::new(404, r#"{"message":"not found"}"#)
    });
    let mut r = HostRunner::new(with_socket(base.path(), &sock));
    r.stop(0, "nonexistent-container-id").unwrap();
    Docker::new(&sock)
        .stop_and_remove("hop-nonexistent")
        .unwrap();
}

// Go: TestDockerRunnerGetStdoutStderrNil
#[test]
fn docker_runner_get_stdout_stderr_nil() {
    let base = TempDir::new("d-nologs");
    let r = HostRunner::new(config(base.path(), false));
    assert!(r.logs(0, "nonexistent", Stream::Stdout).is_none());
    assert!(r.logs(0, "nonexistent", Stream::Stderr).is_none());
}

// Go: TestDockerRunnerCleanup
#[test]
fn docker_runner_cleanup() {
    let base = TempDir::new("d-cleanup");
    let (sock, rx) = fake_docker(base.path(), |m, p| match (m, p) {
        ("GET", "/_ping") => Reply::new(200, "OK"),
        ("GET", p) if p.starts_with("/containers/json") => Reply::new(200, "[]"),
        _ => Reply::new(500, "unexpected"),
    });
    let mut r = HostRunner::new(with_socket(base.path(), &sock));
    // Een lege, bereikbare daemon heeft niets om op te ruimen.
    r.init().unwrap();
    let asked: Vec<String> = rx.try_iter().collect();
    assert!(
        asked
            .iter()
            .any(|a| a.starts_with("GET /containers/json?all=true")),
        "{asked:?}"
    );
    assert!(!asked.iter().any(|a| a.starts_with("DELETE")));
}

// Go: TestDockerRunSurfacesPullErrorFromSuccessfulHTTPStream
#[test]
fn docker_run_surfaces_pull_error_from_successful_http_stream() {
    let base = TempDir::new("d-pullerr");
    let (sock, rx) = fake_docker(base.path(), |m, p| match (m, p) {
        ("POST", p) if p.starts_with("/images/create") => Reply::new(
            200,
            "{\"status\":\"Pulling\"}\n{\"errorDetail\":{\"message\":\"manifest unknown\"},\"error\":\"manifest unknown\"}\n",
        ),
        _ => Reply::new(201, "{}"),
    });
    let cfg = with_socket(base.path(), &sock);
    let err = prepare(
        &cfg,
        types::Driver::Docker,
        &docker_spec("pull-error", "missing:latest"),
        &mut |_, _| {},
    )
    .unwrap_err();
    assert!(err.to_string().contains("manifest unknown"), "{err}");
    let asked: Vec<String> = rx.try_iter().collect();
    assert!(
        !asked.iter().any(|a| a.contains("/containers/create")),
        "een container gemaakt na een pull-fout: {asked:?}"
    );
}

// Go: TestDockerStopReturnsDeleteFailure
#[test]
fn docker_stop_returns_delete_failure() {
    let base = TempDir::new("d-rmfail");
    let (sock, _rx) = fake_docker(base.path(), |m, _| {
        if m == "DELETE" {
            Reply::new(500, r#"{"message":"busy"}"#)
        } else {
            Reply::new(204, "")
        }
    });
    let err = Docker::new(&sock).stop_and_remove("hop-busy").unwrap_err();
    assert!(err.to_string().contains("busy"), "{err}");
}

// Go: TestDockerLogsRejectOversizedFrameWithoutAllocation
#[test]
fn docker_logs_reject_oversized_frame_without_allocation() {
    let mut head = [0u8; 8];
    head[0] = 1;
    head[4..].copy_from_slice(&(MAX_LOG_FRAME + 1).to_be_bytes());
    let mut d = Demux::default();
    let err = d.feed(&head, &mut |_, _| {}).unwrap_err();
    assert!(err.contains("frame too large"), "{err}");
    // En via de stroom van de daemon: de fout komt als regel terug.
    let base = TempDir::new("d-frame");
    let (sock, _rx) = fake_docker(base.path(), |_, _| {
        let mut body = vec![1u8, 0, 0, 0];
        body.extend_from_slice(&(MAX_LOG_FRAME + 1).to_be_bytes());
        Reply { status: 200, body }
    });
    let err = Docker::new(&sock)
        .stream_logs("hop-large-frame", &mut |_, _| {})
        .unwrap_err();
    assert!(err.contains("frame too large"), "{err}");
    // Een geldige stroom met twee frames: elk naar zijn eigen stroom.
    let mut d = Demux::default();
    let mut got = Vec::new();
    let mut frames = Vec::new();
    for (kind, text) in [(1u8, "uit\n"), (2u8, "fout\n")] {
        frames.extend_from_slice(&[kind, 0, 0, 0]);
        frames.extend_from_slice(&u32::try_from(text.len()).unwrap().to_be_bytes());
        frames.extend_from_slice(text.as_bytes());
    }
    // In stukjes van drie bytes: de kop mag over twee reads vallen.
    for chunk in frames.chunks(3) {
        d.feed(chunk, &mut |s, l| got.push((s, l))).unwrap();
    }
    assert_eq!(
        got,
        [
            (Stream::Stdout, "uit".to_string()),
            (Stream::Stderr, "fout".to_string())
        ]
    );
}

// Go: TestDockerUsageMeetViaStatsDeltas
#[test]
fn docker_usage_meet_via_stats_deltas() {
    static CALLS: AtomicU32 = AtomicU32::new(0);
    let base = TempDir::new("d-usage");
    let (sock, _rx) = fake_docker(base.path(), |m, p| {
        if p.contains("/stats") {
            let n = CALLS.fetch_add(1, Ordering::Relaxed);
            let total: u64 = if n == 0 { 1_000_000_000 } else { 3_000_000_000 };
            let body = format!(
                r#"{{"cpu_stats":{{"cpu_usage":{{"total_usage":{total}}}}},"memory_stats":{{"usage":104857600,"stats":{{"inactive_file":4857600}}}}}}"#
            );
            return Reply {
                status: 200,
                body: body.into_bytes(),
            };
        }
        lifecycle(m, p)
    });
    let mut r = HostRunner::new(with_socket(base.path(), &sock));
    let s = TaskSpec {
        cpu_shares: 2048,
        ..docker_spec("usage", "alpine")
    };
    // `launch` zonder `prepare`: de pull hoort bij de voorbereiding.
    let p = prepare(
        &config(base.path(), false),
        types::Driver::Exec,
        &TaskSpec {
            command: "true".into(),
            ..s.clone()
        },
        &mut |_, _| {},
    )
    .unwrap();
    r.launch(0, types::Driver::Docker, &s, p).unwrap();
    // De eerste meting heeft geen venster; geheugen zonder page cache.
    assert_eq!(r.usage("usage"), Some((-1.0, 100_000_000)));
    std::thread::sleep(Duration::from_millis(20));
    let (cpu, mem) = r.usage("usage").unwrap();
    assert!(cpu > 0.0, "cpu={cpu}");
    assert_eq!(mem, 100_000_000);
}

/// Met de echte daemon: pull, start, logs, status, stop.
#[test]
fn docker_real_container() {
    if std::env::var("HOP_TEST_DOCKER").as_deref() != Ok("1") {
        eprintln!("skip: set HOP_TEST_DOCKER=1 to run against the real Docker daemon");
        return;
    }
    let base = TempDir::new("d-real");
    let cfg = HostConfig {
        docker_socket: std::env::var("HOP_DOCKER_SOCKET")
            .map_or_else(|_| "/var/run/docker.sock".into(), Into::into),
        ..config(base.path(), false)
    };
    let s = TaskSpec {
        command: "echo hop-docker-ok; sleep 30".into(),
        ..docker_spec("real-docker", "alpine:latest")
    };
    let p = prepare(&cfg, types::Driver::Docker, &s, &mut |_, _| {}).unwrap();
    let mut r = HostRunner::new(cfg);
    assert!(r.docker_available());
    let t0 = Instant::now();
    assert_eq!(r.launch(ms(t0), types::Driver::Docker, &s, p).unwrap(), 0);
    assert!(tick_until(&mut r, t0, Duration::from_secs(10), |r, now| {
        super::text(r, now, &s.task_id, Stream::Stdout).contains("hop-docker-ok")
    }));
    assert_eq!(r.status(&s.task_id), RunState::Running);
    r.stop(ms(t0), &s.task_id).unwrap();
    assert!(tick_until(&mut r, t0, Duration::from_secs(30), |r, _| {
        r.logs(ms(t0), &s.task_id, Stream::Stdout).is_some()
            && r.status(&s.task_id) == RunState::Failed
    }));
    let d = Docker::new(std::path::Path::new(
        &std::env::var("HOP_DOCKER_SOCKET").unwrap_or_else(|_| "/var/run/docker.sock".into()),
    ));
    assert!(tick_until(&mut r, t0, Duration::from_secs(30), |_, _| {
        d.inspect(&container_name(&s.task_id))
            .is_ok_and(|c| c.is_none())
    }));
}

#[test]
fn docker_logs_demux_exit_code_and_stop_retires_them() {
    static LOGS: [u8; 22] = [
        1, 0, 0, 0, 0, 0, 0, 6, b'h', b'e', b'l', b'l', b'o', b'\n', //
        2, 0, 0, 0, 0, 0, 0, 0,
    ];
    let base = TempDir::new("d-logs");
    let (sock, rx) = fake_docker(base.path(), |m, p| match (m, p) {
        ("GET", p) if p.contains("/logs") => Reply {
            status: 200,
            body: LOGS.to_vec(),
        },
        ("GET", p) if p.ends_with("/json") => {
            Reply::new(200, r#"{"State":{"Running":false,"ExitCode":3}}"#)
        }
        ("POST", p) if p.contains("/stop") => Reply::new(204, ""),
        ("DELETE", _) => Reply::new(204, ""),
        _ => lifecycle(m, p),
    });
    let mut r = HostRunner::new(with_socket(base.path(), &sock));
    let p = super::super::Prepared {
        task_dir: std::path::PathBuf::new(),
        dir: None,
    };
    r.launch(0, types::Driver::Docker, &docker_spec("d1", "img"), p)
        .unwrap();
    let t0 = Instant::now();
    assert!(tick_until(&mut r, t0, Duration::from_secs(5), |r, now| {
        super::text(r, now, "d1", Stream::Stdout) == "hello"
    }));
    assert_eq!(r.status("d1"), RunState::Failed);
    assert_eq!(r.exit_code("d1"), Some(3));
    r.stop(ms(t0), "d1").unwrap();
    assert!(tick_until(&mut r, t0, Duration::from_secs(5), |r, now| {
        r.logs(now, "d1", Stream::Stdout)
            .is_some_and(|l| l.is_closed())
    }));
    let calls: Vec<String> = rx.try_iter().collect();
    let name = container_name("d1");
    assert!(
        calls.contains(&format!("POST /containers/{name}/stop?t=10")),
        "{calls:?}"
    );
    assert!(
        calls.contains(&format!("DELETE /containers/{name}?force=true")),
        "{calls:?}"
    );
}

#[test]
fn demux_splits_frames_across_reads() {
    let mut d = Demux::default();
    let mut got = Vec::new();
    for b in [2u8, 0, 0, 0, 0, 0, 0, 4, b'e', b'r', b'r', b'\n'] {
        d.feed(&[b], &mut |s, l| got.push((s, l))).unwrap();
    }
    assert_eq!(got, [(Stream::Stderr, "err".to_string())]);
    let err = d
        .feed(&[9, 0, 0, 0, 0, 0, 0, 1], &mut |_, _| {})
        .unwrap_err();
    assert!(err.contains("invalid multiplex header"));
}

#[test]
fn json_stream_splits_concatenated_objects() {
    use super::super::docker::JsonStream;
    let mut js = JsonStream::default();
    let mut seen = Vec::new();
    let data = br#"{"a":"}{"} {"b":[1,{"c":2}]}"#;
    for chunk in data.chunks(3) {
        js.feed(chunk, &mut |o| {
            seen.push(String::from_utf8(o.to_vec()).unwrap());
            Ok(())
        })
        .unwrap();
    }
    js.finish().unwrap();
    assert_eq!(seen, [r#"{"a":"}{"}"#, r#"{"b":[1,{"c":2}]}"#]);
}

#[test]
fn create_body_matches_go_shape() {
    use super::super::docker::{create_body, escape};
    let mut s = docker_spec("t", "nginx:latest");
    s.command = "echo \"hi\"".to_string();
    s.env = super::map(&[("A", "1")]);
    s.ports.insert("http".to_string(), 8080);
    s.volumes = super::map(&[("/h", "/c")]);
    s.memory_limit = 1 << 20;
    s.cpu_shares = 512;
    assert_eq!(
        create_body(&s).unwrap(),
        r#"{"Image":"nginx:latest","Env":["A=1","ER_PORT_HTTP=8080"],"Cmd":["/bin/sh","-c","echo \"hi\""],"ExposedPorts":{"8080/tcp":{}},"HostConfig":{"PortBindings":{"8080/tcp":[{"HostPort":"8080"}]},"Binds":["/h:/c"],"Memory":1048576,"CpuShares":512}}"#
    );
    // Een kale spec: het image en een lege HostConfig, zoals Go's omitempty.
    assert_eq!(
        create_body(&docker_spec("t", "x")).unwrap(),
        r#"{"Image":"x","HostConfig":{}}"#
    );
    assert_eq!(
        escape(r#"{"name":["hop-"]}"#, b""),
        "%7B%22name%22%3A%5B%22hop-%22%5D%7D"
    );
}

#[test]
fn docker_unreachable_is_an_error_not_a_hang() {
    let base = TempDir::new("d-none");
    let d = Docker::new(&base.path().join("missing.sock"));
    assert!(!d.ping());
    assert!(matches!(d.pull("x"), Err(HostError::DockerIo { .. })));
    assert!(!HostRunner::new(config(base.path(), false)).docker_available());
}

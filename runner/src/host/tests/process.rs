//! Processen: logs na de stop, terugrollen van een half gemaakte taakmap,
//! quarantaine (Go: `process_logs_test.go`, `process_cleanup_darwin_test.go`).
//!
//! `TestExecProcessGoneBarrierNeverSignalsReusedGroup` staat in
//! `host/runner.rs`: de barrière zit in een privé type.

use std::fs;
use std::time::{Duration, Instant};

use super::super::{HostError, HostRunner, prepare};
use super::{TempDir, config, map, ms, spec, text, tick_until};
use crate::Stream;

// Go: TestExecRunnerBewaartLogsNaDeStop
#[test]
fn exec_runner_bewaart_logs_na_de_stop() {
    let base = TempDir::new("logs");
    let cfg = config(base.path(), false);
    let s = spec(
        "t-exec-retire",
        "echo laatste-woorden; echo op-stderr >&2; sleep 5",
    );
    let p = prepare(&cfg, types::Driver::Exec, &s, &mut |_, _| {}).unwrap();
    let mut r = HostRunner::new(cfg.clone());
    let t0 = Instant::now();
    r.launch(ms(t0), types::Driver::Exec, &s, p).unwrap();
    assert!(
        tick_until(&mut r, t0, Duration::from_secs(3), |r, now| {
            text(r, now, &s.task_id, Stream::Stdout).contains("laatste-woorden")
        }),
        "geen logregel vóór de stop"
    );
    r.stop(ms(t0), &s.task_id).unwrap();
    // De stop is klaar als de taak weg is uit de runner (de map is dan weg).
    let dir = base.path().join(&s.task_id);
    assert!(tick_until(&mut r, t0, Duration::from_secs(5), |_, _| !dir.exists()));
    let now = ms(t0);
    assert!(
        text(&r, now, &s.task_id, Stream::Stdout).contains("laatste-woorden"),
        "de bewaarde tail mist de laatste regel"
    );
    assert!(
        text(&r, now, &s.task_id, Stream::Stderr).contains("op-stderr"),
        "stderr is weg: juist daar staat waarom een proces viel"
    );
    // Na de termijn is het wél weg (geen groei in een herstartlus).
    let later = now + cfg.logs.keep_ms + 1;
    r.tick(later);
    assert!(r.logs(later, &s.task_id, Stream::Stdout).is_none());
}

// Go: TestExecRunnerRollsBackTaskDirWhenArtifactFails
#[test]
fn exec_runner_rolls_back_task_dir_when_artifact_fails() {
    let base = TempDir::new("rollback");
    let cfg = config(base.path(), false);
    // Een schema dat nooit kan: faalt vóór er een map is.
    let mut s = spec("artifact-fails", "true");
    s.artifact = Some(types::Artifact {
        url: "ftp://example.invalid/app".into(),
        ..types::Artifact::default()
    });
    assert!(prepare(&cfg, types::Driver::Exec, &s, &mut |_, _| {}).is_err());
    assert!(!base.path().join(&s.task_id).exists());
    // Een download die pas onderweg faalt: de map was er, en is weer weg.
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    s.artifact = Some(types::Artifact {
        url: format!("http://127.0.0.1:{port}/app"),
        ..types::Artifact::default()
    });
    assert!(prepare(&cfg, types::Driver::Exec, &s, &mut |_, _| {}).is_err());
    assert!(!base.path().join(&s.task_id).exists());
}

// Go: TestSetupTaskDirRollsBackPartialSetup
#[test]
fn setup_task_dir_rolls_back_partial_setup() {
    let base = TempDir::new("partial");
    let hosts = TempDir::new("partial-hosts");
    let good = hosts.path().join("a-good");
    let bad_parent = hosts.path().join("z-bad");
    fs::create_dir(&good).unwrap();
    fs::write(&bad_parent, b"not a directory").unwrap();
    let mut s = spec("partial", "true");
    s.volumes = map(&[
        (good.to_str().unwrap(), "/data"),
        (bad_parent.join("x").to_str().unwrap(), "/bad"),
    ]);
    let cfg = config(base.path(), false);
    assert!(prepare(&cfg, types::Driver::Exec, &s, &mut |_, _| {}).is_err());
    assert!(!base.path().join("partial").exists());
    // Het goede volume is niet aangeraakt.
    assert!(good.is_dir());
}

// Go: TestCleanupTaskDirQuarantinesOnUnmountFailure (darwin)
#[cfg(target_os = "macos")]
#[test]
fn cleanup_task_dir_quarantines_on_unmount_failure() {
    use super::super::taskdir::TaskDir;
    let base = TempDir::new("quarantine");
    let task_dir = base.path().join("quarantine");
    let target = task_dir.join("data");
    fs::create_dir_all(&target).unwrap();
    let child = target.join("still-busy");
    fs::write(&child, b"keep").unwrap();
    let mut d = TaskDir {
        path: task_dir.clone(),
        mounts: vec![target],
        released: false,
    };
    assert!(matches!(d.cleanup(), Err(HostError::Quarantined { .. })));
    assert!(
        child.exists(),
        "taakmap weg ondanks een onbevestigde unmount"
    );
    assert!(!d.released, "de quarantaine verloor het eigendom");
    fs::remove_file(&child).unwrap();
    d.cleanup().unwrap();
    assert!(!task_dir.exists());
}

#[test]
fn exit_codes_and_sigkill_after_grace() {
    let base = TempDir::new("exit");
    let cfg = config(base.path(), false);
    let mut r = HostRunner::new(cfg.clone());
    let t0 = Instant::now();
    let s = spec("t-exit", "exit 3");
    let p = prepare(&cfg, types::Driver::Exec, &s, &mut |_, _| {}).unwrap();
    let pid = r.launch(ms(t0), types::Driver::Exec, &s, p).unwrap();
    assert!(pid > 0);
    assert!(tick_until(&mut r, t0, Duration::from_secs(3), |r, _| {
        r.status("t-exit") == crate::RunState::Failed
    }));
    assert_eq!(r.exit_code("t-exit"), Some(3));
    // Een groep die SIGTERM negeert, krijgt SIGKILL na de genade.
    let s = spec("t-stubborn", "trap '' TERM; sleep 30");
    let p = prepare(&cfg, types::Driver::Exec, &s, &mut |_, _| {}).unwrap();
    r.launch(ms(t0), types::Driver::Exec, &s, p).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    r.stop(ms(t0), "t-stubborn").unwrap();
    r.tick(ms(t0));
    assert_eq!(r.status("t-stubborn"), crate::RunState::Running);
    // De klok van de aanroeper springt voorbij de genade.
    let late = ms(t0) + super::super::GRACE_MS + 1;
    let dir = base.path().join("t-stubborn");
    let end = Instant::now() + Duration::from_secs(5);
    while dir.exists() && Instant::now() < end {
        r.tick(late + ms(t0));
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(!dir.exists(), "SIGKILL kwam niet");
}

#[test]
fn env_ports_and_attrs_reach_the_process() {
    let base = TempDir::new("env");
    let cfg = config(base.path(), false);
    let mut s = spec("t-env", "echo $MY_VAR $ER_PORT_HTTP $ER_ATTR_NODE_OS $HOME");
    s.env = map(&[("MY_VAR", "v1")]);
    s.ports.insert("http".to_string(), 8080);
    s.node_attrs = map(&[("node.os", "linux")]);
    let p = prepare(&cfg, types::Driver::Exec, &s, &mut |_, _| {}).unwrap();
    let want = format!("v1 8080 linux {}", p.task_dir.display());
    let mut r = HostRunner::new(cfg);
    let t0 = Instant::now();
    r.launch(0, types::Driver::Exec, &s, p).unwrap();
    assert!(tick_until(&mut r, t0, Duration::from_secs(5), |r, now| {
        text(r, now, "t-env", Stream::Stdout) == want
    }));
}

#[test]
fn discard_removes_a_prepared_task_dir() {
    let base = TempDir::new("discard");
    let cfg = config(base.path(), false);
    let p = prepare(
        &cfg,
        types::Driver::Exec,
        &spec("t-d", "true"),
        &mut |_, _| {},
    )
    .unwrap();
    let dir = p.task_dir.clone();
    assert!(dir.join(super::super::taskdir::MARKER).is_file());
    HostRunner::new(cfg).discard(p);
    assert!(!dir.exists());
}

#[test]
fn launch_refuses_a_running_id_without_touching_its_dir() {
    let base = TempDir::new("dup");
    let cfg = config(base.path(), false);
    let s = spec("t-dup", "sleep 30");
    let p = prepare(&cfg, types::Driver::Exec, &s, &mut |_, _| {}).unwrap();
    let dir = p.task_dir.clone();
    let mut r = HostRunner::new(cfg.clone());
    r.launch(0, types::Driver::Exec, &s, p).unwrap();
    let p = prepare(&cfg, types::Driver::Exec, &s, &mut |_, _| {}).unwrap();
    let err = r.launch(0, types::Driver::Exec, &s, p).unwrap_err();
    assert!(matches!(err, HostError::TaskExists(_)), "{err}");
    assert!(dir.is_dir(), "de map van de lopende taak verdween");
    r.shutdown();
    assert!(!dir.exists());
}

#[test]
fn init_sweeps_only_marked_task_dirs() {
    let base = TempDir::new("init");
    let stale = base.path().join("old-task");
    fs::create_dir_all(stale.join("tmp")).unwrap();
    fs::write(stale.join(super::super::taskdir::MARKER), b"old-task").unwrap();
    let foreign = base.path().join("not-ours");
    fs::create_dir_all(&foreign).unwrap();
    let mut r = HostRunner::new(config(base.path(), false));
    r.init().unwrap();
    assert!(!stale.exists());
    assert!(foreign.is_dir());
}

#[test]
fn long_lines_are_split_not_buffered() {
    use super::super::process::{Lines, MAX_LINE};
    let mut l = Lines::default();
    let mut got = Vec::new();
    l.feed(&vec![b'x'; MAX_LINE + 10], &mut |s| got.push(s));
    l.feed(b"\nab\ncd", &mut |s| got.push(s));
    l.finish(&mut |s| got.push(s));
    let lens: Vec<usize> = got.iter().map(String::len).collect();
    assert_eq!(lens, vec![MAX_LINE, 10, 2, 2]);
}

#[test]
fn nice_follows_the_cfs_weights() {
    use super::super::process::nice_for;
    assert_eq!(nice_for(8192, 8192), 0);
    assert_eq!(nice_for(0, 8192), 19);
    // ln(8192/1024)/ln(1.25) = 9.3 -> 9.
    assert_eq!(nice_for(1024, 8192), 9);
    assert_eq!(nice_for(1, 1 << 30), 19);
}

//! Isolatie: chroot met namespaces op Linux, `sandbox-exec` op macOS (Go: `isolation_test.go`).
//!
//! Wat echt geïsoleerd draait, vraagt root (Linux) of `sandbox-exec`
//! (macOS); zonder slaat de test zich over met één regel, zoals Go's
//! `skipWithoutIsolation`.

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use super::super::process::plan;
use super::super::{HostConfig, HostRunner, prepare};
use super::{TempDir, config, map, ms, spec, text, tick_until};
use crate::Stream;

/// Of dit OS hier geïsoleerde processen kan draaien.
fn can_isolate() -> bool {
    let ok = if cfg!(target_os = "linux") {
        // Root, en de hulpprogramma's van de wrapper.
        std::process::Command::new("id")
            .arg("-u")
            .output()
            .is_ok_and(|o| o.stdout.starts_with(b"0\n"))
            && ["unshare", "chroot"].iter().all(|t| {
                ["/usr/bin", "/bin", "/usr/sbin", "/sbin"]
                    .iter()
                    .any(|d| Path::new(d).join(t).exists())
            })
    } else {
        Path::new("/usr/bin/sandbox-exec").exists()
    };
    if !ok {
        eprintln!("skip: this host cannot run isolated processes");
    }
    ok
}

/// Een plan zoals Go's `setupCommand`, in een taakmap met `tmp/`.
fn setup(dir: &Path, isolate: bool, s: &super::super::TaskSpec) -> super::super::process::Plan {
    fs::create_dir_all(dir.join("tmp")).unwrap();
    plan(&config(dir, isolate), s, dir).unwrap()
}

/// Draait `s` en geeft zijn stdout zodra `want` erin staat (of na 3 s).
fn run_and_read(cfg: &HostConfig, s: &super::super::TaskSpec, want: &str) -> String {
    let p = prepare(cfg, types::Driver::Exec, s, &mut |_, _| {}).unwrap();
    let mut r = HostRunner::new(cfg.clone());
    let t0 = Instant::now();
    r.launch(ms(t0), types::Driver::Exec, s, p).unwrap();
    let mut out = String::new();
    tick_until(&mut r, t0, Duration::from_secs(3), |r, now| {
        out = text(r, now, &s.task_id, Stream::Stdout);
        out.contains(want)
    });
    r.stop(ms(t0), &s.task_id).unwrap();
    r.shutdown();
    out
}

// Go: TestIsolationEnabledByDefault
#[test]
fn isolation_enabled_by_default() {
    assert!(HostConfig::default().isolate);
}

// Go: TestSetupCommandWithIsolation
#[test]
fn setup_command_with_isolation() {
    let d = TempDir::new("iso-cmd");
    let mut s = spec("test-job", "echo hello");
    s.env = map(&[("PORT", "8080")]);
    let p = setup(d.path(), true, &s);
    assert!(!p.argv.is_empty());
    let first = if cfg!(target_os = "linux") {
        "unshare"
    } else {
        "sandbox-exec"
    };
    assert!(p.argv[0].ends_with(first), "{:?}", p.argv);
    assert_eq!(p.env.get("PORT").map(String::as_str), Some("8080"));
}

// Go: TestSetupCommandWithoutIsolation
#[test]
fn setup_command_without_isolation() {
    let d = TempDir::new("noiso-cmd");
    let p = setup(d.path(), false, &spec("test-job", "echo hello"));
    assert_eq!(p.dir, d.path());
    assert_eq!(p.argv, ["/bin/sh", "-c", "echo hello"]);
}

// Go: TestRunnerRunWithIsolation
#[test]
fn runner_run_with_isolation() {
    if !can_isolate() {
        return;
    }
    let base = TempDir::new("iso-run");
    let out = run_and_read(
        &config(base.path(), true),
        &spec("test-isolated-task", "echo isolated"),
        "isolated",
    );
    assert!(out.contains("isolated"), "{out:?}");
}

// Go: TestRunnerRunWithoutIsolation
#[test]
fn runner_run_without_isolation() {
    let base = TempDir::new("noiso-run");
    let out = run_and_read(
        &config(base.path(), false),
        &spec("test-no-isolation-task", "echo not isolated"),
        "not isolated",
    );
    assert!(out.contains("not isolated"), "{out:?}");
}

// Go: TestIsolationWithVolumes
#[test]
fn isolation_with_volumes() {
    if !can_isolate() {
        return;
    }
    let base = TempDir::new("iso-vol");
    let vol = TempDir::new("iso-vol-host");
    fs::write(vol.path().join("test.txt"), b"volume data").unwrap();
    // Linux: /data in de chroot; macOS: de symlink in de taakmap (de shell
    // staat daar), want sandbox-exec verplaatst de wortel niet.
    let cmd = if cfg!(target_os = "linux") {
        "cat /data/test.txt"
    } else {
        "cat data/test.txt"
    };
    let mut s = spec("test-volumes-task", cmd);
    s.volumes = map(&[(vol.path().to_str().unwrap(), "/data")]);
    let out = run_and_read(&config(base.path(), true), &s, "volume data");
    assert!(out.contains("volume data"), "{out:?}");
    // Het volume overleeft het opruimen van de taakmap.
    assert!(vol.path().join("test.txt").exists());
}

// Go: TestIsolationWithEnvVars
#[test]
fn isolation_with_env_vars() {
    let d = TempDir::new("iso-env");
    let mut s = spec("test-env", "echo $MY_VAR");
    s.env = map(&[("MY_VAR", "test_value")]);
    let p = setup(d.path(), true, &s);
    assert_eq!(p.env.get("MY_VAR").map(String::as_str), Some("test_value"));
}

// Go: TestIsolationWithPorts
#[test]
fn isolation_with_ports() {
    let d = TempDir::new("iso-ports");
    let mut s = spec("test-ports", "echo $ER_PORT_HTTP");
    s.ports.insert("http".into(), 8080);
    s.ports.insert("grpc".into(), 9090);
    let p = setup(d.path(), true, &s);
    assert_eq!(p.env.get("ER_PORT_HTTP").map(String::as_str), Some("8080"));
    assert_eq!(p.env.get("ER_PORT_GRPC").map(String::as_str), Some("9090"));
}

// Go: TestSandboxProfileGeneration
#[test]
fn sandbox_profile_generation() {
    let d = TempDir::new("iso-profile");
    let mut s = spec("test-sandbox", "echo test");
    s.volumes = map(&[("/mnt/data", "/data")]);
    let p = setup(d.path(), true, &s);
    assert!(!p.argv.is_empty());
    #[cfg(target_os = "macos")]
    {
        let prof = super::super::os::sandbox_profile(d.path(), &s);
        assert!(prof.starts_with("(version 1)\n(allow default)"));
        assert!(prof.contains("(subpath \"/mnt/data\")"), "{prof}");
        assert!(d.path().join("sandbox.sb").exists());
    }
}

// Go: TestIsolatedProcessCannotAccessRoot
#[test]
fn isolated_process_cannot_access_root() {
    if !can_isolate() {
        return;
    }
    let base = TempDir::new("iso-root");
    let s = spec(
        "test-isolation-check-task",
        "cat /etc/shadow 2>/dev/null && echo 'ACCESS_GRANTED' || echo 'ACCESS_DENIED'",
    );
    let out = run_and_read(&config(base.path(), true), &s, "ACCESS_");
    assert!(!out.contains("ACCESS_GRANTED"), "{out:?}");
}

// Go: TestCleanupRemovesTaskDir
#[test]
fn cleanup_removes_task_dir() {
    if !can_isolate() {
        return;
    }
    let base = TempDir::new("iso-cleanup");
    let cfg = config(base.path(), true);
    let s = spec("test-cleanup-task", "sleep 10");
    let p = prepare(&cfg, types::Driver::Exec, &s, &mut |_, _| {}).unwrap();
    let mut r = HostRunner::new(cfg);
    let t0 = Instant::now();
    r.launch(ms(t0), types::Driver::Exec, &s, p).unwrap();
    let dir = base.path().join(&s.task_id);
    assert!(
        dir.is_dir(),
        "de taakmap hoort er te zijn terwijl hij draait"
    );
    r.stop(ms(t0), &s.task_id).unwrap();
    assert!(
        tick_until(&mut r, t0, Duration::from_secs(5), |_, _| !dir.exists()),
        "de taakmap bleef na de stop"
    );
}

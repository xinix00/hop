//! macOS (en elk ander niet-Linux-Unix): `sandbox-exec`, volumes als symlinks, geen cgroups.
//!
//! Bezit niets: alleen de vorm van het commando en de koppelingen zoals
//! `process_darwin.go` ze maakte. Zelfde signaturen als `os_linux.rs`.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::TaskSpec;
use super::process::{Plan, lookup_user};

/// Het pad van `sandbox-exec`.
const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

/// Het commando van een taak (Go: `setupCommand`).
///
/// Met isolatie gaat het door `sandbox-exec` met het profiel van
/// [`sandbox_profile`], en doet de shell zelf de `cd` (Go: "use cd in shell
/// command instead of cmd.Dir for sandbox compatibility"). De gebruiker geldt
/// in beide vormen, zoals in Go.
pub(crate) fn plan(isolate: bool, spec: &TaskSpec, task_dir: &Path) -> io::Result<Plan> {
    let dir = task_dir.display().to_string();
    let argv = if isolate {
        let profile = task_dir.join("sandbox.sb");
        fs::write(&profile, sandbox_profile(task_dir, spec))?;
        vec![
            SANDBOX_EXEC.to_string(),
            "-f".to_string(),
            profile.display().to_string(),
            "/bin/sh".to_string(),
            "-c".to_string(),
            format!("cd {} && {}", sh_quote(&dir), spec.command),
        ]
    } else {
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            spec.command.clone(),
        ]
    };
    let mut plan = Plan::new(argv, task_dir.to_path_buf());
    plan.env.insert("HOME".into(), dir.clone());
    plan.env.insert("TMPDIR".into(), format!("{dir}/tmp"));
    plan.env
        .insert("PATH".into(), "/usr/local/bin:/usr/bin:/bin".into());
    if !spec.user.is_empty() {
        plan.creds = lookup_user(&spec.user);
        if plan.creds.is_none() {
            eprintln!(
                "runner: user-missing: user {:?} not found, running as current user",
                spec.user
            );
        }
    }
    Ok(plan)
}

/// Het sandbox-profiel (Go: `generateSandboxProfile`).
///
/// `(allow default)` zoals Go: dyld heeft te veel rechten nodig om dicht te
/// beginnen. De volumes staan er expliciet in, voor als de standaard ooit
/// dichtgaat.
pub(crate) fn sandbox_profile(task_dir: &Path, spec: &TaskSpec) -> String {
    let abs = std::path::absolute(task_dir).unwrap_or_else(|_| task_dir.to_path_buf());
    let mut sb = String::from("(version 1)\n(allow default)\n\n");
    sb.push_str(&format!("; Task directory: {}\n\n", abs.display()));
    if !spec.volumes.is_empty() {
        sb.push_str("; Volumes\n");
        for host in spec.volumes.keys() {
            sb.push_str(&format!(
                "(allow file-read* file-write* (subpath \"{}\"))\n",
                host.replace('\\', "\\\\").replace('"', "\\\"")
            ));
        }
        sb.push('\n');
    }
    sb
}

/// Een volume als symlink (Go: `os.Symlink`).
pub(crate) fn mount_volume(host: &Path, target: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(host, target)
}

/// Haalt een volume weg zoals Go's `os.Remove`: een symlink of een lege map.
///
/// Een map met inhoud gaat níet weg; dat is de quarantaine-toets van Go's
/// `TestCleanupTaskDirQuarantinesOnUnmountFailure`.
pub(crate) fn unmount(target: &Path) -> io::Result<()> {
    let res = match fs::symlink_metadata(target) {
        Ok(m) if m.is_dir() => fs::remove_dir(target),
        Ok(_) => fs::remove_file(target),
        Err(e) => Err(e),
    };
    match res {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// Geen chroot op macOS: niets te koppelen.
pub(crate) fn setup_isolation(_task_dir: &Path) -> Vec<PathBuf> {
    Vec::new()
}

/// Geen `/proc` om over te koppelen.
pub(crate) fn fake_meminfo(_task_dir: &Path, _memory_limit: u64) {}

/// Geen cgroups: geen map.
pub(crate) fn prepare_cgroup(_task_id: &str, _memory_limit: u64) -> Option<PathBuf> {
    None
}

/// Geen cgroups: niets weg te halen.
pub(crate) fn remove_cgroup(_task_id: &str) {}

/// Geen cgroups: niets aan te zetten.
pub(crate) fn ensure_cgroup_controllers() {}

/// Zonder cgroup verandert het commando niet.
pub(crate) fn wrap_cgroup(argv: Vec<String>, _cgroup: &Path) -> Vec<String> {
    argv
}

/// Symlinks zijn geen koppelingen: niets kan een veeg gevaarlijk maken.
pub(crate) fn has_mounts_under(_dir: &Path) -> bool {
    false
}

/// `s` tussen enkele aanhalingstekens voor `/bin/sh`.
fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

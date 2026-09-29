//! Linux: chroot met namespaces, bind-mounts, cgroup v2 voor het geheugen.
//!
//! Bezit niets: alleen de vorm van het commando en de koppelingen zoals
//! `process_linux.go` ze maakte. Zelfde signaturen als `os_darwin.rs`.
//!
//! Go zette chroot, `CLONE_NEW*` en de cgroup-fd in `SysProcAttr`; dat kan in
//! Rust alleen via `pre_exec` (unsafe) of nightly-API's. Deze crate is
//! `forbid(unsafe_code)`, dus hier dezelfde stappen als systeemcommando's die
//! elk `exec` doen, zodat de pid van het kind dezelfde blijft:
//!
//! - `unshare --pid --fork --mount --uts --ipc --kill-child` voor de
//!   namespaces (util-linux). `--fork` omdat een nieuwe PID-namespace pas
//!   voor het volgende kind geldt; `--kill-child` zodat de taak meegaat als
//!   `unshare` een signaal krijgt. Alles blijft in de procesgroep van de
//!   runner, dus `kill -- -<pgid>` raakt ze allemaal.
//! - `chroot <taakmap>` (coreutils), dat ook naar `/` gaat.
//! - `mount --bind`, `mount --make-private`, `umount -l` voor de koppelingen.
//! - Het geheugen: een shell-voorzet die zijn eigen pid in `cgroup.procs`
//!   schrijft en dan `exec`t. Go gebruikte `CLONE_INTO_CGROUP` zodat het kind
//!   bij zijn eerste instructie al in de cgroup zat; de voorzet haalt dat
//!   ook, want de taak zelf start pas na de `exec`.
//!
//! Dit alles vraagt root, net als in Go.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::TaskSpec;
use super::process::{Plan, find_tool, lookup_user};

/// De basis van de cgroups van Hop.
const CGROUP_BASE: &str = "/sys/fs/cgroup/hop";

/// Draait in de chroot en de namespaces vóór het commando van de gebruiker
/// (Go: `procWrapperScript`): een verse procfs die alleen de eigen PID's
/// toont, en de nep-`meminfo` eroverheen. Faalt een mount, dan draait het
/// commando toch, alleen zonder die cosmetica. Het commando komt als `$0`
/// binnen, zonder aanhalingstekens-ellende.
pub(crate) const PROC_WRAPPER: &str = "mount -t proc proc /proc 2>/dev/null
[ -f /.hop-meminfo ] && mount --bind /.hop-meminfo /proc/meminfo 2>/dev/null
exec sh -c \"$0\"";

/// Wat de chroot van de host krijgt, en of het alleen-lezen is (Go: `setupIsolationEnv`).
const BINDS: [(&str, bool); 6] = [
    ("/bin", true),
    ("/usr", true),
    ("/lib", true),
    ("/lib64", true),
    // Schrijfbaar: CoreCLR en anderen schrijven naar /dev/null en /dev/shm.
    ("/dev", false),
    // De CA-bundel van de host, en verder niets uit /etc.
    ("/etc/ssl/certs", true),
];

/// Een hulpprogramma op zijn vaste plek, of zijn kale naam (dan faalt de spawn luid).
fn tool(name: &str) -> String {
    find_tool(name).map_or_else(|| name.to_string(), |p| p.display().to_string())
}

/// Het commando van een taak (Go: `setupCommand`).
///
/// Met isolatie: chroot plus namespaces, `HOME=/`, en géén andere gebruiker
/// (Go negeerde `user` daar ook; de taakmap krijgt wel die eigenaar).
pub(crate) fn plan(isolate: bool, spec: &TaskSpec, task_dir: &Path) -> io::Result<Plan> {
    if isolate {
        let argv = vec![
            tool("unshare"),
            "--pid".into(),
            "--fork".into(),
            "--mount".into(),
            "--uts".into(),
            "--ipc".into(),
            "--kill-child".into(),
            tool("chroot"),
            task_dir.display().to_string(),
            "/bin/sh".into(),
            "-c".into(),
            PROC_WRAPPER.into(),
            spec.command.clone(),
        ];
        let mut plan = Plan::new(argv, PathBuf::from("/"));
        plan.env.insert("HOME".into(), "/".into());
        plan.env.insert("TMPDIR".into(), "/tmp".into());
        plan.env.insert("PATH".into(), "/bin:/usr/bin".into());
        return Ok(plan);
    }
    let dir = task_dir.display().to_string();
    let argv = vec!["/bin/sh".into(), "-c".into(), spec.command.clone()];
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

/// Draait een systeemcommando en maakt van een niet-nul exit een fout met zijn stderr.
fn run(program: &str, args: &[&str]) -> io::Result<()> {
    let out = Command::new(tool(program)).args(args).output()?;
    if out.status.success() {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "{program} {}: {}: {}",
        args.join(" "),
        out.status,
        String::from_utf8_lossy(&out.stderr).trim()
    )))
}

/// Een bind-mount van `host` op `target`, privé zodat een unmount niet naar de host lekt.
pub(crate) fn mount_volume(host: &Path, target: &Path) -> io::Result<()> {
    if fs::metadata(host)?.is_dir() {
        fs::create_dir_all(target)?;
    } else {
        fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(target)?;
    }
    let (h, t) = (host.display().to_string(), target.display().to_string());
    run("mount", &["--bind", &h, &t])?;
    let _ = run("mount", &["--make-private", &t]);
    Ok(())
}

/// Koppelt los met `umount -l` (Go: `MNT_DETACH`): ook een bezette koppeling
/// gaat los, zodat een volgende `remove_dir_all` niet in de bron belandt.
/// "Niet gekoppeld" is geen fout (Go negeerde `ENOENT` en `EINVAL`).
pub(crate) fn unmount(target: &Path) -> io::Result<()> {
    match run("umount", &["-l", &target.display().to_string()]) {
        Ok(()) => Ok(()),
        Err(e) => {
            let s = e.to_string();
            if s.contains("not mounted") || s.contains("no mount point") || !target.exists() {
                Ok(())
            } else {
                Err(e)
            }
        }
    }
}

/// Koppelt de systeempaden in de chroot (Go: `setupIsolationEnv`); geeft wat gekoppeld is.
pub(crate) fn setup_isolation(task_dir: &Path) -> Vec<PathBuf> {
    // Het lege doel voor de procfs die de wrapper in de namespace koppelt.
    let _ = fs::create_dir_all(task_dir.join("proc"));
    let mut mounted = Vec::new();
    for (src, ro) in BINDS {
        if fs::metadata(src).is_err() {
            continue;
        }
        let target = task_dir.join(src.trim_start_matches('/'));
        let t = target.display().to_string();
        if let Err(e) = fs::create_dir_all(&target) {
            eprintln!("runner: isolate-bind: mkdir {t}: {e}");
            continue;
        }
        if let Err(e) = run("mount", &["--rbind", src, &t]) {
            eprintln!("runner: isolate-bind: {e}");
            continue;
        }
        let _ = run("mount", &["--make-rprivate", &t]);
        mounted.push(target);
        if ro && let Err(e) = run("mount", &["-o", "remount,bind,ro", &t]) {
            eprintln!("runner: isolate-bind: read-only {t}: {e}");
        }
    }
    // Alleen resolv.conf uit /etc, zodat shadow en passwd buiten blijven.
    if fs::metadata("/etc/resolv.conf").is_ok() {
        let target = task_dir.join("etc/resolv.conf");
        let ok = fs::create_dir_all(task_dir.join("etc")).is_ok()
            && fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&target)
                .is_ok();
        let t = target.display().to_string();
        if ok && run("mount", &["--bind", "/etc/resolv.conf", &t]).is_ok() {
            let _ = run("mount", &["--make-private", &t]);
            mounted.push(target);
        }
    }
    mounted
}

/// Zet een nep-`/proc/meminfo` klaar op `/.hop-meminfo` (Go: `fakeMeminfo`).
///
/// De wrapper koppelt hem over de verse procfs, zodat een runtime die geen
/// cgroups kent het budget van de job ziet in plaats van het RAM van de host.
pub(crate) fn fake_meminfo(task_dir: &Path, memory_limit: u64) {
    if memory_limit == 0 {
        return;
    }
    let kb = memory_limit / 1024;
    let content = format!(
        "MemTotal:       {kb} kB\nMemFree:        {kb} kB\nMemAvailable:   {kb} kB\n\
         Buffers:        0 kB\nCached:         0 kB\nSwapTotal:      0 kB\n\
         SwapFree:       0 kB\nCommitted_AS:   0 kB\n"
    );
    if let Err(e) = fs::write(task_dir.join(".hop-meminfo"), content) {
        eprintln!("runner: meminfo-fake: {e}");
    }
}

/// Het cgroup-pad van een taak.
fn cgroup_path(task_id: &str) -> PathBuf {
    Path::new(CGROUP_BASE).join(task_id)
}

/// Maakt de cgroup van de taak met `memory.max` (Go: `prepareCgroup`); `None` zonder limiet of bij een fout.
pub(crate) fn prepare_cgroup(task_id: &str, memory_limit: u64) -> Option<PathBuf> {
    if memory_limit == 0 {
        return None;
    }
    let path = cgroup_path(task_id);
    if let Err(e) = fs::create_dir_all(&path) {
        eprintln!(
            "runner: cgroup: create {}: {e}; starting without limit",
            path.display()
        );
        return None;
    }
    if let Err(e) = fs::write(path.join("memory.max"), memory_limit.to_string()) {
        eprintln!("runner: cgroup: memory.max on {}: {e}", path.display());
    }
    Some(path)
}

/// Haalt de cgroup van de taak weg; een taak zonder cgroup is geen fout.
pub(crate) fn remove_cgroup(task_id: &str) {
    match fs::remove_dir(cgroup_path(task_id)) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => {
            eprintln!("runner: cgroup: remove {task_id}: {e}");
        }
        _ => {}
    }
}

/// Zet `+memory` aan in de keten tot [`CGROUP_BASE`] (Go: `ensureCgroupControllers`); best effort.
pub(crate) fn ensure_cgroup_controllers() {
    if let Err(e) = fs::write("/sys/fs/cgroup/cgroup.subtree_control", "+memory") {
        eprintln!("runner: cgroup: +memory on root: {e} (continuing)");
    }
    if let Err(e) = fs::create_dir_all(CGROUP_BASE) {
        eprintln!("runner: cgroup: create {CGROUP_BASE}: {e}");
        return;
    }
    if let Err(e) = fs::write(format!("{CGROUP_BASE}/cgroup.subtree_control"), "+memory") {
        eprintln!("runner: cgroup: +memory on {CGROUP_BASE}: {e}");
    }
}

/// Zet de cgroup-voorzet voor `argv`: de shell schrijft zijn pid in
/// `cgroup.procs` en `exec`t de rest, zodat de taak in de cgroup geboren wordt.
pub(crate) fn wrap_cgroup(argv: Vec<String>, cgroup: &Path) -> Vec<String> {
    let mut out = vec![
        "/bin/sh".to_string(),
        "-c".to_string(),
        "echo $$ > \"$0/cgroup.procs\" 2>/dev/null; exec \"$@\"".to_string(),
        cgroup.display().to_string(),
    ];
    out.extend(argv);
    out
}

/// Of er onder `dir` iets gekoppeld is (volgens `/proc/self/mountinfo`).
///
/// Bij twijfel (mountinfo onleesbaar) is het antwoord ja: dan blijft de map staan.
pub(crate) fn has_mounts_under(dir: &Path) -> bool {
    let Ok(info) = fs::read_to_string("/proc/self/mountinfo") else {
        return true;
    };
    let prefix = dir.display().to_string();
    info.lines().any(|l| {
        l.split(' ')
            .nth(4)
            .is_some_and(|mp| mp == prefix || mp.starts_with(&format!("{prefix}/")))
    })
}

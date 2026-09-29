//! De taakmap van een exec-taak: aanmaken met volumes en isolatie, en weer veilig weghalen.
//!
//! Bezit het pad van één map en de lijst koppelingen die hij er zelf in
//! legde, in volgorde. Opruimen koppelt precies die lijst los, achterstevoren,
//! en haalt de map pas weg als alles los is: een `remove_dir_all` in een nog
//! gekoppelde `/dev` of een volume gooit bestanden van de host weg (Go:
//! `cleanupTaskResources`). Lukt het loskoppelen niet, dan blijft de map
//! staan en blijft deze waarde zijn eigenaar (quarantaine).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::{HostConfig, HostError, Result, TaskSpec, os, process};

/// Het merkteken in elke taakmap. [`sweep_stale`] haalt alleen mappen weg
/// die het dragen, zodat een verkeerd ingestelde `rootfs_base` (zeg `/home`)
/// nooit leeggeveegd wordt.
pub(crate) const MARKER: &str = ".hop-task";

/// Eén taakmap en de koppelingen die erin liggen.
///
/// # Invariants
///
/// `mounts` staat in de volgorde van aankoppelen; `released` is waar zodra
/// de map weg is, en daarna raakt niets hem nog aan.
#[derive(Debug)]
pub(crate) struct TaskDir {
    pub(crate) path: PathBuf,
    pub(crate) mounts: Vec<PathBuf>,
    pub(crate) released: bool,
}

impl TaskDir {
    /// Maakt de taakmap van `spec` onder de basis van `cfg`.
    ///
    /// Een transactie zoals Go's `setupTaskDir`: faalt een stap, dan ruimt
    /// `Drop` op wat er al stond, koppelingen eerst.
    pub(crate) fn create(cfg: &HostConfig, spec: &TaskSpec) -> Result<Self> {
        let id = &spec.task_id;
        if id.is_empty() || id == "." || id == ".." || id.contains('/') {
            return Err(HostError::TaskId(id.clone()));
        }
        let mut dir = Self {
            path: cfg.base().join(id),
            mounts: Vec::new(),
            released: false,
        };
        let owner = if spec.user.is_empty() {
            None
        } else {
            process::lookup_user(&spec.user)
        };
        for d in [dir.path.clone(), dir.path.join("tmp")] {
            fs::create_dir_all(&d).map_err(|e| HostError::io("mkdir", &d, e))?;
            if let Some((uid, gid)) = owner {
                // Zoals Go: best effort, zodat wat het kind maakt van hem is.
                let _ = std::os::unix::fs::chown(&d, Some(uid), Some(gid));
            }
        }
        let marker = dir.path.join(MARKER);
        fs::write(&marker, spec.task_id.as_bytes())
            .map_err(|e| HostError::io("create", &marker, e))?;
        // BTreeMap: gesorteerd op hostpad, zoals Go ze sorteerde.
        for (host, inner) in &spec.volumes {
            dir.add_volume(Path::new(host), inner)?;
        }
        if cfg.isolate {
            let binds = os::setup_isolation(&dir.path);
            dir.mounts.extend(binds);
            os::fake_meminfo(&dir.path, spec.memory_limit);
        }
        Ok(dir)
    }

    /// Het pad van de map.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Koppelt het hostpad `host` aan op `inner` in de taakmap.
    fn add_volume(&mut self, host: &Path, inner: &str) -> Result {
        fs::create_dir_all(host).map_err(|e| HostError::io("create volume host path", host, e))?;
        let target = inside(&self.path, inner).ok_or_else(|| HostError::Mount {
            target: PathBuf::from(inner),
            why: "the path leaves the task directory".to_string(),
        })?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| HostError::io("create volume target dir", parent, e))?;
        }
        os::mount_volume(host, &target).map_err(|e| HostError::Mount {
            target: target.clone(),
            why: format!("{} -> {}: {e}", host.display(), target.display()),
        })?;
        self.mounts.push(target);
        Ok(())
    }

    /// Koppelt alles los (achterstevoren) en haalt de map weg.
    ///
    /// Blijft een koppeling staan, dan blijft de map staan en is het
    /// [`HostError::Quarantined`]; een volgende aanroep probeert opnieuw.
    pub(crate) fn cleanup(&mut self) -> Result {
        if self.released {
            return Ok(());
        }
        let mut stuck = Vec::new();
        for m in self.mounts.iter().rev() {
            if let Err(e) = os::unmount(m) {
                stuck.push(format!("unmount {}: {e}", m.display()));
            }
        }
        if !stuck.is_empty() {
            return Err(HostError::Quarantined {
                path: self.path.clone(),
                why: stuck.join("; "),
            });
        }
        self.mounts.clear();
        match fs::remove_dir_all(&self.path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(HostError::io("remove task directory", &self.path, e)),
        }
        self.released = true;
        Ok(())
    }
}

impl Drop for TaskDir {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            eprintln!("runner: taskdir-leak: {e}");
        }
    }
}

/// `inner` als pad binnen `base`, lexicaal opgeschoond; `None` als het erbuiten komt.
pub(crate) fn inside(base: &Path, inner: &str) -> Option<PathBuf> {
    let mut parts: Vec<&str> = Vec::new();
    for p in inner.split('/') {
        match p {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            p => parts.push(p),
        }
    }
    let mut out = base.to_path_buf();
    for p in parts {
        out.push(p);
    }
    Some(out)
}

/// Haalt de taakmappen van een vorige daemon weg (Go: `Cleanup`, plus het vegen dat Go liet).
///
/// Alleen mappen met [`MARKER`] en zonder koppeling eronder: een map met
/// een nog levende koppeling (na een harde crash) blijft staan met één
/// regel, want `remove_dir_all` erin zou de host raken. Go veegde daarom
/// helemaal niet; de merkteken- en koppelingstoets maken vegen hier veilig.
pub(crate) fn sweep_stale(base: &Path) -> Result {
    fs::create_dir_all(base).map_err(|e| HostError::io("mkdir", base, e))?;
    // Zoals Go: iedereen mag erin, want een taak met `user` maakt er ook mappen.
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(base, fs::Permissions::from_mode(0o777))
        .map_err(|e| HostError::io("chmod", base, e))?;
    let entries = fs::read_dir(base).map_err(|e| HostError::io("read", base, e))?;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.join(MARKER).is_file() {
            continue;
        }
        if os::has_mounts_under(&path) {
            eprintln!(
                "runner: stale-taskdir: {} still has mounts; left in place",
                path.display()
            );
            continue;
        }
        if let Err(e) = fs::remove_dir_all(&path) {
            eprintln!("runner: stale-taskdir: remove {}: {e}", path.display());
        }
    }
    Ok(())
}

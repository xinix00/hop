//! De host-backends van de runner (feature `std`): processen, Docker, downloads, uitpakken en isolatie.
//!
//! Bezit twee dingen, elk met één eigenaar:
//!
//! - [`prepare`]: het trage werk vóór een start (taakmap, artifact
//!   downloaden en uitpakken, `docker pull`). Het is een losse functie die
//!   alleen bezit wat hij krijgt, zodat de daemon hem op een werkthread kan
//!   draaien zonder staat te delen. Het resultaat, [`Prepared`], gaat als
//!   waarde terug naar de eigenaar.
//! - [`HostRunner`]: alle lopende taken van de host (kinderen, containers),
//!   hun logringen en de taakmappen die nog opgeruimd moeten worden. Eén
//!   eigenaar, de daemon-thread; wat van buiten komt (logregels uit de
//!   pijpen, het einde van een `docker stop`), komt als bericht over een
//!   `std::sync::mpsc`-kanaal en wordt in [`HostRunner::tick`] verwerkt. Geen
//!   slot, geen `Arc` (handboek §1).
//!
//! Wat deze module niet bezit: de beslissing óf een taak draait (de agent),
//! de poorttoewijzing (de agent geeft ze toegewezen door) en de klok (`now_ms`
//! komt binnen, zoals overal in deze crate).
//!
//! Geen `unsafe` en geen libc: een signaal gaat via `/bin/kill`, een proces
//! start in een eigen procesgroep via `CommandExt::process_group(0)`, en wat
//! Go met `SysProcAttr` deed (chroot, namespaces, cgroup-fd) gaat via de
//! systeemcommando's `unshare`, `chroot` en `mount` (zie `os_linux.rs`).

mod docker;
mod download;
mod extract;
mod process;
mod runner;
mod taskdir;

#[cfg(target_os = "linux")]
#[path = "os_linux.rs"]
mod os;
#[cfg(not(target_os = "linux"))]
#[path = "os_darwin.rs"]
mod os;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

pub use runner::{GRACE_MS, HostRunner, KILL_TIMEOUT_MS};

use crate::LogPolicy;
use taskdir::TaskDir;

/// De standaardmap voor taakmappen als de config er geen noemt (Go: `/tmp/hop`).
pub const DEFAULT_ROOTFS_BASE: &str = "/tmp/hop";

/// De standaardsocket van de Docker-daemon.
pub const DEFAULT_DOCKER_SOCKET: &str = "/var/run/docker.sock";

/// De configuratie van de host-backends.
#[derive(Clone, Debug)]
pub struct HostConfig {
    /// Waar de taakmappen komen; leeg is [`DEFAULT_ROOTFS_BASE`].
    pub rootfs_base: PathBuf,
    /// Isolatie aan (config `runner.isolate`, standaard aan): chroot met
    /// namespaces op Linux, `sandbox-exec` op macOS.
    pub isolate: bool,
    /// De unix-socket van de Docker-daemon; leeg is [`DEFAULT_DOCKER_SOCKET`].
    pub docker_socket: PathBuf,
    /// Hoeveel uitvoer er per taak bewaard wordt.
    pub logs: LogPolicy,
}

/// Isolatie staat standaard aan, zoals `runner.isolate` in Go (Go: `TestIsolationEnabledByDefault`).
impl Default for HostConfig {
    fn default() -> Self {
        Self {
            rootfs_base: PathBuf::from(DEFAULT_ROOTFS_BASE),
            isolate: true,
            docker_socket: PathBuf::from(DEFAULT_DOCKER_SOCKET),
            logs: LogPolicy::DEFAULT,
        }
    }
}

impl HostConfig {
    /// De basis van de taakmappen, met de standaard als het veld leeg is.
    pub(crate) fn base(&self) -> &Path {
        if self.rootfs_base.as_os_str().is_empty() {
            Path::new(DEFAULT_ROOTFS_BASE)
        } else {
            &self.rootfs_base
        }
    }

    /// De Docker-socket, met de standaard als het veld leeg is.
    pub(crate) fn socket(&self) -> &Path {
        if self.docker_socket.as_os_str().is_empty() {
            Path::new(DEFAULT_DOCKER_SOCKET)
        } else {
            &self.docker_socket
        }
    }
}

/// Alles wat de host nodig heeft om één taak te draaien.
#[derive(Clone, Debug, Default)]
pub struct TaskSpec {
    /// Het taak-id; ook de naam van de taakmap en van de container (`hop-<id>`).
    pub task_id: String,
    /// De jobnaam.
    pub job_name: String,
    /// Het commando voor `/bin/sh -c` (exec; bij docker optioneel).
    pub command: String,
    /// Het container-image (docker).
    pub image: String,
    /// De gebruiker waaronder het proces draait (leeg: de daemon-gebruiker).
    pub user: String,
    /// De env van de job.
    pub env: BTreeMap<String, String>,
    /// De toegewezen poorten, naam naar poort.
    pub ports: BTreeMap<String, u16>,
    /// Hostpad naar pad in de taak.
    pub volumes: BTreeMap<String, String>,
    /// CPU in shares; 1024 is één core. Wordt `nice` (exec) of `CpuShares` (docker).
    pub cpu_shares: i64,
    /// Geheugenlimiet in bytes; 0 is geen limiet.
    pub memory_limit: u64,
    /// Het artifact, al gekozen door de agent (hoogstens één).
    pub artifact: Option<types::Artifact>,
    /// De node-attributen, als `ER_ATTR_*` in de env.
    pub node_attrs: BTreeMap<String, String>,
}

/// Een fout van de host-backends. `Display` noemt het pad, de status of het getal.
#[derive(Debug)]
pub enum HostError {
    /// Een exec-taak zonder commando.
    CommandRequired,
    /// Een docker-taak zonder image.
    ImageRequired,
    /// Een taak-id dat geen mapnaam kan zijn (leeg, `.`, `..`, of met `/`).
    TaskId(String),
    /// Dit driver-type heeft geen host-backend (`hop` draait op HopOS).
    Unsupported(types::Driver),
    /// Er loopt al een taak met dit id.
    TaskExists(String),
    /// Een bestandsoperatie faalde.
    Io {
        /// Wat er gebeurde (`mkdir`, `create`, ...).
        op: &'static str,
        /// Op welk pad.
        path: PathBuf,
        /// De fout van het OS.
        source: io::Error,
    },
    /// Een programma kon niet starten.
    Spawn {
        /// Het programma.
        program: String,
        /// De fout van het OS.
        source: io::Error,
    },
    /// Een volume kon niet aangekoppeld worden.
    Mount {
        /// Het doel in de taakmap.
        target: PathBuf,
        /// Waarom.
        why: String,
    },
    /// Een URL die niet te lezen is.
    Url(String),
    /// Een URL-schema zonder downloader.
    Scheme(String),
    /// Een extract-waarde die Hop niet kent.
    ExtractType(String),
    /// De HTTP(S)-download faalde (ook een foutstatus zoals 404).
    Http(hostnet::Error),
    /// Een `s3://`-artifact zonder `access_key` of `secret_key`.
    S3Credentials,
    /// De S3-download faalde.
    S3(leans3::Error),
    /// Een archief met een pad buiten de taakmap.
    IllegalPath {
        /// `tar` of `zip`.
        archive: &'static str,
        /// De naam in het archief.
        name: String,
    },
    /// Een kapot of onleesbaar archief.
    Archive {
        /// Het formaat.
        archive: &'static str,
        /// Waarom.
        why: String,
    },
    /// De Docker-daemon antwoordde met een onverwachte status.
    Docker {
        /// De operatie (`docker create`, ...).
        op: &'static str,
        /// De status.
        status: u16,
        /// Het begin van de body.
        body: String,
    },
    /// De Docker-daemon was niet te bereiken of het antwoord was onleesbaar.
    DockerIo {
        /// De operatie.
        op: &'static str,
        /// Waarom.
        why: String,
    },
    /// De pull-stroom meldde een fout (met status 200).
    DockerPull(String),
    /// De taakmap kon niet weg omdat een koppeling bleef staan; hij blijft in quarantaine.
    Quarantined {
        /// De taakmap.
        path: PathBuf,
        /// Waarom.
        why: String,
    },
}

impl HostError {
    /// Een bestandsfout op `path`.
    pub(crate) fn io(op: &'static str, path: &Path, source: io::Error) -> Self {
        Self::Io {
            op,
            path: path.to_path_buf(),
            source,
        }
    }

    /// Een leesfout in een archief.
    pub(crate) fn archive(archive: &'static str, e: &io::Error) -> Self {
        Self::Archive {
            archive,
            why: e.to_string(),
        }
    }

    /// Een vaste reden in een archief.
    pub(crate) fn archive_msg(archive: &'static str, why: &str) -> Self {
        Self::Archive {
            archive,
            why: why.to_string(),
        }
    }
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommandRequired => f.write_str("command is required"),
            Self::ImageRequired => f.write_str("image is required for docker runner"),
            Self::TaskId(id) => write!(f, "invalid task id {id:?}"),
            Self::Unsupported(d) => write!(f, "driver {:?} has no host backend", d.as_str()),
            Self::TaskExists(id) => write!(f, "task {id} is already running on this host"),
            Self::Io { op, path, source } => write!(f, "{op} {}: {source}", path.display()),
            Self::Spawn { program, source } => write!(f, "failed to start {program}: {source}"),
            Self::Mount { target, why } => {
                write!(f, "failed to mount volume at {}: {why}", target.display())
            }
            Self::Url(u) => write!(f, "invalid artifact URL: {u}"),
            Self::Scheme(s) => write!(
                f,
                "unsupported URL scheme: {s} (use http://, https://, or s3://)"
            ),
            Self::ExtractType(e) => write!(
                f,
                "unsupported extract type: {e} (use tar.gz, tar.bz2, or zip)"
            ),
            Self::Http(e) => write!(f, "HTTP download failed: {e}"),
            Self::S3Credentials => f.write_str("S3 requires access_key and secret_key in auth"),
            Self::S3(e) => write!(f, "S3 download failed: {e}"),
            Self::IllegalPath { archive, name } => {
                write!(f, "illegal file path in {archive}: {name}")
            }
            Self::Archive { archive, why } => write!(f, "extract {archive} failed: {why}"),
            Self::Docker { op, status, body } => write!(f, "{op} failed ({status}): {body}"),
            Self::DockerIo { op, why } => write!(f, "{op}: {why}"),
            Self::DockerPull(msg) => write!(f, "docker pull failed: {msg}"),
            Self::Quarantined { path, why } => {
                write!(f, "task directory {} quarantined: {why}", path.display())
            }
        }
    }
}

impl std::error::Error for HostError {}

/// Het resultaat van deze module.
pub type Result<T = (), E = HostError> = core::result::Result<T, E>;

/// Wat [`prepare`] klaarzette: de taakmap met zijn koppelingen, of niets (docker).
///
/// Bezit de taakmap tot [`HostRunner::launch`] hem overneemt of
/// [`HostRunner::discard`] hem opruimt. Valt hij zonder een van beide weg,
/// dan ruimt `Drop` de map op (en laat hij hem staan als een koppeling niet
/// los wil: zie [`TaskDir`]).
#[derive(Debug)]
pub struct Prepared {
    /// De taakmap; leeg voor docker.
    pub task_dir: PathBuf,
    dir: Option<TaskDir>,
}

/// Doet het trage werk vóór de start van een taak.
///
/// Exec: de taakmap (met `tmp/`, volumes en, met isolatie, de koppelingen
/// van het systeem), en het artifact erin, gedownload en uitgepakt. Docker:
/// `docker pull` van het image. `progress` krijgt (gelezen, lengte) tijdens
/// de download.
///
/// Statisch: bezit alleen wat hij krijgt en draait op een werkthread. Bij
/// een fout ruimt hij de half gemaakte taakmap zelf op (Go:
/// `TestExecRunnerRollsBackTaskDirWhenArtifactFails`).
pub fn prepare(
    cfg: &HostConfig,
    driver: types::Driver,
    spec: &TaskSpec,
    progress: &mut dyn FnMut(u64, Option<u64>),
) -> Result<Prepared> {
    match driver {
        types::Driver::Exec => prepare_exec(cfg, spec, progress),
        types::Driver::Docker => {
            if spec.image.is_empty() {
                return Err(HostError::ImageRequired);
            }
            docker::Docker::new(cfg.socket()).pull(&spec.image)?;
            Ok(Prepared {
                task_dir: PathBuf::new(),
                dir: None,
            })
        }
        types::Driver::Hop => Err(HostError::Unsupported(driver)),
    }
}

/// De exec-kant van [`prepare`].
fn prepare_exec(
    cfg: &HostConfig,
    spec: &TaskSpec,
    progress: &mut dyn FnMut(u64, Option<u64>),
) -> Result<Prepared> {
    if spec.command.is_empty() {
        return Err(HostError::CommandRequired);
    }
    // Een artifact dat nooit kan lukken, faalt vóór er een map is.
    if let Some(a) = &spec.artifact {
        download::check(a)?;
    }
    let mut dir = TaskDir::create(cfg, spec)?;
    if let Some(a) = &spec.artifact
        && let Err(e) = download::download_artifact(a, dir.path(), progress)
    {
        // De map gaat weg; lukt dat niet, dan zegt TaskDir::drop het.
        let _ = dir.cleanup();
        return Err(e);
    }
    Ok(Prepared {
        task_dir: dir.path().to_path_buf(),
        dir: Some(dir),
    })
}

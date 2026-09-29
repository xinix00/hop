//! Een exec-taak als proces: het commando, de env, de procesgroep, signalen en de logpijpen.
//!
//! Bezit per kind niets langer dan de spawn duurt: het `Child` gaat naar de
//! [`super::HostRunner`], en elke pijp naar een eigen lees-thread die alleen
//! die pijp bezit en regels als bericht over het kanaal stuurt.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::thread;

use super::runner::Event;
use super::{HostConfig, TaskSpec, os};
use crate::Stream;

/// De langste logregel die in één stuk gaat: 64 KiB (Go: `bufio.NewReaderSize`
/// in `PipeReader`). Een langere regel gaat in stukken, zodat de pijp altijd
/// blijft leeglopen zonder de hele regel in het geheugen.
pub(crate) const MAX_LINE: usize = 64 << 10;

/// De hoogste `nice` (Go: `maxNiceValue`).
const MAX_NICE: i64 = 19;

/// Waar hulpprogramma's gezocht worden: vaste plekken, niet de `PATH` van de daemon.
const TOOL_DIRS: [&str; 5] = ["/usr/bin", "/bin", "/usr/sbin", "/sbin", "/usr/local/bin"];

/// Wat er gestart wordt: argv, map, env en eventueel een andere gebruiker.
#[derive(Clone, Debug, Default)]
pub(crate) struct Plan {
    pub(crate) argv: Vec<String>,
    pub(crate) dir: PathBuf,
    pub(crate) env: BTreeMap<String, String>,
    pub(crate) creds: Option<(u32, u32)>,
}

impl Plan {
    /// Een plan met `argv` in `dir` en een lege env.
    pub(crate) fn new(argv: Vec<String>, dir: PathBuf) -> Self {
        Self {
            argv,
            dir,
            env: BTreeMap::new(),
            creds: None,
        }
    }
}

/// Het volledige plan van een exec-taak (Go: `setupCommand` plus `applyNice`).
///
/// De env in Go's volgorde, waarbij de laatste wint: de basis van het
/// platform (`HOME`, `TMPDIR`, `PATH`), `ER_PORT_*`, de env van de job,
/// `ER_ATTR_*`. Met `cpu_shares` gaat er `nice -n` voor, zodat de prioriteit
/// vanaf de eerste instructie geldt en voor de hele groep (Go zette hem na
/// de start, alleen op de leider).
pub(crate) fn plan(cfg: &HostConfig, spec: &TaskSpec, task_dir: &Path) -> io::Result<Plan> {
    let mut plan = os::plan(cfg.isolate, spec, task_dir)?;
    crate::port_env_vars(&spec.ports, &mut plan.env);
    for (k, v) in &spec.env {
        plan.env.insert(k.clone(), v.clone());
    }
    crate::attr_env_vars(&spec.node_attrs, &mut plan.env);
    if spec.cpu_shares > 0 {
        let n = nice_for(spec.cpu_shares, max_shares());
        if n > 0 {
            let mut argv = vec![
                find_tool("nice").map_or_else(|| "nice".into(), |p| p.display().to_string()),
                "-n".into(),
                n.to_string(),
            ];
            argv.append(&mut plan.argv);
            plan.argv = argv;
        }
    }
    Ok(plan)
}

/// De shares van de hele node: 1024 per core (Go: `runtime.NumCPU() * 1024`).
fn max_shares() -> i64 {
    let cores = thread::available_parallelism().map_or(1, usize::from);
    i64::try_from(cores).unwrap_or(1).saturating_mul(1024)
}

/// `nice` bij `shares` van `max` (Go: `applyNice`).
///
/// CFS weegt `1.25^-nice`, dus `nice = ln(max/shares) / ln(1.25)`, afgerond
/// en begrensd op 0..19: twee jobs met 7000 en 1024 shares krijgen zo ~86% en
/// ~14% in plaats van wat een lineaire afbeelding gaf.
pub(crate) fn nice_for(shares: i64, max: i64) -> i64 {
    if shares >= max {
        return 0;
    }
    if shares <= 0 {
        return MAX_NICE;
    }
    // Shares zijn kleine getallen, ver onder 2^52: de f64 is exact.
    let ratio = max as f64 / shares as f64;
    let n = (ratio.ln() / 1.25f64.ln()).round();
    // Begrensd op 0..=19 vóór de cast, dus de cast kapt niets af.
    n.clamp(0.0, MAX_NICE as f64) as i64
}

/// Zoekt `name` op de vaste plekken van [`TOOL_DIRS`].
pub(crate) fn find_tool(name: &str) -> Option<PathBuf> {
    TOOL_DIRS
        .iter()
        .map(|d| Path::new(d).join(name))
        .find(|p| p.is_file())
}

/// De uid en gid van `user` via `id` (Go: `user.Lookup`); `None` als hij niet bestaat.
///
/// `id` en niet `/etc/passwd`: op macOS staan gebruikers in Directory
/// Services, en `id` kent beide.
pub(crate) fn lookup_user(user: &str) -> Option<(u32, u32)> {
    let id = find_tool("id")?;
    let num = |flag: &str| -> Option<u32> {
        let out = Command::new(&id).arg(flag).arg(user).output().ok()?;
        if !out.status.success() {
            return None;
        }
        String::from_utf8_lossy(&out.stdout).trim().parse().ok()
    };
    Some((num("-u")?, num("-g")?))
}

/// Start het plan in een eigen procesgroep, met stdout en stderr als pijpen.
pub(crate) fn spawn(plan: &Plan) -> io::Result<Child> {
    let (program, args) = plan
        .argv
        .split_first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty argv"))?;
    let mut cmd = Command::new(program);
    cmd.args(args)
        .current_dir(&plan.dir)
        .env_clear()
        .envs(&plan.env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // De groep is het handvat voor stop: SIGTERM en SIGKILL gaan naar
        // -pgid en raken zo ook wat de shell startte.
        .process_group(0);
    if let Some((uid, gid)) = plan.creds {
        cmd.uid(uid).gid(gid);
    }
    cmd.spawn()
}

/// Wat een signaal aan een procesgroep opleverde.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Signal {
    /// Afgeleverd (of bij `-0`: de groep leeft).
    Sent,
    /// De groep bestaat niet meer (`ESRCH`).
    Gone,
    /// Iets anders (`EPERM`, geen `kill`); de groep kan nog leven.
    Failed(String),
}

/// Stuurt `sig` (`TERM`, `KILL`, `0`) naar procesgroep `pgid` via `/bin/kill`.
///
/// Geen libc in deze crate (`forbid(unsafe_code)`); `kill` is er op elke
/// Unix en zegt "No such process" als de groep weg is.
pub(crate) fn signal_group(pgid: u32, sig: &str) -> Signal {
    let kill = find_tool("kill").unwrap_or_else(|| PathBuf::from("/bin/kill"));
    let out = match Command::new(kill)
        .arg(format!("-{sig}"))
        .arg("--")
        .arg(format!("-{pgid}"))
        .stdin(Stdio::null())
        .output()
    {
        Ok(o) => o,
        Err(e) => return Signal::Failed(e.to_string()),
    };
    if out.status.success() {
        return Signal::Sent;
    }
    let err = String::from_utf8_lossy(&out.stderr);
    if err.contains("No such process") {
        Signal::Gone
    } else {
        Signal::Failed(err.trim().to_string())
    }
}

/// Snijdt een bytestroom in regels van hoogstens [`MAX_LINE`] en geeft ze aan `emit`.
#[derive(Debug, Default)]
pub(crate) struct Lines {
    buf: Vec<u8>,
}

impl Lines {
    /// Voert `data`; elke volle regel (zonder `\n`) gaat naar `emit`.
    pub(crate) fn feed(&mut self, mut data: &[u8], emit: &mut dyn FnMut(String)) {
        while !data.is_empty() {
            let room = MAX_LINE - self.buf.len();
            let take = data.len().min(room);
            let chunk = data.get(..take).unwrap_or(&[]);
            match chunk.iter().position(|&b| b == b'\n') {
                Some(i) => {
                    self.buf.extend_from_slice(chunk.get(..i).unwrap_or(&[]));
                    emit(self.take());
                    data = data.get(i + 1..).unwrap_or(&[]);
                }
                None => {
                    self.buf.extend_from_slice(chunk);
                    data = data.get(take..).unwrap_or(&[]);
                    if self.buf.len() >= MAX_LINE {
                        emit(self.take());
                    }
                }
            }
        }
    }

    /// Geeft een onafgemaakte laatste regel, als die er is.
    pub(crate) fn finish(&mut self, emit: &mut dyn FnMut(String)) {
        if !self.buf.is_empty() {
            emit(self.take());
        }
    }

    fn take(&mut self) -> String {
        let line = String::from_utf8_lossy(&self.buf).into_owned();
        self.buf.clear();
        line
    }
}

/// Stuurt een regel; een vol kanaal laat hem vallen (een log mag de taak
/// nooit laten wachten), een gesloten kanaal zegt `false`.
pub(crate) fn send_line(
    tx: &SyncSender<Event>,
    task: &str,
    generation: u64,
    stream: Stream,
    line: String,
) -> bool {
    match tx.try_send(Event::Line {
        task: task.to_string(),
        generation,
        stream,
        line,
    }) {
        Ok(()) | Err(TrySendError::Full(_)) => true,
        Err(TrySendError::Disconnected(_)) => false,
    }
}

/// Start een lees-thread die `pipe` bezit en zijn regels als [`Event::Line`] stuurt.
///
/// Aan het einde van de pijp stuurt hij [`Event::Eof`] (blokkerend: dat
/// bericht mag niet vallen, want de runner wacht erop voor hij de logs
/// pensioneert).
pub(crate) fn spawn_reader<R: Read + Send + 'static>(
    mut pipe: R,
    task: String,
    generation: u64,
    stream: Stream,
    tx: SyncSender<Event>,
) -> io::Result<()> {
    thread::Builder::new()
        .name(format!("hop-log-{task}"))
        .spawn(move || {
            let mut lines = Lines::default();
            let mut buf = vec![0u8; MAX_LINE];
            let mut open = true;
            loop {
                let n = match pipe.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                };
                lines.feed(buf.get(..n).unwrap_or(&[]), &mut |l| {
                    open &= send_line(&tx, &task, generation, stream, l);
                });
                if !open {
                    // De runner is weg; blijf de pijp leeg lezen zodat het kind niet vastloopt.
                    continue;
                }
            }
            lines.finish(&mut |l| {
                send_line(&tx, &task, generation, stream, l);
            });
            let _ = tx.send(Event::Eof { task, generation });
        })?;
    Ok(())
}

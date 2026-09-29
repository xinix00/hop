//! [`HostRunner`]: de eigenaar van alle lopende taken van de host en hun logringen.
//!
//! Eén eigenaar, de daemon-thread, die alles als `&mut self` houdt. Wat
//! buiten deze thread gebeurt, bezit zijn eigen stukje en stuurt berichten:
//! een lees-thread per pijp (alleen die pijp), een log-thread per container
//! (alleen zijn verbinding), en een kortlevende werkthread per `docker stop`
//! (alleen de naam). De berichten ([`Event`]) komen over één begrensd
//! `mpsc`-kanaal binnen en worden in [`HostRunner::tick`] verwerkt.
//!
//! Stoppen is niet-blokkerend, zoals Go het in stappen deed maar dan zonder
//! slapen: [`HostRunner::stop`] stuurt SIGTERM naar de procesgroep,
//! [`HostRunner::tick`] kijkt of de groep weg is, stuurt na [`GRACE_MS`]
//! SIGKILL, en ruimt pas op (taakmap, cgroup, logs met pensioen) als de groep
//! bewezen weg is. Blijft de groep leven, dan blijft de taak van de runner
//! (quarantaine): een taakmap onder een levend proces gaat nooit weg.

use std::collections::BTreeMap;
use std::os::unix::process::ExitStatusExt;
use std::process::Child;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread;
use std::time::{Duration, Instant};

use super::docker::{Docker, container_name};
use super::process::{self, Signal};
use super::taskdir::{self, TaskDir};
use super::{HostConfig, HostError, Prepared, Result, TaskSpec, os};
use crate::{LogRing, LogStore, RunState, Stream};

/// Hoe lang een taak na SIGTERM krijgt voor SIGKILL (Go: `gracefulShutdownTimeout`).
pub const GRACE_MS: u64 = 10_000;

/// Hoe lang na SIGKILL de groep weg moet zijn voor de taak in quarantaine gaat (Go: `killTimeout`).
pub const KILL_TIMEOUT_MS: u64 = 1_000;

/// Hoe lang na het einde van een taak de lees-threads nog krijgen om hun
/// laatste regels te sturen voor de logs met pensioen gaan.
const LOG_DRAIN_MS: u64 = 1_000;

/// De diepte van het berichtenkanaal. Vol betekent: een logregel valt (een
/// log mag een taak nooit laten wachten); `Eof` en `Stopped` wachten wel.
const EVENT_QUEUE: usize = 8192;

/// Hoe vaak [`HostRunner::shutdown`] tikt.
const SHUTDOWN_TICK: Duration = Duration::from_millis(50);

/// Een bericht van een thread aan de runner.
#[derive(Debug)]
pub(crate) enum Event {
    /// Een regel uit een pijp of een logstroom.
    Line {
        task: String,
        generation: u64,
        stream: Stream,
        line: String,
    },
    /// Een pijp of logstroom is dicht.
    Eof { task: String, generation: u64 },
    /// `docker stop` en `docker rm` zijn klaar.
    Stopped {
        task: String,
        generation: u64,
        result: Result,
    },
}

/// Een proces en wat het bezit.
struct Proc {
    child: Child,
    pgid: u32,
    dir: TaskDir,
    cgroup: bool,
    /// Het kind is gereaped; zijn pid kan nu hergebruikt worden.
    reaped: bool,
    /// Kleverig: de groep is bewezen weg (`ESRCH`) en krijgt nooit meer een
    /// signaal, ook niet als de kernel het nummer hergebruikt (Go:
    /// `execProcess.groupGone`).
    group_gone: bool,
}

impl Proc {
    /// Stuurt `sig` naar de groep, tenzij die bewezen weg is.
    ///
    /// De barrière van Go's `execProcess.signal`: na één `ESRCH` krijgt dit
    /// groepsnummer nooit meer een signaal, want de kernel kan het nummer
    /// intussen aan een vreemde groep gegeven hebben.
    fn signal(&mut self, sig: &str) -> Signal {
        if self.group_gone {
            return Signal::Gone;
        }
        let r = process::signal_group(self.pgid, sig);
        if r == Signal::Gone {
            self.group_gone = true;
        }
        r
    }
}

/// Een container.
struct Container {
    name: String,
}

/// Wat er draait.
enum Body {
    Proc(Box<Proc>),
    Docker(Container),
}

/// De stopfase van een taak, in milliseconden van de klok van de aanroeper.
#[derive(Default)]
struct Stopping {
    since: u64,
    killed: Option<u64>,
    gone: Option<u64>,
    stuck_logged: bool,
    docker_done: bool,
}

/// Eén lopende taak.
struct Task {
    /// Onderscheidt deze start van een eerdere met hetzelfde id: berichten
    /// van een oude generatie vallen weg.
    generation: u64,
    body: Body,
    open_streams: u8,
    exit: Option<i32>,
    stop: Option<Stopping>,
    cpu_shares: i64,
}

/// De eigenaar van alle lopende taken van de host (processen en containers) en hun logringen.
pub struct HostRunner {
    cfg: HostConfig,
    docker: Docker,
    logs: LogStore,
    tx: SyncSender<Event>,
    rx: Receiver<Event>,
    tasks: BTreeMap<String, Task>,
    /// Taakmappen die niet weg konden (een koppeling bleef); elke tick opnieuw.
    quarantine: Vec<TaskDir>,
    next_generation: u64,
    last_now: u64,
    /// De vorige CPU-stand per container, voor de delta van [`HostRunner::usage`].
    cpu_prev: BTreeMap<String, (u64, Instant)>,
}

impl std::fmt::Debug for HostRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostRunner")
            .field("tasks", &self.tasks.len())
            .field("quarantine", &self.quarantine.len())
            .finish_non_exhaustive()
    }
}

/// De exitcode van een status: de code, of 128 plus het signaal zoals een shell.
fn exit_code(st: std::process::ExitStatus) -> Option<i32> {
    st.code().or_else(|| st.signal().map(|s| 128 + s))
}

/// Reapt het kind als het klaar is; zet de exitcode.
fn reap(p: &mut Proc, exit: &mut Option<i32>) {
    if p.reaped {
        return;
    }
    match p.child.try_wait() {
        Ok(Some(st)) => {
            p.reaped = true;
            *exit = exit_code(st);
        }
        Ok(None) => {}
        // ECHILD: iemand anders reapte het; dan is het in elk geval weg.
        Err(_) => p.reaped = true,
    }
}

impl HostRunner {
    /// Een runner zonder taken.
    pub fn new(cfg: HostConfig) -> Self {
        let (tx, rx) = sync_channel(EVENT_QUEUE);
        Self {
            docker: Docker::new(cfg.socket()),
            logs: LogStore::new(cfg.logs),
            cfg,
            tx,
            rx,
            tasks: BTreeMap::new(),
            quarantine: Vec::new(),
            next_generation: 1,
            last_now: 0,
            cpu_prev: BTreeMap::new(),
        }
    }

    /// Start een voorbereide taak: het proces spawnen of de container maken en starten.
    ///
    /// Geeft de pid (docker: 0). stdout en stderr gaan per stroom naar een
    /// lees-thread die alleen zijn pijp bezit.
    pub fn launch(
        &mut self,
        now_ms: u64,
        driver: types::Driver,
        spec: &TaskSpec,
        mut prepared: Prepared,
    ) -> Result<u32> {
        self.last_now = now_ms;
        if self.tasks.contains_key(&spec.task_id) {
            // Dezelfde id is dezelfde taakmap: opruimen zou die van de lopende
            // taak weggooien. Laat hem staan.
            if let Some(mut d) = prepared.dir.take() {
                d.released = true;
            }
            return Err(HostError::TaskExists(spec.task_id.clone()));
        }
        let generation = self.next_generation;
        self.next_generation += 1;
        match driver {
            types::Driver::Exec => {
                let dir = prepared.dir.take().ok_or_else(|| HostError::Io {
                    op: "launch",
                    path: prepared.task_dir.clone(),
                    source: std::io::Error::other("prepared without a task directory"),
                })?;
                self.launch_proc(spec, dir, generation)
            }
            types::Driver::Docker => self.launch_docker(spec, generation),
            types::Driver::Hop => Err(HostError::Unsupported(driver)),
        }
    }

    /// Het exec-deel van [`HostRunner::launch`].
    fn launch_proc(&mut self, spec: &TaskSpec, dir: TaskDir, generation: u64) -> Result<u32> {
        let mut plan = process::plan(&self.cfg, spec, dir.path())
            .map_err(|e| HostError::io("prepare command in", dir.path(), e))?;
        let cgroup = os::prepare_cgroup(&spec.task_id, spec.memory_limit);
        if let Some(cg) = &cgroup {
            plan.argv = os::wrap_cgroup(plan.argv, cg);
        }
        let mut child = match process::spawn(&plan) {
            Ok(c) => c,
            Err(e) => {
                if cgroup.is_some() {
                    os::remove_cgroup(&spec.task_id);
                }
                // `dir` valt hier weg en ruimt de taakmap op.
                return Err(HostError::Spawn {
                    program: plan.argv.first().cloned().unwrap_or_default(),
                    source: e,
                });
            }
        };
        let pid = child.id();
        let id = &spec.task_id;
        self.logs.open(id);
        let mut open = 0u8;
        if let Some(out) = child.stdout.take()
            && process::spawn_reader(out, id.clone(), generation, Stream::Stdout, self.tx.clone())
                .is_ok()
        {
            open += 1;
        }
        if let Some(err) = child.stderr.take()
            && process::spawn_reader(err, id.clone(), generation, Stream::Stderr, self.tx.clone())
                .is_ok()
        {
            open += 1;
        }
        let proc = Proc {
            child,
            pgid: pid,
            dir,
            cgroup: cgroup.is_some(),
            reaped: false,
            group_gone: false,
        };
        self.tasks.insert(
            id.clone(),
            Task {
                generation,
                body: Body::Proc(Box::new(proc)),
                open_streams: open,
                exit: None,
                stop: None,
                cpu_shares: spec.cpu_shares,
            },
        );
        Ok(pid)
    }

    /// Het docker-deel van [`HostRunner::launch`]: create, start, logs volgen.
    fn launch_docker(&mut self, spec: &TaskSpec, generation: u64) -> Result<u32> {
        if spec.image.is_empty() {
            return Err(HostError::ImageRequired);
        }
        let name = container_name(&spec.task_id);
        self.docker.create(spec)?;
        if let Err(e) = self.docker.start(&name) {
            // Een container die nooit liep, blijft niet achter.
            let _ = self.docker.remove(&name);
            return Err(e);
        }
        let id = &spec.task_id;
        self.logs.open(id);
        let open = u8::from(
            self.docker
                .follow_logs(name.clone(), id.clone(), generation, self.tx.clone())
                .is_ok(),
        );
        self.tasks.insert(
            id.clone(),
            Task {
                generation,
                body: Body::Docker(Container { name }),
                open_streams: open,
                exit: None,
                stop: None,
                cpu_shares: spec.cpu_shares,
            },
        );
        Ok(0)
    }

    /// Ruimt een [`Prepared`] op die nooit gestart wordt (de taak werd tijdens de voorbereiding gestopt).
    ///
    /// Koppelingen gaan eerst los; lukt dat niet, dan blijft de map in
    /// quarantaine bij de runner en wordt er niets van een volume weggegooid.
    pub fn discard(&mut self, mut prepared: Prepared) {
        if let Some(mut dir) = prepared.dir.take()
            && let Err(e) = dir.cleanup()
        {
            eprintln!("runner: discard: {e}");
            self.quarantine.push(dir);
        }
    }

    /// Begint de stop van een taak: SIGTERM naar de groep nu, of `docker stop` op een werkthread.
    ///
    /// Niet-blokkerend; [`HostRunner::tick`] doet de rest (SIGKILL na
    /// [`GRACE_MS`], opruimen als de groep weg is). Een onbekende of al
    /// stoppende taak is `Ok`.
    pub fn stop(&mut self, now_ms: u64, task_id: &str) -> Result {
        self.last_now = now_ms;
        let Some(t) = self.tasks.get_mut(task_id) else {
            return Ok(());
        };
        if t.stop.is_some() {
            return Ok(());
        }
        t.stop = Some(Stopping {
            since: now_ms,
            ..Stopping::default()
        });
        match &mut t.body {
            Body::Proc(p) => {
                reap(p, &mut t.exit);
                if let Signal::Failed(why) = p.signal("TERM") {
                    eprintln!("runner: sigterm: group {}: {why}", p.pgid);
                }
            }
            Body::Docker(c) => {
                let (docker, name, tx) = (self.docker.clone(), c.name.clone(), self.tx.clone());
                let (task, generation) = (task_id.to_string(), t.generation);
                let spawned = thread::Builder::new()
                    .name(format!("hop-dstop-{task_id}"))
                    .spawn(move || {
                        let result = docker.stop_and_remove(&name);
                        let _ = tx.send(Event::Stopped {
                            task,
                            generation,
                            result,
                        });
                    });
                if spawned.is_err() {
                    // Geen thread: dan hier, blokkerend, liever dan nooit.
                    let result = self.docker.stop_and_remove(&c.name);
                    if let Some(st) = t.stop.as_mut() {
                        st.docker_done = true;
                    }
                    result?;
                }
            }
        }
        Ok(())
    }

    /// De toestand van een taak: `try_wait` op het proces, `inspect` op de container.
    ///
    /// Een onbekende taak is `Failed` (Go: geen proces in de map).
    pub fn status(&mut self, task_id: &str) -> RunState {
        let Some(t) = self.tasks.get_mut(task_id) else {
            return RunState::Failed;
        };
        match &mut t.body {
            Body::Proc(p) => {
                reap(p, &mut t.exit);
                if p.reaped {
                    // Is de hele groep weg, dan wordt dat kleverig (zie `Proc::signal`).
                    let _ = p.signal("0");
                    RunState::Failed
                } else {
                    RunState::Running
                }
            }
            Body::Docker(c) => match self.docker.inspect(&c.name) {
                Ok(Some((true, _))) => RunState::Running,
                Ok(Some((false, code))) => {
                    t.exit = Some(code);
                    RunState::Failed
                }
                _ => RunState::Failed,
            },
        }
    }

    /// De exitcode van een afgelopen taak, als die bekend is (signaal `n` is `128 + n`).
    pub fn exit_code(&self, task_id: &str) -> Option<i32> {
        self.tasks.get(task_id).and_then(|t| t.exit)
    }

    /// De ring van een taak, lopend of kort geleden gestopt.
    pub fn logs(&self, now_ms: u64, task_id: &str, stream: Stream) -> Option<&LogRing> {
        self.logs.get(now_ms, task_id, stream)
    }

    /// Of de Docker-daemon antwoordt op `_ping` (dat is `node.docker`).
    pub fn docker_available(&self) -> bool {
        self.docker.ping()
    }

    /// CPU in procenten van de eigen cores (-1 zolang er geen venster is) en
    /// geheugen in bytes, voor een container (Go: `DockerRunner.Usage`).
    /// Processen: `None` (de agent meet die zelf).
    pub fn usage(&mut self, task_id: &str) -> Option<(f64, u64)> {
        let t = self.tasks.get(task_id)?;
        let Body::Docker(c) = &t.body else {
            return None;
        };
        let (cpu, mem) = self.docker.stats(&c.name).ok()?;
        let now = Instant::now();
        let prev = self.cpu_prev.insert(task_id.to_string(), (cpu, now));
        let Some((before, at)) = prev.filter(|(b, at)| cpu >= *b && now > *at) else {
            return Some((-1.0, mem));
        };
        let host = thread::available_parallelism().map_or(1, usize::from);
        let cores = if t.cpu_shares > 0 {
            t.cpu_shares as f64 / 1024.0
        } else {
            host as f64
        };
        let used = (cpu - before) as f64 / now.duration_since(at).as_nanos() as f64;
        Some((used / cores * 100.0, mem))
    }

    /// Het periodieke werk: berichten verwerken, reapen, SIGKILL na de
    /// genade, gestopte taken opruimen, quarantaine opnieuw proberen, oude logs vegen.
    pub fn tick(&mut self, now_ms: u64) {
        self.last_now = now_ms;
        self.drain();
        let mut done = Vec::new();
        for (id, t) in &mut self.tasks {
            if let Body::Proc(p) = &mut t.body {
                reap(p, &mut t.exit);
            }
            let Some(st) = t.stop.as_mut() else {
                continue;
            };
            let gone = match &mut t.body {
                Body::Proc(p) => escalate(p, st, now_ms),
                Body::Docker(_) => st.docker_done,
            };
            if gone && st.gone.is_none() {
                st.gone = Some(now_ms);
            }
            if let Some(at) = st.gone
                && (t.open_streams == 0 || now_ms >= at.saturating_add(LOG_DRAIN_MS))
            {
                done.push(id.clone());
            }
        }
        for id in done {
            self.finish(now_ms, &id);
        }
        self.quarantine.retain_mut(|d| d.cleanup().is_err());
        self.logs.sweep(now_ms);
    }

    /// Verwerkt de berichten van de threads.
    fn drain(&mut self) {
        while let Ok(ev) = self.rx.try_recv() {
            match ev {
                Event::Line {
                    task,
                    generation,
                    stream,
                    line,
                } => {
                    if self
                        .tasks
                        .get(&task)
                        .is_some_and(|t| t.generation == generation)
                        && let Some(ring) = self.logs.live_mut(&task, stream)
                    {
                        ring.write(&line);
                    }
                }
                Event::Eof { task, generation } => {
                    if let Some(t) = self.tasks.get_mut(&task)
                        && t.generation == generation
                    {
                        t.open_streams = t.open_streams.saturating_sub(1);
                    }
                }
                Event::Stopped {
                    task,
                    generation,
                    result,
                } => {
                    if let Err(e) = result {
                        eprintln!("runner: docker-stop: task {task}: {e}");
                    }
                    if let Some(t) = self.tasks.get_mut(&task)
                        && t.generation == generation
                        && let Some(st) = t.stop.as_mut()
                    {
                        st.docker_done = true;
                    }
                }
            }
        }
    }

    /// Ruimt een gestopte taak op: logs met pensioen, taakmap en cgroup weg.
    fn finish(&mut self, now_ms: u64, id: &str) {
        let Some(t) = self.tasks.remove(id) else {
            return;
        };
        self.logs.retire(now_ms, id);
        self.cpu_prev.remove(id);
        if let Body::Proc(p) = t.body {
            let Proc {
                mut dir, cgroup, ..
            } = *p;
            if let Err(e) = dir.cleanup() {
                eprintln!("runner: cleanup: task {id}: {e}");
                self.quarantine.push(dir);
            }
            if cgroup {
                os::remove_cgroup(id);
            }
        }
    }

    /// Ruimt de resten van een vorige daemon op (Go: `Cleanup` van beide runners).
    ///
    /// Taakmappen met het merkteken en zonder koppeling onder
    /// `rootfs_base`, de cgroup-controllers aan (Linux), en als de daemon
    /// antwoordt alle `hop-*`-containers weg. Een fout bij één container
    /// stopt de rest niet; de eerste komt terug.
    pub fn init(&mut self) -> Result {
        taskdir::sweep_stale(self.cfg.base())?;
        os::ensure_cgroup_controllers();
        if !self.docker.ping() {
            return Ok(());
        }
        let mut first = None;
        for id in self.docker.list_hop()? {
            if let Err(e) = self.docker.remove(&id) {
                first.get_or_insert(e);
            }
        }
        first.map_or(Ok(()), Err)
    }

    /// Stopt alles bij het afsluiten en wacht hoogstens de genade plus de kill-termijn.
    pub fn shutdown(&mut self) {
        let start = Instant::now();
        let base = self.last_now;
        let ids: Vec<String> = self.tasks.keys().cloned().collect();
        for id in ids {
            if let Err(e) = self.stop(base, &id) {
                eprintln!("runner: shutdown: stop {id}: {e}");
            }
        }
        let budget = Duration::from_millis(GRACE_MS + KILL_TIMEOUT_MS + LOG_DRAIN_MS);
        loop {
            let elapsed = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
            self.tick(base.saturating_add(elapsed));
            if self.tasks.is_empty() {
                return;
            }
            if start.elapsed() >= budget {
                eprintln!(
                    "runner: shutdown: {} tasks still running after {} ms",
                    self.tasks.len(),
                    budget.as_millis()
                );
                return;
            }
            thread::sleep(SHUTDOWN_TICK);
        }
    }
}

/// De stopstappen van een proces; `true` als de groep bewezen weg is.
fn escalate(p: &mut Proc, st: &mut Stopping, now_ms: u64) -> bool {
    if p.reaped {
        let _ = p.signal("0");
    }
    if p.reaped && p.group_gone {
        return true;
    }
    if st.killed.is_none() && now_ms >= st.since.saturating_add(GRACE_MS) {
        eprintln!(
            "runner: sigkill: group {} did not exit within {GRACE_MS} ms",
            p.pgid
        );
        if let Signal::Failed(why) = p.signal("KILL") {
            eprintln!("runner: sigkill: group {}: {why}", p.pgid);
        }
        if !p.reaped {
            let _ = p.child.kill();
        }
        st.killed = Some(now_ms);
    }
    if let Some(k) = st.killed
        && now_ms >= k.saturating_add(KILL_TIMEOUT_MS)
        && !st.stuck_logged
    {
        // Eigenaar blijven: een taakmap onder een levend proces gaat niet weg.
        eprintln!(
            "runner: quarantine: group {} still alive {KILL_TIMEOUT_MS} ms after SIGKILL",
            p.pgid
        );
        st.stuck_logged = true;
    }
    false
}

impl Drop for HostRunner {
    /// Een runner die wegvalt met lopende taken stopt ze eerst: anders zouden
    /// de taakmappen onder levende processen weggaan.
    fn drop(&mut self) {
        if !self.tasks.is_empty() {
            self.shutdown();
        }
        // Wat na de shutdown nog leeft, houdt zijn taakmap: liever een lek dan
        // een map die onder een lopend proces verdwijnt.
        for t in self.tasks.values_mut() {
            if let Body::Proc(p) = &mut t.body {
                p.dir.released = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Wat alleen van binnen te toetsen is: de signaalbarrière van een proces.

    use std::os::unix::process::CommandExt;
    use std::process::Command;

    use super::*;

    // Go: TestExecProcessGoneBarrierNeverSignalsReusedGroup
    #[test]
    fn exec_process_gone_barrier_never_signals_reused_group() {
        // Een staande groep op het nummer dat de oude taak had.
        let mut stand_in = Command::new("sleep");
        stand_in.arg("10").process_group(0);
        let child = stand_in.spawn().unwrap();
        let pgid = child.id();
        let dir = std::env::temp_dir().join(format!("hop-barrier-{pgid}"));
        let mut p = Proc {
            child,
            pgid,
            dir: TaskDir {
                path: dir,
                mounts: Vec::new(),
                released: true,
            },
            cgroup: false,
            reaped: false,
            // Een eerdere status zag ESRCH voor de oude generatie.
            group_gone: true,
        };
        assert_eq!(p.signal("TERM"), Signal::Gone);
        assert_eq!(p.signal("KILL"), Signal::Gone);
        // De vreemde groep leeft nog: er is niets gestuurd.
        assert_eq!(process::signal_group(pgid, "0"), Signal::Sent);
        let _ = process::signal_group(pgid, "KILL");
        let _ = p.child.wait();
        // Zonder barrière meldt de kernel de lege groep, en dat wordt kleverig.
        p.group_gone = false;
        assert_eq!(p.signal("0"), Signal::Gone);
        assert!(p.group_gone);
    }
}

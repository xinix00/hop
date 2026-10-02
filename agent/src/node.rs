//! De agent-toestandsmachine: toelating, start, stop, herstart, monitor.
//!
//! Bezit alle muteerbare staat van de node als gewone `&mut self`: de jobs,
//! de taken (elk record IS een reservering, welke staat het ook heeft), de
//! herstarttimers en de gezondheidstellers. Er is geen tweede schrijver; wie
//! iets wil, roept een methode en voert de acties uit die terugkomen.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;

use types::{Driver, Job, Nanos, Task, TaskState, Time, TryClone};

use crate::action::{Action, Event, Outcome, StartError, StartOk, Status};
use crate::health::{self, Check, Verdict};
use crate::ids::Ids;
use crate::settings::Settings;
use crate::{DEFAULT_MAX_RESTARTS, DEFAULT_RESTART_WINDOW, Error, MAX_JOBS, MAX_TASKS, Result};

/// HopOS deelt partities uit in blokken van 2 MB (de kooi-map werkt per blok).
const HOP_BLOCK_BYTES: u64 = 2 << 20;

/// Eén taak met wat de agent er verder over bijhoudt.
#[derive(Clone, Debug)]
pub(crate) struct Entry {
    pub(crate) task: Task,
    pub(crate) check: Check,
    /// Wanneer de volgende herstartpoging mag (backoff); `None` = geen.
    pub(crate) restart_at: Option<Nanos>,
    /// Of dit de eerste start van de taak is (niet een vervanging na een crash).
    pub(crate) first_start: bool,
    /// Of de runner hem op dit moment start.
    pub(crate) starting: bool,
}

/// De capaciteit van de node zoals `/capacity` hem meldt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Capacity {
    /// De cores waar de node tegen plant.
    pub cpu_cores: u32,
    /// Het geheugen waar de node tegen plant.
    pub memory_bytes: u64,
    /// Gereserveerde CPU in shares (sharegroups één keer).
    pub cpu_used_shares: i64,
    /// Gereserveerd geheugen in bytes.
    pub memory_used_bytes: u64,
    /// Het aantal taken in [`TaskState::Running`].
    pub tasks_running: usize,
}

/// De staat van één node.
///
/// # Invariants
///
/// Elke taak in `tasks` houdt zijn reservering vast, wat zijn staat ook is.
/// Capaciteit komt alleen vrij door het record te verwijderen, nooit door op
/// staat te filteren.
#[derive(Debug)]
pub struct Agent {
    settings: Settings,
    jobs: BTreeMap<String, Job>,
    tasks: BTreeMap<String, Entry>,
    state_time: Time,
    leader_addr: String,
    lease_expires_at: Time,
    ids: Ids,
    out: Vec<Action>,
    next_monitor: Nanos,
    shutting_down: bool,
}

impl Agent {
    /// Een lege agent voor deze node.
    pub fn new(settings: Settings) -> Self {
        let ids = Ids::new(settings.seed);
        Self {
            settings,
            jobs: BTreeMap::new(),
            tasks: BTreeMap::new(),
            state_time: Time::ZERO,
            leader_addr: String::new(),
            lease_expires_at: Time::ZERO,
            ids,
            out: Vec::new(),
            next_monitor: 0,
            shutting_down: false,
        }
    }

    /// De instellingen van de node.
    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// Vervangt de gedetecteerde systeemgegevens (tests, en een node die later meet).
    pub fn settings_mut(&mut self) -> &mut Settings {
        &mut self.settings
    }

    /// Het node-id.
    pub fn id(&self) -> &str {
        &self.settings.id
    }

    /// Het HTTP-endpoint.
    pub fn endpoint(&self) -> &str {
        &self.settings.endpoint
    }

    /// De node-attributen.
    pub fn attributes(&self) -> &BTreeMap<String, String> {
        &self.settings.attributes
    }

    fn push(&mut self, action: Action) {
        // Een actie die niet in het geheugen past, is een actie die niet
        // gebeurt; de volgende tick of de volgende invoer herstelt het beeld.
        if self.out.try_reserve(1).is_ok() {
            self.out.push(action);
        }
    }

    /// Geeft de opgespaarde acties af, in volgorde.
    pub fn take_actions(&mut self) -> Vec<Action> {
        core::mem::take(&mut self.out)
    }

    /// Laat de tijd verstrijken: vuurt herstarts, de monitor en het wegschrijven.
    pub fn tick(&mut self, now: Nanos) -> Vec<Action> {
        let due: Vec<String> = self
            .tasks
            .iter()
            .filter(|(_, e)| e.restart_at.is_some_and(|t| t <= now))
            .map(|(id, _)| id.clone())
            .collect();
        for id in due {
            if let Some(e) = self.tasks.get_mut(&id) {
                e.restart_at = None;
            }
            self.restart_fire(now, &id);
        }
        if now >= self.next_monitor {
            self.next_monitor = now.saturating_add(self.settings.monitor_interval());
            self.monitor(now);
        }
        self.take_actions()
    }

    // ---- Leader-adres en lease -------------------------------------------------

    /// Zet de leader waar clusteraanroepen heen gaan ("" = geen bekend).
    pub fn set_leader_addr(&mut self, addr: &str) {
        self.leader_addr.clear();
        self.leader_addr.push_str(addr);
    }

    /// De leader waar clusteraanroepen heen gaan; leeg als er geen bekend is.
    pub fn leader_addr(&self) -> &str {
        &self.leader_addr
    }

    /// Zet wanneer onze eigen lease afloopt (nul als we niet leiden).
    pub fn set_lease_expires_at(&mut self, t: Time) {
        self.lease_expires_at = t;
    }

    /// Wanneer onze eigen lease afloopt; nul als we niet leiden.
    pub fn lease_expires_at(&self) -> Time {
        self.lease_expires_at
    }

    // ---- Affinity en artifacts -------------------------------------------------

    /// Of de attributen van deze node aan elke eis voldoen (EN).
    pub fn matches_affinity(&self, affinity: &types::Map<String>) -> bool {
        affinity
            .iter()
            .all(|(k, v)| self.settings.attributes.get(k).is_some_and(|a| a == v))
    }

    /// Het eerste artifact dat bij deze node past; een lege `match` past altijd.
    pub fn resolve_artifact<'a>(
        &self,
        artifacts: &'a [types::Artifact],
    ) -> Option<&'a types::Artifact> {
        artifacts.iter().find(|a| self.matches_affinity(&a.matches))
    }

    /// Een kopie van de job met alleen het artifact van deze node.
    ///
    /// Runners verwachten hoogstens één artifact; elk pad naar een start gaat
    /// hierlangs.
    pub fn resolve_job_for_run(&self, job: &Job) -> Result<Job> {
        let mut copy = job.try_clone()?;
        if job.artifacts.is_empty() {
            return Ok(copy);
        }
        let art = self
            .resolve_artifact(&job.artifacts)
            .ok_or(Error::NoArtifact)?;
        let mut one = Vec::new();
        types::try_push(&mut one, art.try_clone()?)?;
        copy.artifacts = one;
        Ok(copy)
    }

    // ---- Capaciteit ------------------------------------------------------------

    fn sharegroup_of(&self, task: &Task) -> Option<&str> {
        let job = self.jobs.get(&task.job_name)?;
        job.tags
            .get("sharegroup")
            .map(String::as_str)
            .filter(|g| !g.is_empty())
    }

    /// Of er al een taak in sharegroup `grp` is: dan is de pool-CPU al gereserveerd.
    fn sharegroup_running(&self, grp: &str) -> bool {
        self.tasks
            .values()
            .any(|e| self.sharegroup_of(&e.task) == Some(grp))
    }

    /// Wat de node heeft uitgedeeld, zonder de taken van job `exclude`.
    ///
    /// Geen staatfilter: aanwezigheid is de maat. Filteren op staat is hoe dit
    /// eerder ontspoorde: Failed telde als vrij, dus de core van een gecrashte
    /// taak ging naar een nieuwe job terwijl zijn eigen herstart hem terug zou
    /// pakken (26-07: een onplaatsbare app glipte een herstart-flap in en legde
    /// een node met 3 cores plat).
    ///
    /// CPU telt sharegroup-leden één keer (ze delen een pool; "2 apps in pool
    /// web van 2" is 2 cores, niet 4). Geheugen telt per lid: elke app heeft
    /// een eigen partitie.
    pub fn resource_usage_excluding(&self, exclude: Option<&str>) -> (i64, u64) {
        let mut cpu: i64 = 0;
        let mut mem: u64 = 0;
        let mut seen: Vec<&str> = Vec::new();
        for e in self.tasks.values() {
            if exclude == Some(e.task.job_name.as_str()) {
                continue;
            }
            mem = mem.saturating_add(e.task.memory_limit);
            if let Some(grp) = self.sharegroup_of(&e.task) {
                if seen.contains(&grp) {
                    continue;
                }
                if seen.try_reserve(1).is_ok() {
                    seen.push(grp);
                }
            }
            cpu = cpu.saturating_add(e.task.cpu_shares);
        }
        (cpu, mem)
    }

    /// Wat de node heeft uitgedeeld.
    pub fn resource_usage(&self) -> (i64, u64) {
        self.resource_usage_excluding(None)
    }

    /// De capaciteit zoals `/capacity` hem meldt: de effectieve grens, niet het kale ijzer.
    pub fn capacity(&self) -> Capacity {
        let (cpu_used, mem_used) = self.resource_usage();
        let running = self
            .tasks
            .values()
            .filter(|e| e.task.state == TaskState::Running)
            .count();
        let cores = u32::try_from(self.settings.effective_cpu_shares() / 1024).unwrap_or(0);
        Capacity {
            cpu_cores: if cores == 0 {
                self.settings.cpu_cores
            } else {
                cores
            },
            memory_bytes: self.settings.effective_memory_bytes(),
            cpu_used_shares: cpu_used,
            memory_used_bytes: mem_used,
            tasks_running: running,
        }
    }

    // ---- Toelating -------------------------------------------------------------

    /// Neemt een job aan en reserveert zijn capaciteit; geeft het taak-id.
    ///
    /// Met `replace` vervangt hij de taken van dezelfde job: de toelating rekent
    /// hun reservering niet mee en ze worden pas ná een geslaagde toelating
    /// gestopt, dus een weigering laat ze draaien. Dat is het update-pad op een
    /// node zonder ruimte; daarvoor werd zo'n update via preemptie over een
    /// búúrman uitgevochten (gemeten 01-08, welcome-update offerde cloudflared).
    ///
    /// `pool_largest` is de grootste partitie die de HopOS-node nog in één stuk
    /// kan plaatsen, als de runner dat weet.
    pub fn run(
        &mut self,
        now: Nanos,
        mut job: Job,
        replace: bool,
        pool_largest: Option<u64>,
    ) -> Result<String> {
        // Affinity vóór capaciteit: de leader blijft dom.
        if !self.matches_affinity(&job.affinity) {
            return Err(Error::AffinityMismatch);
        }
        if job.driver.is_none() {
            job.driver = Some(Driver::for_image(&job.image));
        }
        if job.driver == Some(Driver::Hop) {
            round_for_hopos(&mut job);
        }
        if self.tasks.len() >= MAX_TASKS {
            return Err(Error::TooManyTasks);
        }
        if !self.jobs.contains_key(&job.name) && self.jobs.len() >= MAX_JOBS {
            return Err(Error::TooManyJobs);
        }
        self.admit(&job, replace, pool_largest)?;

        let task = self.new_task(now, &job)?;
        let id = task.id.clone();
        if replace {
            // De voorgangers gaan eerst weg: hun core en partitie moeten vrij zijn
            // voordat de opvolger plaatst (op HopOS letterlijk dezelfde pool).
            let old: Vec<String> = self
                .tasks
                .iter()
                .filter(|(_, e)| e.task.job_name == job.name)
                .map(|(k, _)| k.clone())
                .collect();
            for t in old {
                self.remove_and_stop(&t);
            }
        }
        self.keep_rollout_flag(&mut job);
        self.jobs.insert(job.name.clone(), job);
        self.insert_entry(task, true);
        self.begin_start(now, &id);
        Ok(id)
    }

    /// Toetst of `job` erbij past; zie [`Agent::run`].
    fn admit(&self, job: &Job, replace: bool, pool_largest: Option<u64>) -> Result {
        let exclude = replace.then_some(job.name.as_str());
        let (used_cpu, used_mem) = self.resource_usage_excluding(exclude);
        // Een lid dat een al lopende pool joint, kost geen extra cores.
        let mut new_cpu = job.cpu_shares;
        if let Some(grp) = job.tags.get("sharegroup").filter(|g| !g.is_empty())
            && self.sharegroup_running(grp)
        {
            new_cpu = 0;
        }
        if job.cpu_shares > 0
            && used_cpu.saturating_add(new_cpu) > self.settings.effective_cpu_shares()
        {
            return Err(Error::NoCapacity);
        }
        let mem_cap = self.settings.effective_memory_bytes();
        if job.memory_limit > 0 && used_mem.saturating_add(job.memory_limit) > mem_cap {
            return Err(Error::NoCapacity);
        }
        // Een som is geen gat. Op een pool van meerdere regio's kunnen de bytes
        // vrij zijn zonder dat één stuk groot genoeg is (GEMETEN 19-08 op een
        // LicheeRV: 60 MB vrij in gaten van 28 en 32, een job van 36 MB elke vijf
        // seconden aangenomen en geweigerd; de node viel drie keer om). Niet bij
        // een vervanging: daar houdt de voorganger zijn partitie nog vast.
        if job.memory_limit > 0
            && !replace
            && job.driver == Some(Driver::Hop)
            && pool_largest.is_some_and(|l| l > 0 && job.memory_limit > l)
        {
            return Err(Error::NoCapacity);
        }
        Ok(())
    }

    /// Een job die de leader stuurt, draagt de HUIDIGE deploying-vlag van de store.
    ///
    /// Deploying heeft één auteur, de update van de leader; op de leader-node IS
    /// deze store die van de leader, en een dispatch naar onszelf die de payload
    /// letterlijk opsloeg, schreef "deploying" terug over het "klaar" dat de
    /// update net zette (traqqr 08-09-2026).
    pub fn keep_rollout_flag(&self, job: &mut Job) {
        job.deploying = self.jobs.get(&job.name).is_some_and(|cur| cur.deploying);
    }

    fn new_task(&mut self, now: Nanos, job: &Job) -> Result<Task> {
        // Geboren als Queued, niet Running: tussen aanname en echte start zit de
        // hele download, en die duurt op een klein board minuten.
        Ok(Task {
            id: self.ids.id()?,
            job_name: types::try_string(&job.name)?,
            driver: types::try_string(job.driver().as_str())?,
            image: types::try_string(&job.image)?,
            state: TaskState::Queued,
            cpu_shares: job.cpu_shares,
            memory_limit: job.memory_limit,
            started_at: Time(now),
            ..Task::default()
        })
    }

    fn insert_entry(&mut self, task: Task, first_start: bool) {
        self.tasks.insert(
            task.id.clone(),
            Entry {
                task,
                check: Check::default(),
                restart_at: None,
                first_start,
                starting: false,
            },
        );
    }

    /// Stuurt de start van een aangenomen taak naar de runner.
    fn begin_start(&mut self, now: Nanos, id: &str) {
        let Some(e) = self.tasks.get(id) else {
            return;
        };
        let resolved = match self.jobs.get(&e.task.job_name) {
            Some(job) => self.resolve_job_for_run(job),
            None => Err(Error::NotFound),
        };
        match resolved {
            Ok(job) => {
                if let Some(e) = self.tasks.get_mut(id) {
                    e.starting = true;
                }
                let task_id = String::from(id);
                self.push(Action::Start {
                    task_id,
                    job: Box::new(job),
                });
            }
            Err(_) => self.start_failed(now, id),
        }
    }

    // ---- Uitkomsten van de runner ----------------------------------------------

    /// Voortgang van de startfase: queued wordt downloading, alleen vooruit.
    ///
    /// Een taak die al Stopping, Failed of Running is, wordt nooit teruggezet
    /// door een late melding.
    pub fn on_progress(&mut self, id: &str, downloaded: u64, total: u64) {
        if let Some(e) = self.tasks.get_mut(id)
            && matches!(e.task.state, TaskState::Queued | TaskState::Downloading)
        {
            e.task.state = TaskState::Downloading;
            e.task.downloaded = downloaded;
            e.task.image_size = total;
        }
    }

    /// De uitkomst van een [`Action::Start`].
    pub fn on_started(
        &mut self,
        now: Nanos,
        id: &str,
        driver: Driver,
        result: core::result::Result<StartOk, StartError>,
    ) {
        if let Some(e) = self.tasks.get_mut(id) {
            e.starting = false;
        }
        match result {
            Err(StartError::NoCapacity) => self.release_unplaceable(id),
            Err(StartError::Failed) => self.start_failed(now, id),
            Ok(ok) => self.started(now, id, driver, ok),
        }
    }

    fn started(&mut self, now: Nanos, id: &str, driver: Driver, ok: StartOk) {
        // Een stop of delete die de start kruiste, heeft het record al weggehaald:
        // dan is dit een geest, en die gaat meteen weer weg.
        let alive = self.tasks.get(id).is_some_and(|e| {
            e.task.state != TaskState::Stopping && self.jobs.contains_key(&e.task.job_name)
        });
        if !alive {
            self.tasks.remove(id);
            let task_id = String::from(id);
            self.push(Action::Stop {
                task_id,
                driver,
                pid: ok.pid,
            });
            return;
        }
        let Some(e) = self.tasks.get_mut(id) else {
            return;
        };
        e.task.state = TaskState::Running;
        e.task.pid = ok.pid;
        e.task.ports = ok.ports;
        // Pas vanaf hier draait de app: de aanmaaktijd is geen uptime.
        e.task.started_at = Time(now);
        e.task.next_restart_at = Time::ZERO;
        let name = e.task.job_name.clone();
        let has_check = self
            .jobs
            .get(&name)
            .is_some_and(|j| j.health_check.is_some());
        let event = if has_check {
            Event::Start
        } else {
            Event::Started
        };
        self.push(Action::Notify { job: name, event });
    }

    /// Een taak die de node aannam maar niet kan PLAATSEN, gaat terug naar de leader.
    ///
    /// Geen crash, dus herstarten is precies fout: elke poging haalt het image
    /// opnieuw en faalt weer, en die churn verhongert de taken die wél draaien
    /// (gemeten 26-07: één onplaatsbare app hield een hele node met 3 cores bezig).
    /// Het record weghalen herstelt ook de boekhouding, en dat stopt de storm:
    /// de volgende poging weigert de toelating vooraf (503).
    fn release_unplaceable(&mut self, id: &str) {
        if let Some(e) = self.tasks.remove(id) {
            self.push(Action::Notify {
                job: e.task.job_name,
                event: Event::Unplaceable,
            });
        }
    }

    fn start_failed(&mut self, now: Nanos, id: &str) {
        let Some(e) = self.tasks.get_mut(id) else {
            return;
        };
        e.task.state = TaskState::Failed;
        let notify = e.first_start;
        let name = e.task.job_name.clone();
        if notify {
            self.push(Action::Notify {
                job: name,
                event: Event::Crash,
            });
        }
        self.schedule_restart(now, id, false);
    }

    // ---- Herstart --------------------------------------------------------------

    /// Begint het herstartpad van een taak. `ran` zegt of de vorige poging de
    /// app echt aan de praat had; alleen dan kan hij een schone lei verdienen.
    fn schedule_restart(&mut self, now: Nanos, id: &str, ran: bool) {
        // Geef de vorige poging vrij voordat we beslissen of hij opnieuw mag;
        // ook het terminale pad mag geen kooi, cgroup of container laten liggen.
        if let Some(e) = self.tasks.get(id) {
            let action = stop_action(&e.task);
            self.push(action);
        }
        self.restart_decide(now, id, ran);
    }

    fn restart_decide(&mut self, now: Nanos, id: &str, ran: bool) {
        let Some(e) = self.tasks.get(id) else {
            return;
        };
        let Some(job) = self.jobs.get(&e.task.job_name) else {
            // De job is weg: de taak ook.
            self.tasks.remove(id);
            return;
        };
        let max = job.max_restarts.unwrap_or(DEFAULT_MAX_RESTARTS);
        let window = if job.restart_window > 0 {
            job.restart_window
        } else {
            DEFAULT_RESTART_WINDOW
        };
        let rand = self.ids.next_u64();
        let Some(e) = self.tasks.get_mut(id) else {
            return;
        };
        // Alleen echte uptime verdient een schoon budget; een trage mislukte start
        // mag zijn eigen teller nooit terugzetten.
        if ran && !e.task.started_at.is_zero() && Time(now).since(e.task.started_at) > window {
            e.task.restart_count = 0;
        }
        e.task.last_failed_at = Time(now);
        let count = e.task.restart_count;
        // -1 = onbeperkt; 0 = nooit.
        if max >= 0 && count >= max {
            e.task.state = TaskState::Failed;
            e.restart_at = None;
            return;
        }
        if count > 0 {
            let at = now.saturating_add(health::restart_delay(count, rand));
            e.task.next_restart_at = Time(at);
            e.restart_at = Some(at);
            return;
        }
        self.restart_fire(now, id);
    }

    /// Vervangt een taak door een verse poging: atomisch, zonder gat in de reservering.
    fn restart_fire(&mut self, now: Nanos, id: &str) {
        // Sluip geen vervanger langs de afsluiting.
        if self.shutting_down {
            return;
        }
        let Some(old) = self.tasks.get(id) else {
            return;
        };
        let Some(job) = self.jobs.get(&old.task.job_name) else {
            self.tasks.remove(id);
            return;
        };
        if self.resolve_job_for_run(job).is_err() {
            if let Some(e) = self.tasks.get_mut(id) {
                e.task.state = TaskState::Failed;
            }
            return;
        }
        let (count, last_failed) = (old.task.restart_count, old.task.last_failed_at);
        let job = match job.try_clone() {
            Ok(j) => j,
            Err(_) => return,
        };
        let Ok(mut replacement) = self.new_task(now, &job) else {
            return;
        };
        replacement.restart_count = count.saturating_add(1);
        replacement.last_failed_at = last_failed;
        let new_id = replacement.id.clone();
        self.tasks.remove(id);
        self.insert_entry(replacement, false);
        self.begin_start(now, &new_id);
    }

    // ---- Monitor ---------------------------------------------------------------

    fn monitor(&mut self, _now: Nanos) {
        let polls: Vec<Action> = self
            .tasks
            .values()
            .filter(|e| e.task.state == TaskState::Running)
            .map(|e| Action::Poll {
                task_id: e.task.id.clone(),
                driver: driver_of(&e.task),
                pid: e.task.pid,
            })
            .collect();
        for p in polls {
            self.push(p);
        }
    }

    /// De uitkomst van een [`Action::Poll`].
    pub fn on_status(&mut self, now: Nanos, id: &str, status: Status) {
        let Some(e) = self.tasks.get(id) else {
            return;
        };
        if e.task.state != TaskState::Running {
            return;
        }
        match status {
            Status::Failed => {
                // Stopping is wat een gecrashte, herstartende taak IS: de reservering
                // blijft staan, dus er gaat geen venster open waarin de leader in de
                // capaciteit dispatcht die deze herstart zo terugpakt.
                let name = e.task.job_name.clone();
                if let Some(e) = self.tasks.get_mut(id) {
                    e.task.state = TaskState::Stopping;
                    e.check = Check::default();
                }
                self.push(Action::Notify {
                    job: name,
                    event: Event::Crash,
                });
                self.schedule_restart(now, id, true);
            }
            Status::Running => self.maybe_probe(now, id),
        }
    }

    fn maybe_probe(&mut self, now: Nanos, id: &str) {
        let Some(e) = self.tasks.get(id) else {
            return;
        };
        let Some(hc) = self
            .jobs
            .get(&e.task.job_name)
            .and_then(|j| j.health_check.as_ref())
        else {
            return;
        };
        // De opstartgratie: trage services krijgen de tijd.
        if hc.initial_timeout > 0 && Time(now).since(e.task.started_at) < hc.initial_timeout {
            return;
        }
        match health::probe_for(&e.task, hc, self.settings.health_timeout()) {
            Ok(probe) => {
                let task_id = String::from(id);
                self.push(Action::Probe { task_id, probe });
            }
            // Geen poort met die naam: een mislukte controle.
            Err(()) => self.on_probe(now, id, Outcome::Tcp(false)),
        }
    }

    /// De uitkomst van een [`Action::Probe`].
    pub fn on_probe(&mut self, now: Nanos, id: &str, outcome: Outcome) {
        let Some(e) = self.tasks.get(id) else {
            return;
        };
        if e.task.state != TaskState::Running {
            return;
        }
        let name = e.task.job_name.clone();
        let threshold = self
            .jobs
            .get(&name)
            .and_then(|j| j.health_check.as_ref())
            .map_or(0, |hc| hc.failure_threshold);
        let Some(e) = self.tasks.get_mut(id) else {
            return;
        };
        let healthy = health::is_healthy(outcome, &e.check);
        match health::apply(&mut e.check, healthy, now, threshold) {
            Verdict::Unhealthy => {
                e.task.state = TaskState::Failed;
                e.check = Check::default();
                self.push(Action::Notify {
                    job: name,
                    event: Event::Crash,
                });
                self.schedule_restart(now, id, true);
            }
            Verdict::Healthy { passed: true } if !e.check.notified_healthy => {
                // De eerste geslaagde controle: klaar voor verkeer.
                e.check.notified_healthy = true;
                self.push(Action::Notify {
                    job: name,
                    event: Event::Started,
                });
            }
            Verdict::Healthy { .. } => {}
        }
    }

    /// Legt het gemeten gebruik van een taak vast (procenten van zijn eigen toewijzing).
    pub fn record_usage(&mut self, id: &str, cpu_percent: f64, mem_percent: f64) {
        if let Some(e) = self.tasks.get_mut(id) {
            e.task.cpu_percent = cpu_percent;
            e.task.mem_percent = mem_percent;
        }
    }

    // ---- Stoppen ---------------------------------------------------------------

    /// Haalt het logische record weg en vraagt de runner één keer om vrijgave.
    ///
    /// Een mislukte fysieke opruiming is runner-quarantaine; hij mag een dode
    /// taak nooit eeuwig in de planner laten staan.
    fn remove_and_stop(&mut self, id: &str) -> bool {
        match self.tasks.remove(id) {
            Some(e) => {
                self.push(stop_action(&e.task));
                true
            }
            None => false,
        }
    }

    /// Stopt één taak (rolling en blue-green updates); `false` als hij er niet is.
    pub fn stop_task(&mut self, id: &str) -> bool {
        self.remove_and_stop(id)
    }

    fn ids_of_job(&self, name: &str) -> Vec<String> {
        self.tasks
            .iter()
            .filter(|(_, e)| e.task.job_name == name)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Stopt alle taken van een job maar houdt de job (preemptie); geeft het aantal.
    pub fn stop_job_tasks(&mut self, name: &str) -> usize {
        let ids = self.ids_of_job(name);
        for id in &ids {
            self.remove_and_stop(id);
        }
        ids.len()
    }

    /// Verwijdert een job en stopt al zijn taken; geeft het aantal.
    pub fn delete_job(&mut self, now: Nanos, name: &str) -> usize {
        self.jobs.remove(name);
        // De klok mee: zonder deze bump geldt een sync-payload van vóór deze
        // delete nog als nieuwer en importeert hij de job opnieuw (15-07).
        self.state_time = Time(now);
        let n = self.stop_job_tasks(name);
        let job = String::from(name);
        self.push(Action::Notify {
            job,
            event: Event::Stop,
        });
        n
    }

    /// Stopt elke taak één keer, ook de mislukte (isolatie); geeft het aantal.
    ///
    /// Aanwezigheid is eigendom, los van staat: er is geen eindstaat "gestopt"
    /// om uit te filteren.
    pub fn stop_all(&mut self) -> usize {
        let ids: Vec<String> = self.tasks.keys().cloned().collect();
        for id in &ids {
            self.remove_and_stop(id);
        }
        ids.len()
    }

    /// Sluit af: geen nieuwe herstarts meer, en elke taak één stoppoging.
    pub fn shutdown(&mut self) -> usize {
        self.shutting_down = true;
        self.stop_all()
    }

    // ---- De job-store ----------------------------------------------------------

    /// Een job op naam.
    pub fn get_job(&self, name: &str) -> Option<&Job> {
        self.jobs.get(name)
    }

    /// Alle jobs.
    pub fn jobs(&self) -> impl Iterator<Item = &Job> {
        self.jobs.values()
    }

    /// Alle taken.
    pub fn tasks(&self) -> impl Iterator<Item = &Task> {
        self.tasks.values().map(|e| &e.task)
    }

    /// Een taak op id.
    pub fn task(&self, id: &str) -> Option<&Task> {
        self.tasks.get(id).map(|e| &e.task)
    }

    /// Of de runner deze taak op dit moment start.
    pub fn is_starting(&self, id: &str) -> bool {
        self.tasks.get(id).is_some_and(|e| e.starting)
    }

    /// Wanneer de volgende herstartpoging van een taak is, als er een wacht.
    pub fn restart_pending(&self, id: &str) -> Option<Nanos> {
        self.tasks.get(id).and_then(|e| e.restart_at)
    }

    /// Het aantal taken per job, ALLE staten: een mislukte taak heeft zijn budget op
    /// en mag niet opnieuw gedispatcht worden.
    pub fn placed_task_counts(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for e in self.tasks.values() {
            if !e.task.job_name.is_empty() {
                *counts.entry(e.task.job_name.clone()).or_insert(0) += 1;
            }
        }
        counts
    }

    /// Slaat een job op (de leader die van een job op afstand hoort).
    pub fn store_job(&mut self, now: Nanos, job: Job) -> Result {
        if !self.jobs.contains_key(&job.name) && self.jobs.len() >= MAX_JOBS {
            return Err(Error::TooManyJobs);
        }
        self.jobs.insert(job.name.clone(), job);
        self.state_time = Time(now);
        Ok(())
    }

    /// Schrijft alleen als de job nog bestaat, zodat een snapshot-herschrijving
    /// een verwijderde job nooit laat herrijzen.
    pub fn update_job(&mut self, now: Nanos, job: Job) -> bool {
        match self.jobs.get_mut(&job.name) {
            Some(slot) => {
                *slot = job;
                self.state_time = Time(now);
                true
            }
            None => false,
        }
    }

    /// Herschrijft alleen de prioriteit van een bestaande job.
    pub fn set_job_priority(&mut self, now: Nanos, name: &str, priority: i64) -> bool {
        match self.jobs.get_mut(name) {
            Some(j) => {
                j.priority = Some(priority);
                self.state_time = Time(now);
                true
            }
            None => false,
        }
    }

    /// Herschrijft alleen de deploying-vlag van een bestaande job.
    pub fn set_job_deploying(&mut self, now: Nanos, name: &str, deploying: bool) -> bool {
        match self.jobs.get_mut(name) {
            Some(j) => {
                j.deploying = deploying;
                self.state_time = Time(now);
                true
            }
            None => false,
        }
    }

    /// Verwijdert alleen de job-definitie.
    pub fn delete_job_definition(&mut self, now: Nanos, name: &str) {
        self.jobs.remove(name);
        self.state_time = Time(now);
    }

    /// Wanneer de job-store het laatst veranderde.
    pub fn state_time(&self) -> Time {
        self.state_time
    }

    /// Neemt jobs van de leader over (alleen zolang deze node leidt).
    pub fn sync_jobs(&mut self, jobs: Vec<Job>, updated: Time) -> Result {
        for job in jobs {
            if !self.jobs.contains_key(&job.name) && self.jobs.len() >= MAX_JOBS {
                return Err(Error::TooManyJobs);
            }
            self.jobs.insert(job.name.clone(), job);
        }
        self.state_time = updated;
        Ok(())
    }

    // ---- Voor tests ----------------------------------------------------------

    #[cfg(test)]
    pub(crate) fn tasks_map(&self) -> &BTreeMap<String, Entry> {
        &self.tasks
    }

    /// Zet een taak rechtstreeks in de staat (tests: de Go-tests schreven `s.tasks`).
    #[cfg(test)]
    pub(crate) fn insert_task(&mut self, task: Task) {
        self.insert_entry(task, false);
    }

    /// Een taak om te muteren (tests).
    #[cfg(test)]
    pub(crate) fn task_mut(&mut self, id: &str) -> Option<&mut Task> {
        self.tasks.get_mut(id).map(|e| &mut e.task)
    }
}

/// Een HopOS-app draait op hele cores en partities in blokken van 2 MB.
///
/// Rond naar boven af, zodat de boekhouding van Hop precies klopt met wat
/// HopOS uitdeelt: een volle node weigert HIER (503), niet pas in de runner.
/// Zonder de geheugenafronding zei de toelating ja waar de runner nee moest
/// zeggen, en pingpongde een onplaatsbare job in een hand-back-lus die via de
/// gemiste watchdog de node velde (gemeten 17/18-08, LicheeRV).
fn round_for_hopos(job: &mut Job) {
    let cores = job.cpu_shares.saturating_add(1023) / 1024;
    job.cpu_shares = cores.max(1).saturating_mul(1024);
    if job.memory_limit > 0 {
        job.memory_limit = job.memory_limit.saturating_add(HOP_BLOCK_BYTES - 1) / HOP_BLOCK_BYTES
            * HOP_BLOCK_BYTES;
    }
}

fn driver_of(task: &Task) -> Driver {
    Driver::parse(&task.driver).unwrap_or(Driver::Exec)
}

fn stop_action(task: &Task) -> Action {
    Action::Stop {
        task_id: task.id.clone(),
        driver: driver_of(task),
        pid: task.pid,
    }
}

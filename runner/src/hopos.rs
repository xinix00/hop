//! De HopOS-backend: elke taak is een native app-image in een eigen kooi.
//!
//! Bezit de boekhouding welke taak welke kooi houdt, de startfase per taak
//! (wachten op het image, stromen, gearmd) en de logringen. De kern bezit
//! de kooien zelf, de cores en de partities; HopOS dwingt isolatie en de
//! geheugenlimiet af in hardware, dus deze runner geeft alleen image, env
//! en limieten door.
//!
//! Poorten: elke taak heeft een eigen netwerkstack op het interne net van
//! HopOS; de node publiceert elke toegewezen poort met stateless DNAT, en de
//! taak bindt hetzelfde nummer (uit `ER_PORT_<NAAM>`).

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::env::{attr_env_vars, port_env_vars};
use crate::logs::{LogPolicy, LogRing, LogStore};
use crate::system::{Slot, SlotApp, SlotState, StartSpec, Streamed, SysError, SystemApi};
use crate::{Error, Result, RunState, Runner, StartRequest, Started, Stream, TaskRef};

/// Het coöperatieve venster voordat een stop escaleert naar de stage-2-intrekking.
///
/// Een gezonde app parkeert binnen ongeveer 100 ms na de killvlag (de
/// bewaaklus pollt elke 50 ms), dus 3 s is ruim; de oude 10 s liet elke
/// weerspannige stop in een delete-storm 10 geserialiseerde seconden kosten
/// (gemeten 15-07: 127 deletes duurden tientallen minuten).
pub const HOP_STOP_TIMEOUT_MS: u64 = 3_000;

/// Hoeveel images de node tegelijk binnenhaalt.
///
/// De grens is fysiek, geen beleid: elke stroom kost de Hop-core een
/// TLS-sessie plus een leesbuffer, en TLS is op een klein board CPU-werk op
/// die core. Wie in de rij staat is gewoon "queued": zichtbaar, en de
/// capaciteit is al geteld.
pub const MAX_CONCURRENT_DOWNLOADS: usize = 4;

/// Hoe lang een logregel uit de kern maximaal is; langer wordt afgekapt.
const LOG_LINE_MAX: usize = 512;

/// Waar de start van een taak is.
#[derive(Debug)]
enum Phase {
    /// De taak wacht op zijn image; de kern heeft nog niets gereserveerd.
    Queued(StartSpec),
    /// De kern gaf `slot`; het image stroomt: `done` van `size` bytes.
    Streaming { slot: Slot, done: u64, size: u64 },
    /// De app draait (of draaide); een stop is een gewone app-stop.
    Armed(Slot),
}

impl Phase {
    /// Het slot, zodra de kern er een gaf.
    fn slot(&self) -> Option<Slot> {
        match self {
            Phase::Queued(_) => None,
            Phase::Streaming { slot, .. } | Phase::Armed(slot) => Some(*slot),
        }
    }
}

/// De start en de kooi van één taak.
#[derive(Debug)]
struct Cage {
    phase: Phase,
    /// De reden van het einde is al in de log gezet (één keer, niet per poll).
    fault_logged: bool,
}

/// De HopOS-runner bovenop een [`SystemApi`].
///
/// # Invariants
///
/// `in_use[slot] == id` precies dan als `cages[id].phase.slot() == Some(slot)`,
/// en het aantal kooien in `Phase::Streaming` is `downloads`.
#[derive(Debug)]
pub struct HopRunner<S> {
    sys: S,
    node_attrs: BTreeMap<String, String>,
    logs: LogStore,
    cages: BTreeMap<String, Cage>,
    in_use: BTreeMap<Slot, String>,
    downloads: usize,
}

impl<S: SystemApi> HopRunner<S> {
    /// Een runner op `sys`; `node_attrs` gaan als `ER_ATTR_*` mee naar elke app.
    pub fn new(sys: S, node_attrs: BTreeMap<String, String>, logs: LogPolicy) -> Self {
        Self {
            sys,
            node_attrs,
            logs: LogStore::new(logs),
            cages: BTreeMap::new(),
            in_use: BTreeMap::new(),
            downloads: 0,
        }
    }

    /// De kern-kant (tests en diagnose).
    pub fn system(&self) -> &S {
        &self.sys
    }

    /// De kern-kant, muteerbaar (tests).
    pub fn system_mut(&mut self) -> &mut S {
        &mut self.sys
    }

    /// De grootste partitie die de node nog kan plaatsen; `None` als hij het niet weet.
    pub fn pool_largest(&self) -> Option<u64> {
        self.sys.pool_largest()
    }

    /// De kooi van een taak, als deze runner hem bezit en de kern er een gaf.
    pub fn slot_of(&self, task_id: &str) -> Option<Slot> {
        self.cages.get(task_id).and_then(|c| c.phase.slot())
    }

    /// Het aantal kooien dat deze runner bezit.
    pub fn cages_in_use(&self) -> usize {
        self.in_use.len()
    }

    /// Draagt de kooien van al draaiende taken over (na een kern-flip).
    ///
    /// Dit is geen tweede waarheid over wat er draait, alleen EIGENDOM: welke
    /// taak deze kooi straks mag stoppen. Zonder deze stap is zo'n bewoner niet
    /// meer te stoppen (GEMETEN 02-09 op de M4: welcome verwijderd, node meldde
    /// hem nog live). Idempotent; pakt nooit een kooi af die al uitgedeeld is.
    pub fn adopt_running(&mut self, slots: &[(String, Slot)]) {
        for (id, slot) in slots {
            if slot.0 < 1 || self.cages.contains_key(id) || self.in_use.contains_key(slot) {
                continue;
            }
            self.in_use.insert(*slot, id.clone());
            self.cages.insert(
                id.clone(),
                Cage {
                    phase: Phase::Armed(*slot),
                    fault_logged: false,
                },
            );
            self.logs.open(id);
        }
    }

    /// Stopt de bewoners die van niemand zijn en geeft hun slots.
    ///
    /// Na een herstart van Hop draaien de apps door, en wie niet in de
    /// bewaarde staat stond (een job die rond de herstart verwijderd werd,
    /// een record dat de leader kwijt is) houdt zijn slot tot de volgende
    /// koude boot: de kern kiest het laagste vrije slot en kent geen
    /// eigenaar. GEMETEN 01-10 op de Pi 4: twee uitgemeten benches in slot 3
    /// en 4 die Hop niet kende; elke plaatsing en elke flip liep daarna op
    /// "slot 5 out of range 1..4". Een node heeft één Hop, dus een bewoner
    /// die hij na [`adopt_running`](Self::adopt_running) niet kent, is van
    /// niemand. Alleen de slots boven het eigen (1); een stop die de kern
    /// niet bevestigt, blijft zijn quarantaine (hij wordt niet hergebruikt).
    pub async fn sweep_strays(&mut self) -> Vec<Slot> {
        let mut stopped = Vec::new();
        for i in 2..=self.sys.num_cores().max(2) {
            let slot = Slot(i);
            if self.in_use.contains_key(&slot) {
                continue;
            }
            if self.sys.slot_status(slot).await.state != SlotState::Running {
                continue;
            }
            if self.sys.stop_slot(slot, HOP_STOP_TIMEOUT_MS).await.is_ok() {
                stopped.push(slot);
            }
        }
        stopped
    }

    /// CPU (procent van de eigen cores) en werkelijk geheugen, zoals de app ze meldt.
    ///
    /// `None` voor een veld dat nog niet gemeten is (de app start nog).
    pub async fn usage(&mut self, task: &TaskRef<'_>) -> (Option<u8>, Option<u64>) {
        if task.pid == 0 {
            return (None, None);
        }
        let slot = self.slot_of(task.id).unwrap_or(Slot(task.pid));
        let s = self.sys.slot_status(slot).await;
        (s.cpu_pct, (s.mem_sys != 0).then_some(s.mem_sys))
    }

    /// Haalt de logregels van alle draaiende apps uit de kern in hun ringen.
    ///
    /// De executor roept dit periodiek; hij is de pomp die in Go een goroutine
    /// per taak was. Elke regel is een call naar de kern; tussen de calls
    /// draaien de andere taken.
    pub async fn pump_logs(&mut self, now: u64) {
        let mut buf = [0u8; LOG_LINE_MAX];
        for (id, cage) in &self.cages {
            let Phase::Armed(slot) = cage.phase else {
                continue;
            };
            while let Some(n) = self.sys.next_log_line(slot, &mut buf).await {
                let line = buf.get(..n).unwrap_or(&[]);
                let text = core::str::from_utf8(line).unwrap_or("<log line not utf-8>");
                if let Some(ring) = self.logs.live_mut(id, Stream::Stdout) {
                    ring.write(text);
                }
            }
        }
        self.logs.sweep(now);
    }

    /// Schrijft een regel in de stdout-ring van een taak.
    fn log(&mut self, task_id: &str, line: &str) {
        if let Some(ring) = self.logs.live_mut(task_id, Stream::Stdout) {
            ring.write(line);
        }
    }

    /// Schrijft de reden dat een taak niet startte in zijn eigen log, en pensioneert die.
    ///
    /// Op een node is er geen console om op terug te vallen, en een start die
    /// faalt heeft nog geen app die het zelf kan zeggen (GEMETEN 12-08 op een
    /// LicheeRV: "failed, restarts 5" en de reden stond alleen op een seriële
    /// lijn die niemand las).
    fn fail(&mut self, now: u64, task_id: &str, err: Error) -> Error {
        let line = format!("hop: this task did not start: {err}");
        self.log(task_id, &line);
        self.logs.retire(now, task_id);
        err
    }

    /// Geeft de boekhouding van een taak vrij en pensioneert zijn logs.
    fn release(&mut self, now: u64, task_id: &str) {
        self.drop_cage(task_id);
        self.logs.retire(now, task_id);
    }

    /// Geeft alleen de boekhouding vrij; de logs blijven open voor de faalreden.
    fn drop_cage(&mut self, task_id: &str) {
        if let Some(cage) = self.cages.remove(task_id) {
            if let Some(slot) = cage.phase.slot() {
                self.in_use.remove(&slot);
            }
            if matches!(cage.phase, Phase::Streaming { .. }) {
                self.downloads = self.downloads.saturating_sub(1);
            }
        }
    }

    /// Bouwt de spec voor een start, zonder de image-maat.
    fn spec(&self, req: &StartRequest<'_>) -> Result<StartSpec> {
        if !req.image.is_empty() {
            return Err(Error::Rejected("containers are not supported on HopOS"));
        }
        if req.artifacts != 1 {
            return Err(Error::Rejected(
                "exactly one artifact (the app image) is required",
            ));
        }
        if !req.extract.is_empty() {
            return Err(Error::Rejected(
                "artifact must be a raw app image (no extract)",
            ));
        }
        let mut env = req.env.clone();
        attr_env_vars(&self.node_attrs, &mut env);
        port_env_vars(req.ports, &mut env);

        // 1024 shares is één core (de Docker/Nomad-conventie), minimaal 1. Met
        // een sharegroup draait de app op één core in een pool van zoveel cores
        // die hij deelt met gelijk-getagde apps.
        let cores = (req.cpu_shares / 1024).max(1);
        let sharegroup = req.tags.get("sharegroup").cloned().unwrap_or_default();
        let (app_cores, pool_cores) = if sharegroup.is_empty() {
            (cores, 1)
        } else {
            (1, cores)
        };
        Ok(StartSpec {
            image_size: 0,
            mem_limit: req.memory_limit,
            core_class: req.tags.get("core-class").cloned().unwrap_or_default(),
            cores: app_cores,
            sharegroup,
            pool_cores,
            env,
            mounts: req.volumes.clone(),
            ports: req.ports.clone(),
            job: req.job_name.to_string(),
        })
    }

    fn placement_err(e: SysError) -> Error {
        match e {
            SysError::NoCapacity(why) => Error::NoCapacity(why),
            other => Error::System(other),
        }
    }

    /// Breekt een stroom af of stopt een app; `Ok` als de kern de vrijgave op zich neemt.
    async fn stop_cage(&mut self, now: u64, task_id: &str, slot: Slot) -> Result {
        match self.sys.stop_slot(slot, HOP_STOP_TIMEOUT_MS).await {
            Ok(()) => {
                self.release(now, task_id);
                Ok(())
            }
            // De kern kon de vrijgave niet bevestigen. De kooi blijft van ons, zodat
            // niemand hem hergebruikt op basis van een onbevestigde vrijgave.
            Err(_) => Err(Error::Quarantined(slot)),
        }
    }

    /// Zet de reden van een einde één keer in de log van de taak.
    async fn log_end_once(&mut self, task_id: &str, slot: Slot) {
        let Some(cage) = self.cages.get_mut(task_id) else {
            return;
        };
        if cage.fault_logged {
            return;
        }
        cage.fault_logged = true;
        let s = self.sys.slot_status(slot).await;
        let line = if s.fault_vec != 0 {
            format!(
                "hop: task failed: stage-2 fault on slot {} (vec {}, ESR {:#x}, FAR {:#x})",
                slot.0,
                s.fault_vec - 1,
                s.fault_esr,
                s.fault_far
            )
        } else if !s.cage.is_empty() {
            format!(
                "hop: task failed: exit code {} (slot {}): {}",
                s.exit_code, slot.0, s.cage
            )
        } else if s.exit_code != 0 {
            format!(
                "hop: task failed: exit code {} (slot {})",
                s.exit_code, slot.0
            )
        } else {
            return;
        };
        self.log(task_id, &line);
    }
}

impl<S: SystemApi> Runner for HopRunner<S> {
    async fn start(&mut self, now: u64, req: &StartRequest<'_>) -> Result<Started> {
        // De log bestaat vóór de eerste faalbare stap: anders is er geen plek
        // om de fout in te schrijven.
        self.logs.open(req.task_id);
        let spec = match self.spec(req) {
            Ok(s) => s,
            Err(e) => return Err(self.fail(now, req.task_id, e)),
        };
        // Het slot kiest de kern pas bij `image_begin`: hij kent de bewoners
        // die een kern-flip overleefden, en wij hoeven geen tweede waarheid
        // bij te houden over welke kooi vrij is.
        self.cages.insert(
            req.task_id.to_string(),
            Cage {
                phase: Phase::Queued(spec),
                fault_logged: false,
            },
        );
        Ok(Started::AwaitImage)
    }

    async fn image_begin(&mut self, now: u64, task_id: &str, size: u64) -> Result {
        let queued = match self.cages.get(task_id) {
            None => return Err(Error::UnknownTask),
            Some(c) => matches!(c.phase, Phase::Queued(_)),
        };
        if !queued {
            return Err(Error::Stream("image already begun"));
        }
        if size == 0 {
            // Verplicht: de plaatsing valideert tegen de image-maat, en een
            // afgekapte stroom moet een luide fout zijn, geen halve app.
            self.drop_cage(task_id);
            return Err(self.fail(now, task_id, Error::Stream("no Content-Length")));
        }
        if self.downloads >= MAX_CONCURRENT_DOWNLOADS {
            return Err(Error::Busy);
        }
        let spec = match self.cages.get_mut(task_id).map(|c| &mut c.phase) {
            Some(Phase::Queued(spec)) => {
                spec.image_size = size;
                spec.clone()
            }
            _ => return Err(Error::UnknownTask),
        };
        let slot = match self.sys.start_slot(&spec).await {
            Ok(slot) => slot,
            Err(e) => {
                // De kern ruimde zijn eigen reserveringen op; wij de onze.
                self.drop_cage(task_id);
                return Err(self.fail(now, task_id, Self::placement_err(e)));
            }
        };
        self.in_use.insert(slot, task_id.to_string());
        if let Some(c) = self.cages.get_mut(task_id) {
            c.phase = Phase::Streaming {
                slot,
                done: 0,
                size,
            };
        }
        self.downloads = self.downloads.saturating_add(1);
        Ok(())
    }

    async fn image_chunk(&mut self, now: u64, task_id: &str, chunk: &[u8]) -> Result<Started> {
        let (slot, done, size) = match self.cages.get(task_id) {
            Some(Cage {
                phase: Phase::Streaming { slot, done, size },
                ..
            }) => (*slot, *done, *size),
            Some(_) => return Err(Error::Stream("image not begun")),
            None => return Ok(Started::Aborted),
        };
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        let total = done.saturating_add(len);
        if total > size {
            // Afbreken; lukt dat niet, dan blijft de kooi in quarantaine.
            if self.sys.stop_slot(slot, HOP_STOP_TIMEOUT_MS).await.is_ok() {
                self.drop_cage(task_id);
            }
            return Err(self.fail(
                now,
                task_id,
                Error::Stream("more bytes than Content-Length"),
            ));
        }
        let streamed = match self.sys.stream_image(slot, chunk).await {
            Ok(s) => s,
            Err(e) => {
                // Een geweigerde brok: de kern brak de stroom af en ruimde op.
                self.drop_cage(task_id);
                return Err(self.fail(now, task_id, Self::placement_err(e)));
            }
        };
        match streamed {
            Streamed::More if total < size => {
                if let Some(c) = self.cages.get_mut(task_id) {
                    c.phase = Phase::Streaming {
                        slot,
                        done: total,
                        size,
                    };
                }
                Ok(Started::AwaitImage)
            }
            Streamed::Placed if total == size => {
                // Gearmd: een stop is vanaf nu een gewone app-stop, geen
                // afbreking meer.
                if let Some(c) = self.cages.get_mut(task_id) {
                    c.phase = Phase::Armed(slot);
                }
                self.downloads = self.downloads.saturating_sub(1);
                // Welke kooi kreeg deze taak? De node weet het, en vooraan in
                // de eigen log komt het heel aan, waar een operator al kijkt
                // (`hop logs`).
                let cage = self.sys.slot_status(slot).await.cage;
                if !cage.is_empty() {
                    self.log(task_id, &cage);
                }
                Ok(Started::Running { pid: slot.0 })
            }
            Streamed::Failed(e) => {
                self.drop_cage(task_id);
                Err(self.fail(now, task_id, Self::placement_err(e)))
            }
            Streamed::More | Streamed::Placed => {
                // De kern en wij tellen anders: nooit een halve app laten
                // staan. Lukt de stop niet, dan blijft de kooi van ons.
                if self.sys.stop_slot(slot, HOP_STOP_TIMEOUT_MS).await.is_ok() {
                    self.drop_cage(task_id);
                }
                Err(self.fail(
                    now,
                    task_id,
                    Error::Stream("kernel and runner disagree on the image size"),
                ))
            }
        }
    }

    async fn stop(&mut self, now: u64, task: &TaskRef<'_>) -> Result {
        // Onbekend = niet van ons, en met opzet: een VEROUDERD taakrecord mag nooit
        // de nieuwe bewoner van dat kooinummer omleggen.
        let Some(cage) = self.cages.get(task.id) else {
            return Ok(());
        };
        match cage.phase.slot() {
            None => {
                self.release(now, task.id);
                Ok(())
            }
            Some(slot) => self.stop_cage(now, task.id, slot).await,
        }
    }

    async fn status(&mut self, _now: u64, task: &TaskRef<'_>) -> Result<RunState> {
        // Zonder pid zit de taak nog in zijn startfase; die bevraagt de monitor
        // niet, en komt er toch iemand, dan is "draait" het eerlijke antwoord.
        if task.pid == 0 {
            return Ok(RunState::Running);
        }
        let slot = self.slot_of(task.id).unwrap_or(Slot(task.pid));
        let s = self.sys.slot_status(slot).await;
        if s.core_on {
            return Ok(RunState::Running);
        }
        if s.app != SlotApp::Exited || s.exit_code != 0 || s.fault_vec != 0 {
            self.log_end_once(task.id, slot).await;
        }
        Ok(RunState::Failed)
    }

    fn logs(&self, now: u64, task_id: &str, stream: Stream) -> Option<&LogRing> {
        self.logs.get(now, task_id, stream)
    }
}

//! De toestand van de leider en de verzoeken van agents: registreren, heartbeat, afmelden.

use alloc::string::String;
use alloc::vec::Vec;

use types::{Agent, Job, Map, Nanos, Time, try_string};

use crate::events::{Event, Events};
use crate::{DEFAULT_AGENT_TIMEOUT, Error, JobStore, MAX_AGENTS, Result, Transport};

/// Eén lopende taak op één agent: wat een update stopt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRef {
    /// De agent.
    pub agent_id: String,
    /// De taak.
    pub task_id: String,
}

/// De leider: de enige auteur van gewenste staat en de enige die plaatst.
///
/// # Invariants
///
/// `agents` is strikt oplopend gesorteerd op id; dat is de deterministische
/// round-robin-volgorde.
pub struct Leader<S> {
    pub(crate) local_agent_id: String,
    pub(crate) store: S,
    pub(crate) agents: Vec<Agent>,
    /// agentID naar jobnaam naar aantal instanties.
    pub(crate) placed: Map<Map<u32>>,
    pub(crate) settled: bool,
    pub(crate) settle_at: Option<Time>,
    pub(crate) round_robin: usize,
    pub(crate) agent_timeout: Nanos,
    pub(crate) ticks: u64,
    pub(crate) events: Events,
    /// Of de gewenste staat veranderde sinds de laatste snapshot.
    pub(crate) dirty: bool,
    /// Wanneer [`Leader::poll_snapshot`] de verandering voor het eerst zag.
    pub(crate) dirty_seen: Option<Time>,
}

impl<S: JobStore> Leader<S> {
    /// Een leider zonder settle-periode (meteen reconcilen), over `store`.
    pub fn new(local_agent_id: String, store: S) -> Self {
        Self {
            local_agent_id,
            store,
            agents: Vec::new(),
            placed: Map::new(),
            settled: true,
            settle_at: None,
            round_robin: 0,
            agent_timeout: DEFAULT_AGENT_TIMEOUT,
            ticks: 0,
            events: Events::default(),
            dirty: false,
            dirty_seen: None,
        }
    }

    /// Zet de timeout waarna een agent zonder heartbeat dood is.
    pub fn set_agent_timeout(&mut self, d: Nanos) {
        self.agent_timeout = d;
    }

    /// Begint een settle-periode van `delay` vanaf `now`.
    ///
    /// Een nieuwe leider weet niet wat er al draait. Zonder settle zou hij
    /// alles dispatchen, en dat zijn duplicaten; met settle registreren de
    /// agents zich eerst met hun `placed`-tellingen, en pas daarna kijkt hij.
    /// De productie-waarde is de agent-timeout ([`Leader::enable_settle`]).
    pub fn settle(&mut self, delay: Nanos, now: Time) {
        if delay == 0 {
            self.settled = true;
            self.settle_at = None;
        } else {
            self.settled = false;
            self.settle_at = Some(Time(now.0.saturating_add(delay)));
        }
    }

    /// Begint de standaard settle-periode: één agent-timeout (30 s).
    pub fn enable_settle(&mut self, now: Time) {
        self.settle(self.agent_timeout, now);
    }

    /// Of de settle-periode voorbij is.
    pub fn is_settled(&self) -> bool {
        self.settled
    }

    /// Wanneer de settle-periode afloopt, zodat de adapter dan [`Leader::tick`] roept.
    pub fn settle_deadline(&self) -> Option<Time> {
        self.settle_at
    }

    /// Het id van de agent op deze node.
    pub fn local_agent_id(&self) -> &str {
        &self.local_agent_id
    }

    /// De store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Geeft de store terug als het leiderschap eindigt.
    pub fn into_store(self) -> S {
        self.store
    }

    /// Alle jobs.
    pub fn jobs(&self) -> &[Job] {
        self.store.jobs()
    }

    /// Eén job op naam.
    pub fn job(&self, name: &str) -> Option<&Job> {
        self.store.get(name)
    }

    /// De eerstvolgende vrije prioriteit (= het aantal jobs).
    pub fn next_priority(&self) -> i64 {
        i64::try_from(self.store.jobs().len()).unwrap_or(i64::MAX)
    }

    /// Het tijdstip van de laatst geladen snapshot.
    pub fn state_time(&self) -> Time {
        self.store.state_time()
    }

    /// De geregistreerde agents, gesorteerd op id.
    pub fn agents(&self) -> &[Agent] {
        &self.agents
    }

    /// Eén agent op id.
    pub fn agent(&self, id: &str) -> Option<&Agent> {
        self.agents.iter().find(|a| a.id == id)
    }

    /// De wachtende meldingen voor `/v1/events`; de rij is daarna leeg.
    pub fn drain_events(&mut self) -> Vec<Event> {
        self.events.drain()
    }

    /// Een melding van buiten (`POST /v1/notify`): `job:<naam>[:<event>]`,
    /// `agent:<id>`, of iets anders (ook leeg) voor "kijk alles opnieuw".
    ///
    /// Het `<event>`-achtervoegsel valt weg: een [`Event`] zegt wát er
    /// veranderde, niet hoe, en een abonnee haalt de rest zelf op.
    pub fn notify(&mut self, topic: &str) {
        if let Some(rest) = topic.strip_prefix("job:") {
            let name = rest.split_once(':').map_or(rest, |(n, _)| n);
            if !name.is_empty() {
                self.events.job(name);
                return;
            }
        } else if let Some(id) = topic.strip_prefix("agent:")
            && !id.is_empty()
        {
            self.events.agent(id);
            return;
        }
        self.events.status();
    }

    /// Een levensteken van agent `id`. `false` voor een onbekende agent: de
    /// adapter geeft dan 404 en de agent registreert zich opnieuw.
    ///
    /// Puur liveness, geen job-uitwisseling (gesloopt 16-07): de leider is
    /// de enige auteur van gewenste staat, en de oude bidirectionele sync
    /// hier was de kraamkamer van de delete-storm-zombies van 15-07.
    pub fn heartbeat(&mut self, id: &str, version: &str, temp_milli_c: i64, now: Time) -> bool {
        let Some(agent) = self.agents.iter_mut().find(|a| a.id == id) else {
            return false;
        };
        agent.last_seen = now;
        if agent.version != version {
            match try_string(version) {
                Ok(v) => agent.version = v,
                Err(_) => return true,
            }
        }
        // Telemetrie, geen scheduling-input.
        agent.temp_milli_c = temp_milli_c;
        true
    }

    /// Registreert een (her)startende agent met wat hij al draait.
    ///
    /// Geeft `false` als hetzelfde id nog levend op een ander adres staat
    /// (een dubbel). Oude staat van een vorige incarnatie gaat weg. Na de
    /// settle-periode stopt dit eerst het overschot dat een terugkerende
    /// agent meebrengt, en reconcilet dan.
    pub fn register_agent(
        &mut self,
        agent: Agent,
        placed: Map<u32>,
        now: Time,
        net: &mut impl Transport,
    ) -> Result<bool> {
        if let Some(existing) = self.agent(&agent.id)
            && existing.endpoint != agent.endpoint
            && now.since(existing.last_seen) < self.agent_timeout
        {
            return Ok(false);
        }
        let id = try_string(&agent.id)?;
        self.remove_agent(&id);
        if self.agents.len() >= MAX_AGENTS {
            return Err(Error::TooMany {
                what: "agents",
                max: MAX_AGENTS,
            });
        }
        let mut agent = agent;
        agent.last_seen = now;
        let at = self
            .agents
            .binary_search_by(|a| a.id.as_str().cmp(&agent.id))
            .unwrap_or_else(|i| i);
        self.agents.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
        // INVARIANT: `at` is de sorteerplek; het id is net weggehaald.
        self.agents.insert(at, agent);
        self.placed.insert(try_string(&id)?, placed)?;

        if self.settled {
            // Een terugkerende agent kan taken meebrengen die de cluster
            // intussen elders plaatste. Dat overschot stoppen op de
            // terugkeerder (hoogstens een oude versie, en er is al een
            // vervanger), dan pas reconcilen.
            self.trim_returning_agent_surplus(&id, net)?;
            self.reconcile_jobs(net)?;
        }
        self.events.agent(&id);
        Ok(true)
    }

    /// Meldt een agent af en reconcilet.
    pub fn unregister_agent(&mut self, id: &str, net: &mut impl Transport) -> Result {
        self.remove_agent(id);
        self.reconcile_jobs(net)
    }

    pub(crate) fn remove_agent(&mut self, id: &str) {
        self.agents.retain(|a| a.id != id);
        self.placed.remove(id);
    }

    /// Per job het totaal aantal geplaatste instanties.
    pub fn placed_counts(&self) -> Result<Map<u32>> {
        let mut out: Map<u32> = Map::new();
        for (_, jobs) in self.placed.iter() {
            for (name, n) in jobs.iter() {
                match out.get_mut(name) {
                    Some(total) => *total = total.saturating_add(*n),
                    None => {
                        out.insert(try_string(name)?, *n)?;
                    }
                }
            }
        }
        Ok(out)
    }

    /// Op welke agents job `name` staat en hoe vaak.
    pub fn placed(&self, name: &str) -> Result<Map<u32>> {
        let mut out = Map::new();
        for (agent, jobs) in self.placed.iter() {
            if let Some(&n) = jobs.get(name)
                && n > 0
            {
                out.insert(try_string(agent)?, n)?;
            }
        }
        Ok(out)
    }

    /// Het aantal instanties van `name` op agent `agent_id`.
    pub(crate) fn placed_on(&self, agent_id: &str, name: &str) -> u32 {
        self.placed
            .get(agent_id)
            .and_then(|jobs| jobs.get(name))
            .copied()
            .unwrap_or(0)
    }

    /// Boekt één instantie van `name` op `agent_id`.
    pub(crate) fn track_placement(&mut self, agent_id: &str, name: &str) -> Result {
        if self.placed.get(agent_id).is_none() {
            self.placed.insert(try_string(agent_id)?, Map::new())?;
        }
        if let Some(jobs) = self.placed.get_mut(agent_id) {
            match jobs.get_mut(name) {
                Some(n) => *n = n.saturating_add(1),
                None => {
                    jobs.insert(try_string(name)?, 1)?;
                }
            }
        }
        Ok(())
    }

    /// Boekt één instantie van `name` op `agent_id` af (niet onder nul).
    pub(crate) fn untrack_one(&mut self, agent_id: &str, name: &str) {
        if let Some(n) = self
            .placed
            .get_mut(agent_id)
            .and_then(|jobs| jobs.get_mut(name))
        {
            *n = n.saturating_sub(1);
        }
    }

    /// Haalt de hele plaatsing van `name` weg en geeft de agents die hem hadden.
    pub(crate) fn take_placement(&mut self, name: &str) -> Result<Vec<Agent>> {
        let mut out = Vec::new();
        for agent in &self.agents {
            if self.placed_on(&agent.id, name) > 0 {
                types::try_push(&mut out, types::TryClone::try_clone(agent)?)?;
            }
        }
        for (_, jobs) in self.placed.iter_mut() {
            jobs.remove(name);
        }
        Ok(out)
    }

    /// Een kopie van de agent met dit id.
    pub(crate) fn agent_copy(&self, id: &str) -> Result<Option<Agent>> {
        match self.agent(id) {
            Some(a) => Ok(Some(types::TryClone::try_clone(a)?)),
            None => Ok(None),
        }
    }

    // De mutaties van de store lopen hierlangs, zodat elke weg de
    // gecommitte staat vies maakt (Go: de dirtyTrackingStore-decorator).

    pub(crate) fn store_put(&mut self, job: Job) -> Result {
        self.store.put(job)?;
        self.dirty = true;
        Ok(())
    }

    pub(crate) fn store_remove(&mut self, name: &str) -> Option<Job> {
        let j = self.store.remove(name);
        if j.is_some() {
            self.dirty = true;
        }
        j
    }

    pub(crate) fn store_set_priority(&mut self, name: &str, p: i64) -> bool {
        let ok = self.store.set_priority(name, p);
        self.dirty |= ok;
        ok
    }

    pub(crate) fn store_set_deploying(&mut self, name: &str, d: bool) -> bool {
        let ok = self.store.set_deploying(name, d);
        self.dirty |= ok;
        ok
    }
}

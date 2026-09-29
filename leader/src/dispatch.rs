//! Plaatsen: round-robin met capaciteit, preemptie op prioriteit, verwijderen.

use alloc::string::String;
use alloc::vec::Vec;

use types::{Agent, Job, Name, TryClone, try_push, try_string};

use crate::{Error, JobStore, Leader, Refusal, Result, RunReply, Transport};

/// De sorteersleutel van een prioriteit: lager is belangrijker, en niet
/// gezet (`None`) komt achteraan.
pub(crate) fn effective_priority(p: Option<i64>) -> i64 {
    p.unwrap_or(i64::MAX)
}

impl<S: JobStore> Leader<S> {
    /// Slaat een job op en plaatst zijn instanties.
    ///
    /// De job wordt altijd opgeslagen, ook als plaatsen faalt: de volgende
    /// reconcile pakt hem op zodra er ruimte is. Tijdens de settle-periode
    /// wordt alleen opgeslagen. `count == -1` is een daemon: één op elke
    /// agent, via dezelfde weg als reconcile.
    pub fn dispatch_job(&mut self, job: Job, net: &mut impl Transport) -> Result {
        if job.name.is_empty() {
            return Err(Error::NameRequired);
        }
        let copy = job.try_clone()?;
        self.store_put(job)?;
        if !self.settled {
            return Ok(());
        }
        if copy.is_daemon() {
            self.reconcile_job(&copy, net)?;
        } else {
            self.dispatch_count(&copy, copy.desired(), net)?;
        }
        self.events.job(&copy.name);
        Ok(())
    }

    /// Plaatst `count` instanties via round-robin (minstens één).
    pub(crate) fn dispatch_count(
        &mut self,
        job: &Job,
        count: usize,
        net: &mut impl Transport,
    ) -> Result {
        let count = count.max(1);
        for i in 0..count {
            if let Err(why) = self.place(job, true, net)? {
                return Err(Error::Dispatch {
                    job: Name::new(&job.name),
                    instance: i + 1,
                    count,
                    why,
                });
            }
        }
        Ok(())
    }

    /// De volgende agent in round-robin-volgorde.
    fn next_agent(&mut self) -> Option<Agent> {
        let n = self.agents.len();
        if n == 0 {
            return None;
        }
        let idx = self.round_robin % n;
        self.round_robin = self.round_robin.wrapping_add(1);
        self.agents.get(idx).and_then(|a| a.try_clone().ok())
    }

    /// Probeert agents tot er één de job aanneemt.
    ///
    /// Eerste ronde: elke agent één keer, round-robin. Tweede ronde (alleen
    /// met `allow_preempt`): op de agents die vol zaten de minst belangrijke
    /// job verdringen die strikt minder belangrijk is.
    ///
    /// Een update plaatst zonder preemptie: de ruimtenood van een update is
    /// boekhouding (oud en nieuw staan even naast elkaar), geen nieuwe vraag,
    /// en een buurman offeren om die overlap te betalen is schade (gemeten
    /// 01-08: een welcome-update verdrong cloudflared).
    ///
    /// De buitenste `Result` is een harde fout (geheugen); de binnenste zegt
    /// of er geplaatst is.
    pub(crate) fn place(
        &mut self,
        job: &Job,
        allow_preempt: bool,
        net: &mut impl Transport,
    ) -> Result<core::result::Result<(), Refusal>> {
        let n = self.agents.len();
        if n == 0 {
            return Ok(Err(Refusal::NoAgents));
        }
        let mut full: Vec<Agent> = Vec::new();
        for _ in 0..n {
            let Some(agent) = self.next_agent() else {
                break;
            };
            match net.run(&agent, job, false) {
                RunReply::Accepted => {
                    self.track_placement(&agent.id, &job.name)?;
                    return Ok(Ok(()));
                }
                RunReply::NoCapacity => try_push(&mut full, agent)?,
                RunReply::AffinityMismatch | RunReply::Rejected(_) | RunReply::Unreachable => {}
            }
        }
        if full.is_empty() {
            return Ok(Err(Refusal::NoneAccepted { tried: n }));
        }
        if !allow_preempt {
            return Ok(Err(Refusal::Full { agents: full.len() }));
        }
        for agent in &full {
            let Some(victim) = self.find_victim(&agent.id, job.priority)? else {
                continue;
            };
            if !net.stop_job(agent, &victim) {
                // De stop faalde en de taken draaien nog: volgende agent.
                continue;
            }
            if let Some(jobs) = self.placed.get_mut(&agent.id) {
                jobs.remove(&victim);
            }
            if net.run(agent, job, false) == RunReply::Accepted {
                self.track_placement(&agent.id, &job.name)?;
                self.events.job(&victim);
                return Ok(Ok(()));
            }
        }
        Ok(Err(Refusal::Full { agents: full.len() }))
    }

    /// De minst belangrijke job op `agent_id` die strikt minder belangrijk is
    /// dan `priority`, of `None`.
    fn find_victim(&self, agent_id: &str, priority: Option<i64>) -> Result<Option<String>> {
        let Some(jobs) = self.placed.get(agent_id) else {
            return Ok(None);
        };
        let mut worst = effective_priority(priority);
        let mut victim: Option<&str> = None;
        for (name, &n) in jobs.iter() {
            if n == 0 {
                continue;
            }
            let Some(j) = self.store.get(name) else {
                continue;
            };
            let ep = effective_priority(j.priority);
            if ep > worst {
                worst = ep;
                victim = Some(name);
            }
        }
        match victim {
            Some(v) => Ok(Some(try_string(v)?)),
            None => Ok(None),
        }
    }

    /// Stopt op een net teruggekeerde agent de jobs die de cluster niet meer nodig heeft.
    ///
    /// Een agent die lang genoeg weg was om uitgezet te worden, komt terug
    /// met de taken die hij liet draaien; de leider plaatste zijn deel
    /// intussen elders, dus nu staat de job boven zijn aantal. Dat overschot
    /// is hoogstens een oude versie en nooit onvervangbaar (er is al een
    /// vervanger, daarom zitten we erboven), dus stoppen op déze agent is
    /// altijd veilig. Zit een job zonder deze agent niet boven zijn aantal
    /// (een echt capaciteitsgat), dan blijven de taken: beschikbaarheid gaat
    /// voor versiezuiverheid. Daemons horen overal en blijven.
    pub(crate) fn trim_returning_agent_surplus(
        &mut self,
        agent_id: &str,
        net: &mut impl Transport,
    ) -> Result {
        let mut redundant: Vec<String> = Vec::new();
        if let Some(jobs) = self.placed.get(agent_id) {
            for (name, &on_agent) in jobs.iter() {
                if on_agent == 0 {
                    continue;
                }
                let Some(job) = self.store.get(name) else {
                    continue;
                };
                if job.is_daemon() {
                    continue;
                }
                let total: u64 = self
                    .placed
                    .iter()
                    .map(|(_, j)| u64::from(j.get(name).copied().unwrap_or(0)))
                    .sum();
                let elsewhere = total.saturating_sub(u64::from(on_agent));
                if elsewhere >= job.desired() as u64 {
                    try_push(&mut redundant, try_string(name)?)?;
                }
            }
        }
        if redundant.is_empty() {
            return Ok(());
        }
        // Eerst uit de boeken, zodat de reconcile hierna de waarheid ziet.
        if let Some(jobs) = self.placed.get_mut(agent_id) {
            for name in &redundant {
                jobs.remove(name);
            }
        }
        let agent = self.agent_copy(agent_id)?;
        for name in &redundant {
            if let Some(a) = &agent {
                net.stop_job(a, name);
            }
            self.events.job(name);
        }
        Ok(())
    }

    /// Boekt een hand-back af: de agent verwijderde de taak omdat hij daar nu
    /// niet past (geen vrije core, geen passende partitie). Dan meteen
    /// reconcilen; misschien past hij elders. Zonder deze afboeking bleef
    /// `placed` voorgoed op 1 staan.
    pub fn mark_unplaced(
        &mut self,
        agent_id: &str,
        name: &str,
        net: &mut impl Transport,
    ) -> Result {
        self.untrack_one(agent_id, name);
        self.reconcile_jobs(net)
    }

    /// Verwijdert een job: uit de store, van elke agent die hem draait, en
    /// reconcilet daarna zodat de vrijgekomen ruimte meteen bruikbaar is.
    ///
    /// Een onbekende naam is geen fout (verwijderen is idempotent).
    pub fn delete_job(&mut self, name: &str, net: &mut impl Transport) -> Result {
        if self.store_remove(name).is_none() {
            return Ok(());
        }
        let agents = self.take_placement(name)?;
        for agent in &agents {
            net.delete_job(agent, name);
        }
        self.events.job(name);
        self.reconcile_jobs(net)
    }

    /// Verplaatst een job naar plek `target` in de prioriteitsvolgorde en
    /// nummert de rest dicht (0..N-1), en reconcilet: een job die omhoog
    /// gaat mag nu verdringen.
    pub fn patch_job_priority(
        &mut self,
        name: &str,
        target: i64,
        net: &mut impl Transport,
    ) -> Result {
        if self.store.get(name).is_none() {
            return Err(Error::NotFound {
                job: Name::new(name),
            });
        }
        let mut order = self.priority_order()?;
        order.retain(|n| n != name);
        let at = usize::try_from(target.max(0))
            .unwrap_or(usize::MAX)
            .min(order.len());
        order.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
        order.insert(at, try_string(name)?);
        for (i, n) in order.iter().enumerate() {
            self.store_set_priority(n, i64::try_from(i).unwrap_or(i64::MAX));
        }
        self.events.job(name);
        self.reconcile_jobs(net)
    }

    /// De jobnamen gesorteerd op (prioriteit, naam).
    pub(crate) fn priority_order(&self) -> Result<Vec<String>> {
        let mut keyed: Vec<(i64, &str)> = Vec::new();
        keyed
            .try_reserve_exact(self.store.jobs().len())
            .map_err(|_| Error::OutOfMemory)?;
        for j in self.store.jobs() {
            keyed.push((effective_priority(j.priority), &j.name));
        }
        keyed.sort_unstable();
        let mut out = Vec::new();
        out.try_reserve_exact(keyed.len())
            .map_err(|_| Error::OutOfMemory)?;
        for (_, n) in keyed {
            out.push(try_string(n)?);
        }
        Ok(out)
    }

    /// Geeft alle jobs unieke, opeenvolgende prioriteiten 0..N-1.
    ///
    /// Alleen de prioriteit wordt herschreven, in de store zelf: een
    /// hele-job-schrijf overschreef in Go het `deploying = false` dat een
    /// update net had gezet (traqqr, 2026-09-08: drie jobs bleven "deploying").
    pub(crate) fn normalize_priorities(&mut self) -> Result {
        let order = self.priority_order()?;
        for (i, name) in order.iter().enumerate() {
            let p = i64::try_from(i).unwrap_or(i64::MAX);
            if self.store.get(name).and_then(|j| j.priority) != Some(p) {
                self.store_set_priority(name, p);
            }
        }
        Ok(())
    }
}

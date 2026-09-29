//! Reconcile, dode agents, de tik, en de taakstatus bij de agents opvragen.
//!
//! Reconcile is een zuivere functie van (gewenste jobs, levende agents,
//! geplaatste tellingen). Hij loopt wanneer een van die drie verandert: een
//! agent sterft, registreert of meldt zich af; een job komt, verandert,
//! verdwijnt of schuift in prioriteit; de settle-periode eindigt. Plus het
//! vangnet elke [`crate::RECONCILE_EVERY_TICKS`] ticks.

use alloc::string::String;
use alloc::vec::Vec;

use types::{Job, Name, Task, Time, TryClone, try_push, try_string};

use crate::{Error, JobStore, Leader, RECONCILE_EVERY_TICKS, Result, RunReply, Transport};

impl<S: JobStore> Leader<S> {
    /// De periodieke tik (Go: elke 10 s). Eindigt de settle-periode, ruimt
    /// dode agents op, en draait elke derde tik een vangnet-reconcile.
    pub fn tick(&mut self, now: Time, net: &mut impl Transport) -> Result {
        if let Some(at) = self.settle_at
            && !self.settled
            && now >= at
        {
            self.settled = true;
            self.settle_at = None;
            self.events.status();
            self.reconcile_jobs(net)?;
        }
        self.check_dead_agents(now, net)?;
        self.ticks = self.ticks.wrapping_add(1);
        if self.settled && self.ticks.is_multiple_of(RECONCILE_EVERY_TICKS) {
            self.reconcile_jobs(net)?;
        }
        Ok(())
    }

    /// Haalt agents weg die langer dan de agent-timeout zwegen, en reconcilet.
    ///
    /// Tijdens de settle-periode niet: dan registreren agents zich nog.
    pub fn check_dead_agents(&mut self, now: Time, net: &mut impl Transport) -> Result {
        if !self.settled {
            return Ok(());
        }
        let mut dead: Vec<String> = Vec::new();
        for a in &self.agents {
            if now.since(a.last_seen) > self.agent_timeout {
                try_push(&mut dead, try_string(&a.id)?)?;
            }
        }
        if dead.is_empty() {
            return Ok(());
        }
        for id in &dead {
            self.remove_agent(id);
            self.events.agent(id);
        }
        self.reconcile_jobs(net)
    }

    /// Zorgt dat elke job zijn aantal instanties heeft, in prioriteitsvolgorde.
    pub fn reconcile_jobs(&mut self, net: &mut impl Transport) -> Result {
        if self.store.jobs().is_empty() || self.agents.is_empty() {
            return Ok(());
        }
        // Eerst de prioriteiten dicht, zodat preemptie unieke waarden ziet.
        self.normalize_priorities()?;
        // Round-robin terug naar het begin: de belangrijkste jobs beginnen
        // op een vaste plek.
        self.round_robin = 0;
        for name in self.priority_order()? {
            let Some(job) = self.store.get(&name) else {
                continue;
            };
            if job.name.is_empty() {
                continue;
            }
            let job = job.try_clone()?;
            // Een job die nu niet past is geen fout van de ronde: hij blijft
            // staan tot het volgende event. Alleen een lege heap stopt de ronde.
            if let Err(Error::OutOfMemory) = self.reconcile_job(&job, net) {
                return Err(Error::OutOfMemory);
            }
        }
        Ok(())
    }

    /// Zorgt dat één job zijn instanties heeft.
    pub(crate) fn reconcile_job(&mut self, job: &Job, net: &mut impl Transport) -> Result {
        if job.is_daemon() {
            return self.reconcile_daemon(job, net);
        }
        let desired = job.desired();
        let total: usize = self
            .agents
            .iter()
            .map(|a| self.placed_on(&a.id, &job.name) as usize)
            .sum();
        if total >= desired {
            return Ok(());
        }
        let r = self.dispatch_count(job, desired - total, net);
        self.events.job(&job.name);
        r
    }

    /// Een daemon hoort op elke agent: stuur hem naar wie hem mist.
    fn reconcile_daemon(&mut self, job: &Job, net: &mut impl Transport) -> Result {
        let mut missing = Vec::new();
        for a in &self.agents {
            if self.placed_on(&a.id, &job.name) == 0 {
                try_push(&mut missing, a.try_clone()?)?;
            }
        }
        let mut dispatched = 0usize;
        for agent in &missing {
            if net.run(agent, job, false) == RunReply::Accepted {
                self.track_placement(&agent.id, &job.name)?;
                dispatched += 1;
            }
        }
        if dispatched > 0 {
            self.events.job(&job.name);
        }
        if dispatched == 0 && !missing.is_empty() {
            return Err(Error::DaemonRejected {
                job: Name::new(&job.name),
            });
        }
        Ok(())
    }

    /// De taken van elke agent die antwoordde, per agent-id.
    pub fn cluster_status(&self, net: &mut impl Transport) -> Result<Vec<(String, Vec<Task>)>> {
        let mut out = Vec::new();
        for a in &self.agents {
            if let Some(tasks) = net.tasks(a) {
                try_push(&mut out, (try_string(&a.id)?, tasks))?;
            }
        }
        Ok(out)
    }

    /// De taken van één job, alleen gevraagd aan de agents waar hij geplaatst is.
    ///
    /// Geeft per agent die antwoordde zijn taken van deze job; een onbekende
    /// job geeft een lege lijst.
    pub fn job_status(
        &self,
        name: &str,
        net: &mut impl Transport,
    ) -> Result<Vec<(String, Vec<Task>)>> {
        let mut out = Vec::new();
        if self.store.get(name).is_none() {
            return Ok(out);
        }
        for a in &self.agents {
            if self.placed_on(&a.id, name) == 0 {
                continue;
            }
            if let Some(mut tasks) = net.tasks(a) {
                tasks.retain(|t| t.job_name == name);
                try_push(&mut out, (try_string(&a.id)?, tasks))?;
            }
        }
        Ok(out)
    }
}

//! Updates van een bestaande job: rolling, recreate, blue-green.
//!
//! Elke policy slaat de nieuwe definitie op met `deploying = true`; die vlag
//! gaat pas uit als de uitrol slaagt. Faalt hij, of sterft de leider
//! halverwege, dan blijft hij staan en rijdt mee in de snapshot: de
//! eerlijke waarheid in plaats van een valse "gezond".

use alloc::vec::Vec;

use types::{Job, Name, TryClone, UpdatePolicy, try_push, try_string};

use crate::{
    Error, JobStore, Leader, ROLLING_UPDATE_DELAY, Refusal, Result, RunReply, TaskRef, Transport,
};

impl<S: JobStore> Leader<S> {
    /// Werkt een bestaande job bij volgens zijn `update_policy` (standaard rolling).
    ///
    /// Zonder eigen prioriteit houdt de job de oude. Draait synchroon, zodat
    /// de API een echte status kan geven. Een vaste poort met een expliciete
    /// rolling of blue-green wordt geweigerd voordat er iets verandert
    /// ([`Job::check_rollable`]): die uitrol zou nooit slagen. Zonder policy
    /// is een vaste poort recreate ([`Job::policy`]).
    pub fn update_job(&mut self, mut job: Job, net: &mut impl Transport) -> Result {
        let Some(old) = self.store.get(&job.name) else {
            return Err(Error::NotFound {
                job: Name::new(&job.name),
            });
        };
        job.check_rollable().map_err(Error::Json)?;
        if job.priority.is_none() {
            job.priority = old.priority;
        }
        job.deploying = true;
        let name = try_string(&job.name)?;
        let r = match job.policy() {
            UpdatePolicy::Rolling => self.update_rolling(job, net),
            UpdatePolicy::Recreate => self.update_recreate(job, net),
            UpdatePolicy::BlueGreen => self.update_blue_green(job, net),
        };
        if r.is_ok() {
            self.store_set_deploying(&name, false);
        }
        self.normalize_priorities()?;
        r
    }

    /// Rolling: per oude taak eerst een nieuwe plaatsen (binnen het aantal),
    /// dan de oude stoppen. De reconcile daarna vult een schaalvergroting aan.
    fn update_rolling(&mut self, job: Job, net: &mut impl Transport) -> Result {
        let count = job.desired();
        let old_tasks = self.snapshot_job_tasks(&job.name, net)?;
        let copy = job.try_clone()?;
        self.store_put(job)?;
        let job = copy;

        for (i, old) in old_tasks.iter().enumerate() {
            let mut replaced = false;
            if i < count {
                // Nooit via preemptie: de tijdelijke oud-plus-nieuw-overlap
                // van een update mag geen buurman kosten (01-08).
                let mut r = self.place(&job, false, net)?;
                if let Err(Refusal::Full { .. }) = r {
                    // Geen ruimte voor oud en nieuw naast elkaar: vervang
                    // ter plekke op de agent van het oude exemplaar. Die stopt
                    // zijn voorganger pas na een geslaagde toelating; weigert
                    // hij, dan draait het oude gewoon door.
                    if let Some(agent) = self.agent_copy(&old.agent_id)?
                        && net.run(&agent, &job, true) == RunReply::Accepted
                    {
                        // Oud is door nieuw verruild: plaatsing per saldo gelijk.
                        replaced = true;
                        r = Ok(());
                    }
                }
                if let Err(why) = r {
                    return Err(Error::Rolling {
                        job: Name::new(&job.name),
                        instance: i + 1,
                        why,
                    });
                }
            }
            if !replaced {
                self.stop_old_task(&job.name, old, net)?;
            }
            self.events.job(&job.name);
            if i + 1 < count {
                net.pause(ROLLING_UPDATE_DELAY);
            }
        }
        self.reconcile_jobs(net)
    }

    /// Recreate: alles stoppen, dan de nieuwe definitie gewoon dispatchen.
    fn update_recreate(&mut self, job: Job, net: &mut impl Transport) -> Result {
        let agents = self.take_placement(&job.name)?;
        for agent in &agents {
            net.stop_job(agent, &job.name);
        }
        self.dispatch_job(job, net)
    }

    /// Blue-green: alles nieuw naast het oude, dan alles oude stoppen.
    ///
    /// Zonder preemptie: blue-green eist oud en nieuw naast elkaar, en als
    /// dat niet past hoort de update te falen mét het oude intact.
    fn update_blue_green(&mut self, job: Job, net: &mut impl Transport) -> Result {
        let old_tasks = self.snapshot_job_tasks(&job.name, net)?;
        let count = job.desired();
        let copy = job.try_clone()?;
        self.store_put(job)?;
        let job = copy;
        for i in 0..count {
            if let Err(why) = self.place(&job, false, net)? {
                return Err(Error::BlueGreen {
                    job: Name::new(&job.name),
                    instance: i + 1,
                    count,
                    why,
                });
            }
        }
        for old in &old_tasks {
            self.stop_old_task(&job.name, old, net)?;
        }
        self.events.job(&job.name);
        Ok(())
    }

    /// Stopt één oud exemplaar en boekt het af, op één plek, zodat de
    /// volgorde (eerst stoppen of eerst plaatsen) de boekhouding niet kan
    /// laten verlopen.
    fn stop_old_task(&mut self, name: &str, old: &TaskRef, net: &mut impl Transport) -> Result {
        if let Some(agent) = self.agent_copy(&old.agent_id)? {
            net.stop_task(&agent, &old.task_id);
            self.untrack_one(&old.agent_id, name);
        }
        Ok(())
    }

    /// Alle lopende taken van een job, gevraagd aan de agents waar hij staat.
    pub fn snapshot_job_tasks(&self, name: &str, net: &mut impl Transport) -> Result<Vec<TaskRef>> {
        let mut refs = Vec::new();
        for (agent_id, tasks) in self.job_status(name, net)? {
            for t in tasks {
                try_push(
                    &mut refs,
                    TaskRef {
                        agent_id: agent_id.try_clone()?,
                        task_id: t.id,
                    },
                )?;
            }
        }
        Ok(refs)
    }
}

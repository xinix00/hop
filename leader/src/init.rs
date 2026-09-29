//! Init-jobs: een cluster die met niets opstart krijgt een basis (18-07).
//!
//! "Schone boot" is: geen gecommitte snapshot (of geen store geconfigureerd)
//! én een lege job-store. Alleen dan worden de init-jobs uit de config
//! gezaaid, via de gewone dispatch-weg, zodat prioriteiten, persistentie en
//! reconcile doen alsof een operator ze indiende. Een onbereikbare store is
//! géén schone boot: nooit zaaien op een opslagfout, anders zet een
//! S3-storing de cluster terug naar zijn basis. Die keuze maakt de adapter
//! (hij roept [`Leader::seed_init_jobs`] alleen na `Ok(false)` van
//! [`Leader::load_committed_state`]).

use alloc::vec::Vec;

use types::json::Value;
use types::{Job, try_push};

use crate::{Error, JobStore, Leader, Result, Transport};

/// Leest de ruwe init-jobs uit de config als jobs, strikt.
///
/// Onbekende velden, een ontbrekende naam of een job zonder iets om te
/// draaien zijn config-fouten: luid bij het opstarten is beter dan een stil
/// genegeerde typefout.
pub fn decode_init_jobs(specs: &[Value]) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    jobs.try_reserve_exact(specs.len())
        .map_err(|_| Error::OutOfMemory)?;
    for (index, spec) in specs.iter().enumerate() {
        let wrap = |cause| Error::InitJob { index, cause };
        let mut job = Job::from_value(spec, true).map_err(wrap)?;
        if job.name.is_empty() {
            return Err(wrap(types::Error::Invalid {
                field: types::Name::new("name"),
                why: "name required",
            }));
        }
        job.apply_hop_shorthand();
        job.check_runnable().map_err(wrap)?;
        try_push(&mut jobs, job)?;
    }
    Ok(jobs)
}

impl<S: JobStore> Leader<S> {
    /// Zaait de init-jobs. Een naam die al bestaat wordt overgeslagen: een
    /// zaadje overschrijft nooit staat van de operator. Een job die nu niet
    /// past is geen fout; de reconcile probeert het later opnieuw.
    pub fn seed_init_jobs(&mut self, jobs: Vec<Job>, net: &mut impl Transport) -> Result {
        for mut job in jobs {
            if self.store.get(&job.name).is_some() {
                continue;
            }
            if job.priority.is_none() {
                job.priority = Some(self.next_priority());
            }
            if let Err(Error::OutOfMemory) = self.dispatch_job(job, net) {
                return Err(Error::OutOfMemory);
            }
        }
        Ok(())
    }
}

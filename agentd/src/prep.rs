//! De voorbereiding van taken: een vaste pool werkthreads voor het trage deel van een start.
//!
//! Een start op de host heeft een traag deel (de taakmap, een artifact
//! downloaden en uitpakken, een docker pull: seconden tot minuten) en een
//! snel deel (het proces spawnen). Het trage deel draait hier, zodat de
//! eigenaar intussen verzoeken en heartbeats blijft beantwoorden; het snelle
//! deel doet de eigenaar zelf ([`runner::host::HostRunner::launch`]).
//!
//! Elke werker heeft zijn eigen rij en bezit alleen de opdracht die hij
//! krijgt. De eigenaar houdt bij wie bezig is en houdt de rest in een eigen
//! wachtrij: een gedeelde rij voor meerdere werkers zou een slot vragen
//! (handboek §1), en de eigenaar weet toch al wie er vrij is.

use std::collections::VecDeque;
use std::sync::mpsc::{self, Sender};
use std::time::{Duration, Instant};

use runner::host::{HostConfig, TaskSpec};
use types::Driver;

use crate::msg::Msg;

/// Hoeveel starts tegelijk hun trage deel doen (Go: 4 downloads tegelijk).
pub(crate) const WORKERS: usize = 4;

/// Hoe vaak een download zijn voortgang meldt.
const PROGRESS_EVERY: Duration = Duration::from_millis(500);

/// Eén opdracht.
#[derive(Debug)]
pub(crate) struct PrepJob {
    /// De taak.
    pub(crate) task_id: String,
    /// De runner.
    pub(crate) driver: Driver,
    /// Wat er gestart wordt.
    pub(crate) spec: Box<TaskSpec>,
}

/// De pool, zoals de eigenaar hem ziet.
#[derive(Debug)]
pub(crate) struct Prep {
    workers: Vec<Sender<PrepJob>>,
    busy: Vec<bool>,
    queue: VecDeque<PrepJob>,
}

impl Prep {
    /// Start [`WORKERS`] werkers; `cfg` bouwt de runner-config van elke werker.
    pub(crate) fn spawn(
        cfg: &dyn Fn() -> HostConfig,
        owner: &Sender<Msg>,
    ) -> std::io::Result<Self> {
        let mut workers = Vec::new();
        for i in 0..WORKERS {
            let (tx, rx) = mpsc::channel::<PrepJob>();
            let owner = owner.clone();
            let host = cfg();
            std::thread::Builder::new()
                .name(format!("prep-{i}"))
                .spawn(move || {
                    for job in rx {
                        let msg = work(i, &host, job, &owner);
                        if owner.send(msg).is_err() {
                            return;
                        }
                    }
                })?;
            workers.push(tx);
        }
        Ok(Self {
            busy: vec![false; workers.len()],
            workers,
            queue: VecDeque::new(),
        })
    }

    /// Geeft de opdracht aan een vrije werker, of zet hem in de rij.
    pub(crate) fn submit(&mut self, job: PrepJob) {
        self.queue.push_back(job);
        self.dispatch();
    }

    /// Werker `i` is klaar: de volgende uit de rij.
    pub(crate) fn done(&mut self, i: usize) {
        if let Some(b) = self.busy.get_mut(i) {
            *b = false;
        }
        self.dispatch();
    }

    /// Haalt de wachtende opdracht van een taak weg (gestopt voor zijn start).
    pub(crate) fn cancel(&mut self, task_id: &str) {
        self.queue.retain(|j| j.task_id != task_id);
    }

    fn dispatch(&mut self) {
        while let Some(i) = self.busy.iter().position(|b| !b) {
            let Some(job) = self.queue.pop_front() else {
                return;
            };
            let Some(w) = self.workers.get(i) else {
                return;
            };
            match w.send(job) {
                Ok(()) => {
                    if let Some(b) = self.busy.get_mut(i) {
                        *b = true;
                    }
                }
                // Een werker die weg is, krijgt niets meer; zijn opdracht is
                // verloren, en de monitor van de agent ziet een taak die
                // nooit startte.
                Err(_) => {
                    if let Some(b) = self.busy.get_mut(i) {
                        *b = true;
                    }
                }
            }
        }
    }
}

/// Het werk van één opdracht: de voorbereiding, met voortgang onderweg.
fn work(i: usize, host: &HostConfig, job: PrepJob, owner: &Sender<Msg>) -> Msg {
    let mut last = Instant::now();
    let id = job.task_id.clone();
    let mut progress = |got: u64, total: Option<u64>| {
        if last.elapsed() >= PROGRESS_EVERY {
            last = Instant::now();
            let _ = owner.send(Msg::Progress {
                task_id: id.clone(),
                got,
                total: total.unwrap_or(0),
            });
        }
    };
    let result = runner::host::prepare(host, job.driver, &job.spec, &mut progress)
        .map_err(|e| e.to_string());
    Msg::Prepared {
        worker: i,
        task_id: job.task_id,
        driver: job.driver,
        spec: job.spec,
        result,
    }
}

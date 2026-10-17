//! De watcher op de host: twee threads.
//!
//! - De stroom bezit de SSE-verbinding naar de agent (hoplib's
//!   `host::Agent::events`, met herverbinden en backoff) en geeft elke
//!   gebeurtenis door. Hij blokkeert in zijn lees; daarom een eigen thread.
//! - De watcher bezit de cache ([`hoplb::Watcher`]) en de wacht
//!   ([`Pending`]): hij wacht op een gebeurtenis of op het einde van de
//!   wacht, vraagt de agent, en geeft de nieuwe tabel aan de eigenaar.
//!
//! Go deed hetzelfde met een goroutine voor de regels en een `select` over
//! de regels en de debounce-timer.

use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender};
use std::time::{Duration, Instant};

use core::ops::ControlFlow;

use hoplb::Watcher;
use hoplb::watch::{Pending, RETRY, Sync};
use hoplib::host::Agent;
use hoplib::{Client, Event};

use crate::log;
use crate::owner::Msg;

/// Hoe lang de watcher slaapt als er niets wacht; een gebeurtenis wekt hem
/// eerder.
const IDLE_WAIT: Duration = Duration::from_secs(3600);

/// Start de stroom en de watcher.
pub(crate) fn spawn(
    client: Client,
    watcher: Watcher,
    owner: SyncSender<Msg>,
) -> std::io::Result<()> {
    let (tx, rx) = mpsc::channel();
    let stream_client = client.clone();
    std::thread::Builder::new()
        .name("events".into())
        .spawn(move || events(stream_client, &tx))?;
    std::thread::Builder::new()
        .name("watcher".into())
        .spawn(move || run(&Agent::new(client), watcher, &rx, &owner))?;
    Ok(())
}

/// De stroom: elke gebeurtenis naar de watcher, tot die weg is.
fn events(client: Client, to: &Sender<Event>) {
    let base = client.base.clone();
    let agent = Agent::new(client);
    agent.events(
        |e| {
            if e.kind == "ping" {
                log!("SSE connected to {base}/v1/events, seeding routes");
            }
            match to.send(e.clone()) {
                Ok(()) => ControlFlow::Continue(()),
                Err(_) => ControlFlow::Break(()),
            }
        },
        |err, retry_in| log!("SSE disconnected: {err}, reconnecting in {retry_in:?}"),
    );
}

/// De lus van de watcher.
fn run(agent: &Agent, mut w: Watcher, from: &Receiver<Event>, owner: &SyncSender<Msg>) {
    let epoch = Instant::now();
    let now = || u64::try_from(epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let mut pending = Pending::new();
    loop {
        let wait = pending
            .due()
            .map_or(IDLE_WAIT, |d| Duration::from_nanos(d.saturating_sub(now())));
        match from.recv_timeout(wait) {
            Ok(e) if e.is_resync() => {
                // Een nieuwe verbinding of gemiste meldingen: alles opnieuw,
                // meteen (Go: `w.sync()` direct na het verbinden).
                pending.clear();
                if !full(agent, &mut w, owner) {
                    pending.retry(now(), RETRY);
                }
            }
            Ok(e) => {
                if let Some((job, is_full)) = w.classify(&e.data)
                    && let Err(err) = pending.push(&job, is_full, now())
                {
                    log!("pending: {err}");
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        match pending.take(now()) {
            None => {}
            Some(Sync::Full) => {
                if !full(agent, &mut w, owner) {
                    pending.retry(now(), RETRY);
                }
            }
            Some(Sync::Jobs(jobs)) => {
                if !jobs_sync(agent, &mut w, &jobs) && !full(agent, &mut w, owner) {
                    pending.retry(now(), RETRY);
                    continue;
                }
                publish(&w, owner);
            }
        }
    }
}

/// Alles opnieuw: agents, jobs, taken (Go: `sync`); `false` als het faalde.
fn full(agent: &Agent, w: &mut Watcher, owner: &SyncSender<Msg>) -> bool {
    let agents = match agent.agents() {
        Ok(a) => a,
        Err(e) => {
            log!("Failed to fetch agents: {e}");
            return false;
        }
    };
    let jobs = match agent.jobs() {
        Ok(j) => j,
        Err(e) => {
            log!("Failed to fetch jobs: {e}");
            return false;
        }
    };
    let tasks = match agent.tasks() {
        Ok(t) => t,
        Err(e) => {
            log!("Failed to fetch tasks: {e}");
            return false;
        }
    };
    if let Err(e) = w.apply_full(&agents, &jobs, tasks) {
        log!("sync: {e}");
        return false;
    }
    publish(w, owner);
    true
}

/// De taken van elke job in `jobs` (Go: `syncJob`); `false` bij de eerste
/// fout (Go viel dan terug op een volledige synchronisatie).
fn jobs_sync(agent: &Agent, w: &mut Watcher, jobs: &[String]) -> bool {
    for job in jobs {
        let st = match agent.job_status(job) {
            Ok(st) => st,
            Err(e) => {
                log!("Failed to fetch job status for {job}: {e}");
                return false;
            }
        };
        if let Err(e) = w.apply_job(job, st) {
            log!("sync {job}: {e}");
            return false;
        }
    }
    true
}

/// Bouwt de tabel en geeft hem aan de eigenaar.
fn publish(w: &Watcher, owner: &SyncSender<Msg>) {
    match w.build_routes() {
        Ok(t) => {
            log!(
                "Updated routes: {} patterns, {} total backends",
                t.len(),
                t.backends()
            );
            let _ = owner.send(Msg::Routes(t));
        }
        Err(e) => log!("build routes: {e}"),
    }
}

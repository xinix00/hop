//! De eigenaar: één thread die de metrics en de geldende routetabel bezit.
//!
//! Alles wat die twee aanraakt, is een [`Msg`] in zijn rij; hij houdt ze als
//! gewone `&mut` (handboek §1). Een werker die een kopie van de tabel wil,
//! vraagt hem ([`Msg::Snapshot`]) als [`GENERATION`] verder is dan zijn
//! eigen kopie: publiceren is de generatie ophogen, ophalen doet de lezer
//! zelf (handboek §2.1, "publiceren en intrekken").

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};

use hoplb::{Metrics, Record, RouteTable};

use crate::log;

/// Hoeveel berichten er in de rij van de eigenaar passen. Vol is voor een
/// meting droppen (en tellen), voor al het andere wachten: een verzoek mag
/// niet wachten op de boekhouding, een tabel mag niet verloren gaan.
pub(crate) const INBOX: usize = 4096;

/// De generatie van de geldende tabel; de eigenaar hoogt hem op na elke
/// nieuwe tabel. Een teller, dus een atomic (handboek §1.3).
pub(crate) static GENERATION: AtomicU64 = AtomicU64::new(0);

/// Metingen die wegvielen omdat de rij vol was.
static DROPPED: AtomicU64 = AtomicU64::new(0);

/// Wat de eigenaar krijgt.
pub(crate) enum Msg {
    /// Eén afgehandeld verzoek.
    Record(Record),
    /// Een nieuwe tabel van de watcher.
    Routes(RouteTable),
    /// Een werker wil een kopie van de geldende tabel, met zijn generatie.
    Snapshot(SyncSender<(u64, RouteTable)>),
    /// De admin-poort wil de Prometheus-tekst.
    Scrape(SyncSender<String>),
}

/// Stuurt een meting; vol is droppen en tellen, de eerste keer luid.
pub(crate) fn record(to: &SyncSender<Msg>, r: Record) {
    if let Err(TrySendError::Full(_)) = to.try_send(Msg::Record(r))
        && DROPPED.fetch_add(1, Relaxed) == 0
    {
        log!("owner inbox full ({INBOX}): dropping metrics records HOPLB_METRICS_DROP");
    }
}

/// De lus van de eigenaar; komt terug als elke zender weg is.
pub(crate) fn run(inbox: Receiver<Msg>) {
    let mut metrics = Metrics::new();
    let mut routes = RouteTable::new();
    let mut folded = 0;
    while let Ok(msg) = inbox.recv() {
        match msg {
            Msg::Record(r) => {
                if let Err(e) = metrics.record(&r) {
                    log!("metrics: {e}");
                }
                if metrics.folded() > folded {
                    if folded == 0 {
                        log!(
                            "metrics: over {} series, folding new ones into {:?} HOPLB_METRICS_FOLD",
                            hoplb::metrics::MAX_SERIES,
                            hoplb::metrics::OVERFLOW_DOMAIN
                        );
                    }
                    folded = metrics.folded();
                }
            }
            Msg::Routes(t) => {
                routes = t;
                GENERATION.fetch_add(1, Relaxed);
            }
            Msg::Snapshot(reply) => {
                if let Ok(copy) = routes.try_clone() {
                    let _ = reply.send((GENERATION.load(Relaxed), copy));
                }
            }
            Msg::Scrape(reply) => {
                let mut out = String::new();
                if metrics.render(&mut out).is_ok() {
                    let _ = reply.send(out);
                }
            }
        }
    }
}

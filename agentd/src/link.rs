//! De uitgaande aanroepen die de eigenaar niet mogen ophouden: register, heartbeat, notify, en de probes.
//!
//! Twee threads, elk met zijn eigen rij:
//!
//! - de link-thread doet de aanroepen bij de leader. Een register laat de
//!   leader reconcilen, en die kan daarbij deze node om een `/run` vragen:
//!   wachtte de eigenaar zelf op het antwoord, dan wachtte hij op zichzelf.
//!   Het antwoord gaat als [`Msg::Link`] terug naar [`agent::Election`];
//! - de probe-thread doet de gezondheidscontroles (HTTP, TCP, bestand),
//!   elk met zijn eigen termijn, en meldt [`Msg::Probe`].

use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc::{self, Sender};
use std::time::{Duration, UNIX_EPOCH};

use agent::{LinkError, Outcome, Probe, Request as LinkRequest};
use hostnet::{Call, Http};
use types::{Nanos, Time};

use crate::msg::Msg;

/// Hoe lang een register of heartbeat mag duren (Go: 5 s).
const LINK_TIMEOUT: Duration = Duration::from_secs(5);

/// Een aanroep bij een leader.
#[derive(Debug)]
pub(crate) enum LinkJob {
    /// Register of heartbeat: het antwoord gaat terug naar de verkiezing.
    Election {
        /// De aanroep, zoals de verkiezing hem gaf.
        req: LinkRequest,
        /// `POST` naar deze URL.
        url: String,
        /// De JSON-body.
        body: String,
    },
    /// Een melding (`POST /v1/notify`); het antwoord doet er niet toe.
    Notify {
        /// De URL.
        url: String,
        /// De JSON-body.
        body: String,
    },
}

/// Een gezondheidscontrole voor de probe-thread.
#[derive(Debug)]
pub(crate) struct ProbeJob {
    /// De taak.
    pub(crate) task_id: String,
    /// Wat er gecontroleerd wordt.
    pub(crate) probe: Probe,
}

/// Een ondertekende POST; de status, of `None` bij een transportfout.
fn post(http: &Http, key: &[u8], url: &str, body: &str) -> Option<u16> {
    let sig = auth::sign_call(key, "POST", url, body.as_bytes())
        .map(|s| String::from_utf8_lossy(&s).into_owned());
    let mut headers: Vec<(&str, &str)> = vec![("Content-Type", "application/json")];
    if let Some(s) = &sig {
        headers.push((auth::AUTH_HEADER, s));
    }
    let call = Call {
        method: "POST",
        url,
        headers: &headers,
        body: Some(body.as_bytes()),
        timeout: LINK_TIMEOUT,
    };
    http.request(&call, 1 << 20).ok().map(|r| r.status)
}

/// Start de link-thread met sleutel `key`.
pub(crate) fn spawn_link(key: Vec<u8>, owner: Sender<Msg>) -> std::io::Result<Sender<LinkJob>> {
    let (tx, rx) = mpsc::channel::<LinkJob>();
    std::thread::Builder::new()
        .name(String::from("link"))
        .spawn(move || {
            let http = Http::new();
            for job in rx {
                match job {
                    LinkJob::Election { req, url, body } => {
                        let result = match post(&http, &key, &url, &body) {
                            Some(200..=299) => Ok(()),
                            Some(404) => Err(LinkError::NotRegistered),
                            _ => Err(LinkError::Failed),
                        };
                        if owner.send(Msg::Link { req, result }).is_err() {
                            return;
                        }
                    }
                    LinkJob::Notify { url, body } => {
                        // Een melding die niet aankomt, ziet de leader bij zijn
                        // volgende reconcile alsnog; niets om op te wachten.
                        let _ = post(&http, &key, &url, &body);
                    }
                }
            }
        })?;
    Ok(tx)
}

/// Een duur in nanoseconden als `Duration`, met een ondergrens van 100 ms.
fn timeout(ns: Nanos) -> Duration {
    Duration::from_nanos(ns).max(Duration::from_millis(100))
}

/// Voert één controle uit.
fn check(http: &Http, probe: &Probe) -> Outcome {
    match probe {
        Probe::Http {
            port,
            path,
            timeout: t,
        } => {
            let url = format!("http://127.0.0.1:{port}{path}");
            let call = Call::get(&url, timeout(*t));
            Outcome::Http(http.request(&call, 64 << 10).ok().map(|r| r.status))
        }
        Probe::Tcp { port, timeout: t } => {
            let addr = SocketAddr::from(([127, 0, 0, 1], *port));
            Outcome::Tcp(TcpStream::connect_timeout(&addr, timeout(*t)).is_ok())
        }
        Probe::File { path } => Outcome::File(
            std::fs::metadata(path)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| Time(u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))),
        ),
    }
}

/// Start de probe-thread.
pub(crate) fn spawn_probes(owner: Sender<Msg>) -> std::io::Result<Sender<ProbeJob>> {
    let (tx, rx) = mpsc::channel::<ProbeJob>();
    std::thread::Builder::new()
        .name(String::from("probe"))
        .spawn(move || {
            let http = Http::new();
            for job in rx {
                let outcome = check(&http, &job.probe);
                let msg = Msg::Probe {
                    task_id: job.task_id,
                    outcome,
                };
                if owner.send(msg).is_err() {
                    return;
                }
            }
        })?;
    Ok(tx)
}

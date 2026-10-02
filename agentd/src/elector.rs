//! De leader-verkiezing op de host: een lease-thread die de opslag bezit, en de [`agent::Discoverer`] van de eigenaar.
//!
//! Go's `AsyncDiscoverer` (`internal/agentloop/async.go` op
//! github.com/xinix00/hop, tag v1.0.7), met één
//! eigenaar per staat in plaats van een ops-kanaal naar een goroutine:
//!
//! - De lease-thread ([`spawn`]) bezit de [`discovery::Discovery`] (de
//!   handle en generatie van onze lease) en de backend (S3, hoplockserver of
//!   in geheugen). Hij voert één opdracht tegelijk uit en meldt de uitkomst
//!   als [`Msg::Lease`].
//! - De eigenaar bezit een [`Elector`] (de gedeelde, `no_std`-versie uit
//!   `agent`, dezelfde als op HopOS): het laatste antwoord van de opslag en
//!   de timer van onze eigen lease. [`agent::Election`] vraagt hem, en elke
//!   vraag antwoordt meteen uit wat de laatst voltooide aanroep zei.
//!
//! Waarom: op een trage opslag (Bunny Storage: 4 tot 20 s per PUT, gemeten
//! 08-09-2026) hield een synchrone renew de heartbeat van de leader zelf
//! voorbij de dood-drempel. Hier is de lease de timer: een renew die
//! slaagt verlengt `expires_at`, en "nog steeds van mij" is een vergelijking
//! met de klok, wat de opslag nu ook doet.

use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{SystemTime, UNIX_EPOCH};

use agent::{LeaseOp, LeaseOps, LeaseReply};
use discovery::{Backend, Discovery};

use crate::msg::Msg;

/// Nu in milliseconden sinds 1970, de klok van de lease.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Eén synchrone claim, vóór er threads zijn (de boot: is de lock vrij, dan
/// leidt deze node meteen, zonder de takeover-drempel van vier tikken).
pub(crate) fn claim_now<B: Backend>(disc: &mut Discovery, backend: &mut B) -> bool {
    disc.try_become_leader(Some(backend), now_ms())
}

/// Start de lease-thread; hij bezit `disc` en `backend` tot het kanaal dichtgaat.
///
/// Een fout van de opslag (403, DNS, een termijn) komt één keer op de log,
/// en opnieuw pas als hij verandert: een storing van een uur is één regel.
pub(crate) fn spawn(
    mut disc: Discovery,
    mut backend: store::Lease,
    owner: Sender<Msg>,
) -> std::io::Result<Sender<LeaseOp>> {
    let (tx, rx): (Sender<LeaseOp>, Receiver<LeaseOp>) = mpsc::channel();
    std::thread::Builder::new()
        .name(String::from("lease"))
        .spawn(move || {
            let mut said = String::new();
            for op in rx {
                let now = now_ms();
                let reply = match op {
                    LeaseOp::Read => {
                        let (leader, ok) = disc.leader_state(Some(&mut backend), now);
                        LeaseReply::Read { leader, ok }
                    }
                    LeaseOp::Claim => {
                        LeaseReply::Claimed(disc.try_become_leader(Some(&mut backend), now))
                    }
                    LeaseOp::Renew => {
                        let (renewed, displaced) = disc.renew_lease(Some(&mut backend), now);
                        LeaseReply::Renewed(renewed, displaced)
                    }
                    LeaseOp::Release => {
                        disc.release_leadership(Some(&mut backend));
                        continue;
                    }
                };
                match backend.last_error() {
                    Some(e) => {
                        let now_said = e.to_string();
                        if now_said != said {
                            eprintln!("hop: lease store: {now_said} HOP_LEASE_STORE");
                            said = now_said;
                        }
                    }
                    None => said.clear(),
                }
                if owner.send(Msg::Lease(reply)).is_err() {
                    return;
                }
            }
        })?;
    Ok(tx)
}

/// De rij naar de lease-thread, als [`LeaseOps`] van de gedeelde [`agent::Elector`].
#[derive(Debug)]
pub(crate) struct Ops(pub(crate) Sender<LeaseOp>);

impl LeaseOps for Ops {
    fn send(&mut self, op: LeaseOp) -> bool {
        self.0.send(op).is_ok()
    }
}

/// De elector van de daemon: de gedeelde van `agent`, over de lease-thread.
pub(crate) type Elector = agent::Elector<Ops>;

/// Een elector over de lease-thread `ops`, met leases van `ttl_ms`.
///
/// `holding` is de uitkomst van de boot-claim ([`claim_now`]).
pub(crate) fn elector(ops: Sender<LeaseOp>, ttl_ms: u64, holding: bool) -> Elector {
    agent::Elector::new(Ops(ops), now_ms, ttl_ms, holding)
}

//! De leader-verkiezing op de host: een lease-thread die de opslag bezit, en de [`Discoverer`] van de eigenaar.
//!
//! Go's `AsyncDiscoverer` (`OLD/internal/agentloop/async.go`), met één
//! eigenaar per staat in plaats van een ops-kanaal naar een goroutine:
//!
//! - De lease-thread ([`spawn`]) bezit de [`discovery::Discovery`] (de
//!   handle en generatie van onze lease) en de backend (S3, hoplockserver of
//!   in geheugen). Hij voert één opdracht tegelijk uit en meldt de uitkomst
//!   als [`Msg::Lease`].
//! - De eigenaar bezit een [`Elector`]: het laatste antwoord van de opslag
//!   en de timer van onze eigen lease. [`agent::Election`] vraagt hem, en
//!   elke vraag antwoordt meteen uit wat de laatst voltooide aanroep zei.
//!
//! Waarom: op een trage opslag (Bunny Storage: 4 tot 20 s per PUT, gemeten
//! 08-09-2026) hield een synchrone renew de heartbeat van de leader zelf
//! voorbij de dood-drempel. Hier is de lease de timer: een renew die
//! slaagt verlengt `expires_at`, en "nog steeds van mij" is een vergelijking
//! met de klok, wat de opslag nu ook doet.

use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{SystemTime, UNIX_EPOCH};

use agent::Discoverer;
use discovery::{Backend, Discovery};
use types::Time;
use types::time::MILLISECOND;

use crate::msg::{LeaseReply, Msg};

/// Een opdracht aan de lease-thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LeaseOp {
    /// Wie leidt er?
    Read,
    /// Probeer de lease te claimen.
    Claim,
    /// Vernieuw onze lease.
    Renew,
    /// Laat de lease los (naar beste kunnen).
    Release,
}

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

/// De verkiezingskant van de eigenaar: het laatste antwoord en onze lease-timer.
///
/// # Invariants
///
/// Hoogstens één opdracht van elke soort staat uit (`reading`, `claiming`,
/// `renewing`); een antwoord zet de vlag terug.
#[derive(Debug)]
pub(crate) struct Elector {
    ops: Sender<LeaseOp>,
    ttl_ms: u64,
    leader: Option<String>,
    leader_at: Option<u64>,
    store_ok: bool,
    reading: bool,
    holding: bool,
    expires_at: u64,
    renewing: bool,
    next_renew: u64,
    claiming: bool,
    claimed: bool,
    displaced: bool,
    /// Voor de log: wat de eigenaar nog moet melden.
    pub(crate) notes: Vec<String>,
}

impl Elector {
    /// Een elector over de lease-thread `ops`, met leases van `ttl_ms`.
    ///
    /// `holding` is de uitkomst van de boot-claim ([`claim_now`]).
    pub(crate) fn new(ops: Sender<LeaseOp>, ttl_ms: u64, holding: bool) -> Self {
        let now = now_ms();
        Self {
            ops,
            ttl_ms,
            leader: None,
            leader_at: None,
            store_ok: false,
            reading: false,
            holding,
            expires_at: if holding {
                now.saturating_add(ttl_ms)
            } else {
                0
            },
            renewing: false,
            next_renew: now.saturating_add(ttl_ms / 3),
            claiming: false,
            // Een boot-claim die raak was, meldt de eerste vraag van de
            // verkiezing als gewonnen (Go: `TryBecomeLeaderSync`).
            claimed: holding,
            displaced: false,
            notes: Vec::new(),
        }
    }

    fn send(&self, op: LeaseOp) -> bool {
        self.ops.send(op).is_ok()
    }

    /// De renew-cadans: zolang we houden, één renew tegelijk, elke TTL/3.
    pub(crate) fn tick(&mut self, now: u64) {
        if self.holding && !self.renewing && now >= self.next_renew {
            self.next_renew = now.saturating_add(self.ttl_ms / 3);
            self.renewing = self.send(LeaseOp::Renew);
        }
    }

    /// Verwerkt een antwoord van de lease-thread.
    pub(crate) fn on_reply(&mut self, r: LeaseReply) {
        let now = now_ms();
        match r {
            LeaseReply::Read { leader, ok } => {
                self.reading = false;
                self.leader = leader;
                self.leader_at = Some(now);
                self.store_ok = ok;
            }
            LeaseReply::Claimed(got) => {
                self.claiming = false;
                if got {
                    self.holding = true;
                    self.claimed = true;
                    self.expires_at = now.saturating_add(self.ttl_ms);
                    self.next_renew = now.saturating_add(self.ttl_ms / 3);
                    // Wij leiden; de lus kent zijn eigen adres.
                    self.leader = None;
                    self.leader_at = None;
                }
            }
            LeaseReply::Renewed(renewed, displaced) => {
                self.renewing = false;
                if !self.holding {
                    // Losgelaten terwijl de renew onderweg was.
                    return;
                }
                if renewed {
                    self.expires_at = now.saturating_add(self.ttl_ms);
                } else if displaced {
                    self.holding = false;
                    self.displaced = true;
                    self.notes.push(String::from(
                        "hop: lease renew: another owner holds the lease, stepping down HOP_LEASE_DISPLACED",
                    ));
                } else {
                    let left = self.expires_at.saturating_sub(now) / 1000;
                    self.notes.push(format!(
                        "hop: lease renew failed; lease valid for another {left} s HOP_LEASE_RENEW_FAIL"
                    ));
                }
            }
        }
    }
}

impl Discoverer for Elector {
    fn get_leader(&mut self) -> Option<String> {
        let now = now_ms();
        let fresh = self
            .leader_at
            .is_some_and(|at| now.saturating_sub(at) < self.ttl_ms);
        if fresh {
            return self.leader.clone();
        }
        if !self.reading {
            self.reading = self.send(LeaseOp::Read);
        }
        None
    }

    fn try_become_leader(&mut self) -> bool {
        if self.claimed {
            self.claimed = false;
            return true;
        }
        if self.holding || self.claiming {
            return false;
        }
        self.claiming = self.send(LeaseOp::Claim);
        false
    }

    fn renew_lease(&mut self) -> (bool, bool) {
        if self.displaced {
            self.displaced = false;
            return (false, true);
        }
        (self.holding && now_ms() < self.expires_at, false)
    }

    fn release_leadership(&mut self) {
        self.holding = false;
        self.claimed = false;
        self.displaced = false;
        self.expires_at = 0;
        self.send(LeaseOp::Release);
    }

    fn store_reachable(&self) -> bool {
        self.leader_at.is_some() && self.store_ok
    }

    fn invalidate(&mut self) {
        self.leader_at = None;
    }

    fn lease_expires_at(&self) -> Option<Time> {
        self.holding
            .then(|| Time(self.expires_at.saturating_mul(MILLISECOND)))
    }
}

#[cfg(test)]
mod tests {
    //! De tests van `OLD/internal/agentloop/async_test.go` voor zover ze de
    //! elector zelf raken: antwoorden komen uit het laatste antwoord, en er
    //! staat hoogstens één opdracht van elke soort uit.

    use super::*;

    fn elector(holding: bool) -> (Elector, Receiver<LeaseOp>) {
        let (tx, rx) = mpsc::channel();
        (Elector::new(tx, 30_000, holding), rx)
    }

    // TestAsyncGetLeaderNeverBlocks: de eerste vraag geeft leeg en start één
    // lees; het antwoord komt bij een latere vraag.
    #[test]
    fn get_leader_asks_once_and_answers_from_the_last_read() {
        let (mut e, rx) = elector(false);
        assert_eq!(e.get_leader(), None);
        assert_eq!(e.get_leader(), None);
        assert_eq!(rx.try_iter().collect::<Vec<_>>(), vec![LeaseOp::Read]);
        e.on_reply(LeaseReply::Read {
            leader: Some("10.0.0.2:9080".into()),
            ok: true,
        });
        assert_eq!(e.get_leader().as_deref(), Some("10.0.0.2:9080"));
        assert!(e.store_reachable());
        e.invalidate();
        assert_eq!(e.get_leader(), None);
        assert!(!e.store_reachable());
        assert_eq!(rx.try_iter().collect::<Vec<_>>(), vec![LeaseOp::Read]);
    }

    // TestAsyncTryBecomeLeaderReportsOnce.
    #[test]
    fn claim_is_reported_once_on_the_next_ask() {
        let (mut e, rx) = elector(false);
        assert!(!e.try_become_leader());
        assert!(!e.try_become_leader());
        assert_eq!(rx.try_iter().collect::<Vec<_>>(), vec![LeaseOp::Claim]);
        e.on_reply(LeaseReply::Claimed(true));
        assert!(e.try_become_leader());
        assert!(!e.try_become_leader());
        assert_eq!(e.renew_lease(), (true, false));
        assert!(e.lease_expires_at().is_some());
    }

    #[test]
    fn boot_claim_is_reported_once() {
        let (mut e, rx) = elector(true);
        assert!(e.try_become_leader());
        assert!(!e.try_become_leader());
        assert!(rx.try_iter().next().is_none());
    }

    // TestAsyncDisplacedReportedOnce.
    #[test]
    fn displaced_renew_steps_down_once() {
        let (mut e, rx) = elector(true);
        e.tick(now_ms() + 11_000);
        assert_eq!(rx.try_iter().collect::<Vec<_>>(), vec![LeaseOp::Renew]);
        // Nog een tik terwijl de renew loopt: geen tweede.
        e.tick(now_ms() + 22_000);
        assert!(rx.try_iter().next().is_none());
        e.on_reply(LeaseReply::Renewed(false, true));
        assert_eq!(e.renew_lease(), (false, true));
        assert_eq!(e.renew_lease(), (false, false));
        assert!(e.lease_expires_at().is_none());
    }

    #[test]
    fn unreachable_renew_keeps_the_timer() {
        let (mut e, _rx) = elector(true);
        e.tick(now_ms() + 11_000);
        e.on_reply(LeaseReply::Renewed(false, false));
        // De lease van de boot-claim geldt nog: blijven leiden.
        assert_eq!(e.renew_lease(), (true, false));
        assert_eq!(e.notes.len(), 1);
    }

    // TestAsyncReleaseIsImmediate: loslaten vergeet de lease meteen; het
    // verwijderen loopt op de lease-thread.
    #[test]
    fn release_forgets_the_lease_and_ignores_a_late_renew() {
        let (mut e, rx) = elector(true);
        e.tick(now_ms() + 11_000);
        e.release_leadership();
        assert_eq!(
            rx.try_iter().collect::<Vec<_>>(),
            vec![LeaseOp::Renew, LeaseOp::Release]
        );
        e.on_reply(LeaseReply::Renewed(true, false));
        assert_eq!(e.renew_lease(), (false, false));
    }

    // TestAsyncRenewAnswersFromTheLeaseTimer: terwijl een renew hangt,
    // antwoordt de elector uit de lease-timer: van ons tot de lease afloopt,
    // daarna "niet vernieuwd" (tijdelijk, niet verdrongen); een renew die
    // alsnog lukt, zet de timer opnieuw.
    #[test]
    fn renew_answers_from_the_lease_timer() {
        let (mut e, rx) = elector(true);
        e.tick(now_ms() + 11_000);
        assert_eq!(rx.try_iter().collect::<Vec<_>>(), vec![LeaseOp::Renew]);
        assert_eq!(e.renew_lease(), (true, false));
        // De TTL is om zonder antwoord van de opslag.
        e.expires_at = now_ms().saturating_sub(1);
        assert_eq!(e.renew_lease(), (false, false));
        e.on_reply(LeaseReply::Renewed(true, false));
        assert_eq!(e.renew_lease(), (true, false));
    }

    // TestAsyncInvalidateForcesARead: na `invalidate` vraagt de volgende
    // `get_leader` de opslag opnieuw, ook binnen de TTL.
    #[test]
    fn invalidate_forces_a_read() {
        let (mut e, rx) = elector(false);
        e.get_leader();
        e.on_reply(LeaseReply::Read {
            leader: Some("10.0.0.2:9080".into()),
            ok: true,
        });
        assert_eq!(e.get_leader().as_deref(), Some("10.0.0.2:9080"));
        e.invalidate();
        assert_eq!(e.get_leader(), None);
        assert_eq!(
            rx.try_iter().collect::<Vec<_>>(),
            vec![LeaseOp::Read, LeaseOp::Read]
        );
        // De lease verliep: het verse antwoord is "niemand".
        e.on_reply(LeaseReply::Read {
            leader: None,
            ok: true,
        });
        assert_eq!(e.get_leader(), None);
        assert!(rx.try_iter().next().is_none());
    }

    // TestAsyncStoreReachableFollowsTheRead: onwaar vóór elke lees, onwaar
    // als de opslag niet antwoordde, waar als hij antwoordde (ook zonder
    // leider).
    #[test]
    fn store_reachable_follows_the_read() {
        let (mut e, _rx) = elector(false);
        assert!(!e.store_reachable());
        e.get_leader();
        e.on_reply(LeaseReply::Read {
            leader: None,
            ok: false,
        });
        assert!(!e.store_reachable());
        e.invalidate();
        e.get_leader();
        e.on_reply(LeaseReply::Read {
            leader: None,
            ok: true,
        });
        assert!(e.store_reachable());
    }
}

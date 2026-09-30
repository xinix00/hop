//! De verkiezingskant van de eigenaar: het laatste antwoord van de lease-opslag en de timer van onze eigen lease.
//!
//! Go's `AsyncDiscoverer` (`OLD/internal/agentloop/async.go`), met één
//! eigenaar per staat in plaats van een ops-kanaal naar een goroutine. De
//! opslag zelf (S3, hoplockserver, in geheugen) is van een andere eigenaar:
//! een thread op de host, een taak op HopOS. De [`Elector`] stuurt die
//! eigenaar een [`LeaseOp`] via zijn [`LeaseOps`], en krijgt het antwoord
//! later als [`LeaseReply`] terug ([`Elector::on_reply`]). [`crate::Election`]
//! vraagt de elector als [`Discoverer`], en elke vraag antwoordt meteen uit
//! wat de laatst voltooide aanroep zei.
//!
//! Waarom: op een trage opslag (Bunny Storage: 4 tot 20 s per PUT, gemeten
//! 08-09-2026) hield een synchrone renew de heartbeat van de leader zelf
//! voorbij de dood-drempel. Hier is de lease de timer: een renew die slaagt
//! verlengt `expires_at`, en "nog steeds van mij" is een vergelijking met de
//! klok, wat de opslag nu ook doet.
//!
//! Sans-I/O en `no_std`, zodat de daemon (`agentd`) en de HopOS-bewoner
//! (`agentd-hopos`) dezelfde elector draaien: alleen de rij naar de
//! opslag-eigenaar ([`LeaseOps`]) en de klok verschillen.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use types::Time;
use types::time::MILLISECOND;

use crate::election::Discoverer;

/// Een opdracht aan de eigenaar van de lease-opslag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseOp {
    /// Wie leidt er?
    Read,
    /// Probeer de lease te claimen.
    Claim,
    /// Vernieuw onze lease.
    Renew,
    /// Laat de lease los (naar beste kunnen).
    Release,
}

/// Wat de eigenaar van de lease-opslag terugmeldt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseReply {
    /// De leider volgens de opslag, en of de opslag antwoordde.
    Read {
        /// Het adres van de leider, als er een levende lease is.
        leader: Option<String>,
        /// Of de opslag antwoordde.
        ok: bool,
    },
    /// Een claim: `true` als wij de lease nu houden.
    Claimed(bool),
    /// Een vernieuwing: `(renewed, displaced)`.
    Renewed(bool, bool),
}

/// De rij naar de eigenaar van de lease-opslag.
pub trait LeaseOps {
    /// Zet `op` in de rij; `false` als de eigenaar weg is of de rij vol.
    fn send(&mut self, op: LeaseOp) -> bool;
}

/// De verkiezingskant van de eigenaar: het laatste antwoord en onze lease-timer.
///
/// # Invariants
///
/// Hoogstens één opdracht van elke soort staat uit (`reading`, `claiming`,
/// `renewing`); een antwoord zet de vlag terug.
#[derive(Debug)]
pub struct Elector<O> {
    ops: O,
    /// Nu in milliseconden op de wandklok: de lease vergelijkt tijden van
    /// verschillende nodes, dus een monotone klok zou hier liegen.
    clock: fn() -> u64,
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
    notes: Vec<String>,
}

impl<O: LeaseOps> Elector<O> {
    /// Een elector over de rij `ops`, met de klok `clock` (ms) en leases van `ttl_ms`.
    ///
    /// `holding` is de uitkomst van de boot-claim: is de lock vrij, dan leidt
    /// deze node meteen, zonder de takeover-drempel van vier tikken.
    pub fn new(ops: O, clock: fn() -> u64, ttl_ms: u64, holding: bool) -> Self {
        let now = clock();
        Self {
            ops,
            clock,
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

    fn send(&mut self, op: LeaseOp) -> bool {
        self.ops.send(op)
    }

    /// De rij naar de opslag-eigenaar (tests en diagnose).
    pub fn ops(&self) -> &O {
        &self.ops
    }

    /// De rij naar de opslag-eigenaar, muteerbaar (tests).
    pub fn ops_mut(&mut self) -> &mut O {
        &mut self.ops
    }

    /// Of we denken de lease te houden.
    pub fn is_holding(&self) -> bool {
        self.holding
    }

    /// De regels die de eigenaar nog op de log moet zetten.
    pub fn take_notes(&mut self) -> Vec<String> {
        core::mem::take(&mut self.notes)
    }

    fn note(&mut self, line: String) {
        if self.notes.try_reserve(1).is_ok() {
            self.notes.push(line);
        }
    }

    /// De renew-cadans: zolang we houden, één renew tegelijk, elke TTL/3.
    pub fn tick(&mut self, now: u64) {
        if self.holding && !self.renewing && now >= self.next_renew {
            self.next_renew = now.saturating_add(self.ttl_ms / 3);
            self.renewing = self.send(LeaseOp::Renew);
        }
    }

    /// Verwerkt een antwoord van de opslag-eigenaar.
    pub fn on_reply(&mut self, r: LeaseReply) {
        let now = (self.clock)();
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
                    self.note(String::from(
                        "hop: lease renew: another owner holds the lease, stepping down HOP_LEASE_DISPLACED",
                    ));
                } else {
                    let left = self.expires_at.saturating_sub(now) / 1000;
                    self.note(format!(
                        "hop: lease renew failed; lease valid for another {left} s HOP_LEASE_RENEW_FAIL"
                    ));
                }
            }
        }
    }
}

impl<O: LeaseOps> Discoverer for Elector<O> {
    fn get_leader(&mut self) -> Option<String> {
        let now = (self.clock)();
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
        (self.holding && (self.clock)() < self.expires_at, false)
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
    use core::sync::atomic::{AtomicU64, Ordering::Relaxed};

    /// Een klok die de test zet; elke test die hem verzet, zet hem terug.
    static NOW: AtomicU64 = AtomicU64::new(1_000_000);

    fn now() -> u64 {
        NOW.load(Relaxed)
    }

    /// De rij als lijst: wat de elector stuurde.
    #[derive(Debug, Default)]
    struct Sent(Vec<LeaseOp>);

    impl LeaseOps for Sent {
        fn send(&mut self, op: LeaseOp) -> bool {
            self.0.push(op);
            true
        }
    }

    fn elector(holding: bool) -> Elector<Sent> {
        Elector::new(Sent::default(), now, 30_000, holding)
    }

    fn sent(e: &mut Elector<Sent>) -> Vec<LeaseOp> {
        core::mem::take(&mut e.ops_mut().0)
    }

    // TestAsyncGetLeaderNeverBlocks: de eerste vraag geeft leeg en start één
    // lees; het antwoord komt bij een latere vraag.
    #[test]
    fn get_leader_asks_once_and_answers_from_the_last_read() {
        let mut e = elector(false);
        assert_eq!(e.get_leader(), None);
        assert_eq!(e.get_leader(), None);
        assert_eq!(sent(&mut e), vec![LeaseOp::Read]);
        e.on_reply(LeaseReply::Read {
            leader: Some("10.0.0.2:9080".into()),
            ok: true,
        });
        assert_eq!(e.get_leader().as_deref(), Some("10.0.0.2:9080"));
        assert!(e.store_reachable());
        e.invalidate();
        assert_eq!(e.get_leader(), None);
        assert!(!e.store_reachable());
        assert_eq!(sent(&mut e), vec![LeaseOp::Read]);
    }

    // TestAsyncTryBecomeLeaderReportsOnce.
    #[test]
    fn claim_is_reported_once_on_the_next_ask() {
        let mut e = elector(false);
        assert!(!e.try_become_leader());
        assert!(!e.try_become_leader());
        assert_eq!(sent(&mut e), vec![LeaseOp::Claim]);
        e.on_reply(LeaseReply::Claimed(true));
        assert!(e.try_become_leader());
        assert!(!e.try_become_leader());
        assert_eq!(e.renew_lease(), (true, false));
        assert!(e.lease_expires_at().is_some());
    }

    #[test]
    fn boot_claim_is_reported_once() {
        let mut e = elector(true);
        assert!(e.try_become_leader());
        assert!(!e.try_become_leader());
        assert!(sent(&mut e).is_empty());
    }

    // TestAsyncDisplacedReportedOnce.
    #[test]
    fn displaced_renew_steps_down_once() {
        let mut e = elector(true);
        e.tick(now() + 11_000);
        assert_eq!(sent(&mut e), vec![LeaseOp::Renew]);
        // Nog een tik terwijl de renew loopt: geen tweede.
        e.tick(now() + 22_000);
        assert!(sent(&mut e).is_empty());
        e.on_reply(LeaseReply::Renewed(false, true));
        assert_eq!(e.renew_lease(), (false, true));
        assert_eq!(e.renew_lease(), (false, false));
        assert!(e.lease_expires_at().is_none());
        assert_eq!(e.take_notes().len(), 1);
    }

    #[test]
    fn unreachable_renew_keeps_the_timer() {
        let mut e = elector(true);
        e.tick(now() + 11_000);
        e.on_reply(LeaseReply::Renewed(false, false));
        // De lease van de boot-claim geldt nog: blijven leiden.
        assert_eq!(e.renew_lease(), (true, false));
        assert_eq!(e.take_notes().len(), 1);
    }

    // TestAsyncReleaseIsImmediate: loslaten vergeet de lease meteen; het
    // verwijderen loopt bij de opslag-eigenaar.
    #[test]
    fn release_forgets_the_lease_and_ignores_a_late_renew() {
        let mut e = elector(true);
        e.tick(now() + 11_000);
        e.release_leadership();
        assert_eq!(sent(&mut e), vec![LeaseOp::Renew, LeaseOp::Release]);
        e.on_reply(LeaseReply::Renewed(true, false));
        assert_eq!(e.renew_lease(), (false, false));
    }

    // TestAsyncRenewAnswersFromTheLeaseTimer: terwijl een renew hangt,
    // antwoordt de elector uit de lease-timer: van ons tot de lease afloopt,
    // daarna "niet vernieuwd" (tijdelijk, niet verdrongen); een renew die
    // alsnog lukt, zet de timer opnieuw.
    #[test]
    fn renew_answers_from_the_lease_timer() {
        let mut e = elector(true);
        e.tick(now() + 11_000);
        assert_eq!(sent(&mut e), vec![LeaseOp::Renew]);
        assert_eq!(e.renew_lease(), (true, false));
        // De TTL is om zonder antwoord van de opslag.
        e.expires_at = now().saturating_sub(1);
        assert_eq!(e.renew_lease(), (false, false));
        e.on_reply(LeaseReply::Renewed(true, false));
        assert_eq!(e.renew_lease(), (true, false));
    }

    // TestAsyncInvalidateForcesARead: na `invalidate` vraagt de volgende
    // `get_leader` de opslag opnieuw, ook binnen de TTL.
    #[test]
    fn invalidate_forces_a_read() {
        let mut e = elector(false);
        e.get_leader();
        e.on_reply(LeaseReply::Read {
            leader: Some("10.0.0.2:9080".into()),
            ok: true,
        });
        assert_eq!(e.get_leader().as_deref(), Some("10.0.0.2:9080"));
        e.invalidate();
        assert_eq!(e.get_leader(), None);
        assert_eq!(sent(&mut e), vec![LeaseOp::Read, LeaseOp::Read]);
        // De lease verliep: het verse antwoord is "niemand".
        e.on_reply(LeaseReply::Read {
            leader: None,
            ok: true,
        });
        assert_eq!(e.get_leader(), None);
        assert!(sent(&mut e).is_empty());
    }

    // TestAsyncStoreReachableFollowsTheRead: onwaar vóór elke lees, onwaar
    // als de opslag niet antwoordde, waar als hij antwoordde (ook zonder
    // leider).
    #[test]
    fn store_reachable_follows_the_read() {
        let mut e = elector(false);
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

    // Een vraag die de opslag-eigenaar niet bereikt (weg of vol), staat
    // niet uit: de volgende vraag probeert het opnieuw.
    #[test]
    fn a_refused_send_does_not_count_as_asked() {
        #[derive(Debug, Default)]
        struct Gone(u32);
        impl LeaseOps for Gone {
            fn send(&mut self, _op: LeaseOp) -> bool {
                self.0 += 1;
                false
            }
        }
        let mut e = Elector::new(Gone::default(), now, 30_000, false);
        assert_eq!(e.get_leader(), None);
        assert_eq!(e.get_leader(), None);
        assert!(!e.try_become_leader());
        assert!(!e.try_become_leader());
        assert_eq!(e.ops().0, 4);
    }
}

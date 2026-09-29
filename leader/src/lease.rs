//! De leader-lease: wie er leidt, bewezen met compare-and-swap op één record.
//!
//! Deze module bezit het protocol (claimen, vernieuwen, loslaten, lezen),
//! niet de opslag. De opslag is een [`LeaseStore`]: een blob met een
//! ondoorzichtige handle (bij S3 de ETag) waarop elke schrijf een
//! voorwaarde zet. Wederzijdse uitsluiting zit helemaal in die voorwaarde:
//! geen quorum, geen log. De echte implementatie over S3 of hoplockserver
//! komt later; [`MemLease`] is de nep voor tests en standalone.
//!
//! Tijd komt binnen als parameter; deze module leest geen klok.

use alloc::string::String;

use types::{Nanos, Time, TryClone};

/// Het leaserecord in de opslag.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LeaseState {
    /// Hoogt op bij elke leiderswissel; strikt stijgend over de levensduur
    /// van de sleutel, en dus bruikbaar als fencing-token.
    pub generation: u64,
    /// Tot wanneer de lease geldt als hij niet vernieuwd wordt.
    pub expires_at: Time,
    /// Wie hem houdt (`ip:port`); informatief, de uitsluiting hangt er niet van af.
    pub owner: String,
}

/// Waarom een opslagoperatie niet lukte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseError {
    /// Er is geen record.
    NoLease,
    /// De voorwaarde klopte niet: iemand anders schreef sinds onze lezing.
    Held,
    /// De opslag was niet bereikbaar (netwerk, timeout).
    Unreachable,
    /// De heap was op.
    OutOfMemory,
}

/// De opslag onder de lease: lezen, en schrijven of wissen op voorwaarde.
///
/// Implementaties garanderen dat `write` en `delete` lineariseerbaar zijn:
/// van twee gelijktijdige schrijvers op dezelfde vorige handle wint er
/// hoogstens één.
pub trait LeaseStore {
    /// Het record en de handle die er nu bij hoort, of [`LeaseError::NoLease`].
    fn read(&mut self) -> Result<(LeaseState, String), LeaseError>;

    /// Schrijft `state`. Een lege `prev` eist dat er geen record is; anders
    /// moet de opgeslagen handle gelijk zijn aan `prev`. Geeft de nieuwe handle.
    fn write(&mut self, prev: &str, state: &LeaseState) -> Result<String, LeaseError>;

    /// Wist het record, maar alleen als de handle nog klopt.
    fn delete(&mut self, handle: &str) -> Result<(), LeaseError>;
}

/// De uitkomst van een vernieuwing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Renewal {
    /// Vernieuwd: we leiden nog.
    Renewed,
    /// De opslag meldt een andere eigenaar: nu aftreden.
    Displaced,
    /// De opslag was niet bereikbaar: een haperende verbinding. De leider mag
    /// doorgaan zolang hij zijn agents ziet, want niemand anders kan de lease
    /// pakken zolang de opslag ook voor hen onbereikbaar is.
    Unreachable,
}

/// De claim van deze node op de lease: wie we zijn en welke handle we houden.
#[derive(Debug)]
pub struct Claim {
    owner: String,
    ttl: Nanos,
    handle: String,
    generation: u64,
}

impl Claim {
    /// Een claim voor `owner` (het geadverteerde `ip:port`) met leases van `ttl`.
    pub fn new(owner: String, ttl: Nanos) -> Self {
        Self {
            owner,
            ttl,
            handle: String::new(),
            generation: 0,
        }
    }

    /// Wie we zijn.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Eén poging om de lease te claimen. Drie wegen naar succes: aanmaken
    /// als er niets is, verversen als we hem al houden, of overnemen als hij
    /// verlopen is.
    pub fn try_become_leader(&mut self, store: &mut impl LeaseStore, now: Time) -> bool {
        self.try_claim(store, now).is_ok()
    }

    fn try_claim(&mut self, store: &mut impl LeaseStore, now: Time) -> Result<(), LeaseError> {
        match store.read() {
            Err(LeaseError::NoLease) => self.refresh(store, "", 1, now),
            Err(e) => Err(e),
            Ok((state, handle)) if state.owner == self.owner => {
                self.refresh(store, &handle, state.generation, now)
            }
            Ok((state, handle)) if now > state.expires_at => {
                self.refresh(store, &handle, state.generation.saturating_add(1), now)
            }
            Ok(_) => Err(LeaseError::Held),
        }
    }

    /// Vernieuwt de lease op de handle die we houden: één voorwaardelijke
    /// schrijf, geen lezing. De CAS is het bewijs; op een trage opslag
    /// kost elke aanroep seconden (Bunny, 2026-09-08: 4 tot 20 s per PUT).
    pub fn renew(&mut self, store: &mut impl LeaseStore, now: Time) -> Renewal {
        let r = if self.handle.is_empty() {
            // Nog geen handle (vers proces): de volle weg.
            self.try_claim(store, now)
        } else {
            let (handle, generation) = (core::mem::take(&mut self.handle), self.generation);
            let r = self.refresh(store, &handle, generation, now);
            if r.is_err() && self.handle.is_empty() {
                self.handle = handle;
            }
            r
        };
        match r {
            Ok(()) => Renewal::Renewed,
            Err(LeaseError::Held) => Renewal::Displaced,
            Err(_) => Renewal::Unreachable,
        }
    }

    /// Laat de lease los (best effort), zodat een ander meteen kan
    /// overnemen in plaats van de TTL uit te zitten.
    pub fn release(&mut self, store: &mut impl LeaseStore) {
        let handle = core::mem::take(&mut self.handle);
        if !handle.is_empty() {
            // Best effort: mislukt het, dan verloopt de lease vanzelf.
            let _ = store.delete(&handle);
        }
    }

    /// De huidige leider, en of de opslag antwoordde.
    ///
    /// De twee lege antwoorden betekenen het tegenovergestelde voor een agent
    /// die zijn leider kwijt is: "niemand leidt" (niemand plaatst onze taken
    /// opnieuw; houden) en "onbereikbaar" (misschien zijn wij de geïsoleerde;
    /// de fail-safe geldt).
    pub fn leader_state(&self, store: &mut impl LeaseStore, now: Time) -> (Option<String>, bool) {
        match store.read() {
            Err(LeaseError::NoLease) => (None, true),
            Err(_) => (None, false),
            Ok((state, _)) if now > state.expires_at => (None, true),
            Ok((state, _)) => (Some(state.owner), true),
        }
    }

    /// Of wij volgens de opslag de leider zijn.
    pub fn is_leader(&self, store: &mut impl LeaseStore, now: Time) -> bool {
        matches!(self.leader_state(store, now), (Some(o), _) if o == self.owner)
    }

    fn refresh(
        &mut self,
        store: &mut impl LeaseStore,
        prev: &str,
        generation: u64,
        now: Time,
    ) -> Result<(), LeaseError> {
        let state = LeaseState {
            generation,
            expires_at: Time(now.0.saturating_add(self.ttl)),
            owner: self
                .owner
                .try_clone()
                .map_err(|_| LeaseError::OutOfMemory)?,
        };
        self.handle = store.write(prev, &state)?;
        self.generation = generation;
        Ok(())
    }
}

/// Een lease in geheugen: de nep voor tests en standalone.
#[derive(Debug, Default)]
pub struct MemLease {
    state: Option<(LeaseState, String)>,
    seq: u64,
    /// Hoe vaak er gelezen is (voor de test dat vernieuwen niet leest).
    pub reads: u64,
}

impl MemLease {
    /// Een lege opslag.
    pub fn new() -> Self {
        Self::default()
    }

    fn next_handle(&mut self) -> Result<String, LeaseError> {
        self.seq += 1;
        let mut s = String::new();
        let mut n = self.seq;
        let mut digits = [0u8; 20];
        let mut len = 0;
        while n > 0 || len == 0 {
            if let Some(d) = digits.get_mut(len) {
                *d = b'0' + (n % 10) as u8;
            }
            n /= 10;
            len += 1;
        }
        s.try_reserve(len).map_err(|_| LeaseError::OutOfMemory)?;
        for d in digits.iter().take(len).rev() {
            s.push(char::from(*d));
        }
        Ok(s)
    }
}

impl LeaseStore for MemLease {
    fn read(&mut self) -> Result<(LeaseState, String), LeaseError> {
        self.reads += 1;
        match &self.state {
            None => Err(LeaseError::NoLease),
            Some((s, h)) => {
                let s = LeaseState {
                    generation: s.generation,
                    expires_at: s.expires_at,
                    owner: s.owner.try_clone().map_err(|_| LeaseError::OutOfMemory)?,
                };
                Ok((s, h.try_clone().map_err(|_| LeaseError::OutOfMemory)?))
            }
        }
    }

    fn write(&mut self, prev: &str, state: &LeaseState) -> Result<String, LeaseError> {
        match (&self.state, prev.is_empty()) {
            (Some(_), true) | (None, false) => return Err(LeaseError::Held),
            (Some((_, h)), false) if h != prev => return Err(LeaseError::Held),
            _ => {}
        }
        let handle = self.next_handle()?;
        let copy = LeaseState {
            generation: state.generation,
            expires_at: state.expires_at,
            owner: state
                .owner
                .try_clone()
                .map_err(|_| LeaseError::OutOfMemory)?,
        };
        let ret = handle.try_clone().map_err(|_| LeaseError::OutOfMemory)?;
        self.state = Some((copy, handle));
        Ok(ret)
    }

    fn delete(&mut self, handle: &str) -> Result<(), LeaseError> {
        match &self.state {
            None => Err(LeaseError::NoLease),
            Some((_, h)) if h != handle => Err(LeaseError::Held),
            Some(_) => {
                self.state = None;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! De lease-tests van `OLD/internal/discovery/discovery_test.go` en
    //! `hoplock/mem`. De klok is hier een getal.

    use super::*;
    use alloc::string::ToString;
    use types::time::{HOUR, SECOND};

    const NOW: Time = Time(1_000_000 * SECOND);

    fn claim(owner: &str, ttl: Nanos) -> Claim {
        Claim::new(owner.to_string(), ttl)
    }

    /// Een opslag die nooit antwoordt: een geïsoleerde node tijdens een storing.
    struct Unreachable;

    impl LeaseStore for Unreachable {
        fn read(&mut self) -> Result<(LeaseState, String), LeaseError> {
            Err(LeaseError::Unreachable)
        }
        fn write(&mut self, _: &str, _: &LeaseState) -> Result<String, LeaseError> {
            Err(LeaseError::Unreachable)
        }
        fn delete(&mut self, _: &str) -> Result<(), LeaseError> {
            Err(LeaseError::Unreachable)
        }
    }

    // De split-brain-wacht: verdrongen is aftreden, onbereikbaar is doorgaan.
    #[test]
    fn renew_lease_distinguishes_displaced_from_unreachable() {
        let mut store = MemLease::new();
        let mut a = claim("10.0.0.1:8080", 30 * SECOND);
        let mut b = claim("10.0.0.2:8080", 30 * SECOND);
        assert!(a.try_become_leader(&mut store, NOW));
        assert_eq!(b.renew(&mut store, NOW), Renewal::Displaced);
        assert_eq!(a.renew(&mut store, NOW), Renewal::Renewed);
        let mut c = claim("10.0.0.3:8080", 30 * SECOND);
        assert_eq!(c.renew(&mut Unreachable, NOW), Renewal::Unreachable);
    }

    #[test]
    fn try_become_leader_creates() {
        let mut store = MemLease::new();
        let mut d = claim("192.168.1.10:8080", 30 * SECOND);
        assert!(d.try_become_leader(&mut store, NOW));
        assert_eq!(
            d.leader_state(&mut store, NOW).0.as_deref(),
            Some("192.168.1.10:8080")
        );
        assert!(d.is_leader(&mut store, NOW));
    }

    #[test]
    fn try_become_leader_denied_when_held_by_other() {
        let mut store = MemLease::new();
        let mut other = claim("192.168.1.20:8080", 30 * SECOND);
        assert!(other.try_become_leader(&mut store, NOW));
        let mut mine = claim("192.168.1.10:8080", 30 * SECOND);
        assert!(!mine.try_become_leader(&mut store, NOW));
        assert_eq!(
            mine.leader_state(&mut store, NOW).0.as_deref(),
            Some("192.168.1.20:8080")
        );
    }

    #[test]
    fn try_become_leader_takes_over_expired() {
        let mut store = MemLease::new();
        let mut other = claim("192.168.1.20:8080", 10 * SECOND);
        assert!(other.try_become_leader(&mut store, NOW));
        let later = Time(NOW.0 + HOUR);
        let mut mine = claim("192.168.1.10:8080", 10 * SECOND);
        assert!(mine.try_become_leader(&mut store, later));
        assert_eq!(
            mine.leader_state(&mut store, later).0.as_deref(),
            Some("192.168.1.10:8080")
        );
    }

    #[test]
    fn renew_keeps_handle() {
        let mut store = MemLease::new();
        let mut d = claim("192.168.1.10:8080", 30 * SECOND);
        assert!(d.try_become_leader(&mut store, NOW));
        for _ in 0..3 {
            assert_eq!(d.renew(&mut store, NOW), Renewal::Renewed);
        }
    }

    #[test]
    fn release_allows_takeover() {
        let mut store = MemLease::new();
        let mut a = claim("192.168.1.10:8080", 30 * SECOND);
        assert!(a.try_become_leader(&mut store, NOW));
        a.release(&mut store);
        let mut b = claim("192.168.1.20:8080", 30 * SECOND);
        assert!(b.try_become_leader(&mut store, NOW));
        assert_eq!(
            b.leader_state(&mut store, NOW).0.as_deref(),
            Some("192.168.1.20:8080")
        );
    }

    #[test]
    fn generation_monotonic() {
        let mut store = MemLease::new();
        let mut a = claim("a:1", 10 * SECOND);
        assert!(a.try_become_leader(&mut store, NOW));
        assert_eq!(store.read().unwrap().0.generation, 1);
        assert_eq!(a.renew(&mut store, NOW), Renewal::Renewed);
        assert_eq!(store.read().unwrap().0.generation, 1);
        let later = Time(NOW.0 + HOUR);
        let mut b = claim("b:1", 10 * SECOND);
        assert!(b.try_become_leader(&mut store, later));
        assert_eq!(store.read().unwrap().0.generation, 2);
    }

    #[test]
    fn leader_state_separates_no_leader_from_unreachable() {
        let d = claim("10.0.0.1:8080", 30 * SECOND);
        assert_eq!(d.leader_state(&mut MemLease::new(), NOW), (None, true));
        assert_eq!(d.leader_state(&mut Unreachable, NOW), (None, false));
    }

    #[test]
    fn renew_lease_does_not_read() {
        let mut store = MemLease::new();
        let mut d = claim("10.0.0.1:8080", 30 * SECOND);
        assert!(d.try_become_leader(&mut store, NOW));
        let before = store.reads;
        for _ in 0..3 {
            assert_eq!(d.renew(&mut store, NOW), Renewal::Renewed);
        }
        assert_eq!(store.reads, before);
        // Iemand anders nam de lease: de CAS zegt het, ook zonder lezing.
        let mut other = claim("10.0.0.2:8080", 30 * SECOND);
        d.release(&mut store);
        assert!(other.try_become_leader(&mut store, NOW));
        d.handle = "stale".to_string();
        assert_eq!(d.renew(&mut store, NOW), Renewal::Displaced);
    }

    // hoplock/mem: aanmaken, CAS, wissen.
    #[test]
    fn backend_read_write_delete() {
        let mut b = MemLease::new();
        assert_eq!(b.read(), Err(LeaseError::NoLease));
        let s1 = LeaseState {
            generation: 1,
            expires_at: Time(NOW.0 + 30 * SECOND),
            owner: "alice".to_string(),
        };
        let h1 = b.write("", &s1).unwrap();
        assert!(!h1.is_empty());
        let (got, h) = b.read().unwrap();
        assert_eq!(h, h1);
        assert_eq!(got, s1);
        assert_eq!(b.write("", &s1), Err(LeaseError::Held));
        assert_eq!(b.write("wrong", &s1), Err(LeaseError::Held));
        let h2 = b.write(&h1, &s1).unwrap();
        assert_ne!(h2, h1);
        assert_eq!(b.delete(&h1), Err(LeaseError::Held));
        assert_eq!(b.delete(&h2), Ok(()));
        assert_eq!(b.delete(&h2), Err(LeaseError::NoLease));
    }
}

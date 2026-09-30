//! De leader-lease: welke node leidt, en mag ik leiden.
//!
//! Deze crate bezit de lease-logica van een node: de laatst geziene handle
//! (ETag) en generatie van de lease die wij houden, en de regels voor
//! aanmaken, vernieuwen, overnemen en loslaten. Hij bezit NIET de opslag: de
//! wederzijdse uitsluiting woont helemaal in de backend, waar elke claim een
//! voorwaardelijke schrijf is op de laatst geziene handle.
//!
//! Sans-I/O. De stappen ([`Discovery::plan_claim`], [`Discovery::renew_op`],
//! [`Discovery::on_write`], [`leader_state`]) bouwen de operatie en lezen het
//! antwoord; de aanroeper voert hem uit tegen S3, hoplockserver of wat de
//! node ook heeft. Voor een backend die meteen antwoordt (de in-memory van
//! standalone, en de tests) zijn er de gemaksmethoden
//! [`Discovery::try_become_leader`] en verder, die dezelfde stappen tegen een
//! [`Backend`] draaien.
//!
//! In Go heette dit `internal/discovery`; "nodes vinden" is hier dus: de
//! leader vinden via de lease.

#![cfg_attr(not(test), no_std)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]
#![forbid(unsafe_code)]

extern crate alloc;

mod mem;
mod store;
pub mod wire;

use alloc::string::String;
use core::fmt;

pub use mem::MemBackend;
pub use store::{StateStoreKind, state_store_for};

/// De ondergrens voor één round-trip naar de lock-store, in milliseconden.
pub const MIN_BACKEND_TIMEOUT_MS: u64 = 5_000;

/// Wat de lease-opslag over de lease zegt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseState {
    /// Stijgt bij elke overname; een vernieuwing houdt hem gelijk.
    pub generation: u64,
    /// Tot wanneer de lease geldt, in milliseconden op de klok van de aanroeper.
    pub expires_at: u64,
    /// Het leader-adres ("ip:poort") van de houder.
    pub owner: String,
}

/// Een fout van de lease-opslag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// Er is geen lease (Read); geen fout voor de lus, wel een antwoord.
    NoLease,
    /// Iemand anders houdt de lease (412 op de voorwaardelijke schrijf).
    LeaseHeld,
    /// De opslag antwoordde niet: transport, timeout, 5xx.
    Unreachable,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NoLease => f.write_str("lease: no lease"),
            Error::LeaseHeld => f.write_str("lease: held by another owner"),
            Error::Unreachable => f.write_str("lease: store unreachable"),
        }
    }
}

/// Het resultaat-type van deze crate.
pub type Result<T = (), E = Error> = core::result::Result<T, E>;

/// Een lease-opslag die meteen antwoordt.
///
/// Dit is het contract van hoplock: elke schrijf is voorwaardelijk op de
/// vorige handle, en een lege handle betekent "alleen als er nog niets is".
pub trait Backend {
    /// Leest de lease en zijn handle; [`Error::NoLease`] als er geen is.
    fn read(&mut self) -> Result<(LeaseState, String)>;
    /// Schrijft de lease als de huidige handle `prev` is en geeft de nieuwe.
    fn write(&mut self, prev: &str, state: &LeaseState) -> Result<String>;
    /// Verwijdert de lease als de huidige handle `handle` is.
    fn delete(&mut self, handle: &str) -> Result;
}

/// Het tijdsbudget voor één backend-aanroep bij een lease van `ttl_ms`.
///
/// Een derde van de lease, nooit onder [`MIN_BACKEND_TIMEOUT_MS`]. Een renew
/// is een lees plus een voorwaardelijke schrijf; op een trage object-store
/// (Bunny Storage: 4 tot 20 s per PUT, gemeten 08-09-2026) liet een vaste 5 s
/// elke renew en overname verlopen, en verloor het cluster zijn leader zodra
/// de lease afliep. Door het budget aan de TTL te knopen vangt een operator
/// een trage store op met alleen `timeouts.leader_lease`: bij 120 s mag elke
/// aanroep 40 s duren en blijven er nog twee ticks over.
pub fn backend_timeout_for(ttl_ms: u64) -> u64 {
    let third = ttl_ms / 3;
    if third > MIN_BACKEND_TIMEOUT_MS {
        third
    } else {
        MIN_BACKEND_TIMEOUT_MS
    }
}

/// Wie leidt volgens een leesantwoord, en of de opslag antwoordde.
///
/// De twee lege antwoorden betekenen tegengestelde dingen voor een agent die
/// zijn leader kwijt is: "de opslag zegt dat niemand leidt" (niemand plaatst
/// onze taken opnieuw; houden) en "de opslag is onbereikbaar" (misschien zijn
/// wij de geïsoleerde; de fail-safe geldt).
pub fn leader_state(read: &Result<(LeaseState, String)>, now: u64) -> (Option<&str>, bool) {
    match read {
        Err(Error::NoLease) => (None, true),
        Err(_) => (None, false),
        Ok((state, _)) if now > state.expires_at => (None, true),
        Ok((state, _)) => (Some(state.owner.as_str()), true),
    }
}

/// Wat een claim na het lezen moet doen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Claim {
    /// Schrijf `state` voorwaardelijk op handle `prev` (leeg = aanmaken).
    Write {
        /// De handle waarop de schrijf voorwaardelijk is.
        prev: String,
        /// De lease die geschreven wordt.
        state: LeaseState,
    },
    /// Stop: een andere houder of de opslag zelf zegt nee.
    Fail(Error),
}

/// Wat een renew als eerste doet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RenewOp {
    /// Eén voorwaardelijke schrijf op de handle die we houden, zonder lees.
    Write {
        /// De handle die we houden.
        prev: String,
        /// De vernieuwde lease.
        state: LeaseState,
    },
    /// Nog geen handle (vers proces): het volle claimpad, beginnend met een lees.
    Claim,
}

/// Het lease-handvat van één node.
///
/// # Invariants
///
/// `handle` is leeg of de handle van de laatste schrijf die slaagde; `generation`
/// hoort bij die schrijf.
#[derive(Debug)]
pub struct Discovery {
    owner: String,
    ttl_ms: u64,
    timeout_ms: u64,
    handle: String,
    generation: u64,
}

impl Discovery {
    /// Maakt het handvat voor een node op `owner` ("ip:poort") met een lease van `ttl_ms`.
    pub fn new(owner: String, ttl_ms: u64) -> Self {
        Self {
            owner,
            ttl_ms,
            timeout_ms: backend_timeout_for(ttl_ms),
            handle: String::new(),
            generation: 0,
        }
    }

    /// Het leader-adres waarmee deze node zich meldt.
    pub fn node_addr(&self) -> &str {
        &self.owner
    }

    /// Het tijdsbudget per backend-aanroep, afgeleid van de TTL.
    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    /// Of we denken een lease te houden (een handle hebben).
    pub fn holds_handle(&self) -> bool {
        !self.handle.is_empty()
    }

    fn lease(&self, generation: u64, now: u64) -> LeaseState {
        LeaseState {
            generation,
            expires_at: now.saturating_add(self.ttl_ms),
            owner: self.owner.clone(),
        }
    }

    /// Beslist na een lees wat een claim schrijft.
    ///
    /// Drie wegen naar succes: aanmaken als er niets is, vernieuwen als de
    /// lease al van ons is, overnemen als hij verlopen is (generatie + 1).
    pub fn plan_claim(&self, read: Result<(LeaseState, String)>, now: u64) -> Claim {
        match read {
            Err(Error::NoLease) => Claim::Write {
                prev: String::new(),
                state: self.lease(1, now),
            },
            Err(e) => Claim::Fail(e),
            Ok((state, handle)) if state.owner == self.owner => Claim::Write {
                prev: handle,
                state: self.lease(state.generation, now),
            },
            Ok((state, handle)) if now > state.expires_at => Claim::Write {
                prev: handle,
                state: self.lease(state.generation.saturating_add(1), now),
            },
            Ok(_) => Claim::Fail(Error::LeaseHeld),
        }
    }

    /// De eerste stap van een renew.
    ///
    /// Met een handle is het één voorwaardelijke PUT en geen lees: de CAS is
    /// het bewijs. Schreef iemand anders de lease intussen, dan zegt de store
    /// 412 en zijn we verdrongen. Zo blijft het store-verkeer van de leader één
    /// aanroep per renew (Bunny: elke aanroep kost seconden).
    pub fn renew_op(&self, now: u64) -> RenewOp {
        if self.handle.is_empty() {
            RenewOp::Claim
        } else {
            RenewOp::Write {
                prev: self.handle.clone(),
                state: self.lease(self.generation, now),
            }
        }
    }

    /// Verwerkt het antwoord op een schrijf van `state`.
    pub fn on_write(&mut self, state: &LeaseState, result: Result<String>) -> Result {
        let handle = result?;
        self.handle = handle;
        self.generation = state.generation;
        Ok(())
    }

    /// Geeft de handle af om de lease te verwijderen; leeg als we niets houden.
    pub fn take_handle(&mut self) -> String {
        core::mem::take(&mut self.handle)
    }

    /// Zet de handle met de hand (tests en een overdracht na een kern-flip).
    pub fn set_handle(&mut self, handle: String, generation: u64) {
        self.handle = handle;
        self.generation = generation;
    }

    fn claim<B: Backend>(&mut self, backend: &mut B, now: u64) -> Result {
        match self.plan_claim(backend.read(), now) {
            Claim::Write { prev, state } => {
                let result = backend.write(&prev, &state);
                self.on_write(&state, result)
            }
            Claim::Fail(e) => Err(e),
        }
    }

    /// Het huidige leader-adres, of `None` zonder levende lease of zonder antwoord.
    pub fn get_leader<B: Backend>(&self, backend: Option<&mut B>, now: u64) -> Option<String> {
        self.leader_state(backend, now).0
    }

    /// De leader plus of de opslag antwoordde; zonder backend (standalone) telt als antwoord.
    pub fn leader_state<B: Backend>(
        &self,
        backend: Option<&mut B>,
        now: u64,
    ) -> (Option<String>, bool) {
        let Some(backend) = backend else {
            return (None, true);
        };
        let read = backend.read();
        let (leader, ok) = leader_state(&read, now);
        (leader.map(String::from), ok)
    }

    /// Probeert de lease te claimen; `true` als we hem nu houden.
    pub fn try_become_leader<B: Backend>(&mut self, backend: Option<&mut B>, now: u64) -> bool {
        match backend {
            Some(b) => self.claim(b, now).is_ok(),
            None => false,
        }
    }

    /// Vernieuwt de lease: `(renewed, displaced)`.
    ///
    /// Bij `renewed == false` onderscheidt `displaced` de twee soorten falen:
    /// `true` is een andere eigenaar (nu aftreden), `false` is een
    /// onbereikbare opslag (een blip: blijven leiden mag zolang er agents zijn,
    /// want niemand anders kan de lease dan nemen).
    pub fn renew_lease<B: Backend>(&mut self, backend: Option<&mut B>, now: u64) -> (bool, bool) {
        let Some(backend) = backend else {
            return (false, false);
        };
        let result = match self.renew_op(now) {
            RenewOp::Write { prev, state } => {
                let written = backend.write(&prev, &state);
                self.on_write(&state, written)
            }
            RenewOp::Claim => self.claim(backend, now),
        };
        match result {
            Ok(()) => (true, false),
            Err(e) => (false, e == Error::LeaseHeld),
        }
    }

    /// Verwijdert de lease naar beste kunnen, zodat een ander meteen kan overnemen.
    pub fn release_leadership<B: Backend>(&mut self, backend: Option<&mut B>) {
        let handle = self.take_handle();
        if let Some(backend) = backend
            && !handle.is_empty()
        {
            // Naar beste kunnen: lukt het niet, dan loopt de lease vanzelf af.
            let _ = backend.delete(&handle);
        }
    }

    /// Of deze node de huidige leader is.
    pub fn is_leader<B: Backend>(&self, backend: Option<&mut B>, now: u64) -> bool {
        self.get_leader(backend, now).as_deref() == Some(self.owner.as_str())
    }
}

#[cfg(test)]
mod tests;

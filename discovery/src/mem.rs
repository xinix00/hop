//! De in-memory lease-opslag: standalone (één node) en de tests.
//!
//! Bezit één lease en een handle-teller. Bezit geen klok: verlopen is de zaak
//! van wie leest.

use alloc::string::String;

use crate::{Backend, Error, LeaseState, Result};

/// Een lease-opslag in het geheugen met dezelfde CAS-regels als S3.
#[derive(Debug, Default)]
pub struct MemBackend {
    lease: Option<(LeaseState, String)>,
    next: u64,
}

impl MemBackend {
    /// Een lege opslag.
    pub fn new() -> Self {
        Self::default()
    }

    fn fresh_handle(&mut self) -> String {
        self.next = self.next.wrapping_add(1);
        alloc::format!("mem-{}", self.next)
    }
}

impl Backend for MemBackend {
    fn read(&mut self) -> Result<(LeaseState, String)> {
        self.lease.clone().ok_or(Error::NoLease)
    }

    fn write(&mut self, prev: &str, state: &LeaseState) -> Result<String> {
        let current = self.lease.as_ref().map(|(_, h)| h.as_str());
        // Een lege prev betekent "alleen aanmaken"; anders moet hij de huidige zijn.
        let ok = match current {
            None => prev.is_empty(),
            Some(h) => h == prev,
        };
        if !ok {
            return Err(Error::LeaseHeld);
        }
        let handle = self.fresh_handle();
        self.lease = Some((state.clone(), handle.clone()));
        Ok(handle)
    }

    fn delete(&mut self, handle: &str) -> Result {
        match &self.lease {
            Some((_, h)) if h == handle => {
                self.lease = None;
                Ok(())
            }
            Some(_) => Err(Error::LeaseHeld),
            None => Ok(()),
        }
    }
}

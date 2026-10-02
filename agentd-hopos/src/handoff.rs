//! De overdracht van een verbinding van de acceptor aan een vrije werker.
//!
//! Een listener van de stack heeft één wachtplek voor een waker, dus één
//! taak accepteert per poort; de verbindingen gaan als waarde naar een vaste
//! pool werkers (handboek §2: geen taak per verbinding, de pool is een
//! constante). Dezelfde vorm als `apps/welcome` in HopOS.
//!
//! Waarom een pool en niet één taak per poort: een open stroom (`hop
//! events`, `hop logs --follow`) houdt zijn werker vast zolang hij loopt, en
//! met één taak per poort zou de API zolang dicht zijn.
//!
//! Een leesbare tabel in de zin van §1.1: één schrijver per vak (de acceptor
//! zet, de werker neemt), elke lening duurt één methode en nooit over een
//! `.await`. Alle taken draaien op de ene app-core.

use alloc::vec::Vec;
use core::cell::RefCell;
use core::future::poll_fn;
use core::task::{Poll, Waker};

/// De tabel van één pool.
pub struct Handoff<T> {
    /// Wat klaarligt voor elke werker.
    ready: RefCell<Vec<Option<T>>>,
    /// Welke werker een verbinding heeft (van de overdracht tot [`Handoff::free`]).
    busy: RefCell<Vec<bool>>,
    wakers: RefCell<Vec<Option<Waker>>>,
}

impl<T> Handoff<T> {
    /// Een pool van `workers` werkers.
    pub fn new(workers: usize) -> Self {
        let mut ready = Vec::new();
        let mut busy = Vec::new();
        let mut wakers = Vec::new();
        ready.resize_with(workers, || None);
        busy.resize(workers, false);
        wakers.resize_with(workers, || None);
        Self {
            ready: RefCell::new(ready),
            busy: RefCell::new(busy),
            wakers: RefCell::new(wakers),
        }
    }

    /// Geeft `t` aan de eerste vrije werker en wekt hem; `Err(t)` terug als
    /// ze allemaal bezig zijn.
    pub fn give(&self, t: T) -> Result<usize, T> {
        let free = self.busy.borrow().iter().position(|b| !*b);
        let Some(i) = free else {
            return Err(t);
        };
        if let Some(b) = self.busy.borrow_mut().get_mut(i) {
            *b = true;
        }
        if let Some(r) = self.ready.borrow_mut().get_mut(i) {
            *r = Some(t);
        }
        let w = self.wakers.borrow_mut().get_mut(i).and_then(Option::take);
        if let Some(w) = w {
            w.wake();
        }
        Ok(i)
    }

    /// Wacht als werker `i` op zijn volgende verbinding.
    pub async fn take(&self, i: usize) -> T {
        poll_fn(|cx| {
            let got = self.ready.borrow_mut().get_mut(i).and_then(Option::take);
            match got {
                Some(t) => Poll::Ready(t),
                None => {
                    if let Some(w) = self.wakers.borrow_mut().get_mut(i) {
                        *w = Some(cx.waker().clone());
                    }
                    Poll::Pending
                }
            }
        })
        .await
    }

    /// Werker `i` is klaar met zijn verbinding en weer vrij.
    pub fn free(&self, i: usize) {
        if let Some(b) = self.busy.borrow_mut().get_mut(i) {
            *b = false;
        }
    }

    /// Of elke werker een verbinding heeft: de volgende verbinding zou
    /// moeten wachten. Een werker die dit ziet terwijl hij een verzoek
    /// afhandelt, houdt zijn verbinding daarna niet open (`Connection:
    /// close`), zodat een wachter nooit op de stilte van een keep-alive-
    /// client hoeft te wachten.
    pub fn none_free(&self) -> bool {
        self.busy.borrow().iter().all(|b| *b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_free_only_when_every_worker_holds_a_connection() {
        let pool: Handoff<u8> = Handoff::new(2);
        assert!(!pool.none_free());
        let a = pool.give(1).unwrap();
        assert!(!pool.none_free(), "one worker is still free");
        let b = pool.give(2).unwrap();
        assert!(pool.none_free());
        assert_eq!(
            pool.give(3),
            Err(3),
            "nobody free: the connection comes back"
        );
        pool.free(a);
        assert!(!pool.none_free());
        assert_eq!(pool.give(3), Ok(a));
        pool.free(b);
        pool.free(a);
        assert!(!pool.none_free());
    }
}

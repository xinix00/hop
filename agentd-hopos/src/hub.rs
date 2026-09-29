//! De brievenbus tussen de verbindingstaken en de eigenaar-taak van de node.
//!
//! Handboek §1: de staat van de node heeft één eigenaar, en wie er iets mee
//! wil stuurt een bericht. Een verbindingstaak zet zijn verzoek in de bus
//! ([`Hub::ask`]) en wacht op zijn eigen antwoordvak; de eigenaar haalt de
//! verzoeken op ([`Hub::next`]), handelt ze af met `&mut` op zijn staat, en
//! zet het antwoord in het vak van die taak ([`Hub::answer`]).
//!
//! De bus is een leesbare tabel in de zin van §1.1: geen logica, één
//! schrijver per vak, en elke lening duurt één methode, nooit over een
//! `.await`. Het handboek wil zo'n tabel in een `Local<RefCell<..>>`; de
//! `Local` van HopOS zit in de crate `sync`, die hier geen afhankelijkheid
//! is, dus de binary lekt één `Hub` bij boot en geeft de taken
//! `&'static Hub`. De executor van de app-core eist geen `Send`, en alle
//! taken draaien op die ene core.
//!
//! Het aantal vakken is vast: één per verbindingstaak, de pool is een
//! constante van de binary.

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::cell::RefCell;
use core::future::poll_fn;
use core::task::{Poll, Waker};

use api::Request;
use hop_http::Reply;

use crate::Port;

/// Eén verzoek in de bus.
type Msg = (usize, Port, Request);

/// De bus.
pub struct Hub {
    inbox: RefCell<VecDeque<Msg>>,
    replies: RefCell<Vec<Option<Reply>>>,
    wakers: RefCell<Vec<Option<Waker>>>,
    owner: RefCell<Option<Waker>>,
}

impl Hub {
    /// Een bus met `slots` antwoordvakken, één per verbindingstaak.
    pub fn new(slots: usize) -> Self {
        let mut replies = Vec::new();
        let mut wakers = Vec::new();
        replies.resize_with(slots, || None);
        wakers.resize_with(slots, || None);
        Self {
            inbox: RefCell::new(VecDeque::new()),
            replies: RefCell::new(replies),
            wakers: RefCell::new(wakers),
            owner: RefCell::new(None),
        }
    }

    /// Stuurt `req` van verbindingstaak `slot` naar de eigenaar en wacht op het antwoord.
    pub async fn ask(&self, slot: usize, port: Port, req: Request) -> Reply {
        self.inbox.borrow_mut().push_back((slot, port, req));
        if let Some(w) = self.owner.borrow_mut().take() {
            w.wake();
        }
        poll_fn(|cx| {
            let got = self
                .replies
                .borrow_mut()
                .get_mut(slot)
                .and_then(Option::take);
            match got {
                Some(r) => Poll::Ready(r),
                None => {
                    if let Some(w) = self.wakers.borrow_mut().get_mut(slot) {
                        *w = Some(cx.waker().clone());
                    }
                    Poll::Pending
                }
            }
        })
        .await
    }

    /// Het volgende verzoek voor de eigenaar, als er een is.
    pub fn next(&self) -> Option<Msg> {
        self.inbox.borrow_mut().pop_front()
    }

    /// Zet het antwoord in vak `slot` en wekt die taak.
    pub fn answer(&self, slot: usize, reply: Reply) {
        if let Some(r) = self.replies.borrow_mut().get_mut(slot) {
            *r = Some(reply);
        }
        let w = self
            .wakers
            .borrow_mut()
            .get_mut(slot)
            .and_then(Option::take);
        if let Some(w) = w {
            w.wake();
        }
    }

    /// Wacht tot er een verzoek ligt (level-triggered: ligt er al een, dan meteen).
    pub async fn wait(&self) {
        poll_fn(|cx| {
            if self.inbox.borrow().is_empty() {
                *self.owner.borrow_mut() = Some(cx.waker().clone());
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
    }
}

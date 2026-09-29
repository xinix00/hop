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
//!
//! Drie soorten post ([`Question`]): een verzoek (antwoord: een [`Reply`]),
//! de vraag van een open stroom wat er bij kwam (antwoord: een [`Chunk`]),
//! en de afmelding van een stroom (geen antwoord).

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::cell::RefCell;
use core::future::poll_fn;
use core::task::{Poll, Waker};

use api::Request;
use hop_http::{Ask, Chunk, Reply};

use crate::Port;

/// Wat een verbindingstaak de eigenaar vraagt.
#[derive(Debug, PartialEq, Eq)]
pub enum Question {
    /// Een verzoek op een poort; het antwoord is een [`Reply`].
    Http(Port, Request),
    /// Een open stroom vraagt wat er bij kwam; het antwoord is een [`Chunk`].
    Poll(Ask),
    /// Een open stroom is af: de eigenaar telt hem af. Geen antwoord.
    StreamDone,
}

/// Wat de eigenaar terugzet in het vak van een verbindingstaak.
#[derive(Debug)]
pub enum Answer {
    /// Op een [`Question::Http`].
    Reply(Reply),
    /// Op een [`Question::Poll`].
    Chunk(Chunk),
}

/// Eén vraag in de bus, met het vak van de vrager.
type Msg = (usize, Question);

/// De bus.
pub struct Hub {
    inbox: RefCell<VecDeque<Msg>>,
    replies: RefCell<Vec<Option<Answer>>>,
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
        match self.question(slot, Question::Http(port, req)).await {
            Answer::Reply(r) => r,
            // Een eigenaar die op een verzoek een hap zet, is een fout in
            // de eigenaar; de verbinding krijgt een luide 500.
            Answer::Chunk(_) => Reply::Plain(api::Response::error(500, "owner answered a chunk")),
        }
    }

    /// Vraagt de eigenaar wat stroom `slot` sinds `ask` mist.
    pub async fn poll(&self, slot: usize, ask: Ask) -> Chunk {
        match self.question(slot, Question::Poll(ask)).await {
            Answer::Chunk(c) => c,
            Answer::Reply(_) => Chunk {
                done: true,
                ..Chunk::default()
            },
        }
    }

    /// Meldt de stroom van `slot` af; wacht niet.
    pub fn stream_done(&self, slot: usize) {
        self.post(slot, Question::StreamDone);
    }

    /// Zet een vraag in de bus en wekt de eigenaar.
    fn post(&self, slot: usize, q: Question) {
        self.inbox.borrow_mut().push_back((slot, q));
        if let Some(w) = self.owner.borrow_mut().take() {
            w.wake();
        }
    }

    /// Zet een vraag in de bus en wacht op het antwoord in vak `slot`.
    async fn question(&self, slot: usize, q: Question) -> Answer {
        self.post(slot, q);
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
    pub fn answer(&self, slot: usize, reply: Answer) {
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

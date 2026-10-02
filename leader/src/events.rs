//! Meldingen voor de SSE-stroom (`/v1/events`): wat er veranderde, niet hoe.
//!
//! Go had een EventBus met een kanaal van één per abonnee: gelijke
//! meldingen vallen samen en de nieuwste wint. Hier verzamelt de leider de
//! meldingen in een begrensde rij zonder dubbelen; de adapter haalt ze op
//! met [`crate::Leader::drain_events`] en verdeelt ze over zijn abonnees.

use alloc::string::String;
use alloc::vec::Vec;

use types::try_string;

/// Hoeveel verschillende meldingen er hoogstens wachten. Een abonnee wil
/// weten dát er iets veranderde; loopt de rij over, dan wordt het één
/// [`Event::Status`] ("kijk alles opnieuw").
pub const MAX_EVENTS: usize = 64;

/// Wat er veranderde.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// De cluster als geheel (settle voorbij, of te veel om op te noemen).
    Status,
    /// Een agent (geregistreerd, dood, weg).
    Agent(String),
    /// Een job (geplaatst, bijgewerkt, verwijderd).
    Job(String),
}

impl Event {
    /// Het onderwerp zoals Go het schreef: `status`, `agent:<id>`, `job:<naam>`.
    pub fn topic_prefix(&self) -> (&'static str, &str) {
        match self {
            Self::Status => ("status", ""),
            Self::Agent(id) => ("agent:", id),
            Self::Job(name) => ("job:", name),
        }
    }
}

/// De wachtende meldingen.
#[derive(Debug, Default)]
pub(crate) struct Events {
    queue: Vec<Event>,
    overflowed: bool,
}

impl Events {
    pub(crate) fn status(&mut self) {
        self.push(Event::Status);
    }

    pub(crate) fn agent(&mut self, id: &str) {
        if let Ok(id) = try_string(id) {
            self.push(Event::Agent(id));
        } else {
            self.overflowed = true;
        }
    }

    pub(crate) fn job(&mut self, name: &str) {
        if let Ok(name) = try_string(name) {
            self.push(Event::Job(name));
        } else {
            self.overflowed = true;
        }
    }

    fn push(&mut self, e: Event) {
        if self.overflowed || self.queue.contains(&e) {
            return;
        }
        if self.queue.len() >= MAX_EVENTS || self.queue.try_reserve(1).is_err() {
            self.overflowed = true;
            return;
        }
        self.queue.push(e);
    }

    pub(crate) fn drain(&mut self) -> Vec<Event> {
        let mut out = core::mem::take(&mut self.queue);
        if core::mem::take(&mut self.overflowed) {
            out.clear();
            out.push(Event::Status);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    //! De tests van `internal/leader/events_test.go` uit de Go-generatie
    //! (github.com/xinix00/hop, tag v1.0.7), naar de rij vertaald.

    use super::*;
    use alloc::string::ToString;

    #[test]
    fn event_bus_notify() {
        let mut e = Events::default();
        e.job("x");
        assert_eq!(e.drain(), [Event::Job("x".to_string())]);
        assert!(e.drain().is_empty());
    }

    #[test]
    fn event_bus_coalescing() {
        // Tien gelijke meldingen zijn er één.
        let mut e = Events::default();
        for _ in 0..10 {
            e.job("x");
        }
        assert_eq!(e.drain().len(), 1);
    }

    #[test]
    fn event_bus_overflow_becomes_status() {
        let mut e = Events::default();
        for i in 0..(MAX_EVENTS + 5) {
            e.job(&i.to_string());
        }
        assert_eq!(e.drain(), [Event::Status]);
        e.agent("a");
        assert_eq!(e.drain(), [Event::Agent("a".to_string())]);
    }

    #[test]
    fn event_topics_match_go() {
        assert_eq!(Event::Status.topic_prefix(), ("status", ""));
        assert_eq!(Event::Job("a".to_string()).topic_prefix(), ("job:", "a"));
    }
}

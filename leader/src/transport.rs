//! Wat de leider bij een agent laat doen, als trait.
//!
//! In Go was dit HTTP naar `agent.Endpoint`: `POST /run`, `POST /stop/{job}`,
//! `POST /stop-task/{id}`, `DELETE /delete/{job}`, `GET /tasks`, elk
//! ondertekend met de API-sleutel. Die kant hoort bij de adapter; de leider
//! ziet alleen de uitkomst.

use alloc::vec::Vec;

use types::{Agent, Job, Nanos, Task};

/// Het antwoord van een agent op "draai deze job".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunReply {
    /// Aangenomen (200, 201 of 202).
    Accepted,
    /// De node past niet bij de affinity (406): deze agent kan hem nooit draaien.
    AffinityMismatch,
    /// Vol (503): deze agent kan hem later misschien wel draaien.
    NoCapacity,
    /// Een andere status.
    Rejected(u16),
    /// Niet bereikt (verbinding, timeout).
    Unreachable,
}

impl RunReply {
    /// Vertaalt een HTTP-status van `POST /run` naar een antwoord.
    pub fn from_status(status: u16) -> Self {
        match status {
            200..=202 => Self::Accepted,
            406 => Self::AffinityMismatch,
            503 => Self::NoCapacity,
            s => Self::Rejected(s),
        }
    }
}

/// De verbinding van de leider met zijn agents.
///
/// Elke methode blokkeert tot de agent antwoordde of de timeout verstreek;
/// de adapter kiest de timeouts (Go: 5 s voor `/run` en `/tasks`, 60 s voor
/// stoppen en verwijderen, want Docker doet 10 s SIGTERM plus 10 s SIGKILL).
pub trait Transport {
    /// `POST /run` (met `?replace=1` als `replace`): start een instantie.
    ///
    /// Bij `replace` laat de agent hem alleen toe als hij past met zijn
    /// eigen voorganger weggedacht, en stopt die voorganger pas ná de
    /// toelating; een weigering laat het oude draaien.
    fn run(&mut self, agent: &Agent, job: &Job, replace: bool) -> RunReply;

    /// `POST /stop/{job}`: stopt de taken van een job, maar houdt de
    /// definitie. `true` als de stop bevestigd is.
    fn stop_job(&mut self, agent: &Agent, job: &str) -> bool;

    /// `POST /stop-task/{id}`: stopt precies één taak.
    fn stop_task(&mut self, agent: &Agent, task_id: &str);

    /// `DELETE /delete/{job}`: verwijdert de job op die agent.
    fn delete_job(&mut self, agent: &Agent, job: &str);

    /// `GET /tasks`: de taken van de agent, of `None` als hij niet antwoordde.
    fn tasks(&mut self, agent: &Agent) -> Option<Vec<Task>>;

    /// Wacht `d` nanoseconden: de pauze tussen twee stappen van een rolling
    /// update. De host-adapter slaapt; een test telt.
    fn pause(&mut self, d: Nanos) {
        let _ = d;
    }
}

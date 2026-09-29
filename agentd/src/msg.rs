//! De berichten aan de eigenaar-thread, en wat hij terugstuurt.
//!
//! Handboek §1: de staat van de node heeft één eigenaar, en wie er iets mee
//! wil stuurt een bericht. Elke andere thread (verbindingen, de lease, de
//! uitgaande aanroepen, de voorbereiding van taken, de probes, de opslag)
//! praat alleen via [`Msg`] met hem; een antwoord gaat over een eigen
//! kanaal dat de vrager bezit.

use std::sync::mpsc::SyncSender;

use agent::{LinkError, Outcome, Request as LinkRequest};
use api::{Request, Response};
use runner::host::{Prepared, TaskSpec};
use types::Driver;

/// Welke API een verzoek krijgt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Port {
    /// De agent-API (poort P).
    Agent,
    /// De leader-API (poort P + 1000).
    Leader,
}

/// Wat de eigenaar op een HTTP-verzoek antwoordt.
#[derive(Debug)]
pub(crate) enum Reply {
    /// Een gewoon antwoord.
    Plain(Response),
    /// Een SSE-momentopname: de kop, elke regel één `data:`-gebeurtenis.
    Events {
        /// Status en headers.
        head: Response,
        /// De regels.
        lines: Vec<String>,
    },
    /// Geef het verzoek door aan de leader op dit adres; dat doet de
    /// verbindingsthread zelf, want de leader kan tijdens het verzoek deze
    /// node aanroepen, en een eigenaar die op zijn eigen leader wacht zou
    /// dan op zichzelf wachten.
    Proxy {
        /// `ip:poort` van de leader.
        leader: String,
    },
}

/// Wat de lease-thread terugmeldt.
#[derive(Debug)]
pub(crate) enum LeaseReply {
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

/// Een bericht aan de eigenaar.
#[derive(Debug)]
pub(crate) enum Msg {
    /// Een verzoek van een verbindingsthread, met het vak voor het antwoord.
    Http {
        /// Welke API.
        port: Port,
        /// Het verzoek.
        req: Request,
        /// Waar het antwoord heen gaat.
        reply: SyncSender<Reply>,
    },
    /// Een werker is klaar met de voorbereiding van een taak.
    Prepared {
        /// Welke werker (hij is weer vrij).
        worker: usize,
        /// De taak.
        task_id: String,
        /// De runner.
        driver: Driver,
        /// Wat de runner moet starten.
        spec: Box<TaskSpec>,
        /// De voorbereiding, of waarom die faalde.
        result: Result<Prepared, String>,
    },
    /// Voortgang van een download.
    Progress {
        /// De taak.
        task_id: String,
        /// Binnen tot nu toe.
        got: u64,
        /// De totale maat, als bekend.
        total: u64,
    },
    /// Een antwoord van de lease-opslag.
    Lease(LeaseReply),
    /// Het antwoord op een aanroep bij de leader (register, heartbeat).
    Link {
        /// De aanroep.
        req: LinkRequest,
        /// De uitkomst.
        result: Result<(), LinkError>,
    },
    /// De uitkomst van een gezondheidscontrole.
    Probe {
        /// De taak.
        task_id: String,
        /// De uitkomst.
        outcome: Outcome,
    },
    /// Het wegschrijven van de clusterstaat faalde.
    SnapshotFailed(String),
}

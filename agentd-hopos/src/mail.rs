//! De post van de cluster: de rijen tussen de eigenaar-taak en de taken die het net op gaan.
//!
//! Handboek §1: de staat van de node heeft één eigenaar (de [`crate::Node`]),
//! en wie iets wil, stuurt een bericht. De eigenaar wacht nooit op het net:
//! wat de lease-opslag, een andere node of de clusterstaat raakt, gaat als
//! opdracht naar een taak die de verbinding bezit ([`crate::lease`],
//! [`crate::link`], [`crate::relay`]), en het antwoord komt als [`Mail`] in
//! de [`Inbox`] van de eigenaar. Elke rij is een `sync::mpsc::Mailbox` met een
//! vaste maat in de bron (handboek §2).
//!
//! Vol is een keuze van de zender. Een opdracht van de eigenaar die niet
//! past, wordt geweigerd en luid geteld (de volgende tik doet hem opnieuw);
//! een antwoord aan de eigenaar mag niet verloren gaan (een elector die op
//! een lees wacht, zou dan voor altijd wachten), dus een taak wacht met
//! [`deliver`] tot er plaats is.

use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;
use core::time::Duration;

use agent::{LeaseOp, LeaseReply, LinkError, Request as LinkRequest};
use leader::RunReply;
use sync::Full;
use sync::mpsc::Mailbox;
use types::Task;

/// Opdrachten aan de lease-taak: één uitstaande van elke soort (de elector).
pub const LEASE_Q: usize = 8;
/// Opdrachten aan de link-taak: register, heartbeat en de meldingen.
pub const LINK_Q: usize = 32;
/// Opdrachten aan de staat-taak: een lees bij het leider worden, en snapshots.
pub const STATE_Q: usize = 4;
/// Aanroepen van de leader bij andere agents: een reconcile van een cluster
/// met tientallen jobs past erin; wat niet past, doet de volgende reconcile.
pub const DISPATCH_Q: usize = 64;
/// Antwoorden aan de eigenaar.
pub const INBOX_Q: usize = 64;

/// Hoe lang een taak wacht als de [`Inbox`] vol is, voor hij het opnieuw probeert.
pub const INBOX_RETRY: Duration = Duration::from_millis(10);

/// Een opdracht aan de staat-taak.
#[derive(Debug, PartialEq, Eq)]
pub enum StateOp {
    /// Lees de gecommitte staat (bij het leider worden).
    Load,
    /// Overschrijf de gecommitte staat met deze snapshot.
    Save(Vec<u8>),
}

/// Een aanroep bij de leader op een andere node.
#[derive(Debug, PartialEq, Eq)]
pub enum LinkJob {
    /// Register of heartbeat: het antwoord gaat terug naar de verkiezing.
    Election {
        /// De aanroep, zoals de verkiezing hem gaf.
        req: LinkRequest,
        /// `POST` naar deze URL.
        url: String,
        /// De JSON-body.
        body: String,
    },
    /// Een melding (`POST /v1/notify`); het antwoord doet er niet toe.
    Notify {
        /// De URL.
        url: String,
        /// De JSON-body.
        body: String,
    },
}

/// Een aanroep van de leader bij een agent op een andere node.
#[derive(Debug, PartialEq, Eq)]
pub enum Remote {
    /// `POST /run` (met `?replace=1`).
    Run {
        /// Het id van de agent.
        agent: String,
        /// Zijn endpoint (`http://ip:poort`).
        endpoint: String,
        /// De naam van de job.
        job: String,
        /// De jobspec als JSON.
        body: String,
        /// Vervangen in plaats van erbij.
        replace: bool,
    },
    /// `POST /stop/{job}`.
    StopJob {
        /// Het endpoint.
        endpoint: String,
        /// De job.
        job: String,
    },
    /// `POST /stop-task/{id}`.
    StopTask {
        /// Het endpoint.
        endpoint: String,
        /// De taak.
        task: String,
    },
    /// `DELETE /delete/{job}`.
    DeleteJob {
        /// Het endpoint.
        endpoint: String,
        /// De job.
        job: String,
    },
    /// `GET /tasks`: de takenlijst van de agent, voor de boeken van de leader.
    Tasks {
        /// Het id van de agent.
        agent: String,
        /// Het endpoint.
        endpoint: String,
    },
}

/// Een antwoord aan de eigenaar.
#[derive(Debug)]
pub enum Mail {
    /// Van de lease-taak.
    Lease(LeaseReply),
    /// Een regel voor de console van een taak die zelf niet logt.
    Note(String),
    /// Het antwoord op een register of heartbeat.
    Link {
        /// De aanroep.
        req: LinkRequest,
        /// De uitkomst.
        result: Result<(), LinkError>,
    },
    /// De gecommitte staat, gelezen voor het leider worden.
    Loaded(Result<Option<Vec<u8>>, String>),
    /// Een snapshot kwam niet aan.
    SaveFailed(String),
    /// Het antwoord van een agent op `POST /run`.
    Ran {
        /// Het id van de agent.
        agent: String,
        /// De job.
        job: String,
        /// Het antwoord.
        reply: RunReply,
    },
    /// De takenlijst van een agent; `None` als hij niet antwoordde.
    Tasks {
        /// Het id van de agent.
        agent: String,
        /// De taken.
        tasks: Option<Vec<Task>>,
    },
}

/// De rij naar de lease-taak.
pub type LeaseQueue = Mailbox<LeaseOp, LEASE_Q>;
/// De rij naar de link-taak.
pub type LinkQueue = Mailbox<LinkJob, LINK_Q>;
/// De rij naar de staat-taak.
pub type StateQueue = Mailbox<StateOp, STATE_Q>;
/// De rij naar de dispatch-taak.
pub type DispatchQueue = Mailbox<Remote, DISPATCH_Q>;
/// De brievenbus van de eigenaar.
pub type Inbox = Mailbox<Mail, INBOX_Q>;

/// Slapen op het timerwiel van de executor; de tests geven een `yield`.
pub trait Nap {
    /// Slaapt `d`.
    fn nap(&self, d: Duration) -> impl Future<Output = ()>;
}

/// Zet `mail` in de inbox, en wacht tot er plaats is als hij vol is.
pub async fn deliver<N: Nap>(inbox: &Inbox, mail: Mail, nap: &N) {
    let mut mail = mail;
    loop {
        match inbox.try_send(mail) {
            Ok(()) => return,
            Err(Full(back)) => {
                mail = back;
                nap.nap(INBOX_RETRY).await;
            }
        }
    }
}

/// De rij naar de lease-taak als [`agent::LeaseOps`] van de elector.
#[derive(Clone, Copy)]
pub struct LeaseOps(pub &'static LeaseQueue);

impl agent::LeaseOps for LeaseOps {
    fn send(&mut self, op: LeaseOp) -> bool {
        self.0.try_send(op).is_ok()
    }
}

impl core::fmt::Debug for LeaseOps {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("LeaseOps")
            .field("queued", &self.0.len())
            .finish()
    }
}

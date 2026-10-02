//! Wat de agent van de wereld vraagt, en wat de wereld terugmeldt.
//!
//! Bezit alleen de vorm van de berichten; de agent maakt ze, de executor voert
//! ze uit.

use alloc::boxed::Box;
use alloc::string::String;

use types::{Driver, Job, Map, Nanos, Time};

/// Een handeling die de executor voor de agent uitvoert, in de volgorde waarin hij komt.
///
/// De volgorde telt: bij een vervanging komen de stops van de voorgangers
/// vóór de start van de opvolger, zodat hun core en partitie vrij zijn voordat
/// hij plaatst.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Start een taak: wijs de poorten van `job.ports` toe (0 = dynamisch) en
    /// roep de runner; meld de uitkomst met [`crate::Agent::on_started`].
    Start {
        /// Het taak-id.
        task_id: String,
        /// De job, al teruggebracht tot het ene artifact dat bij deze node past.
        job: Box<Job>,
    },
    /// Stop een taak bij de runner; het logische record is al weg.
    Stop {
        /// Het taak-id.
        task_id: String,
        /// Welke runner hem heeft.
        driver: Driver,
        /// Wat de runner bij de start teruggaf.
        pid: i64,
    },
    /// Vraag de runner hoe een taak ervoor staat; meld met [`crate::Agent::on_status`].
    Poll {
        /// Het taak-id.
        task_id: String,
        /// Welke runner hem heeft.
        driver: Driver,
        /// Wat de runner bij de start teruggaf.
        pid: i64,
    },
    /// Voer een gezondheidscontrole uit; meld met [`crate::Agent::on_probe`].
    Probe {
        /// Het taak-id.
        task_id: String,
        /// Wat er gecontroleerd wordt.
        probe: Probe,
    },
    /// Meld de leader een taakgebeurtenis (`POST /v1/notify`).
    Notify {
        /// De jobnaam.
        job: String,
        /// De gebeurtenis.
        event: Event,
    },
}

/// Een gezondheidscontrole als opdracht.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Probe {
    /// HTTP GET op de poort van de taak; 200 tot en met 399 is gezond.
    Http {
        /// De poort.
        port: u16,
        /// Het pad.
        path: String,
        /// De timeout.
        timeout: Nanos,
    },
    /// Een TCP-verbinding; slagen is gezond.
    Tcp {
        /// De poort.
        port: u16,
        /// De timeout.
        timeout: Nanos,
    },
    /// De mtime van een bestand; gezond als hij sinds de vorige controle veranderde.
    File {
        /// Het absolute pad.
        path: String,
    },
}

/// De uitkomst van een [`Probe`], als waarde.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// De HTTP-status, of `None` bij een transportfout of timeout.
    Http(Option<u16>),
    /// Of de verbinding lukte.
    Tcp(bool),
    /// De mtime van het bestand, of `None` als het er niet is.
    File(Option<Time>),
}

/// Een gebeurtenis voor de leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    /// Het proces draait, de health check nog niet geslaagd.
    Start,
    /// Klaar voor verkeer.
    Started,
    /// Gecrasht of ongezond.
    Crash,
    /// Taken van een verwijderde job gestopt.
    Stop,
    /// Aangenomen maar niet te plaatsen; teruggegeven aan de leader.
    Unplaceable,
}

impl Event {
    /// De naam op de draad.
    pub fn as_str(self) -> &'static str {
        match self {
            Event::Start => "start",
            Event::Started => "started",
            Event::Crash => "crash",
            Event::Stop => "stop",
            Event::Unplaceable => "unplaceable",
        }
    }
}

/// Een geslaagde start.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct StartOk {
    /// Procesnummer of kooi.
    pub pid: i64,
    /// De toegewezen poorten.
    pub ports: Map<u16>,
}

/// Een mislukte start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartError {
    /// De runner kan de taak niet plaatsen: teruggeven, niet herstarten.
    NoCapacity,
    /// Iets anders (poort bezet, download, runner): een crash die herstart.
    Failed,
}

/// Wat de runner over een taak zegt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Draait.
    Running,
    /// Weg.
    Failed,
}

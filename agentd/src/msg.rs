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
        /// Of het antwoord een stroom is: per brok doorspoelen in plaats van
        /// bufferen (`/v1/events`, een log-tail). De eigenaar telde hem al
        /// als open stroom; de thread meldt [`Msg::StreamDone`].
        stream: bool,
    },
    /// Volg de log van een taak: de kop, dan steeds [`Poll::Logs`] bij de
    /// eigenaar tot de ring dicht is of de lezer weg (een open stroom).
    Follow {
        /// Status en headers.
        head: Response,
        /// De taak.
        task_id: String,
        /// De stroom.
        stream: runner::Stream,
    },
    /// De meldingen van de leader als SSE: de kop, [`api::PING`], dan
    /// steeds [`Poll::Events`] vanaf `seq` (een open stroom).
    Subscribe {
        /// Status en headers.
        head: Response,
        /// Het volgnummer waarmee de lezer begint.
        seq: u64,
    },
    /// `GET /v1/tasks`: vraag elke agent zijn taken; de thread doet de
    /// rondgang, met één totale termijn, zodat de eigenaar nergens op wacht.
    Tasks {
        /// `(id, endpoint)` van elke agent.
        agents: Vec<(String, String)>,
    },
    /// Geef het verzoek door aan één agent, ondertekend met de clustersleutel.
    Agent {
        /// Het endpoint van de agent.
        endpoint: String,
        /// Pad en query op de agent.
        path: String,
        /// Of het een (getelde) stroom is.
        stream: bool,
    },
}

impl Reply {
    /// Of dit antwoord een stroom is die de eigenaar als open telt.
    pub(crate) fn is_stream(&self) -> bool {
        matches!(
            self,
            Self::Proxy { stream: true, .. }
                | Self::Agent { stream: true, .. }
                | Self::Follow { .. }
                | Self::Subscribe { .. }
        )
    }
}

/// Wat een open stroom de eigenaar vraagt: wat er sinds zijn volgnummer bij kwam.
#[derive(Debug)]
pub(crate) enum Poll {
    /// De regels van een taak na `seq` (0: alles wat de ring nog heeft).
    Logs {
        /// De taak.
        task_id: String,
        /// De stroom.
        stream: runner::Stream,
        /// Het volgnummer van de laatste regel die de lezer al heeft.
        seq: u64,
    },
    /// De meldingen na `seq`.
    Events {
        /// Het volgnummer waarmee de lezer vraagt.
        seq: u64,
    },
}

/// Het antwoord op een [`Poll`]: de SSE-bytes, het nieuwe volgnummer, en
/// of de stroom klaar is (de ring dicht, of deze node leidt niet meer).
#[derive(Debug, Default)]
pub(crate) struct Chunk {
    /// De SSE-gebeurtenissen, klaar voor de draad.
    pub(crate) text: String,
    /// Het volgnummer voor de volgende vraag.
    pub(crate) seq: u64,
    /// Na deze bytes is de stroom af.
    pub(crate) done: bool,
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
    /// Een open stroom vraagt wat er bij kwam.
    Poll {
        /// De vraag.
        poll: Poll,
        /// Waar het antwoord heen gaat.
        reply: SyncSender<Chunk>,
    },
    /// Een open stroom is dicht (de thread meldt het in `Drop`): de
    /// eigenaar telt hem af.
    StreamDone,
}

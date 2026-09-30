//! De lezer van `/v1/events`: bytes erin, hele gebeurtenissen eruit.
//!
//! Bezit alleen de rest van een half binnengekomen gebeurtenis. Een stroom
//! komt in happen die niet op gebeurtenisgrenzen vallen; pas een lege regel
//! sluit er een af (de SSE-regel). Commentaar (`: keepalive`) valt weg. De
//! vorm is die van hop's `api::stream`: `event: <soort>\ndata: <json>\n\n`,
//! met als soorten `ping` (de eerste van elke stroom), `status` (kijk alles
//! opnieuw), `job {"name"}`, `task {"job","event"}` en `agent {"id"}`.
//!
//! Herverbinden is niet van deze lezer maar van de aanroeper: na een
//! verbroken stroom [`Events::reset`], en de nieuwe stroom begint weer met
//! `ping`.

use alloc::string::String;
use alloc::vec::Vec;

use types::json::{self, Value};
use types::{try_push_str, try_string};

use crate::{Error, Result};

/// De grootste gebeurtenis die de lezer bewaart voor hij een einde ziet: 64 KiB.
///
/// Een melding van de agent is tientallen bytes (`{"job":"api","event":
/// "started"}`); een stroom die 64 KiB zonder lege regel stuurt, is geen
/// SSE, en zonder grens zou hij het geheugen van een slot opeten.
pub const MAX_EVENT_BYTES: usize = 64 << 10;

/// Eén gebeurtenis: de soort en de data.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Event {
    /// `event:`; leeg voor een gebeurtenis zonder soort (een logregel).
    pub kind: String,
    /// De `data:`-regels, met `\n` ertussen.
    pub data: String,
}

/// Waar een gebeurtenis over gaat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Topic {
    /// De stroom staat (de eerste gebeurtenis van elke verbinding).
    Ping,
    /// Iets veranderde dat niet in één melding past, of de lezer miste
    /// meldingen: lees alles opnieuw.
    Status,
    /// De definitie van een job veranderde (ook zijn tags).
    Job {
        /// De job.
        name: String,
    },
    /// Een taak van een job veranderde (gestart, gestopt, gefaald, ...).
    Task {
        /// De job.
        job: String,
        /// Wat er gebeurde; leeg als de agent het niet zei.
        event: String,
    },
    /// Een agent kwam of ging.
    Agent {
        /// De agent.
        id: String,
    },
    /// Een soort die deze versie niet kent.
    Other,
}

impl Event {
    /// Of de lezer nu alles opnieuw moet lezen: bij `ping` (een nieuwe
    /// verbinding, dus misschien gemiste meldingen) en bij `status`.
    pub fn is_resync(&self) -> bool {
        matches!(self.kind.as_str(), "ping" | "status")
    }

    /// Waar deze gebeurtenis over gaat, uit de soort en de JSON in de data.
    pub fn topic(&self) -> Result<Topic> {
        let field = |v: &Value, k: &str| -> Result<String> {
            let s = v
                .as_object()
                .and_then(|o| o.get(k))
                .and_then(Value::as_str)
                .unwrap_or_default();
            Ok(try_string(s)?)
        };
        match self.kind.as_str() {
            "ping" => Ok(Topic::Ping),
            "status" => Ok(Topic::Status),
            "job" => {
                let v = json::parse_str(&self.data)?;
                Ok(Topic::Job {
                    name: field(&v, "name")?,
                })
            }
            "task" => {
                let v = json::parse_str(&self.data)?;
                Ok(Topic::Task {
                    job: field(&v, "job")?,
                    event: field(&v, "event")?,
                })
            }
            "agent" => {
                let v = json::parse_str(&self.data)?;
                Ok(Topic::Agent {
                    id: field(&v, "id")?,
                })
            }
            _ => Ok(Topic::Other),
        }
    }

    /// De job waar deze gebeurtenis over gaat (een `job`- of `task`-melding),
    /// zoals Go's `ParseJobFromSSE`; `None` voor de rest.
    pub fn job(&self) -> Option<String> {
        match self.topic().ok()? {
            Topic::Job { name } if !name.is_empty() => Some(name),
            Topic::Task { job, .. } if !job.is_empty() => Some(job),
            _ => None,
        }
    }
}

/// De lezer: [`Events::push`] de bytes van de stroom, [`Events::next`] de gebeurtenissen.
#[derive(Debug, Default)]
pub struct Events {
    rest: Vec<u8>,
}

impl Events {
    /// Een lege lezer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Voegt een hap van de stroom toe.
    ///
    /// Faalt als de heap op is, of als er meer dan [`MAX_EVENT_BYTES`]
    /// zonder einde ligt; de lezer is dan leeg en de stroom hoort dicht.
    pub fn push(&mut self, bytes: &[u8]) -> Result {
        if self.rest.try_reserve(bytes.len()).is_err() {
            self.rest = Vec::new();
            return Err(Error::OutOfMemory);
        }
        self.rest.extend_from_slice(bytes);
        if self.rest.len() > MAX_EVENT_BYTES && find_blank_line(&self.rest).is_none() {
            let len = self.rest.len();
            self.rest = Vec::new();
            return Err(Error::TooLarge {
                len,
                max: MAX_EVENT_BYTES,
            });
        }
        Ok(())
    }

    /// De volgende hele gebeurtenis, of `None` als er nog geen af is.
    ///
    /// Een gebeurtenis die geen UTF-8 is, valt weg (SSE is UTF-8); zonder
    /// geheugen voor de strings faalt hij, en valt hij ook weg.
    #[expect(
        clippy::should_implement_trait,
        reason = "een fout (geen geheugen) is iets anders dan 'nog niets af', dus Result<Option>"
    )]
    pub fn next(&mut self) -> Result<Option<Event>> {
        while let Some((text_end, frame_end)) = find_blank_line(&self.rest) {
            let parsed = match self.rest.get(..text_end).map(core::str::from_utf8) {
                Some(Ok(text)) => parse(text),
                _ => Ok(None),
            };
            self.rest.drain(..frame_end);
            if let Some(e) = parsed? {
                return Ok(Some(e));
            }
        }
        Ok(None)
    }

    /// Gooit de rest van een half gelezen gebeurtenis weg: voor een nieuwe verbinding.
    pub fn reset(&mut self) {
        self.rest.clear();
    }
}

/// Het einde van de eerste gebeurtenis: (einde van de tekst, einde van de
/// lege regel). SSE staat `\n`, `\r\n` en `\r` toe; de agent schrijft `\n`,
/// een proxy soms `\r\n`, en `\r` alleen kent niemand.
fn find_blank_line(b: &[u8]) -> Option<(usize, usize)> {
    let lf = b.windows(2).position(|w| w == b"\n\n").map(|i| (i, i + 2));
    let crlf = b
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| (i, i + 4));
    match (lf, crlf) {
        (Some(a), Some(c)) => Some(if a.0 <= c.0 { a } else { c }),
        (a, c) => a.or(c),
    }
}

/// Eén gebeurtenis uit zijn regels; `None` voor alleen commentaar.
fn parse(text: &str) -> Result<Option<Event>> {
    let mut e = Event::default();
    let mut any = false;
    let mut lines = 0usize;
    for line in text.lines() {
        if line.starts_with(':') {
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => {
                e.kind = try_string(value)?;
                any = true;
            }
            "data" => {
                if lines > 0 {
                    try_push_str(&mut e.data, "\n")?;
                }
                try_push_str(&mut e.data, value)?;
                lines += 1;
                any = true;
            }
            _ => {}
        }
    }
    Ok(any.then_some(e))
}

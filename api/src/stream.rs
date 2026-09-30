//! De stromen van de API: de meldingen van `/v1/events` en de regels van een log-tail, als SSE.
//!
//! Bezit de rij van recente meldingen ([`EventLog`]) en de vorm op de draad
//! (Go's `httputil.SSE`: `event: <soort>\ndata: <json>\n\n`). Bezit geen
//! abonnees en geen verbindingen: een stroom is een lezer die zijn eigen
//! volgnummer onthoudt en de eigenaar vraagt wat er sindsdien bij kwam
//! ([`EventLog::since`]), precies zoals `runner::LogRing::since` voor de
//! logregels. Zo heeft de eigenaar geen tabel van wachtende verbindingen,
//! geen kanaal per abonnee en geen wekker per verbinding; een lezer die
//! wegging, is gewoon een lezer die niet meer vraagt.
//!
//! Waarom een rij en niet Go's kanaal per abonnee: de meldingen hebben één
//! eigenaar (de node), en een lezer die een ronde mist, moet de gemiste
//! meldingen alsnog zien of weten dat hij ze miste. Valt hij buiten de rij,
//! dan krijgt hij één `status` ("kijk alles opnieuw"), zoals de overloop van
//! Go's bus.

use alloc::collections::VecDeque;
use alloc::string::String;

use leader::Event;
use types::json;

/// Hoeveel meldingen de rij bewaart: die van de leader
/// ([`leader::MAX_EVENTS`]). Een lezer vraagt elke halve seconde; zoveel
/// meldingen in een halve seconde is een cluster dat alles tegelijk
/// verandert, en dan is `status` het goede antwoord.
pub const EVENT_LOG_CAP: usize = leader::MAX_EVENTS;

/// De eerste gebeurtenis van elke `/v1/events`-stroom, zoals Go: de lezer
/// weet dan dat de stroom staat voordat er iets gebeurt.
pub const PING: &str = "event: ping\ndata: {}\n\n";

/// Een SSE-commentaar dat een stille stroom levend houdt; een client negeert
/// hem. De schrijf ervan is ook hoe een stroom merkt dat zijn lezer weg is.
pub const KEEPALIVE: &str = ": keepalive\n\n";

/// Of een verzoek om een levende stroom vraagt (`?follow=1`) in plaats van
/// een momentopname.
///
/// `/logs/{taak}/{stroom}` op een agent geeft zonder `follow` wat er nu is
/// en sluit (de vorm van v3, voor scripts); met `follow=1` blijft de stroom
/// open en komen nieuwe regels erbij tot de taak stopt (Go's gedrag).
///
/// De clusterroute `/v1/agents/{id}/logs/...` op de leader volgt zonder
/// query, zoals in Go: het dashboard vraagt hem zo en verwacht een levende
/// log. Daar is `follow=0` de momentopname (`hop logs` zonder `--follow`);
/// de leader zet `follow=1` op de doorgifte als de aanroeper niets zei.
pub fn is_follow(req: &crate::Request) -> bool {
    matches!(req.query_param("follow"), Some("1" | "true"))
}

/// De recente meldingen, met een volgnummer per melding.
///
/// # Invariants
///
/// `next` is het volgnummer dat de volgende melding krijgt; de rij houdt de
/// laatste hoogstens [`EVENT_LOG_CAP`] meldingen, oudste eerst, met
/// oplopende nummers die op `next - 1` eindigen.
#[derive(Debug, Default)]
pub struct EventLog {
    next: u64,
    ring: VecDeque<(u64, String)>,
}

impl EventLog {
    /// Een lege rij.
    pub fn new() -> Self {
        Self::default()
    }

    /// Het volgnummer waarmee een nieuwe lezer begint: hij ziet wat hierna komt.
    pub fn seq(&self) -> u64 {
        self.next
    }

    /// Voegt een melding toe als onderwerp van Go's bus: `job:<naam>`,
    /// `job:<naam>:<event>`, `agent:<id>`, of iets anders voor `status`.
    ///
    /// Zonder geheugen valt de melding weg en telt hij toch: een lezer ziet
    /// dan een gat en krijgt `status`. Een melding mag de node niet omleggen.
    pub fn push_topic(&mut self, topic: &str) {
        let seq = self.next;
        self.next = self.next.wrapping_add(1);
        if self.ring.len() >= EVENT_LOG_CAP {
            self.ring.pop_front();
        }
        let mut t = String::new();
        if t.try_reserve(topic.len()).is_err() || self.ring.try_reserve(1).is_err() {
            return;
        }
        t.push_str(topic);
        self.ring.push_back((seq, t));
    }

    /// Voegt een melding van de leader toe ([`leader::Leader::drain_events`]).
    pub fn push(&mut self, e: &Event) {
        match e {
            Event::Status => self.push_topic(""),
            Event::Agent(id) => {
                let mut t = String::from("agent:");
                t.push_str(id);
                self.push_topic(&t);
            }
            Event::Job(name) => {
                let mut t = String::from("job:");
                t.push_str(name);
                self.push_topic(&t);
            }
        }
    }

    /// De SSE-gebeurtenissen na volgnummer `seq`, aan `out` toegevoegd; het
    /// volgnummer waarmee de lezer de volgende keer vraagt.
    ///
    /// Miste de lezer iets (hij liep verder achter dan de rij, een melding
    /// viel weg zonder geheugen, of zijn nummer komt uit een vorige rij),
    /// dan krijgt hij eerst één `status`: kijk alles opnieuw.
    pub fn since(&self, seq: u64, out: &mut String) -> u64 {
        if seq > self.next {
            frame(out, "status", "{}");
            return self.next;
        }
        let mut frames = String::new();
        let mut expect = seq;
        let mut gap = false;
        for (s, topic) in self.ring.iter().filter(|(s, _)| *s >= seq) {
            gap |= *s != expect;
            topic_frame(topic, &mut frames);
            expect = s.wrapping_add(1);
        }
        gap |= expect != self.next;
        if gap {
            frame(out, "status", "{}");
        }
        if out.try_reserve(frames.len()).is_ok() {
            out.push_str(&frames);
        }
        self.next
    }
}

/// Voegt `event: <kind>\ndata: <data>\n\n` toe aan `out`.
fn frame(out: &mut String, kind: &str, data: &str) {
    if out
        .try_reserve(kind.len() + data.len() + "event: \ndata: \n\n".len())
        .is_err()
    {
        return;
    }
    out.push_str("event: ");
    out.push_str(kind);
    out.push_str("\ndata: ");
    out.push_str(data);
    out.push_str("\n\n");
}

/// Een JSON-object van één of twee stringvelden; `{}` zonder geheugen.
fn object(fields: &[(&str, &str)]) -> String {
    let mut s = String::from("{");
    for (i, (k, v)) in fields.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        if json::write_string(k, &mut s).is_err() {
            return String::from("{}");
        }
        s.push(':');
        if json::write_string(v, &mut s).is_err() {
            return String::from("{}");
        }
    }
    s.push('}');
    s
}

/// De SSE-gebeurtenis van een onderwerp, zoals Go's `handleEvents`:
/// `agent:<id>` is `agent {"id"}`, `job:<naam>:<event>` is
/// `task {"job","event"}`, `job:<naam>` is `job {"name"}`, de rest `status {}`.
pub fn topic_frame(topic: &str, out: &mut String) {
    if let Some(id) = topic.strip_prefix("agent:") {
        frame(out, "agent", &object(&[("id", id)]));
    } else if let Some(rest) = topic.strip_prefix("job:") {
        match rest.split_once(':') {
            Some((name, event)) => {
                frame(out, "task", &object(&[("job", name), ("event", event)]));
            }
            None => frame(out, "job", &object(&[("name", rest)])),
        }
    } else {
        frame(out, "status", "{}");
    }
}

/// Voegt één logregel toe als SSE-gebeurtenis zonder soort (`data: <regel>\n\n`).
///
/// Een regel met een regeleinde erin zou de framing breken; die wordt
/// gesplitst in meer `data:`-regels van één gebeurtenis, zoals SSE dat
/// voorschrijft.
pub fn data_frame(line: &str, out: &mut String) {
    if out.try_reserve(line.len() + 8).is_err() {
        return;
    }
    for part in line.split('\n') {
        out.push_str("data: ");
        out.push_str(part.strip_suffix('\r').unwrap_or(part));
        out.push('\n');
    }
    out.push('\n');
}

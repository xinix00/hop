//! Een SSE-lezer voor de CLI: bytes erin, hele gebeurtenissen eruit.
//!
//! Bezit alleen de rest van een half binnengekomen gebeurtenis. Een stroom
//! komt in happen die niet op gebeurtenisgrenzen vallen; pas een lege regel
//! sluit er een af (de SSE-regel). Commentaar (`: keepalive`) valt weg.

/// Eén gebeurtenis: de soort (`event:`, leeg als die er niet was) en de data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Event {
    /// `event:`; leeg voor een logregel.
    pub(crate) kind: String,
    /// De `data:`-regels, met `\n` ertussen.
    pub(crate) data: String,
}

/// De lezer.
#[derive(Debug, Default)]
pub(crate) struct Reader {
    rest: Vec<u8>,
}

impl Reader {
    /// Voegt een hap toe en geeft de gebeurtenissen die nu af zijn.
    pub(crate) fn feed(&mut self, bytes: &[u8]) -> Vec<Event> {
        self.rest.extend_from_slice(bytes);
        let mut out = Vec::new();
        while let Some(end) = find_blank_line(&self.rest) {
            let frame: Vec<u8> = self.rest.drain(..end.1).collect();
            let text = String::from_utf8_lossy(frame.get(..end.0).unwrap_or_default());
            if let Some(e) = parse(&text) {
                out.push(e);
            }
        }
        out
    }
}

/// Het einde van de eerste gebeurtenis: (einde van de tekst, einde van de
/// lege regel). SSE staat `\n`, `\r\n` en `\r` toe; wij kennen de eerste
/// twee.
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
fn parse(text: &str) -> Option<Event> {
    let mut kind = String::new();
    let mut data: Vec<&str> = Vec::new();
    let mut any = false;
    for line in text.lines() {
        if line.starts_with(':') {
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => {
                kind = String::from(value);
                any = true;
            }
            "data" => {
                data.push(value);
                any = true;
            }
            _ => {}
        }
    }
    any.then(|| Event {
        kind,
        data: data.join("\n"),
    })
}
